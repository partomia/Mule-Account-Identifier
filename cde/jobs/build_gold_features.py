"""
Stage 5 - Gold (mule features, one row per active account per snapshot)

Snapshot dates are every Friday from FIRST_SNAPSHOT up to the load's as-of
date, plus the as-of date itself (today's book, the rows to score). An account
is in a snapshot when, on that day, it is open, has moved money in the last 30
days, and is not already known: no fraud report resolved to it, not frozen, no
investigator CONFIRMED_MULE. Already-known mules are the bank's existing
catch; the model is scored on the ones it has not caught yet.

Every feature uses only what was known on the snapshot date: money-movement
windows end on it, a credit's hold time is censored at it, pass-through only
counts credits whose next 24 hours are observed, and graph / network features
come from that date's graph (build_identity_graph.py).

Label: is_mule_90d = 1 if the account is confirmed as a mule in
(snapshot_date, snapshot_date + 90]: frozen after a 1930 / NCRP complaint, a
Suspect Registry match or an internal FRM review, or marked CONFIRMED_MULE by
an investigator. NULL until 90 days have passed.

Writes via MERGE INTO once the table exists: changed rows (mostly labels
maturing) are updated, new snapshots inserted, rows no longer produced (last
load's mid-week as-of row) deleted. Each load is one Iceberg snapshot, which
the CAI job records for lineage.

Reads:
  <prefix>_silver.{customer, account, txn, session, report, decision, identity_clusters}
Writes:
  <prefix>_gold.mule_features

Usage:
  spark-submit build_gold_features.py [--db-prefix P]
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_mule_acct"
FIRST_SNAPSHOT = date(2025, 5, 2)   # must match build_identity_graph.py
HORIZON_DAYS = 90
MAX_HOLD_HOURS = 720.0
MIN_MONTHLY_INCOME = 5000.0
FRAUD_FREEZES = ("CYBER_COMPLAINT", "SUSPECT_REGISTRY", "INTERNAL_FRM")

FEATURES = [
    "account_age_days", "min_kyc_flag", "dormant_reactivated_flag",
    "inflow_to_declared_income_30d", "distinct_senders_7d", "distinct_receivers_7d", "pass_through_ratio_7d",
    "median_hold_hours_30d", "night_txn_share_30d", "round_amount_share_30d",
    "new_device_logins_30d", "vpa_or_mobile_changes_30d",
    "accounts_on_same_device", "cifs_sharing_mobile", "person_cluster_size", "ring_size",
    "hops_to_known_mule", "credits_from_complainants_30d",
]
LABEL = "is_mule_90d"
INFO = ["cif", "branch_code", "product_code", "person_cluster_id", "ring_id"]
COLUMNS = ["account_id", "snapshot_date", *INFO, *FEATURES, LABEL]


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    args, _ = p.parse_known_args(argv)
    return args


def snapshot_dates(as_of: date) -> list[date]:
    out, d = [], FIRST_SNAPSHOT
    while d <= as_of:
        out.append(d)
        d += timedelta(days=7)
    if not out or out[-1] != as_of:
        out.append(as_of)
    return out


def in_window(date_col: str, snaps: list[date], days: int):
    """Array of the snapshot dates d with date_col in (d - days, d]."""
    arr = F.array(*[F.lit(d) for d in snaps])
    t = F.col(date_col)
    return F.filter(arr, lambda s: (s >= t) & (s < F.date_add(t, days)))


def ratio(num: str, den: str):
    """num / den, 0 when there is nothing to divide by (Spark 4 runs ANSI mode: x / 0 fails the job)."""
    return F.when(F.col(den) > 0, F.col(num) / F.col(den)).otherwise(F.lit(0.0))


def snapshot_end(col: str = "snapshot_date"):
    return F.to_timestamp(F.concat(F.col(col).cast("string"), F.lit(" 23:59:59")))


def money_features(txn: DataFrame, snaps: list[date]) -> tuple[DataFrame, DataFrame]:
    """(account_id, snapshot_date) money-movement features and the active population."""
    ts = F.col("txn_ts").cast("long")
    w_next = Window.partitionBy("account_id").orderBy(ts).rowsBetween(1, Window.unboundedFollowing)
    w_24h = Window.partitionBy("account_id").orderBy(ts).rangeBetween(1, 86400)
    t = (txn.select("account_id", "txn_ts", "txn_date", "direction", "amount", "counterparty_vpa")
         .withColumn("is_cr", (F.col("direction") == "CR").cast("int"))
         .withColumn("next_debit_ts", F.min(F.when(F.col("direction") == "DR", F.col("txn_ts"))).over(w_next))
         .withColumn("out_24h", F.coalesce(F.sum(F.when(F.col("direction") == "DR", F.col("amount"))).over(w_24h),
                                           F.lit(0.0)))
         .withColumn("night", F.hour("txn_ts").isin(23, 0, 1, 2, 3, 4).cast("int"))
         .withColumn("round_cr", ((F.col("is_cr") == 1) & (F.col("amount") % 1000 == 0)).cast("int")))

    m30 = t.withColumn("snapshot_date", F.explode(in_window("txn_date", snaps, 30)))
    end = snapshot_end()
    hold = F.least(F.lit(MAX_HOLD_HOURS),
                   (F.least(F.coalesce("next_debit_ts", end), end).cast("long") - F.col("txn_ts").cast("long")) / 3600.0)
    f30 = (m30.groupBy("account_id", "snapshot_date")
           .agg(F.count("*").alias("txns_30d"),
                F.sum(F.col("amount") * F.col("is_cr")).alias("credits_30d"),
                F.sum("night").alias("night_30d"),
                F.sum("is_cr").alias("n_credits_30d"),
                F.sum("round_cr").alias("round_credits_30d"),
                F.percentile_approx(F.when(F.col("is_cr") == 1, hold), 0.5).alias("median_hold_hours_30d")))

    m7 = t.withColumn("snapshot_date", F.explode(in_window("txn_date", snaps, 7)))
    observed = (F.col("is_cr") == 1) & (F.col("txn_ts").cast("long") + 86400 <= end.cast("long"))
    f7 = (m7.groupBy("account_id", "snapshot_date")
          .agg(F.countDistinct(F.when(F.col("is_cr") == 1, F.col("counterparty_vpa"))).alias("distinct_senders_7d"),
               F.countDistinct(F.when(F.col("is_cr") == 0, F.col("counterparty_vpa"))).alias("distinct_receivers_7d"),
               F.sum(F.when(observed, F.least("amount", "out_24h"))).alias("out_7d"),
               F.sum(F.when(observed, F.col("amount"))).alias("in_7d")))
    return f30, f7


def build(spark: SparkSession, db_prefix: str) -> tuple[DataFrame, date]:
    s = f"{db_prefix}_silver"
    account = spark.table(f"{s}.account")
    customer = spark.table(f"{s}.customer")
    txn = spark.table(f"{s}.txn")
    as_of = account.agg(F.max("batch_as_of")).first()[0]
    snaps = snapshot_dates(as_of)
    logger.info("as of %s: %d snapshot dates %s -> %s", as_of, len(snaps), snaps[0], snaps[-1])
    d = F.col("snapshot_date")

    f30, f7 = money_features(txn, snaps)

    report = spark.table(f"{s}.report")
    decision = spark.table(f"{s}.decision")
    known_from = (report.select("account_id", F.col("report_date").alias("k"))
                  .unionByName(account.where(F.col("freeze_date").isNotNull())
                               .select("account_id", F.col("freeze_date").alias("k")))
                  .unionByName(decision.where(F.col("decision") == "CONFIRMED_MULE")
                               .select("account_id", F.col("decision_date").alias("k")))
                  .groupBy("account_id").agg(F.min("k").alias("known_from")))
    confirmed_on = (account.where(F.col("freeze_reason").isin(*FRAUD_FREEZES))
                    .select("account_id", F.col("freeze_date").alias("c"))
                    .unionByName(decision.where(F.col("decision") == "CONFIRMED_MULE")
                                 .select("account_id", F.col("decision_date").alias("c")))
                    .groupBy("account_id").agg(F.min("c").alias("confirmed_on")))

    pop = (f30.join(account.select("account_id", "cif", "branch_code", "product_code", "open_date", "dormant_from",
                                   "reactivated_on"), "account_id")
           .where(F.col("open_date") <= d)
           .join(known_from, "account_id", "left")
           .where(F.col("known_from").isNull() | (F.col("known_from") > d)))

    session = spark.table(f"{s}.session")
    first_device = session.where(F.col("device_id").isNotNull()).groupBy("cif", "device_id").agg(
        F.min("session_date").alias("session_date"))
    new_devices = (first_device.withColumn("snapshot_date", F.explode(in_window("session_date", snaps, 30)))
                   .groupBy("cif", "snapshot_date").agg(F.count("*").alias("new_device_logins_30d")))
    changes = (session.where(F.col("event_type").isin("MOBILE_CHANGE", "VPA_CREATE"))
               .withColumn("snapshot_date", F.explode(in_window("session_date", snaps, 30)))
               .groupBy("cif", "snapshot_date").agg(F.count("*").alias("vpa_or_mobile_changes_30d")))
    has_device = first_device.groupBy("cif").agg(F.min("session_date").alias("first_device_date"))
    own_accounts = (account.select("cif", "open_date").join(F.broadcast(spark.createDataFrame(
        [(x,) for x in snaps], "snapshot_date date")), F.col("open_date") <= d)
        .groupBy("cif", "snapshot_date").agg(F.count("*").alias("own_accounts")))

    complainants = (report.where(F.col("complainant_vpa").isNotNull())
                    .groupBy("complainant_vpa").agg(F.min("report_date").alias("complained_on")))
    from_victims = (txn.where(F.col("direction") == "CR")
                    .join(complainants, F.col("counterparty_vpa") == F.col("complainant_vpa"))
                    .withColumn("snapshot_date", F.explode(in_window("txn_date", snaps, 30)))
                    .where(F.col("complained_on") <= d)
                    .groupBy("account_id", "snapshot_date").agg(F.count("*").alias("credits_from_complainants_30d")))

    clusters = spark.table(f"{s}.identity_clusters")
    labelled = F.date_add(d, HORIZON_DAYS) <= F.lit(as_of)
    confirmed = F.coalesce((F.col("confirmed_on") > d) & (F.col("confirmed_on") <= F.date_add(d, HORIZON_DAYS)),
                           F.lit(False))
    income_m = F.greatest(F.lit(MIN_MONTHLY_INCOME), F.col("declared_annual_income") / 12)
    feats = (pop.join(f7, ["account_id", "snapshot_date"], "left")
             .join(customer.select("cif", "kyc_type", "declared_annual_income"), "cif", "left")
             .join(new_devices, ["cif", "snapshot_date"], "left")
             .join(changes, ["cif", "snapshot_date"], "left")
             .join(has_device, "cif", "left")
             .join(own_accounts, ["cif", "snapshot_date"], "left")
             .join(clusters, ["cif", "snapshot_date"], "left")
             .join(from_victims, ["account_id", "snapshot_date"], "left")
             .join(confirmed_on, "account_id", "left")
             .select(
                 "account_id", "snapshot_date", "cif", "branch_code", "product_code",
                 F.coalesce("person_cluster_id", "cif").alias("person_cluster_id"),
                 F.coalesce("ring_id", "cif").alias("ring_id"),
                 F.datediff(d, "open_date").cast("int").alias("account_age_days"),
                 F.coalesce(F.col("kyc_type") == "MIN_KYC", F.lit(False)).cast("int").alias("min_kyc_flag"),
                 F.coalesce(F.col("dormant_from").isNotNull() & (F.col("reactivated_on") > F.date_sub(d, 90))
                            & (F.col("reactivated_on") <= d), F.lit(False)).cast("int").alias("dormant_reactivated_flag"),
                 F.round(F.coalesce("credits_30d", F.lit(0.0)) / income_m, 3).alias("inflow_to_declared_income_30d"),
                 F.coalesce("distinct_senders_7d", F.lit(0)).cast("int").alias("distinct_senders_7d"),
                 F.coalesce("distinct_receivers_7d", F.lit(0)).cast("int").alias("distinct_receivers_7d"),
                 F.round(ratio("out_7d", "in_7d"), 3).alias("pass_through_ratio_7d"),
                 F.round(F.coalesce("median_hold_hours_30d", F.lit(MAX_HOLD_HOURS)), 1).alias("median_hold_hours_30d"),
                 F.round(ratio("night_30d", "txns_30d"), 3).alias("night_txn_share_30d"),
                 F.round(ratio("round_credits_30d", "n_credits_30d"), 3)
                 .alias("round_amount_share_30d"),
                 F.coalesce("new_device_logins_30d", F.lit(0)).cast("int").alias("new_device_logins_30d"),
                 F.coalesce("vpa_or_mobile_changes_30d", F.lit(0)).cast("int").alias("vpa_or_mobile_changes_30d"),
                 F.coalesce("accounts_on_same_device",
                            F.when(F.col("first_device_date") <= d, F.col("own_accounts")), F.lit(0))
                 .cast("int").alias("accounts_on_same_device"),
                 F.coalesce("cifs_sharing_mobile", F.lit(1)).cast("int").alias("cifs_sharing_mobile"),
                 F.coalesce("person_cluster_size", F.lit(1)).cast("int").alias("person_cluster_size"),
                 F.coalesce("ring_size", F.lit(1)).cast("int").alias("ring_size"),
                 F.coalesce("hops_to_known_mule", F.lit(3)).cast("int").alias("hops_to_known_mule"),
                 F.coalesce("credits_from_complainants_30d", F.lit(0)).cast("int").alias("credits_from_complainants_30d"),
                 F.when(labelled, confirmed.cast("int")).cast("int").alias(LABEL)))
    return feats.select(*COLUMNS), as_of


def bootstrap_or_merge(spark: SparkSession, target: str, source_view: str) -> None:
    if spark.catalog.tableExists(target):
        changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in COLUMNS[2:])
        spark.sql(f"""
            MERGE INTO {target} t
            USING {source_view} s
            ON t.account_id = s.account_id AND t.snapshot_date = s.snapshot_date
            WHEN MATCHED AND ({changed}) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)
        logger.info("Merged into %s (new snapshot)", target)
    else:
        spark.sql(f"""
            CREATE TABLE {target}
            USING iceberg
            PARTITIONED BY (months(snapshot_date))
            TBLPROPERTIES ('format-version'='2')
            AS SELECT * FROM {source_view}
        """)
        logger.info("Bootstrapped %s (first run)", target)


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("mule-build-gold-features").getOrCreate()
    try:
        gold_db = f"{args.db_prefix}_gold"
        target = f"{gold_db}.mule_features"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")
        feats, as_of = build(spark, args.db_prefix)
        feats.withColumn("loaded_at", F.current_timestamp()).createOrReplaceTempView("mule_features_src")
        bootstrap_or_merge(spark, target, "mule_features_src")

        summary = (spark.table(target).groupBy("snapshot_date")
                   .agg(F.count("*").alias("accounts"), F.sum(LABEL).alias("mules"),
                        F.round(F.avg(LABEL) * 100, 3).alias("mule_pct"))
                   .orderBy(F.col("snapshot_date").desc()))
        summary.show(8, truncate=False)
        logger.info("%s rows in %s (as of %s)", spark.table(target).count(), target, as_of)
    finally:
        if own:
            spark.stop()


if __name__ == "__main__":
    main()

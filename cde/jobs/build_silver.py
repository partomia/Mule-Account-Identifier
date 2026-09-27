"""
Stage 3 - Silver (standardised keys, hashed identities, identity links)

Deterministic join on the primary keys: every re-sent duplicate is removed
(latest ingested record wins), every key is standardised the same way in every
source, and every account resolves to its CIF:

  mobile   digits only, last 10            VPA      trimmed, lower case
  device   trimmed, lower case             IP       /24 prefix
  PAN, Aadhaar ref, mobile, email, address  salted SHA-256 (raw values never leave bronze)

Fraud reports name an account, a VPA or (Suspect Registry) a mobile number;
each is resolved to our accounts here: VPA through the account's own VPAs,
mobile through every customer who registered that number.

Identity links (cif, identifier) with the date each was first seen come from
onboarding (PAN, mobile, address), mobile changes and device fingerprints in
digital sessions. The bank's own devices (ref.bank_devices: branch kiosks,
BC-agent terminals) and identifiers held by more than --max-hub-cifs customers
are hubs and never become edges. Two customers
sharing a non-hub identifier get an edge, dated the day both held it, so the
graph job can build each snapshot's graph from what was known on that day.

Investigator decisions captured in the app (bronze.investigator_decisions, if
the table exists) are carried into silver.decision: tomorrow's labels.

Reads:
  <prefix>_bronze.{kyc_onboarding, cbs_accounts, upi_transactions, digital_sessions,
                   fraud_reports, investigator_decisions?}
  <prefix>_ref.{product_map, bank_devices}
Writes (drop + recreate every run):
  <prefix>_silver.{customer, account, account_vpa, txn, session, report, decision,
                   identity_links, identity_hubs, identity_edges}

Usage:
  spark-submit build_silver.py [--db-prefix P] [--hash-salt S] [--max-hub-cifs N]
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_mule_acct"
DEMO_SALT = "mule-demo-salt-2026"   # a real deployment uses a managed key, never a constant
MAX_HUB_CIFS = 25
LINK_TYPES = ("DEVICE", "MOBILE", "PAN", "ADDRESS")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    p.add_argument("--hash-salt", default=DEMO_SALT)
    p.add_argument("--max-hub-cifs", type=int, default=MAX_HUB_CIFS)
    args, _ = p.parse_known_args(argv)
    return args


def dedupe(df: DataFrame, *keys: str) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def _write(df: DataFrame, name: str, partition_col=None) -> None:
    w = df.writeTo(name).using("iceberg").tableProperty("format-version", "2")
    if partition_col is not None:
        w = w.partitionedBy(partition_col)
    w.createOrReplace()


def norm_mobile(c):
    digits = F.regexp_replace(c, "[^0-9]", "")
    return F.when(F.length(digits) >= 10, F.substring(digits, -10, 10))


def norm_vpa(c):
    return F.lower(F.trim(c))


def hashed(c, salt: str):
    return F.when(c.isNotNull(), F.sha2(F.concat(F.lit(salt), c), 256))


def mask_mobile(c):
    return F.when(c.isNotNull(), F.concat(F.substring(c, 1, 2), F.lit("xxxxx"), F.substring(c, -3, 3)))


def identity_edges(links: DataFrame, max_hub: int, bank_devices: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Customer pairs sharing a non-hub identifier, dated when both held it."""
    counts = links.groupBy("link_type", "identifier").agg(F.countDistinct("cif").alias("cifs"))
    own = bank_devices.select(F.lit("DEVICE").alias("link_type"), F.lower(F.trim("device_id")).alias("identifier"),
                              F.lit(True).alias("bank_device"))
    counts = counts.join(own, ["link_type", "identifier"], "left")
    hubs = counts.where((F.col("cifs") > max_hub) | F.col("bank_device").isNotNull()).drop("bank_device")
    shared = counts.where(F.col("cifs").between(2, max_hub) & F.col("bank_device").isNull()).select("link_type", "identifier")
    l = links.join(shared, ["link_type", "identifier"])
    a, b = l.alias("a"), l.alias("b")
    edges = (a.join(b, (F.col("a.link_type") == F.col("b.link_type")) & (F.col("a.identifier") == F.col("b.identifier"))
                    & (F.col("a.cif") < F.col("b.cif")))
             .select(F.col("a.cif").alias("src_cif"), F.col("b.cif").alias("dst_cif"), F.col("a.link_type"),
                     F.col("a.identifier"),
                     F.greatest(F.col("a.first_seen"), F.col("b.first_seen")).alias("first_seen"))
             .groupBy("src_cif", "dst_cif", "link_type")
             .agg(F.min("first_seen").alias("first_seen"), F.min("identifier").alias("identifier")))
    return edges, hubs


def run(spark: SparkSession, args: argparse.Namespace) -> None:
    db = args.db_prefix
    bronze, silver = f"{db}_bronze", f"{db}_silver"
    salt = args.hash_salt
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver}")
    as_of = spark.table(f"{bronze}.cbs_accounts").agg(F.max("batch_as_of")).first()[0]

    k = dedupe(spark.table(f"{bronze}.kyc_onboarding"), "cif")
    mobile = norm_mobile(F.col("mobile"))
    address = F.regexp_replace(F.upper(F.concat_ws(" ", "address_line", "city", "pincode")), "[^A-Z0-9]", "")
    customer = k.select(
        "cif", F.trim("customer_name").alias("customer_name"), "dob", "gender",
        F.upper(F.trim("kyc_type")).alias("kyc_type"), "onboarding_channel", "onboarding_date", "home_branch",
        F.upper(F.trim("occupation")).alias("occupation"), "declared_annual_income",
        hashed(F.upper(F.trim("pan")), salt).alias("pan_hash"),
        hashed(F.regexp_replace("aadhaar_ref", "[^0-9]", ""), salt).alias("aadhaar_hash"),
        hashed(mobile, salt).alias("mobile_hash"), mask_mobile(mobile).alias("mobile_masked"),
        hashed(F.lower(F.trim("email")), salt).alias("email_hash"),
        hashed(address, salt).alias("address_hash"), "city", "pincode", "batch_as_of")

    products = spark.table(f"{db}_ref.product_map").select("product_code", "product_name")
    account = (dedupe(spark.table(f"{bronze}.cbs_accounts"), "account_id").join(products, "product_code", "left")
               .select("account_id", "cif", "product_code", "product_name", "branch_code", "open_date",
                       F.upper(F.trim("status")).alias("status"), "dormant_from", "reactivated_on", "freeze_date",
                       "freeze_reason", "batch_as_of"))

    t = dedupe(spark.table(f"{bronze}.upi_transactions"), "txn_id").where(F.col("amount") > 0)
    own = F.when(F.col("direction") == "CR", F.col("payee_vpa")).otherwise(F.col("payer_vpa"))
    other = F.when(F.col("direction") == "CR", F.col("payer_vpa")).otherwise(F.col("payee_vpa"))
    txn = (t.join(account.select("account_id", "cif"), "account_id")
           .select("txn_id", "utr", "txn_ts", F.to_date("txn_ts").alias("txn_date"), "account_id", "cif",
                   F.upper(F.trim("direction")).alias("direction"), "amount",
                   norm_vpa(own).alias("own_vpa"), norm_vpa(other).alias("counterparty_vpa"),
                   "counterparty_account_id", F.upper(F.trim("channel")).alias("channel")))

    s = dedupe(spark.table(f"{bronze}.digital_sessions"), "session_id")
    event = F.upper(F.trim("event_type"))
    session = s.select(
        "session_id", "cif", "session_ts", F.to_date("session_ts").alias("session_date"), "channel",
        F.lower(F.trim("device_id")).alias("device_id"),
        F.regexp_extract("ip_address", r"^(\d+\.\d+\.\d+)\.", 1).alias("ip_prefix"), "geo_city",
        event.alias("event_type"),
        F.when(event == "MOBILE_CHANGE", hashed(norm_mobile(F.col("new_value")), salt)).alias("new_mobile_hash"),
        F.when(event == "VPA_CREATE", norm_vpa(F.col("new_value"))).alias("new_vpa"), "account_id")

    account_vpa = (txn.select("own_vpa", "account_id", "txn_date").withColumnRenamed("own_vpa", "vpa")
                   .unionByName(session.where(F.col("new_vpa").isNotNull())
                                .select(F.col("new_vpa").alias("vpa"), "account_id",
                                        F.col("session_date").alias("txn_date")))
                   .where(F.col("vpa").isNotNull() & F.col("account_id").isNotNull())
                   .groupBy("vpa", "account_id").agg(F.min("txn_date").alias("first_seen")))

    # Every mobile a customer has registered: onboarding plus later changes.
    mobiles = (customer.select("cif", "mobile_hash", F.col("onboarding_date").alias("first_seen"))
               .unionByName(session.where(F.col("new_mobile_hash").isNotNull())
                            .select("cif", F.col("new_mobile_hash").alias("mobile_hash"),
                                    F.col("session_date").alias("first_seen")))
               .groupBy("cif", "mobile_hash").agg(F.min("first_seen").alias("first_seen")))

    r = dedupe(spark.table(f"{bronze}.fraud_reports"), "report_id")
    rep = r.select("report_id", "report_ts", F.to_date("report_ts").alias("report_date"),
                   F.upper(F.trim("source")).alias("source"), "category", "amount",
                   norm_vpa(F.col("complainant_vpa")).alias("complainant_vpa"), "txn_utr",
                   F.trim("reported_account_id").alias("reported_account_id"),
                   norm_vpa(F.col("reported_vpa")).alias("reported_vpa"),
                   hashed(norm_mobile(F.col("reported_mobile")), salt).alias("reported_mobile_hash"))
    by_acct = (rep.where(F.col("reported_account_id").isNotNull())
               .withColumn("account_id", F.col("reported_account_id")).withColumn("resolved_via", F.lit("ACCOUNT")))
    by_vpa = (rep.where(F.col("reported_account_id").isNull() & F.col("reported_vpa").isNotNull())
              .join(account_vpa.select(F.col("vpa").alias("reported_vpa"), "account_id"), "reported_vpa")
              .withColumn("resolved_via", F.lit("VPA")))
    by_mobile = (rep.where(F.col("reported_account_id").isNull() & F.col("reported_vpa").isNull()
                           & F.col("reported_mobile_hash").isNotNull())
                 .join(mobiles.select(F.col("mobile_hash").alias("reported_mobile_hash"), "cif"), "reported_mobile_hash")
                 .join(account.select("cif", "account_id"), "cif").drop("cif")
                 .withColumn("resolved_via", F.lit("MOBILE")))
    report = (by_acct.unionByName(by_vpa).unionByName(by_mobile)
              .join(account.select("account_id", "cif"), "account_id", "left")
              .select("report_id", "report_ts", "report_date", "source", "category", "amount", "complainant_vpa",
                      "txn_utr", "account_id", "cif", "resolved_via", "reported_vpa", "reported_mobile_hash"))
    unresolved = rep.join(report.select("report_id").distinct(), "report_id", "left_anti").count()

    dec_table = f"{bronze}.investigator_decisions"
    if spark.catalog.tableExists(dec_table):
        decision = (dedupe(spark.table(dec_table), "decision_id")
                    .select("decision_id", "run_date", "account_id", "cif", F.upper("decision").alias("decision"),
                            "action", "maker", "checker", "notes", "decided_at",
                            F.to_date("decided_at").alias("decision_date")))
    else:
        decision = spark.createDataFrame([], "decision_id string, run_date date, account_id string, cif string, "
                                             "decision string, action string, maker string, checker string, "
                                             "notes string, decided_at timestamp, decision_date date")

    links = (customer.select("cif", F.lit("PAN").alias("link_type"), F.col("pan_hash").alias("identifier"),
                             F.col("onboarding_date").alias("first_seen"))
             .unionByName(customer.select("cif", F.lit("ADDRESS").alias("link_type"),
                                          F.col("address_hash").alias("identifier"),
                                          F.col("onboarding_date").alias("first_seen")))
             .unionByName(mobiles.select("cif", F.lit("MOBILE").alias("link_type"),
                                         F.col("mobile_hash").alias("identifier"), "first_seen"))
             .unionByName(session.where(F.col("device_id").isNotNull())
                          .groupBy("cif", F.col("device_id").alias("identifier"))
                          .agg(F.min("session_date").alias("first_seen"))
                          .select("cif", F.lit("DEVICE").alias("link_type"), "identifier", "first_seen"))
             .where(F.col("identifier").isNotNull()))
    edges, hubs = identity_edges(links, args.max_hub_cifs, spark.table(f"{db}_ref.bank_devices"))

    _write(customer, f"{silver}.customer")
    _write(account, f"{silver}.account")
    _write(account_vpa, f"{silver}.account_vpa")
    _write(txn, f"{silver}.txn", F.months("txn_date"))
    _write(session, f"{silver}.session", F.months("session_date"))
    _write(report, f"{silver}.report")
    _write(decision, f"{silver}.decision")
    _write(links, f"{silver}.identity_links")
    _write(hubs, f"{silver}.identity_hubs")
    _write(edges, f"{silver}.identity_edges")

    bad_hash = spark.table(f"{silver}.customer").where(~F.col("pan_hash").rlike("^[0-9a-f]{64}$")).count()
    if bad_hash:
        raise RuntimeError(f"silver.customer: {bad_hash} pan_hash values are not SHA-256 hex")
    for name in ("customer", "account", "account_vpa", "txn", "session", "report", "decision",
                 "identity_links", "identity_hubs", "identity_edges"):
        logger.info("%s.%s: %d rows", silver, name, spark.table(f"{silver}.{name}").count())
    logger.info("reports resolved to accounts: %d rows, %d reports unresolved", spark.table(f"{silver}.report").count(),
                unresolved)
    spark.table(f"{silver}.identity_edges").groupBy("link_type").count().orderBy("link_type").show()
    spark.table(f"{silver}.identity_hubs").groupBy("link_type").agg(F.count("*").alias("hubs"),
                                                                   F.max("cifs").alias("max_cifs")).show()
    logger.info("Silver as of %s complete", as_of)


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("mule-build-silver").getOrCreate()
    try:
        run(spark, args)
    finally:
        if own:
            spark.stop()


if __name__ == "__main__":
    main()

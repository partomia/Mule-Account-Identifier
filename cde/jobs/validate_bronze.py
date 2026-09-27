"""
Stage 2 - Validate (bronze gate)

Data-quality gate on the five source extracts. Exits non-zero on a hard
failure so the Airflow DAG stops before anything reaches silver / gold (and
therefore before anything reaches an investigator). Re-sent duplicate records
are expected and only logged: silver removes them.

Hard checks:
  * every extract is non-empty and has no null keys; nothing dated after the as-of date
  * account_id unique in cbs_accounts; product and branch codes known in ref
  * every account belongs to a known CIF, every transaction to a known account,
    every session to a known CIF (< 0.1% orphans)
  * transaction amounts positive, direction CR / DR
  * no missing day in upi_transactions between the first day and the as-of date
  * PAN well formed in kyc_onboarding.pan, and a raw PAN appears in no other
    column of any extract (identity documents stay in their governed column)

Reads:
  <prefix>_bronze.{kyc_onboarding, cbs_accounts, upi_transactions, digital_sessions, fraud_reports}
  <prefix>_ref.{branch_map, product_map}

Usage:
  spark-submit validate_bronze.py [--db-prefix P]
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_mule_acct"
MAX_BAD_RATE = 0.001
PAN = r"[A-Z]{5}[0-9]{4}[A-Z]"
PAN_ANYWHERE = rf"(^|[^A-Z0-9]){PAN}([^A-Z0-9]|$)"

# table -> (key columns, event date column)
EXTRACTS = {
    "kyc_onboarding": (["cif"], "onboarding_date"),
    "cbs_accounts": (["account_id"], "open_date"),
    "upi_transactions": (["txn_id"], "txn_ts"),
    "digital_sessions": (["session_id"], "session_ts"),
    "fraud_reports": (["report_id"], "report_ts"),
}


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    args, _ = p.parse_known_args(argv)
    return args


def _rate(bad: int, rows: int) -> float:
    return bad / rows if rows else 0.0


def validate(spark: SparkSession, db_prefix: str) -> list[str]:
    bronze, ref = f"{db_prefix}_bronze", f"{db_prefix}_ref"
    errors = []
    tables = {name: spark.table(f"{bronze}.{name}") for name in EXTRACTS}
    as_of = tables["cbs_accounts"].agg(F.max("batch_as_of")).first()[0]
    logger.info("load as of %s", as_of)

    for name, (keys, date_col) in EXTRACTS.items():
        df = tables[name]
        null_key = F.lit(False)
        for k in keys:
            null_key = null_key | F.col(k).isNull()
        s = df.agg(F.count("*").alias("rows"),
                   F.sum(F.when(null_key, 1).otherwise(0)).alias("null_keys"),
                   F.sum(F.when(F.to_date(date_col) > F.lit(as_of), 1).otherwise(0)).alias("future"),
                   F.min(date_col).alias("first"), F.max(date_col).alias("last")).first()
        logger.info("%-18s rows=%-10s %s -> %s", name, s["rows"], s["first"], s["last"])
        if not s["rows"]:
            errors.append(f"{name} is empty")
            continue
        if s["null_keys"]:
            errors.append(f"{name}: {s['null_keys']} rows with a null key ({', '.join(keys)})")
        if s["future"]:
            errors.append(f"{name}: {s['future']} rows dated after the as-of date {as_of}")
        dupes = df.groupBy(*keys).count().where("count > 1").count()
        if dupes:
            logger.info("%-18s duplicate keys: %d (removed in silver)", name, dupes)

    kyc, acct, txn, sess = (tables[k] for k in ("kyc_onboarding", "cbs_accounts", "upi_transactions",
                                                  "digital_sessions"))
    n, ids = acct.agg(F.count("*"), F.countDistinct("account_id")).first()
    if n != ids:
        errors.append(f"cbs_accounts: {n - ids} duplicate account_id")
    for col, ref_table in (("product_code", "product_map"), ("branch_code", "branch_map")):
        unknown = acct.select(col).distinct().join(spark.table(f"{ref}.{ref_table}"), col, "left_anti").collect()
        if unknown:
            errors.append(f"cbs_accounts: unknown {col} " + ", ".join(str(r[0]) for r in unknown[:10]))

    for name, df, key, parent in (("cbs_accounts", acct, "cif", kyc), ("upi_transactions", txn, "account_id", acct),
                                  ("digital_sessions", sess, "cif", kyc)):
        orphans = df.join(parent.select(key).distinct(), key, "left_anti").count()
        rate = _rate(orphans, df.count())
        if rate > MAX_BAD_RATE:
            errors.append(f"{name}: {rate:.3%} rows for an unknown {key} (limit {MAX_BAD_RATE:.1%})")

    s = txn.agg(F.count("*").alias("rows"),
                F.sum(F.when(F.col("amount") <= 0, 1).otherwise(0)).alias("bad_amount"),
                F.sum(F.when(~F.col("direction").isin("CR", "DR"), 1).otherwise(0)).alias("bad_dir"),
                F.min(F.to_date("txn_ts")).alias("first"),
                F.countDistinct(F.to_date("txn_ts")).alias("days")).first()
    for col, label in (("bad_amount", "non-positive amount"), ("bad_dir", "direction not CR / DR")):
        if _rate(s[col], s["rows"]) > MAX_BAD_RATE:
            errors.append(f"upi_transactions: {label} in {_rate(s[col], s['rows']):.3%} of rows")
    expected = (as_of - s["first"]).days + 1
    if s["days"] < expected:
        errors.append(f"upi_transactions: {expected - s['days']} missing days between {s['first']} and {as_of}")

    bad_pan = kyc.where(~F.col("pan").rlike(f"^{PAN}$") | F.col("pan").isNull()).count()
    if _rate(bad_pan, kyc.count()) > MAX_BAD_RATE:
        errors.append(f"kyc_onboarding: {bad_pan} malformed PAN values")
    for name, df in tables.items():
        cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string" and f.name != "pan"]
        hit = F.lit(False)
        for c in cols:
            hit = hit | F.upper(F.col(c)).rlike(PAN_ANYWHERE)
        leaks = df.where(hit).count()
        if leaks:
            errors.append(f"{name}: raw PAN found outside kyc_onboarding.pan in {leaks} rows")
    return errors


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("mule-validate-bronze").getOrCreate()
    try:
        errors = validate(spark, args.db_prefix)
    finally:
        if own:
            spark.stop()
    if errors:
        for e in errors:
            logger.error("VALIDATION FAILED: %s", e)
        sys.exit(1)
    logger.info("Bronze validation passed")


if __name__ == "__main__":
    main()

"""
Data-quality checks for one layer, recorded per run (plain PySpark).

Runs after each stage in the DAG:

  generate -> validate_bronze (= --layer bronze) -> silver -> identity graph -> dq silver
    -> gold -> dq gold -> CAI sync + scoring -> dq publish

Every check carries a severity:
  critical  the load is wrong (null or duplicate keys, orphans above 0.1%, future-dated rows,
            raw PAN outside its column, reconciliation gaps, label leakage, a partial alert
            publish): the job exits 1 after recording, so Airflow stops the DAG and yesterday's
            alert queue stays in place.
  warning   worth a look (re-sent duplicates, a rate past half its limit, a volume swing, the
            mule rate outside its usual band): recorded, the pipeline continues.

All results, pass or fail, are appended to <prefix>_ref.dq_results (Iceberg), one row per
check, with the Iceberg snapshot id of the table checked. The columns match the churn
project's results table; expectation_type holds this project's rule kinds (RULES) and
kwargs the limit, so a rate can be charted against it. gx_version is NULL: no Great
Expectations here. The dashboards' report views read this table; nothing here reads them.

Usage:
  spark-submit dq_check.py --layer bronze|silver|gold|publish [--as-of YYYY-MM-DD]
                           [--db-prefix P] [--pipeline-run ID]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("dq_check")
logging.getLogger("py4j").setLevel(logging.WARNING)

DEFAULT_DB_PREFIX = "rsingh_mule_acct"
LAYERS = ("bronze", "silver", "gold", "publish")
CRITICAL, WARNING = "critical", "warning"
RULES = ("row_count", "not_null", "unique", "in_set", "range", "not_after_as_of", "reconciliation_equal",
         "rate_at_most", "volume_change")
MAX_BAD_RATE = 0.001                # orphans, bad amounts / directions, malformed PAN (critical)
MAX_DUPLICATE_RATE = 0.01           # re-sent duplicates in bronze, removed in silver (warning)
MAX_UNRESOLVED_REPORTS = 0.01       # complaints silver cannot resolve to an account; silver drops them (warning)
HORIZON_DAYS = 90
LABEL = "is_mule_90d"
PAN = r"[A-Z]{5}[0-9]{4}[A-Z]"
PAN_ANYWHERE = rf"(^|[^A-Z0-9]){PAN}([^A-Z0-9]|$)"
HOLDOUT_BANDS = 7
TIERS = {"T1_FREEZE_REVIEW": "alerts_t1", "T2_HOLD_MONITOR": "alerts_t2", "T3_WATCHLIST": "alerts_t3"}

RESULT_SCHEMA = (
    "pipeline_run string, run_id string, run_ts timestamp, as_of date, layer string, "
    "table_name string, check_name string, expectation_type string, column_name string, "
    "severity string, success boolean, observed_value string, element_count bigint, "
    "unexpected_count bigint, unexpected_pct double, kwargs string, table_snapshot_id bigint, "
    "gx_version string"
)

# bronze extract -> (key column, event date column)
EXTRACTS = {
    "kyc_onboarding": ("cif", "onboarding_date"),
    "cbs_accounts": ("account_id", "open_date"),
    "upi_transactions": ("txn_id", "txn_ts"),
    "digital_sessions": ("session_id", "session_ts"),
    "fraud_reports": ("report_id", "report_ts"),
}
# silver table -> key columns (unique together, never null)
SILVER = {
    "customer": ["cif"],
    "account": ["account_id"],
    "account_vpa": ["vpa", "account_id"],
    "txn": ["txn_id"],
    "session": ["session_id"],
    "report": ["report_id", "account_id"],
    "identity_links": ["cif", "link_type", "identifier"],
    "identity_edges": ["src_cif", "dst_cif", "link_type", "identifier"],
    "identity_clusters": ["snapshot_date", "cif"],
    "graph_edges": ["snapshot_date", "src_cif", "dst_cif", "link_type"],
}
SILVER_LAYER_OF = {"identity_clusters": "graph", "graph_edges": "graph"}
# gold features: not-null severity, and the valid range of each
CRITICAL_FEATURES = {"account_age_days", "min_kyc_flag", "pass_through_ratio_7d", "person_cluster_size",
                     "ring_size", "hops_to_known_mule"}
FEATURE_RANGES = {
    "pass_through_ratio_7d": (0, 1), "night_txn_share_30d": (0, 1), "round_amount_share_30d": (0, 1),
    "hops_to_known_mule": (1, 3), "account_age_days": (0, None), "median_hold_hours_30d": (0, None),
    "inflow_to_declared_income_30d": (0, None), "distinct_senders_7d": (0, None),
    "distinct_receivers_7d": (0, None), "new_device_logins_30d": (0, None),
    "vpa_or_mobile_changes_30d": (0, None), "accounts_on_same_device": (0, None),
    "cifs_sharing_mobile": (1, None), "person_cluster_size": (1, None), "ring_size": (1, None),
    "credits_from_complainants_30d": (0, None),
}
FLAGS = ("min_kyc_flag", "dormant_reactivated_flag")
MULE_RATE_BAND = (0.0002, 0.002)    # latest labelled snapshot; 0.065% on the federal book
VOLUME_BAND = {"bronze": (0.95, 1.10), "silver": (0.95, 1.10), "gold": (0.95, 1.10)}


@dataclass
class Result:
    table_name: str
    check_name: str
    expectation_type: str
    severity: str
    success: bool
    observed_value: str | None = None
    column_name: str | None = None
    element_count: int | None = None
    unexpected_count: int | None = None
    unexpected_pct: float | None = None
    kwargs: str | None = None
    table: str | None = field(default=None, repr=False)     # full name, for the snapshot id


# ---------------------------------------------------------------- rules (pure Python)

def _pct(bad: int | None, rows: int | None) -> float | None:
    return 100.0 * bad / rows if bad is not None and rows else (0.0 if bad == 0 else None)


def _kw(**kw) -> str:
    return json.dumps({k: v for k, v in kw.items() if v is not None}, default=str)


def row_count(table: str, rows: int, name: str = "row count", min_rows: int = 1, severity: str = CRITICAL,
              ) -> Result:
    return Result(table, name, "row_count", severity, rows >= min_rows, str(rows), element_count=rows,
                  kwargs=_kw(min_value=min_rows))


def not_null(table: str, column: str, rows: int, nulls: int, severity: str = CRITICAL, name: str | None = None,
             ) -> Result:
    return Result(table, name or f"{column} not null", "not_null", severity, nulls == 0, str(nulls), column,
                  rows, nulls, _pct(nulls, rows))


def unique(table: str, columns: list[str], rows: int, distinct: int, severity: str = CRITICAL,
           name: str | None = None) -> Result:
    dupes = rows - distinct
    return Result(table, name or f"{', '.join(columns)} unique", "unique", severity, dupes == 0, str(dupes),
                  ",".join(columns), rows, dupes, _pct(dupes, rows))


def in_set(table: str, column: str, rows: int, bad: int, values, severity: str = CRITICAL,
           name: str | None = None) -> Result:
    return Result(table, name or f"{column} in {sorted(values)}", "in_set", severity, bad == 0, str(bad), column,
                  rows, bad, _pct(bad, rows), _kw(value_set=sorted(values)))


def in_range(table: str, column: str, rows: int, bad: int, lo=None, hi=None, severity: str = CRITICAL,
             name: str | None = None) -> Result:
    span = f"{lo if lo is not None else '-inf'} to {hi if hi is not None else 'inf'}"
    return Result(table, name or f"{column} within {span}", "range", severity, bad == 0, str(bad), column, rows,
                  bad, _pct(bad, rows), _kw(min_value=lo, max_value=hi))


def not_after(table: str, column: str, rows: int, future: int, as_of: date, severity: str = CRITICAL,
              name: str | None = None) -> Result:
    return Result(table, name or f"{column} not after the as-of date", "not_after_as_of", severity, future == 0,
                  str(future), column, rows, future, _pct(future, rows), _kw(as_of=as_of))


def equal(table: str, name: str, expected: int, actual: int, severity: str = CRITICAL) -> Result:
    diff = abs(actual - expected)
    return Result(table, name, "reconciliation_equal", severity, diff == 0, str(actual), element_count=expected,
                  unexpected_count=diff, unexpected_pct=_pct(diff, expected), kwargs=_kw(expected=expected))


def rate_at_most(table: str, name: str, rows: int, bad: int, max_rate: float, severity: str = CRITICAL,
                 column: str | None = None) -> Result:
    rate = bad / rows if rows else 0.0
    return Result(table, name, "rate_at_most", severity, rate <= max_rate, f"{rate:.6f}", column, rows, bad,
                  _pct(bad, rows), _kw(max_rate=max_rate))


def rate_tiers(table: str, what: str, rows: int, bad: int, limit: float, column: str | None = None) -> list[Result]:
    """A rate limit as two checks: critical at the limit, and a warning at half of it, so a
    rate creeping towards the limit shows before it stops the pipeline."""
    return [rate_at_most(table, f"{what} at most {limit:.2%}", rows, bad, limit, CRITICAL, column),
            rate_at_most(table, f"{what} at most {limit / 2:.2%} (warning: half the limit)", rows, bad,
                         limit / 2, WARNING, column)]


def volume_change(table: str, rows: int, previous: float | None, band: tuple[float, float],
                  name: str = "rows against the previous load") -> list[Result]:
    if not previous:
        return []
    ratio = rows / previous
    lo, hi = band
    return [Result(table, f"{name} within {lo - 1:+.0%} / {hi - 1:+.0%}", "volume_change", WARNING,
                   lo <= ratio <= hi, f"{ratio:.4f}", element_count=rows,
                   kwargs=_kw(min_value=lo, max_value=hi, previous=int(previous)))]


def failed_critical(results: list[Result]) -> list[Result]:
    return [r for r in results if not r.success and r.severity == CRITICAL]


# ---------------------------------------------------------------- Spark helpers

def _F():
    from pyspark.sql import functions as F
    return F


def _distinct(df, cols: list[str]) -> int:
    return df.select(*cols).distinct().count()


def _count_if(cond):
    F = _F()
    return F.sum(F.when(cond, 1).otherwise(0))


def _orphans(df, key: str, parent) -> int:
    return df.join(parent.select(key).distinct(), key, "left_anti").count()


def _pan_hit(df, skip: tuple[str, ...] = ()):
    F = _F()
    hit = F.lit(False)
    for f in df.schema.fields:
        if f.dataType.simpleString() == "string" and f.name not in skip:
            hit = hit | F.coalesce(F.upper(F.col(f.name)).rlike(PAN_ANYWHERE), F.lit(False))
    return hit


# ---------------------------------------------------------------- layers

def bronze_checks(spark, db: str, as_of: date, previous: dict) -> list[Result]:
    F = _F()
    bronze, ref = f"{db}_bronze", f"{db}_ref"
    tables = {name: spark.table(f"{bronze}.{name}") for name in EXTRACTS}
    out = []
    for name, (key, date_col) in EXTRACTS.items():
        df = tables[name]
        s = df.agg(F.count("*").alias("rows"), _count_if(F.col(key).isNull()).alias("null_keys"),
                   _count_if(F.to_date(date_col) > F.lit(as_of)).alias("future"),
                   _count_if(~(F.col("batch_as_of") == F.lit(as_of)) | F.col("batch_as_of").isNull()).alias("batch"),
                   _count_if(_pan_hit(df, skip=("pan",) if name == "kyc_onboarding" else ())).alias("pan"),
                   F.countDistinct(key).alias("keys")).first()
        rows = s["rows"]
        out += [
            row_count(name, rows),
            not_null(name, key, rows, s["null_keys"]),
            not_after(name, date_col, rows, s["future"], as_of),
            in_set(name, "batch_as_of", rows, s["batch"], [as_of.isoformat()], name="batch is for the as-of date"),
            rate_at_most(name, f"re-sent duplicate {key} at most {MAX_DUPLICATE_RATE:.0%} (removed in silver)",
                         rows, rows - s["null_keys"] - s["keys"], MAX_DUPLICATE_RATE, WARNING, key),
            in_set(name, "*", rows, s["pan"], ["no PAN pattern"], name="no raw PAN outside kyc_onboarding.pan"),
            *volume_change(name, rows, previous.get((name, "row count")), VOLUME_BAND["bronze"]),
        ]
    kyc, acct, txn, sess = (tables[k] for k in ("kyc_onboarding", "cbs_accounts", "upi_transactions",
                                                  "digital_sessions"))
    n_acct = acct.count()
    out.append(unique("cbs_accounts", ["account_id"], n_acct, _distinct(acct, ["account_id"])))
    for col, ref_table in (("product_code", "product_map"), ("branch_code", "branch_map")):
        unknown = acct.join(spark.table(f"{ref}.{ref_table}").select(col), col, "left_anti").count()
        out.append(in_set("cbs_accounts", col, n_acct, unknown, [f"{ref_table}.{col}"],
                          name=f"{col} known in ref.{ref_table}"))
    for name, df, key, parent in (("cbs_accounts", acct, "cif", kyc), ("upi_transactions", txn, "account_id", acct),
                                  ("digital_sessions", sess, "cif", kyc)):
        out += rate_tiers(name, f"rows for an unknown {key}", df.count(), _orphans(df, key, parent), MAX_BAD_RATE,
                          key)
    s = txn.agg(F.count("*").alias("rows"), _count_if(F.col("amount") <= 0).alias("bad_amount"),
                _count_if(~F.col("direction").isin("CR", "DR")).alias("bad_dir"),
                F.min(F.to_date("txn_ts")).alias("first"), F.countDistinct(F.to_date("txn_ts")).alias("days")).first()
    out += rate_tiers("upi_transactions", "non-positive amount", s["rows"], s["bad_amount"], MAX_BAD_RATE, "amount")
    out += rate_tiers("upi_transactions", "direction not CR / DR", s["rows"], s["bad_dir"], MAX_BAD_RATE,
                      "direction")
    expected_days = (as_of - s["first"]).days + 1 if s["first"] else 0
    out.append(equal("upi_transactions", f"a transaction on every day since {s['first']}", expected_days,
                     s["days"]))
    n_kyc = kyc.count()
    bad_pan = kyc.where(~F.coalesce(F.col("pan").rlike(f"^{PAN}$"), F.lit(False))).count()
    out += rate_tiers("kyc_onboarding", "malformed or missing PAN", n_kyc, bad_pan, MAX_BAD_RATE, "pan")
    for r in out:
        r.table = f"{bronze}.{r.table_name}"
    return out


def silver_checks(spark, db: str, as_of: date, previous: dict) -> list[Result]:
    F = _F()
    silver, bronze = f"{db}_silver", f"{db}_bronze"
    out = []
    frames = {}
    for name, keys in SILVER.items():
        df = frames[name] = spark.table(f"{silver}.{name}")
        rows = df.count()
        out += [row_count(name, rows), unique(name, keys, rows, _distinct(df, keys))]
        nulls = df.agg(*[_count_if(F.col(k).isNull()).alias(k) for k in keys]).first()
        out += [not_null(name, k, rows, nulls[k]) for k in keys]
        if name in ("customer", "account", "txn"):
            out += volume_change(name, rows, previous.get((name, "row count")), VOLUME_BAND["silver"])

    b = {k: spark.table(f"{bronze}.{k}") for k in ("kyc_onboarding", "cbs_accounts", "upi_transactions",
                                                   "fraud_reports")}
    out.append(equal("customer", "rows = distinct bronze cif (deduplicated)",
                     b["kyc_onboarding"].select("cif").distinct().count(), frames["customer"].count()))
    out.append(equal("account", "rows = distinct bronze account_id (deduplicated)",
                     b["cbs_accounts"].select("account_id").distinct().count(), frames["account"].count()))
    known = (b["upi_transactions"].where(F.col("amount") > 0)
             .join(b["cbs_accounts"].select("account_id").distinct(), "account_id", "left_semi"))
    out.append(equal("txn", "rows = distinct positive bronze txn_id of known accounts",
                     known.select("txn_id").distinct().count(), frames["txn"].count()))
    report_ids = frames["report"].select("report_id").distinct()
    out.append(equal("report", "report_ids not in bronze fraud_reports", 0,
                     report_ids.join(b["fraud_reports"].select("report_id"), "report_id", "left_anti").count()))
    bronze_reports = b["fraud_reports"].select("report_id").distinct().count()
    out.append(rate_at_most("report", f"complaints not resolved to an account at most {MAX_UNRESOLVED_REPORTS:.0%}",
                            bronze_reports, bronze_reports - report_ids.count(), MAX_UNRESOLVED_REPORTS, WARNING))

    cust = frames["customer"]
    n = cust.count()
    for col in ("pan_hash", "aadhaar_hash", "mobile_hash"):
        bad = cust.where(F.col(col).isNull() | ~F.col(col).rlike("^[0-9a-f]{64}$")).count()
        out.append(in_set("customer", col, n, bad, ["^[0-9a-f]{64}$"], name=f"{col} is a SHA-256 hex digest"))
    leaks = cust.where(_pan_hit(cust)).count()
    out.append(in_set("customer", "*", n, leaks, ["no PAN pattern"], name="no raw PAN in any customer column"))
    t = frames["txn"].agg(F.count("*").alias("rows"), _count_if(F.col("amount") <= 0).alias("amount"),
                          _count_if(~F.col("direction").isin("CR", "DR")).alias("direction")).first()
    out.append(in_range("txn", "amount", t["rows"], t["amount"], lo=0.01, name="amount positive"))
    out.append(in_set("txn", "direction", t["rows"], t["direction"], ["CR", "DR"]))
    cl = frames["identity_clusters"]
    latest = cl.agg(F.max("snapshot_date")).first()[0]
    out.append(Result("identity_clusters", "latest snapshot is the as-of date", "in_set", CRITICAL,
                      latest == as_of, str(latest), "snapshot_date", 1, int(latest != as_of),
                      kwargs=_kw(value_set=[as_of.isoformat()])))
    hops = cl.where(~F.col("hops_to_known_mule").between(1, 3) | F.col("hops_to_known_mule").isNull()).count()
    out.append(in_range("identity_clusters", "hops_to_known_mule", cl.count(), hops, 1, 3))
    for r in out:
        r.table = f"{silver}.{r.table_name}"
    return out


def gold_checks(spark, db: str, as_of: date, previous: dict) -> list[Result]:
    F = _F()
    t, full = "mule_features", f"{db}_gold.mule_features"
    df = spark.table(full)
    matured = F.date_add(F.col("snapshot_date"), HORIZON_DAYS) <= F.lit(as_of)
    feats = sorted(set(FEATURE_RANGES) | set(FLAGS) | CRITICAL_FEATURES)
    aggs = [F.count("*").alias("rows"), _count_if(F.col("snapshot_date") > F.lit(as_of)).alias("future"),
            _count_if(F.col("snapshot_date") == F.lit(as_of)).alias("today"),
            _count_if(matured & F.col(LABEL).isNull()).alias("unlabelled_matured"),
            _count_if(~matured & F.col(LABEL).isNotNull()).alias("leaked"),
            _count_if(F.col(LABEL).isNotNull() & ~F.col(LABEL).isin(0, 1)).alias("bad_label"),
            _count_if(matured).alias("matured")]
    aggs += [_count_if(F.col(c).isNull()).alias(f"null_{c}") for c in feats]
    for c, (lo, hi) in FEATURE_RANGES.items():
        bad = F.lit(False)
        if lo is not None:
            bad = bad | (F.col(c) < lo)
        if hi is not None:
            bad = bad | (F.col(c) > hi)
        aggs.append(_count_if(bad).alias(f"range_{c}"))
    aggs += [_count_if(~F.col(c).isin(0, 1)).alias(f"flag_{c}") for c in FLAGS]
    for k in ("account_id", "snapshot_date", "cif"):
        aggs.append(_count_if(F.col(k).isNull()).alias(f"key_{k}"))
    s = df.agg(*aggs).first()
    rows = s["rows"]
    out = [row_count(t, rows),
           unique(t, ["account_id", "snapshot_date"], rows, _distinct(df, ["account_id", "snapshot_date"]),
                  name="one row per account per snapshot"),
           *[not_null(t, k, rows, s[f"key_{k}"]) for k in ("account_id", "snapshot_date", "cif")],
           not_after(t, "snapshot_date", rows, s["future"], as_of),
           row_count(t, s["today"], name="today's snapshot present"),
           in_set(t, LABEL, rows, s["bad_label"], [0, 1], name="label is 0/1"),
           not_null(t, LABEL, s["matured"], s["unlabelled_matured"],
                    name=f"label known once {HORIZON_DAYS} days have passed"),
           Result(t, f"no label before {HORIZON_DAYS} days have passed (leakage)", "not_null", CRITICAL,
                  s["leaked"] == 0, str(s["leaked"]), LABEL, rows - s["matured"], s["leaked"],
                  _pct(s["leaked"], rows - s["matured"]), _kw(rule="label IS NULL while maturing"))]
    out += [not_null(t, c, rows, s[f"null_{c}"], CRITICAL if c in CRITICAL_FEATURES else WARNING) for c in feats]
    out += [in_range(t, c, rows, s[f"range_{c}"], lo, hi) for c, (lo, hi) in FEATURE_RANGES.items()]
    out += [in_set(t, c, rows, s[f"flag_{c}"], [0, 1]) for c in FLAGS]
    latest = (df.where(matured).groupBy("snapshot_date").agg(F.avg(LABEL).alias("rate"), F.count("*").alias("n"))
              .orderBy(F.col("snapshot_date").desc()).limit(1).collect())
    if latest:
        lo, hi = MULE_RATE_BAND
        rate = latest[0]["rate"] or 0.0
        out.append(Result(t, f"latest labelled mule rate within {lo:.2%}-{hi:.2%}", "range", WARNING,
                          lo <= rate <= hi, f"{rate:.6f}", LABEL, latest[0]["n"],
                          kwargs=_kw(min_value=lo, max_value=hi, snapshot_date=latest[0]["snapshot_date"])))
    out += volume_change(t, s["today"], previous.get((t, "today's snapshot present")), VOLUME_BAND["gold"],
                         name="today's snapshot rows against the previous load")
    for r in out:
        r.table = full
    return out


def publish_checks(spark, db: str, as_of: date, previous: dict) -> list[Result]:
    """The CAI scoring job's output for the as-of date: the run row and what it says was published."""
    F = _F()
    gold = f"{db}_gold"
    if not spark.catalog.tableExists(f"{gold}.mule_model_run"):
        run, out = [], [row_count("mule_model_run", 0, name="a run row for the as-of date")]
    else:
        runs = spark.table(f"{gold}.mule_model_run").where(F.col("run_date") == F.lit(as_of))
        out = [row_count("mule_model_run", runs.count(), name="a run row for the as-of date")]
        run = runs.orderBy(F.col("run_ts").desc()).limit(1).collect()
    if not run:
        out[0].table = f"{gold}.mule_model_run"
        return out
    run = run[0]
    published = bool(run["alerts_published"])
    alerts = spark.table(f"{gold}.mule_alerts").where(F.col("run_date") == F.lit(as_of))
    by_tier = {r["tier"]: r["n"] for r in alerts.where(F.col("run_id") == run["run_id"])
               .groupBy("tier").agg(F.count("*").alias("n")).collect()}
    for tier, col in TIERS.items():
        out.append(equal("mule_alerts", f"{tier} rows = the run row", run[col] if published else 0,
                         by_tier.get(tier, 0)))
    if published:
        n_alerts = alerts.count()
        other = alerts.where(F.col("run_id") != run["run_id"]).count()
        out.append(Result("mule_alerts", "every alert carries the run's run_id", "in_set", CRITICAL, other == 0,
                          str(other), "run_id", n_alerts, other, _pct(other, n_alerts),
                          _kw(value_set=[run["run_id"]])))
        rings = spark.table(f"{gold}.mule_rings").where(F.col("run_id") == run["run_id"]).count()
        out.append(row_count("mule_rings", rings, name="rings published with the alerts"))
    bands = spark.table(f"{gold}.mule_holdout").where(F.col("run_id") == run["run_id"]).count()
    out.append(equal("mule_holdout", f"{HOLDOUT_BANDS} holdout bands for the run", HOLDOUT_BANDS, bands))
    book = spark.table(f"{gold}.mule_features").where(F.col("snapshot_date") == F.lit(run["snapshot_date"])).count()
    out.append(equal("mule_model_run", "scored accounts = gold rows of the scored snapshot", book,
                     run["scored_accounts"]))
    for r in out:
        r.table = f"{gold}.{r.table_name}"
    return out


LAYER_CHECKS = {"bronze": bronze_checks, "silver": silver_checks, "gold": gold_checks, "publish": publish_checks}


# ---------------------------------------------------------------- running

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--layer", choices=LAYERS, required=True)
    p.add_argument("--as-of", default=None, help="load date (default: max batch_as_of in bronze.cbs_accounts)")
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    p.add_argument("--pipeline-run", default=None,
                   help="groups the layers of one DAG run (Airflow run_id); default manual-<as_of>")
    args, _ = p.parse_known_args(argv)
    return args


def resolve_as_of(spark, db: str, as_of: str | None) -> date:
    if as_of:
        return datetime.strptime(as_of, "%Y-%m-%d").date()
    return spark.table(f"{db}_bronze.cbs_accounts").agg(_F().max("batch_as_of")).first()[0]


def _snapshot_id(spark, table: str) -> int | None:
    try:
        row = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
        return int(row[0]) if row else None
    except Exception:  # lineage is best effort
        return None


def previous_counts(spark, results_table: str, layer: str, as_of: date) -> dict:
    """Last recorded row counts per table from earlier loads, for the volume checks."""
    if not spark.catalog.tableExists(results_table):
        return {}
    rows = spark.sql(f"""
        SELECT table_name, check_name, observed_value FROM (
          SELECT table_name, check_name, observed_value,
                 ROW_NUMBER() OVER (PARTITION BY table_name, check_name ORDER BY as_of DESC, run_ts DESC) rn
          FROM {results_table}
          WHERE layer = '{layer}' AND expectation_type = 'row_count' AND as_of < DATE '{as_of.isoformat()}')
        WHERE rn = 1""").collect()
    out = {}
    for r in rows:
        if re.fullmatch(r"\d+", r.observed_value or ""):
            out[(r.table_name, r.check_name)] = float(r.observed_value)
    return out


def records(results: list[Result], snapshots: dict, *, pipeline_run: str, run_id: str, run_ts: datetime,
            as_of: date, layer: str) -> list[dict]:
    out = []
    for r in results:
        d = asdict(r)
        table = d.pop("table")
        d.update(pipeline_run=pipeline_run, run_id=run_id, run_ts=run_ts, as_of=as_of, layer=layer,
                 table_snapshot_id=snapshots.get(table), gx_version=None)
        out.append(d)
    return out


def write_results(spark, results_table: str, rows: list[dict]) -> None:
    df = spark.createDataFrame(rows, RESULT_SCHEMA)
    if spark.catalog.tableExists(results_table):
        df.writeTo(results_table).append()
    else:
        (df.writeTo(results_table).using("iceberg").tableProperty("format-version", "2")
         .partitionedBy(_F().months("as_of")).create())


def run(spark, args: argparse.Namespace) -> list[Result]:
    db, layer = args.db_prefix, args.layer
    as_of = resolve_as_of(spark, db, args.as_of)
    results_table = f"{db}_ref.dq_results"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}_ref")
    results = LAYER_CHECKS[layer](spark, db, as_of, previous_counts(spark, results_table, layer, as_of))
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    run_id = f"{layer}-{as_of:%Y%m%d}-{uuid.uuid4().hex[:8]}"
    snapshots = {t: _snapshot_id(spark, t) for t in {r.table for r in results}}
    rows = records(results, snapshots, pipeline_run=args.pipeline_run or f"manual-{as_of.isoformat()}",
                   run_id=run_id, run_ts=run_ts, as_of=as_of, layer=layer)
    write_results(spark, results_table, rows)
    for r in results:
        logger.info("%-4s %-8s %-18s %-62s %s", "PASS" if r.success else "FAIL", r.severity, r.table_name,
                    r.check_name[:62], r.observed_value)
    failed = [r for r in results if not r.success]
    logger.info("%s as of %s: %d checks, %d failed (%d critical); results in %s (run %s)", layer, as_of,
                len(results), len(failed), len(failed_critical(results)), results_table, run_id)
    return results


def main(argv=None, spark=None) -> None:
    args = parse_args(argv)
    own = spark is None
    if own:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName(f"mule-dq-{args.layer}").getOrCreate()
    try:
        results = run(spark, args)
    finally:
        if own:
            spark.stop()
    critical = failed_critical(results)
    if critical:
        for r in critical:
            logger.error("DATA QUALITY GATE FAILED: %s: %s (observed %s)", r.table_name, r.check_name,
                         r.observed_value)
        sys.exit(1)
    logger.info("%s data quality gate passed", args.layer)


if __name__ == "__main__":
    main()

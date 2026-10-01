#!/usr/bin/env python3
"""
The "Mule Investigation Command Centre" and "Mule Data Health" dashboards in
Cloudera Data Visualization, as code.

Datasets, visuals and sheets are declared below (DASHBOARDS); this script turns
them into one Data Visualization export file (dataviz/mule_dashboards.json) and
imports it through the migration REST API of the CAI application "Mule Data
Visualization" (ci/cai_jobs.py DATAVIZ). UUIDs and primary keys are fixed per
artefact, so an import updates both dashboards in place. Datasets read the
rsingh_mule_acct_report views (sql/dataviz_views.sql); see docs/DATAVIZ.md.

  set -a; source .env; set +a
  python dataviz/build_dashboard.py              # connection (if missing), file, import
  python dataviz/build_dashboard.py --no-import  # write the file only
  python dataviz/build_dashboard.py --verify     # every visual through the Data API, KPIs against Impala

Needs MULE_CAI_HOST, MULE_CAI_API_KEY (CAI proxy authentication) and, for the column
types and a new connection, MULE_IMPALA_USER / MULE_IMPALA_PASSWORD. The password goes
to the Data Visualization connection only; it is never printed or written to the file.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from ci.cai_jobs import DATAVIZ  # noqa: E402

OUT = ROOT / "dataviz" / "mule_dashboards.json"
TITLE = "Mule Investigation Command Centre"
HEALTH_TITLE = "Mule Data Health"
DB = "rsingh_mule_acct_report"
CONNECTION = "rsingh-mule-acct-impala"
NS = uuid.UUID("78b6c29c-4f37-40d2-81ec-cdeef2987950")
DASHBOARD_PK = 7000                   # the Command Centre; Data Health is 7001
DATASET_PK0 = 7100
VISUAL_PK0 = 7200                     # Command Centre visuals 7201..; Data Health 7401..

DATASETS = {                          # key: (name, view, integer columns that are dimensions)
    "alerts": ("Mule - Alerts", "v_alerts", {"is_latest", "risk_rank", "ring_size", "hops_to_known_mule"}),
    "reasons": ("Mule - Alert reasons", "v_alert_reasons", {"is_latest"}),
    "rings": ("Mule - Rings", "v_rings", {"is_latest", "ring_rank", "min_hops_to_known_mule"}),
    "book": ("Mule - Book weekly", "v_book_weekly", set()),
    "runs": ("Mule - Model runs", "v_model_run", {"is_latest", "gate_passed", "alerts_published"}),
    "holdout": ("Mule - Holdout", "v_holdout", {"is_latest"}),
    "dq": ("Mule - Data quality", "v_dq", {"is_latest", "layer_order", "table_snapshot_id"}),
    "dq_run": ("Mule - DQ runs", "v_dq_run", {"is_latest", "layer_order"}),
}

LATEST = "[is_latest] = 1"
T1 = "sum(case when [tier] = 'T1_FREEZE_REVIEW' then 1 else 0 end)"
MULE_RATE = "sum([mules]) / sum([labelled])"

# A visual: dims are (column, alias); measures are (expression, alias); filters are expressions.
# pos is (column, row, width, height) on a 64-column grid.
SHEETS = [
    ("Alert queue", [
        dict(type="kpi", ds="alerts", title="Alerts on today's queue", measures=[("sum(1)", "Alerts")],
             filters=[LATEST], pos=(1, 1, 13, 10)),
        dict(type="kpi", ds="alerts", title="Freeze reviews (T1)", measures=[(T1, "T1 freeze reviews")],
             filters=[LATEST], pos=(14, 1, 13, 10)),
        dict(type="kpi", ds="alerts", title="Rings touched",
             measures=[("count(distinct case when [in_ring] = 1 then [ring_id] end)", "Rings")],
             filters=[LATEST], pos=(27, 1, 13, 10)),
        dict(type="kpi", ds="alerts", title="Expected mules on the queue",
             measures=[("round(sum([p_mule_adj]), 0)", "Expected mules")], filters=[LATEST], pos=(40, 1, 13, 10)),
        dict(type="kpi", ds="alerts", title="Alerts one hop from a known mule",
             measures=[("sum([near_known_mule])", "Near a known mule")], filters=[LATEST], pos=(53, 1, 12, 10)),
        dict(type="trellis-bars", ds="alerts", title="Alerts by region and tier",
             x=[("region", "Region")], measures=[("sum(1)", "Alerts")], color=[("tier", "Tier")],
             filters=[LATEST], pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="alerts", title="Top 10 branches by alerts",
             x=[("branch_name", "Branch")], measures=[("sum(1)", "Alerts")], color=[("region", "Region")],
             filters=[LATEST], sort_desc=True, limit=10, pos=(33, 11, 32, 22)),
        dict(type="trellis-bars", ds="reasons", title="Why accounts are flagged: alerts per reason code",
             x=[("reason_code", "Reason code")], measures=[("sum(1)", "Alerts")], color=[("tier", "Tier")],
             filters=[LATEST], sort_desc=True, pos=(1, 33, 32, 22)),
        dict(type="trellis-bars", ds="alerts", title="Alerts by product and KYC type",
             x=[("product_name", "Product")], measures=[("sum(1)", "Alerts")], color=[("kyc_type", "KYC")],
             filters=[LATEST], sort_desc=True, pos=(33, 33, 32, 22)),
        dict(type="table", ds="alerts", title="The morning's top 25 accounts to investigate",
             dims=[("risk_rank", "Rank"), ("account_id", "Account"), ("branch_name", "Branch"),
                   ("product_name", "Product"), ("tier", "Tier"), ("action", "Action"), ("reasons", "Reasons"),
                   ("ring_size", "Ring size"), ("hops_to_known_mule", "Hops to known mule")],
             measures=[("round(max([p_mule_adj]), 4)", "p(mule)")],
             filters=[LATEST, "[risk_rank] <= 25"], sort_dim="risk_rank", limit=25, pos=(1, 55, 64, 26)),
    ]),
    ("Rings and network", [
        dict(type="kpi", ds="rings", title="Rings with alerts", measures=[("sum(1)", "Rings")],
             filters=[LATEST], pos=(1, 1, 21, 10)),
        dict(type="kpi", ds="rings", title="Largest alerted ring (customers)",
             measures=[("max([ring_size])", "Largest ring")], filters=[LATEST], pos=(22, 1, 21, 10)),
        dict(type="kpi", ds="rings", title="Alerted accounts in rings",
             measures=[("sum([accounts_alerted])", "Accounts")], filters=[LATEST], pos=(43, 1, 22, 10)),
        dict(type="trellis-bars", ds="book", title="90-day mule rate by distance to a known mule",
             x=[("hops_band", "Distance")], measures=[(MULE_RATE, "Mule rate")],
             filters=["[labelled] > 0"], pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="book", title="90-day mule rate by pass-through (credits forwarded in 24h)",
             x=[("pass_through_band", "Pass-through")], measures=[(MULE_RATE, "Mule rate")],
             filters=["[labelled] > 0"], pos=(33, 11, 32, 22)),
        dict(type="table", ds="rings", title="Top 15 rings to investigate first",
             dims=[("ring_rank", "Rank"), ("ring_id", "Ring"), ("top_tier", "Top tier"), ("min_hops_to_known_mule", "Hops to known mule"),
                   ("top_reason_codes", "Top reasons")],
             measures=[("round(max([ring_risk]), 3)", "Ring risk"), ("max([ring_size])", "Customers"),
                       ("sum([accounts_scored])", "Accounts scored"), ("sum([accounts_alerted])", "Alerted"),
                       ("round(max([alerted_share]), 2)", "Alerted share")],
             filters=[LATEST, "[ring_rank] <= 15"], sort_dim="ring_rank", limit=15, pos=(1, 33, 64, 22)),
    ]),
    ("Trends", [
        dict(type="trellis-lines", ds="book", title="Active book by region (accounts per snapshot)",
             x=[("snapshot_date", "Snapshot")], measures=[("sum([accounts])", "Accounts")],
             color=[("region", "Region")], pos=(1, 1, 32, 24)),
        dict(type="trellis-lines", ds="book", title="90-day mule rate by region",
             x=[("snapshot_date", "Snapshot")], measures=[(MULE_RATE, "Mule rate")], color=[("region", "Region")],
             filters=["[labelled] > 0"], pos=(33, 1, 32, 24)),
        dict(type="trellis-lines", ds="book", title="Mules per snapshot by product",
             x=[("snapshot_date", "Snapshot")], measures=[("sum([mules])", "Mules")],
             color=[("product_name", "Product")], filters=["[labelled] > 0"], pos=(1, 25, 32, 22)),
        dict(type="trellis-bars", ds="alerts", title="Published alerts per run date, by tier",
             x=[("run_date", "Run date")], measures=[("sum(1)", "Alerts")], color=[("tier", "Tier")],
             pos=(33, 25, 32, 22)),
    ]),
    ("Model trust", [
        dict(type="kpi", ds="runs", title="Holdout AUC", measures=[("max([holdout_auc])", "AUC")],
             filters=[LATEST], pos=(1, 1, 13, 10)),
        dict(type="kpi", ds="runs", title="Mules caught in the top 1% (model)",
             measures=[("max([capture_top1])", "Capture top 1%")], filters=[LATEST], pos=(14, 1, 13, 10)),
        dict(type="kpi", ds="runs", title="Mules caught in the top 1% (rules only)",
             measures=[("max([rules_capture_top1])", "Rules capture top 1%")], filters=[LATEST], pos=(27, 1, 13, 10)),
        dict(type="kpi", ds="runs", title="Precision of the T1 band (top 0.2%)",
             measures=[("max([precision_top02])", "Precision top 0.2%")], filters=[LATEST], pos=(40, 1, 13, 10)),
        dict(type="kpi", ds="runs", title="Lift over rules", measures=[("max([lift_over_rules])", "Lift")],
             filters=[LATEST], pos=(53, 1, 12, 10)),
        dict(type="trellis-lines", ds="holdout", title="Holdout: share of mules caught, model against rules",
             x=[("band_label", "Risk band")],
             measures=[("max([model_cum_capture])", "Model"), ("max([rules_cum_capture])", "Rules only")],
             filters=[LATEST], pos=(1, 11, 32, 24)),
        dict(type="trellis-bars", ds="holdout", title="Holdout: mule rate in each risk band",
             x=[("band_label", "Risk band")], measures=[("max([mule_rate])", "Mule rate")],
             filters=[LATEST], pos=(33, 11, 32, 24)),
        dict(type="table", ds="runs", title="Daily runs: trust numbers and the KPI gate",
             dims=[("run_date", "Run date"), ("model_family", "Model"), ("device", "Device"),
                   ("triggered_by", "Triggered by"), ("gate_passed", "Gate passed"),
                   ("alerts_published", "Published")],
             measures=[("sum([scored_accounts])", "Scored"), ("sum([alerts_total])", "Alerts"),
                       ("max([holdout_auc])", "AUC"), ("max([capture_top1])", "Capture top 1%"),
                       ("max([precision_top02])", "Precision top 0.2%"), ("max([lift_over_rules])", "Lift"),
                       ("round(max([duration_s]), 0)", "Seconds")],
             sort_dim="run_date", sort_asc=False, pos=(1, 35, 64, 20)),
    ]),
    ("Data quality", [
        dict(type="kpi", ds="dq", title="Checks", measures=[("sum(1)", "Checks")], filters=[LATEST],
             pos=(1, 1, 21, 10)),
        dict(type="kpi", ds="dq", title="Failed checks", measures=[("sum([failed])", "Failed")], filters=[LATEST],
             pos=(22, 1, 21, 10)),
        dict(type="kpi", ds="dq", title="Critical failures",
             measures=[("sum(case when [severity] = 'critical' then [failed] else 0 end)", "Critical failures")],
             filters=[LATEST], pos=(43, 1, 22, 10)),
        dict(type="trellis-bars", ds="dq", title="Checks passed and failed by layer",
             x=[("layer_label", "Layer")], measures=[("sum([passed])", "Passed"), ("sum([failed])", "Failed")],
             filters=[LATEST], pos=(1, 11, 24, 22)),
        dict(type="table", ds="dq", title="Failed checks (empty on a healthy pipeline)",
             dims=[("layer", "Layer"), ("table_name", "Table"), ("check_name", "Check"), ("severity", "Severity")],
             measures=[("sum([expected])", "Expected"), ("sum([actual])", "Actual"), ("sum([diff])", "Diff")],
             filters=[LATEST, "[failed] = 1"], may_be_empty=True, pos=(25, 11, 40, 22)),
        dict(type="table", ds="dq", title="Every check: reconciliation against source counts and integrity",
             dims=[("layer_label", "Layer"), ("table_name", "Table"), ("check_name", "Check"),
                   ("severity", "Severity"), ("rule", "Rule"), ("note", "Note")],
             measures=[("sum([expected])", "Expected"), ("sum([actual])", "Actual"), ("sum([passed])", "Passed")],
             filters=[LATEST], sort_dim="layer_label", pos=(1, 33, 64, 26)),
    ]),
]

PASS_RATE = "round(100 * sum([passed]) / sum(1), 2)"
CRITICAL_FAILED = "sum(case when [severity] = 'critical' then [failed] else 0 end)"
WARNINGS_FAILED = "sum(case when [severity] = 'warning' then [failed] else 0 end)"

# Data Health: the checks cde/jobs/dq_check.py records after every layer (rsingh_mule_acct_ref.dq_results).
HEALTH_SHEETS = [
    ("Health now", [
        dict(type="kpi", ds="dq", title="Checks in the latest run", measures=[("sum(1)", "Checks")],
             filters=[LATEST], pos=(1, 1, 11, 10)),
        dict(type="kpi", ds="dq", title="Pass rate %", measures=[(PASS_RATE, "Pass rate %")],
             filters=[LATEST], pos=(12, 1, 11, 10)),
        dict(type="kpi", ds="dq", title="Critical failures (stop the pipeline)",
             measures=[(CRITICAL_FAILED, "Critical failures")], filters=[LATEST], pos=(23, 1, 11, 10)),
        dict(type="kpi", ds="dq", title="Warnings failed", measures=[(WARNINGS_FAILED, "Warnings")],
             filters=[LATEST], pos=(34, 1, 11, 10)),
        dict(type="kpi", ds="dq", title="Near misses (passed, some rows unexpected)",
             measures=[("sum([near_miss])", "Near misses")], filters=[LATEST], pos=(45, 1, 10, 10)),
        dict(type="kpi", ds="dq", title="Rows under check", measures=[("sum([row_count])", "Rows")],
             filters=[LATEST], pos=(55, 1, 10, 10)),
        dict(type="trellis-bars", ds="dq", title="Checks by layer and category",
             x=[("layer_label", "Layer")], measures=[("sum(1)", "Checks")], color=[("category", "Category")],
             filters=[LATEST], pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="dq", title="Checks by category and severity",
             x=[("category", "Category")], measures=[("sum(1)", "Checks")], color=[("severity", "Severity")],
             filters=[LATEST], pos=(33, 11, 32, 22)),
        dict(type="table", ds="dq", title="Table scorecard (latest run of each layer)",
             dims=[("layer_label", "Layer"), ("table_name", "Table")],
             measures=[("sum(1)", "Checks"), ("sum([passed])", "Passed"), ("sum([failed])", "Failed"),
                       (CRITICAL_FAILED, "Critical failed"), ("sum([near_miss])", "Near misses"),
                       ("max([row_count])", "Rows")],
             filters=[LATEST], sort_dim="layer_label", pos=(1, 33, 64, 26)),
    ]),
    ("Trends", [
        dict(type="trellis-lines", ds="dq", title="Pass rate % by as-of date and layer",
             x=[("as_of", "As of")], measures=[(PASS_RATE, "Pass rate %")], color=[("layer_label", "Layer")],
             pos=(1, 1, 32, 22)),
        dict(type="trellis-bars", ds="dq_run", title="Checks passed and failed per pipeline run",
             x=[("run_label", "Pipeline run")], measures=[("sum([passed])", "Passed"), ("sum([failed])", "Failed")],
             pos=(33, 1, 32, 22)),
        dict(type="trellis-lines", ds="dq", title="Bronze volume per table (rows per load)",
             x=[("as_of", "As of")], measures=[("sum([row_count])", "Rows")], color=[("table_name", "Table")],
             filters=["[layer] = 'bronze'", "[row_count] > 0"], pos=(1, 23, 64, 22)),
        dict(type="table", ds="dq", title="Rate checks against their limits (% of rows)",
             dims=[("as_of", "As of"), ("layer_label", "Layer"), ("table_name", "Table"), ("check_name", "Check"),
                   ("severity", "Severity")],
             measures=[("max([rate_pct])", "Rate %"), ("max([limit_pct])", "Limit %"),
                       ("sum([unexpected_count])", "Rows over")],
             filters=["[rule] = 'rate_at_most'"], sort_dim="as_of", sort_asc=False, pos=(1, 45, 64, 26)),
    ]),
    ("Pipeline runs", [
        dict(type="trellis-bars", ds="dq_run", title="Minutes after the bronze gate, per gate",
             x=[("run_label", "Pipeline run")], measures=[("max([minutes_after_bronze])", "Minutes")],
             color=[("layer_label", "Gate")], pos=(1, 1, 32, 22)),
        dict(type="trellis-bars", ds="dq_run", title="Rows checked per run and layer",
             x=[("run_label", "Pipeline run")], measures=[("sum([rows_checked])", "Rows checked")],
             color=[("layer_label", "Layer")], pos=(33, 1, 32, 22)),
        dict(type="table", ds="dq_run", title="Every gate run",
             dims=[("checked_at", "Checked at"), ("run_label", "Pipeline run"), ("run_type", "Run type"),
                   ("layer_label", "Gate"), ("pipeline_run", "Airflow run id")],
             measures=[("max([minutes_after_bronze])", "Minutes after bronze"), ("sum([checks])", "Checks"),
                       ("sum([passed])", "Passed"), ("sum([failed])", "Failed"),
                       ("sum([critical_failed])", "Critical failed"), ("sum([warnings_failed])", "Warnings failed"),
                       ("sum([near_misses])", "Near misses"), ("sum([table_count])", "Tables"),
                       ("sum([rows_checked])", "Rows checked")],
             sort_dim="checked_at", sort_asc=False, pos=(1, 23, 64, 26)),
    ]),
    ("Check details", [
        dict(type="table", ds="dq", title="Near misses: passed, but some rows were unexpected",
             dims=[("layer_label", "Layer"), ("table_name", "Table"), ("check_name", "Check"),
                   ("severity", "Severity"), ("observed_value", "Observed")],
             measures=[("sum([unexpected_count])", "Unexpected rows"), ("max([unexpected_pct])", "Unexpected %")],
             filters=[LATEST, "[near_miss] = 1"], may_be_empty=True, sort_dim="layer_label", pos=(1, 1, 64, 18)),
        dict(type="table", ds="dq", title="Failed checks over all runs (empty on a healthy pipeline)",
             dims=[("run_label", "Pipeline run"), ("layer_label", "Layer"), ("table_name", "Table"),
                   ("check_name", "Check"), ("severity", "Severity"), ("observed_value", "Observed"),
                   ("note", "Limit")],
             measures=[("sum([unexpected_count])", "Unexpected rows")],
             filters=["[failed] = 1"], may_be_empty=True, sort_dim="run_label", sort_asc=False,
             pos=(1, 19, 64, 18)),
        dict(type="table", ds="dq", title="Every check of the latest run",
             dims=[("layer_label", "Layer"), ("table_name", "Table"), ("check_name", "Check"),
                   ("category", "Category"), ("severity", "Severity"), ("rule", "Rule"),
                   ("column_name", "Column"), ("observed_value", "Observed"), ("note", "Limit")],
             measures=[("sum([passed])", "Passed"), ("sum([unexpected_count])", "Unexpected rows")],
             filters=[LATEST], sort_dim="layer_label", pos=(1, 37, 64, 30)),
    ]),
]

# The Command Centre's uid parts are unprefixed (its UUIDs predate Data Health).
DASHBOARDS = [
    dict(title=TITLE, pk=DASHBOARD_PK, visual_pk0=VISUAL_PK0, uid=(), sheets=SHEETS, main_ds="alerts",
         subtitle="Daily mule alert queue, rings, trends, model trust and data quality (federal CDW)"),
    dict(title=HEALTH_TITLE, pk=DASHBOARD_PK + 1, visual_pk0=VISUAL_PK0 + 200, uid=("data-health",),
         sheets=HEALTH_SHEETS, main_ds="dq",
         subtitle="Every data quality check after bronze, silver, gold and the publish (rsingh_mule_acct_ref.dq_results)"),
]

SHELVES = {
    "kpi": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("compare_shelf", 1, 2), ("label_shelf", 1, 2),
            ("tooltip_shelf", 1, 2), ("x_shelf", 1, 1), ("y_shelf", 1, 1), ("filters_shelf", 2, 3)],
    "table": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-bars": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                     ("drill_shelf", 1, 1), ("label_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-lines": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                      ("filters_shelf", 2, 3)],
}


def uid(*parts: str) -> str:
    return str(uuid.uuid5(NS, "/".join(parts)))


def column_types(ds_key: str) -> dict[str, str]:
    from mule.storage import get_storage
    view = DATASETS[ds_key][1]
    df = get_storage("impala").query(f"DESCRIBE {DB}.{view}")
    return {r["name"]: r["type"].upper() for _, r in df.iterrows()}


def is_dim(ds_key: str, col: str, typ: str) -> bool:
    return col in DATASETS[ds_key][2] or not any(t in typ for t in ("INT", "DOUBLE", "FLOAT", "DECIMAL"))


def dataset_record(key: str, pk: int, types: dict[str, str], conn_id: int, dashboards: list[int]) -> dict:
    name, view, _ = DATASETS[key]
    table = f"{DB}.{view}"
    cols = [{"alias": c, "type": t, "name": c, "isdim": is_dim(key, c, t)} for c, t in types.items()]
    return {"model": "datasets.dataset", "pk": pk, "fields": {
        "dataconnection": conn_id, "dataset_name": name, "dataset_type": "singletable", "dataset_detail": table,
        "dataset_description": f"{table} (sql/dataviz_views.sql)",
        "dataset_info": json.dumps([{"tablename": table, "columns": cols}]),
        "dataset_tablenames": json.dumps([table]), "uuid": uid("dataset", key), "imported_uuid": None,
        "cache_sequence": 0, "dataset_settings": "{}", "search_enabled": False, "dashboards": dashboards,
        "version_id": pk, "version_group_id": pk, "is_active_version": True,
        "version_name": "mule-command-centre", "is_named_version": False}}


def dim_item(col: str, alias: str, typ: str) -> dict:
    return {"dataset_colname": col, "dataset_coltype": typ, "expression_for_trigger": f"[{col}]", "col_alias": alias}


def measure_item(expr: str, alias: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "expr_hasagg": True, "col_alias": alias,
            "dataset_colname": alias, "dataset_coltype": "DOUBLE"}


def filter_item(expr: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "filter_input": {}, "filter_data": [],
            "dataset_colname": "", "dataset_coltype": "STRING", "filter_column": ""}


def visual_record(v: dict, pk: int, sheet: str, types: dict[str, str], dataset_pk: int, dash: dict) -> dict:
    kind = v["type"]
    shelves = {name: [] for name, _, _ in SHELVES[kind]}
    sources = {}

    def add_dims(shelf, pairs):
        for col, alias in pairs:
            shelves[shelf].append(dim_item(col, alias, types[col]))
            sources[f"[{col}] as 'sub:{alias}'"] = shelf

    def add_measures(shelf, pairs):
        for expr, alias in pairs:
            shelves[shelf].append(measure_item(expr, alias))
            sources[f"{expr} as 'sub:{alias}'"] = shelf

    if kind in ("kpi", "table"):
        add_dims("dimensions_shelf", v.get("dims", []))
        add_measures("aggregates_shelf", v["measures"])
    else:
        add_dims("x_shelf", v["x"])
        add_measures("y_shelf", v["measures"])
        add_dims("color_shelf", v.get("color", []))
    for expr in v.get("filters", []):
        shelves["filters_shelf"].append(filter_item(expr))
        sources[expr] = "filters_shelf"
    if v.get("sort_desc"):
        shelves["y_shelf"][0]["order"] = {"priority": 1, "ascending": False}
    if v.get("sort_dim"):
        shelf = "dimensions_shelf" if kind == "table" else "x_shelf"
        item = next(i for i in shelves[shelf] if i["dataset_colname"] == v["sort_dim"])
        item["order"] = {"priority": 1, "ascending": v.get("sort_asc", True)}
    report = {
        "report_title": v["title"], "report_subtitle": "", "dashboard_id": dash["pk"],
        "limit": v.get("limit", 1000), "sample_pct": "Off", "selected_segments": [], "report_derived_data": [],
        "click_behaviors": {}, "sort_orders_asc": {}, "user_settings": {}, **shelves,
        "core": {"viz_type": kind, "saved_shelf_sources": sources,
                 "shelves": [{"name": n, "shelf_type": s, "column_type": c} for n, s, c in SHELVES[kind]]},
    }
    return {"model": "reports.report", "pk": pk, "fields": {
        "report_name": "", "report_description": f"{dash['title']} / {sheet}", "dataset": dataset_pk,
        "workspace": 1, "report_type": kind, "report_mode": "", "dashboard_url_name": "",
        "report_data": json.dumps({"report_data": report, "report_type": kind}), "shared_visual_dashboards": None,
        "parent_report": None, "uuid": uid("visual", *dash["uid"], sheet, v["title"]), "imported_uuid": None,
        "has_css_styles": False, "report_search_text": ""}}


def widgets(pairs: list[tuple[int, tuple]]) -> list[dict]:
    return [{"col": c, "row": r, "size_x": w, "size_y": h, "id": f"uri-{i}-widget-{pk}"}
            for i, (pk, (c, r, w, h)) in enumerate(pairs, 1)]


def dashboard_record(d: dict, visuals: list[dict], ds_pk: dict[str, int], types: dict) -> dict:
    sheets, pk = [], d["visual_pk0"]
    for order, (sheet, items) in enumerate(d["sheets"], 1):
        placed = []
        for v in items:
            pk += 1
            visuals.append(visual_record(v, pk, sheet, types[v["ds"]], ds_pk[v["ds"]], d))
            placed.append((pk, v["pos"]))
        sheets.append({"sheet_id": order, "order": order, "sheet_handle_title": sheet, "behaviors": {},
                       "visual_widgets": widgets(placed), "control_widgets": []})
    dash = {"report_title": d["title"], "numColumns": 64, "report_subtitle": d["subtitle"],
            "dashboard_widgets": sheets[0]["visual_widgets"], "dashboard_sheets": sheets,
            "user_settings": {"dashboard_width": "1280", "display_filters": "true",
                              "permit_csv_download_dashboard": "true"},
            "global_control_widgets": [], "control_widgets": [], "click_behavior": {}}
    return {"model": "reports.report", "pk": d["pk"], "fields": {
        "report_name": d["title"], "report_description": "docs/DATAVIZ.md",
        "dataset": ds_pk[d["main_ds"]], "workspace": 1, "report_type": "dashboard", "report_mode": None,
        "dashboard_url_name": "", "report_data": json.dumps(dash), "shared_visual_dashboards": "[]",
        "parent_report": None, "uuid": uid("dashboard", *d["uid"]), "imported_uuid": None, "has_css_styles": False,
        "report_search_text": None}}


def build(conn_id: int, version: dict) -> dict:
    ds_pk = {k: DATASET_PK0 + i for i, k in enumerate(DATASETS)}
    types = {k: column_types(k) for k in DATASETS}
    visuals = []
    dashboards = [dashboard_record(d, visuals, ds_pk, types) for d in DASHBOARDS]
    used_by = {k: [d["pk"] for d in DASHBOARDS if any(v["ds"] == k for _, items in d["sheets"] for v in items)]
               for k in DATASETS}
    return {"segments": [], "staticasset": [], "dashboards": dashboards, "appgroupmembership": [],
            "reportannotation": [], "events": [], "customcss": [], "reportimage": [], "dateranges": [],
            "visuals": visuals, "colorpalette": [], "appgroups": [],
            "datasets": [dataset_record(k, ds_pk[k], types[k], conn_id, used_by[k]) for k in DATASETS],
            "version": version}


def impala_sql(v: dict) -> str:
    """A KPI tile's number as plain Impala SQL on its view ([col] -> col)."""
    strip = lambda e: re.sub(r"\[(\w+)\]", r"\1", e)  # noqa: E731
    where = " AND ".join(strip(f) for f in v.get("filters", [])) or "TRUE"
    return f"SELECT {strip(v['measures'][0][0])} FROM {DB}.{DATASETS[v['ds']][1]} WHERE {where}"


class DataViz:
    """The application behind the CAI proxy, authenticated with the CAI API key."""

    def __init__(self):
        host = os.environ["MULE_CAI_HOST"].split("://", 1)[-1].split("/", 1)[0]
        self.url = f"https://{DATAVIZ['subdomain']}.{host}"
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {os.environ['MULE_CAI_API_KEY']}"

    def get(self, path: str, **params):
        r = self.s.get(self.url + path, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def connection(self) -> int:
        from mule.storage import get_storage
        found = next((c for c in self.get("/arc/adminapi/v1/connections") if c["name"] == CONNECTION), None)
        if found:
            print(f"connection {CONNECTION}: exists ({found['id']})")
            return found["id"]
        params = {"HOST": get_storage("impala").cfg["host"], "PORT": "443", "USERNAME": os.environ["MULE_IMPALA_USER"],
                  "MODE": "http", "HS2_HTTP_SQLPATH": "cliservice", "SOCK": "ssl", "AUTH": "ldap",
                  "SOCKET_TIMEOUT": 600, "IMPERSONATION": False, "APP_NAME": "viz", "CONCURRENCY": 100,
                  "CONCURRENCY_USER": 5, "QUERY_TIMEOUT": 120, "QUERY_LOADING_WARNING_SECONDS": 20,
                  "CACHE": {"ENABLED": 1, "RETENTION": 1800}}
        body = {"name": CONNECTION, "type": "impyla", "info": {"PARAMS": params},
                "password": os.environ["MULE_IMPALA_PASSWORD"]}
        r = self.s.post(self.url + "/arc/adminapi/v1/connections", data={"data": json.dumps([body])}, timeout=60)
        if r.status_code != 200:
            raise SystemExit(f"connection {CONNECTION}: HTTP {r.status_code}")
        conn_id = r.json()[0]["id"]
        print(f"connection {CONNECTION}: created ({conn_id})")
        return conn_id

    def version(self) -> dict:
        return self.get("/arc/migration/api/export/", dashboards="[]", filename="version", dry_run="False")["version"]

    def verify(self) -> int:
        """Every visual's query through the Data API (Data Visualization -> its connection -> Impala);
        each KPI tile's number is also computed directly in Impala and compared."""
        from mule.storage import get_storage
        impala = get_storage("impala")
        ids = {d["name"]: d["id"] for d in self.get("/arc/adminapi/v1/datasets")}
        failed = 0
        for d in DASHBOARDS:
            for sheet, items in d["sheets"]:
                for v in items:
                    failed += not self.verify_visual(v, f"{d['title']} / {sheet}", ids, impala)
        return failed

    def verify_visual(self, v: dict, where: str, ids: dict, impala) -> bool:
        dims = v.get("dims", []) + v.get("x", []) + v.get("color", [])
        dsreq = {"version": 1, "type": "SQL", "limit": v.get("limit", 1000),
                 "dimensions": [{"type": "SIMPLE", "expr": f"[{c}] as '{a}'"} for c, a in dims],
                 "aggregates": [{"expr": f"{e} as '{a}'"} for e, a in v["measures"]],
                 "filters": v.get("filters", []), "dataset_id": ids[DATASETS[v["ds"]][0]]}
        r = self.s.post(self.url + "/arc/api/data", data={"version": 1, "dsreq": json.dumps(dsreq)}, timeout=300)
        rows = json.loads(r.json()["rows"]) if r.status_code == 200 else None
        ok = bool(rows) or (rows is not None and v.get("may_be_empty", False))
        note = f"{len(rows)} rows, first {rows[0] if rows else None}" if rows is not None else \
            f"HTTP {r.status_code} {r.text[:200]}"
        if ok and v["type"] == "kpi":
            want = impala.query(impala_sql(v)).iloc[0, 0]
            got = rows[0][-1] if isinstance(rows[0], list) else list(rows[0].values())[-1]
            ok = abs(float(got) - float(want)) < 1e-6
            note = f"{got} (Impala {want})"
        print(f"{'ok  ' if ok else 'FAIL'} {where} / {v['title']}: {note}")
        return ok

    def import_file(self, path: Path) -> None:
        with path.open("rb") as f:
            r = self.s.post(self.url + "/arc/migration/api/import/", files={"import_file": f},
                            data={"dry_run": "False", "dataconnection_name": CONNECTION}, timeout=300)
        print(f"import: HTTP {r.status_code} {r.text[:500]}")
        if r.status_code != 200:
            raise SystemExit(1)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--no-import", action="store_true", help="write the export file only")
    p.add_argument("--verify", action="store_true", help="only run every visual's query through the Data API")
    args = p.parse_args()
    viz = DataViz()
    if args.verify:
        return 1 if viz.verify() else 0
    conn_id = viz.connection()
    doc = build(conn_id, viz.version())
    OUT.write_text(json.dumps(doc, indent=1) + "\n")
    print(f"wrote {OUT.relative_to(ROOT)}: {len(doc['dashboards'])} dashboards, {len(doc['datasets'])} datasets, "
          f"{len(doc['visuals'])} visuals, {sum(len(d['sheets']) for d in DASHBOARDS)} sheets")
    if not args.no_import:
        viz.import_file(OUT)
        print(f"open {viz.url}/arc/apps/ -> Dashboards -> {TITLE} / {HEALTH_TITLE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

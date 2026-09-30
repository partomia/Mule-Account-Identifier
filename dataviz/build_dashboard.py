#!/usr/bin/env python3
"""
The "Mule Investigation Command Centre" dashboard in Cloudera Data Visualization, as code.

Datasets, visuals and sheets are declared below; this script turns them into a
Data Visualization export file (dataviz/mule_command_centre.json) and imports it
through the migration REST API of the CAI application "Mule Data Visualization"
(ci/cai_jobs.py DATAVIZ). UUIDs are fixed per artefact, so an import updates the
dashboard in place. Datasets read the rsingh_mule_acct_report views
(sql/dataviz_views.sql); see docs/DATAVIZ.md.

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

OUT = ROOT / "dataviz" / "mule_command_centre.json"
TITLE = "Mule Investigation Command Centre"
DB = "rsingh_mule_acct_report"
CONNECTION = "rsingh-mule-acct-impala"
NS = uuid.UUID("78b6c29c-4f37-40d2-81ec-cdeef2987950")
DASHBOARD_PK = 7000
DATASET_PK0 = 7100
VISUAL_PK0 = 7200

DATASETS = {                          # key: (name, view, integer columns that are dimensions)
    "alerts": ("Mule - Alerts", "v_alerts", {"is_latest", "risk_rank", "ring_size", "hops_to_known_mule"}),
    "reasons": ("Mule - Alert reasons", "v_alert_reasons", {"is_latest"}),
    "rings": ("Mule - Rings", "v_rings", {"is_latest", "ring_rank", "min_hops_to_known_mule"}),
    "book": ("Mule - Book weekly", "v_book_weekly", set()),
    "runs": ("Mule - Model runs", "v_model_run", {"is_latest", "gate_passed", "alerts_published"}),
    "holdout": ("Mule - Holdout", "v_holdout", {"is_latest"}),
    "dq": ("Mule - Data quality", "v_dq", set()),
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
        dict(type="kpi", ds="dq", title="Checks", measures=[("sum(1)", "Checks")], pos=(1, 1, 21, 10)),
        dict(type="kpi", ds="dq", title="Failed checks", measures=[("sum([failed])", "Failed")], pos=(22, 1, 21, 10)),
        dict(type="kpi", ds="dq", title="Critical failures",
             measures=[("sum(case when [severity] = 'critical' then [failed] else 0 end)", "Critical failures")],
             pos=(43, 1, 22, 10)),
        dict(type="trellis-bars", ds="dq", title="Checks passed and failed by layer",
             x=[("layer", "Layer")], measures=[("sum([passed])", "Passed"), ("sum([failed])", "Failed")],
             pos=(1, 11, 24, 22)),
        dict(type="table", ds="dq", title="Failed checks (empty on a healthy pipeline)",
             dims=[("layer", "Layer"), ("table_name", "Table"), ("check_name", "Check"), ("severity", "Severity")],
             measures=[("sum([expected])", "Expected"), ("sum([actual])", "Actual"), ("sum([diff])", "Diff")],
             filters=["[failed] = 1"], pos=(25, 11, 40, 22)),
        dict(type="table", ds="dq", title="Every check: reconciliation against source counts and integrity",
             dims=[("layer", "Layer"), ("table_name", "Table"), ("check_name", "Check"), ("severity", "Severity"),
                   ("rule", "Rule"), ("note", "Note")],
             measures=[("sum([expected])", "Expected"), ("sum([actual])", "Actual"), ("sum([passed])", "Passed")],
             sort_dim="layer", pos=(1, 33, 64, 26)),
    ]),
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


def dataset_record(key: str, pk: int, types: dict[str, str], conn_id: int) -> dict:
    name, view, _ = DATASETS[key]
    table = f"{DB}.{view}"
    cols = [{"alias": c, "type": t, "name": c, "isdim": is_dim(key, c, t)} for c, t in types.items()]
    return {"model": "datasets.dataset", "pk": pk, "fields": {
        "dataconnection": conn_id, "dataset_name": name, "dataset_type": "singletable", "dataset_detail": table,
        "dataset_description": f"{table} (sql/dataviz_views.sql)",
        "dataset_info": json.dumps([{"tablename": table, "columns": cols}]),
        "dataset_tablenames": json.dumps([table]), "uuid": uid("dataset", key), "imported_uuid": None,
        "cache_sequence": 0, "dataset_settings": "{}", "search_enabled": False, "dashboards": [DASHBOARD_PK],
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


def visual_record(v: dict, pk: int, sheet: str, types: dict[str, str], dataset_pk: int) -> dict:
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
        "report_title": v["title"], "report_subtitle": "", "dashboard_id": DASHBOARD_PK,
        "limit": v.get("limit", 1000), "sample_pct": "Off", "selected_segments": [], "report_derived_data": [],
        "click_behaviors": {}, "sort_orders_asc": {}, "user_settings": {}, **shelves,
        "core": {"viz_type": kind, "saved_shelf_sources": sources,
                 "shelves": [{"name": n, "shelf_type": s, "column_type": c} for n, s, c in SHELVES[kind]]},
    }
    return {"model": "reports.report", "pk": pk, "fields": {
        "report_name": "", "report_description": f"{TITLE} / {sheet}", "dataset": dataset_pk,
        "workspace": 1, "report_type": kind, "report_mode": "", "dashboard_url_name": "",
        "report_data": json.dumps({"report_data": report, "report_type": kind}), "shared_visual_dashboards": None,
        "parent_report": None, "uuid": uid("visual", sheet, v["title"]), "imported_uuid": None,
        "has_css_styles": False, "report_search_text": ""}}


def widgets(pairs: list[tuple[int, tuple]]) -> list[dict]:
    return [{"col": c, "row": r, "size_x": w, "size_y": h, "id": f"uri-{i}-widget-{pk}"}
            for i, (pk, (c, r, w, h)) in enumerate(pairs, 1)]


def build(conn_id: int, version: dict) -> dict:
    ds_pk = {k: DATASET_PK0 + i for i, k in enumerate(DATASETS)}
    types = {k: column_types(k) for k in DATASETS}
    visuals, sheets, pk = [], [], VISUAL_PK0
    for order, (sheet, items) in enumerate(SHEETS, 1):
        placed = []
        for v in items:
            pk += 1
            visuals.append(visual_record(v, pk, sheet, types[v["ds"]], ds_pk[v["ds"]]))
            placed.append((pk, v["pos"]))
        sheets.append({"sheet_id": order, "order": order, "sheet_handle_title": sheet, "behaviors": {},
                       "visual_widgets": widgets(placed), "control_widgets": []})
    dash = {"report_title": TITLE, "numColumns": 64,
            "report_subtitle": "Daily mule alert queue, rings, trends, model trust and data quality (federal CDW)",
            "dashboard_widgets": sheets[0]["visual_widgets"], "dashboard_sheets": sheets,
            "user_settings": {"dashboard_width": "1280", "display_filters": "true",
                              "permit_csv_download_dashboard": "true"},
            "global_control_widgets": [], "control_widgets": [], "click_behavior": {}}
    dashboard = {"model": "reports.report", "pk": DASHBOARD_PK, "fields": {
        "report_name": TITLE, "report_description": "docs/DATAVIZ.md",
        "dataset": ds_pk["alerts"], "workspace": 1, "report_type": "dashboard", "report_mode": None,
        "dashboard_url_name": "", "report_data": json.dumps(dash), "shared_visual_dashboards": "[]",
        "parent_report": None, "uuid": uid("dashboard"), "imported_uuid": None, "has_css_styles": False,
        "report_search_text": None}}
    return {"segments": [], "staticasset": [], "dashboards": [dashboard], "appgroupmembership": [],
            "reportannotation": [], "events": [], "customcss": [], "reportimage": [], "dateranges": [],
            "visuals": visuals, "colorpalette": [], "appgroups": [],
            "datasets": [dataset_record(k, ds_pk[k], types[k], conn_id) for k in DATASETS], "version": version}


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
        for sheet, items in SHEETS:
            for v in items:
                dims = v.get("dims", []) + v.get("x", []) + v.get("color", [])
                dsreq = {"version": 1, "type": "SQL", "limit": v.get("limit", 1000),
                         "dimensions": [{"type": "SIMPLE", "expr": f"[{c}] as '{a}'"} for c, a in dims],
                         "aggregates": [{"expr": f"{e} as '{a}'"} for e, a in v["measures"]],
                         "filters": v.get("filters", []), "dataset_id": ids[DATASETS[v["ds"]][0]]}
                r = self.s.post(self.url + "/arc/api/data", data={"version": 1, "dsreq": json.dumps(dsreq)},
                                timeout=300)
                rows = json.loads(r.json()["rows"]) if r.status_code == 200 else None
                ok = bool(rows) or (rows is not None and "[failed] = 1" in v.get("filters", []))
                note = f"{len(rows)} rows, first {rows[0] if rows else None}" if rows is not None else \
                    f"HTTP {r.status_code} {r.text[:200]}"
                if ok and v["type"] == "kpi":
                    want = impala.query(impala_sql(v)).iloc[0, 0]
                    got = rows[0][-1] if isinstance(rows[0], list) else list(rows[0].values())[-1]
                    ok = abs(float(got) - float(want)) < 1e-6
                    note = f"{got} (Impala {want})"
                failed += not ok
                print(f"{'ok  ' if ok else 'FAIL'} {sheet} / {v['title']}: {note}")
        return failed

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
    print(f"wrote {OUT.relative_to(ROOT)}: {len(doc['datasets'])} datasets, {len(doc['visuals'])} visuals, "
          f"{len(SHEETS)} sheets")
    if not args.no_import:
        viz.import_file(OUT)
        print(f"open {viz.url}/arc/apps/ -> Dashboards -> {TITLE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

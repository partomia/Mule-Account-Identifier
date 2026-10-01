"""The Data Visualization dashboards are generated from one declaration, read only the
report views, and their export file carries no credentials."""

import json
import re

from ci.cai_jobs import DATAVIZ
from tests.conftest import ROOT, _load

BUILD = _load(ROOT / "dataviz" / "build_dashboard.py")
VIEWS_SQL = (ROOT / "sql" / "dataviz_views.sql").read_text()
EXPORT = json.loads((ROOT / "dataviz" / "mule_dashboards.json").read_text())
RUNNER = _load(ROOT / "scripts" / "run_impala_sql.py")
VISUALS = [(d["title"], sheet, v) for d in BUILD.DASHBOARDS for sheet, items in d["sheets"] for v in items]


def test_every_dataset_is_a_report_view_recreated_by_drop_and_create():
    created = re.findall(r"CREATE VIEW rsingh_mule_acct_report\.(\w+) AS", VIEWS_SQL)
    dropped = re.findall(r"DROP VIEW IF EXISTS rsingh_mule_acct_report\.(\w+);", VIEWS_SQL)
    assert created == dropped and not re.search(r"^\s*CREATE OR REPLACE", VIEWS_SQL, re.M | re.I)
    assert {view for _, view, _ in BUILD.DATASETS.values()} == set(created)
    assert {d["fields"]["dataset_detail"] for d in EXPORT["datasets"]} == {f"{BUILD.DB}.{v}" for v in created}


def test_the_export_file_matches_the_declaration():
    assert [d["fields"]["report_name"] for d in EXPORT["dashboards"]] == [d["title"] for d in BUILD.DASHBOARDS]
    assert len(EXPORT["visuals"]) == len(VISUALS)
    for dash, d in zip(EXPORT["dashboards"], BUILD.DASHBOARDS):
        data = json.loads(dash["fields"]["report_data"])
        assert [s["sheet_handle_title"] for s in data["dashboard_sheets"]] == [s for s, _ in d["sheets"]]
        placed = {int(w["id"].rsplit("-", 1)[1]) for s in data["dashboard_sheets"] for w in s["visual_widgets"]}
        mine = {v["pk"] for v in EXPORT["visuals"] if json.loads(v["fields"]["report_data"])["report_data"]
                ["dashboard_id"] == dash["pk"]}
        assert placed == mine and len(mine) == sum(len(items) for _, items in d["sheets"])
    uuids = [a["fields"]["uuid"] for k in ("datasets", "visuals", "dashboards") for a in EXPORT[k]]
    pks = [a["pk"] for k in ("visuals", "dashboards") for a in EXPORT[k]]
    assert len(set(uuids)) == len(uuids) and len(set(pks)) == len(pks)
    used = {v["ds"] for _, _, v in VISUALS}
    assert all(d["fields"]["dashboards"] for d in EXPORT["datasets"]) and used == set(BUILD.DATASETS)


def test_the_command_centre_keeps_its_uuids_and_keys():
    first = BUILD.DASHBOARDS[0]
    assert first["title"] == BUILD.TITLE and first["uid"] == () and first["sheets"] is BUILD.SHEETS
    dash = EXPORT["dashboards"][0]
    assert (dash["pk"], dash["fields"]["uuid"]) == (7000, "b12c41dd-296d-5bc8-8270-2e98ef07b74b")
    cc = [(sheet, v) for sheet, items in BUILD.SHEETS for v in items]
    assert len(cc) == 34
    for (sheet, v), rec in zip(cc, EXPORT["visuals"][:34]):
        assert rec["fields"]["uuid"] == BUILD.uid("visual", sheet, v["title"])
    assert [r["pk"] for r in EXPORT["visuals"][:34]] == list(range(7201, 7235))
    assert EXPORT["visuals"][0]["fields"]["uuid"] == "bbc7450f-362f-59dc-9f00-9ea604fb7be1"
    assert EXPORT["visuals"][33]["fields"]["uuid"] == "9fba3343-e35d-5042-85ca-b2525822c375"
    assert [d["pk"] for d in EXPORT["datasets"][:7]] == list(range(7100, 7107))


def test_visuals_only_use_columns_of_their_dataset():
    for title, sheet, v in VISUALS:
        view = BUILD.DATASETS[v["ds"]][1]
        body = VIEWS_SQL.split(f"CREATE VIEW rsingh_mule_acct_report.{view} AS", 1)[1].split(";", 1)[0]
        used = {c for c, _ in v.get("dims", []) + v.get("x", []) + v.get("color", [])}
        used |= set(re.findall(r"\[(\w+)\]", " ".join([e for e, _ in v["measures"]] + v.get("filters", []))))
        missing = {c for c in used if not re.search(rf"\b{c}\b", body)}
        assert not missing, (title, sheet, v["title"], missing)


def test_sheets_fit_the_grid_without_overlap():
    sheets = [(d["title"], sheet, items) for d in BUILD.DASHBOARDS for sheet, items in d["sheets"]]
    for title, sheet, items in sheets:
        cells = set()
        for v in items:
            c, r, w, h = v["pos"]
            assert c >= 1 and c + w - 1 <= 64, (title, sheet, v["title"])
            box = {(x, y) for x in range(c, c + w) for y in range(r, r + h)}
            assert not cells & box, (title, sheet, v["title"])
            cells |= box


def test_kpi_tiles_translate_to_impala_sql():
    kpi = next(v for _, items in BUILD.SHEETS for v in items if v["title"] == "Freeze reviews (T1)")
    assert BUILD.impala_sql(kpi) == (
        "SELECT sum(case when tier = 'T1_FREEZE_REVIEW' then 1 else 0 end) "
        "FROM rsingh_mule_acct_report.v_alerts WHERE is_latest = 1")


def test_names_are_prefixed_and_the_runner_splits_statements():
    assert DATAVIZ["subdomain"].startswith("rsingh-mule-acct") and BUILD.CONNECTION.startswith("rsingh-mule-acct")
    assert BUILD.DB.startswith("rsingh_mule_acct")
    assert RUNNER.statements("-- c\nSELECT 1;\nSELECT\n2;\n") == ["SELECT 1", "SELECT\n2;"]


def test_no_credentials_in_the_export_and_nothing_in_the_pipeline_reads_the_views():
    text = json.dumps(EXPORT).lower()
    assert not any(w in text for w in ("password", "apikey", "bearer"))
    users = [p for p in [*ROOT.glob("cde/**/*.py"), *ROOT.glob("cai/**/*.py"), *ROOT.glob("mule/**/*.py"),
                         *ROOT.glob("app/**/*.py")] if "mule_acct_report" in p.read_text()]
    assert users == []

"""Data quality checks (cde/jobs/dq_check.py): the rules, the result rows and the DAG wiring."""
import re
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
JOBS = ROOT / "cde" / "jobs"
sys.path.insert(0, str(JOBS))
import dq_check as dq  # noqa: E402
import validate_bronze  # noqa: E402

DAG = (ROOT / "cde" / "dags" / "mule_dag.py").read_text()
RESERVED = {w.strip().lower() for w in (ROOT / "tests" / "data" / "impala_reserved_words.txt").read_text().splitlines()
            if w.strip() and not w.startswith("#")}
# the churn project's dq_results columns, in order: one Data Health layout for both projects
CHURN_COLUMNS = ["pipeline_run", "run_id", "run_ts", "as_of", "layer", "table_name", "check_name",
                 "expectation_type", "column_name", "severity", "success", "observed_value", "element_count",
                 "unexpected_count", "unexpected_pct", "kwargs", "table_snapshot_id", "gx_version"]


def schema_columns():
    return [c.strip().split()[0] for c in dq.RESULT_SCHEMA.split(",")]


def test_result_schema_matches_churn_and_avoids_reserved_words():
    assert schema_columns() == CHURN_COLUMNS
    assert not set(CHURN_COLUMNS) & RESERVED


def test_rate_tiers_warn_at_half_the_limit_before_failing():
    crit, warn = dq.rate_tiers("txn", "bad amount", rows=10_000, bad=7, limit=0.001)
    assert (crit.severity, crit.success) == (dq.CRITICAL, True)
    assert (warn.severity, warn.success) == (dq.WARNING, False)
    assert warn.unexpected_count == 7 and warn.unexpected_pct == pytest.approx(0.07)
    crit, warn = dq.rate_tiers("txn", "bad amount", rows=10_000, bad=11, limit=0.001)
    assert not crit.success and dq.failed_critical([crit, warn]) == [crit]


def test_row_count_equal_and_range_rules():
    assert not dq.row_count("txn", 0).success
    assert dq.row_count("txn", 5).observed_value == "5"
    assert dq.equal("txn", "rows", 10, 10).success and not dq.equal("txn", "rows", 10, 9).success
    r = dq.in_range("mule_features", "hops_to_known_mule", rows=100, bad=0, lo=1, hi=3)
    assert r.success and r.expectation_type == "range"


def test_volume_change_is_a_warning_and_needs_a_previous_load():
    assert dq.volume_change("txn", 100, None, (0.95, 1.10)) == []
    [r] = dq.volume_change("txn", 120, 100, (0.95, 1.10))
    assert (r.severity, r.success, r.observed_value) == (dq.WARNING, False, "1.2000")
    assert dq.failed_critical([r]) == []
    [r] = dq.volume_change("txn", 101, 100, (0.95, 1.10))
    assert r.success


def test_every_expectation_type_is_a_declared_rule():
    made = [dq.row_count("t", 1), dq.not_null("t", "c", 1, 0), dq.unique("t", ["c"], 1, 1),
            dq.in_set("t", "c", 1, 0, [0, 1]), dq.in_range("t", "c", 1, 0, 0, 1),
            dq.not_after("t", "c", 1, 0, date(2026, 9, 29)), dq.equal("t", "n", 1, 1),
            dq.rate_at_most("t", "n", 1, 0, 0.1), *dq.volume_change("t", 1, 1, (0.9, 1.1))]
    assert {r.expectation_type for r in made} == set(dq.RULES)


def test_records_fill_the_run_columns():
    r = dq.not_null("account", "account_id", 50, 0)
    r.table = "rsingh_mule_acct_silver.account"
    [row] = dq.records([r], {"rsingh_mule_acct_silver.account": 42}, pipeline_run="manual-2026-09-29",
                       run_id="silver-20260929-abcd1234", run_ts=datetime(2026, 9, 30, 1, 0),
                       as_of=date(2026, 9, 29), layer="silver")
    assert set(row) == set(CHURN_COLUMNS)
    assert row["table_snapshot_id"] == 42 and row["gx_version"] is None and row["success"] is True


def test_parse_args_defaults_and_layers():
    a = dq.parse_args(["--layer", "gold"])
    assert (a.layer, a.as_of, a.pipeline_run, a.db_prefix) == ("gold", None, None, "rsingh_mule_acct")
    assert dq.LAYERS == ("bronze", "silver", "gold", "publish")
    with pytest.raises(SystemExit):
        dq.parse_args(["--layer", "report"])


def test_validate_bronze_runs_the_bronze_layer(monkeypatch):
    seen = []
    monkeypatch.setattr(validate_bronze.dq_check, "main", lambda argv, spark=None: seen.append(argv))
    validate_bronze.main(["--db-prefix", "rsingh_mule_acct", "--pipeline-run", "r1"])
    assert seen == [["--layer", "bronze", "--db-prefix", "rsingh_mule_acct", "--pipeline-run", "r1"]]


def test_dag_gates_every_layer_with_the_airflow_run_id():
    for task, layer in (("dq_silver", "silver"), ("dq_gold", "gold"), ("dq_publish", "publish")):
        assert f'dq_task("{task}", "{layer}")' in DAG
    assert '"--pipeline-run", RUN' in DAG and 'RUN = "{{ run_id }}"' in DAG
    chain = re.search(r"^\s*(generate >>.*)$", DAG, re.M).group(1).replace(" ", "").split(">>")
    assert chain == ["generate", "validate", "silver", "graph", "dq_silver", "gold", "dq_gold", "sync", "score",
                     "dq_publish"]


def test_dq_writers_never_read_the_report_views():
    for f in ("dq_check.py", "validate_bronze.py"):
        src = (JOBS / f).read_text()
        assert not re.search(r"mule_acct_report|\{db\}_report\b|v_dq", src), f
    assert '{db}_ref.dq_results' in (JOBS / "dq_check.py").read_text()

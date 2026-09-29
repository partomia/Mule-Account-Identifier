"""CAI setup (ci/), sync-code job, Airflow Variables and the DAG's contract with them."""
import ast
import re
import urllib.error
from pathlib import Path

import pytest

from cai.jobs import sync_code
from cde.scripts import set_airflow_variables as av
from ci import setup_cai
from ci.cai_jobs import BY_NAME, JOB_VARIABLES, JOBS, MODEL, SYNC_JOB
from mule.storage import TABLE_COLUMNS, ImpalaStorage

ROOT = Path(__file__).resolve().parents[1]
DAG = (ROOT / "cde" / "dags" / "mule_dag.py").read_text()
RESERVED = {w.strip().lower() for w in (ROOT / "tests" / "data" / "impala_reserved_words.txt").read_text().splitlines()
            if w.strip() and not w.startswith("#")}


class FakeWorkbench:
    base = "https://cai.example.site/api/v2"

    def __init__(self, jobs=()):
        self.jobs = {j["name"]: dict(j) for j in jobs}
        self.calls = []

    def __call__(self, method, path, body=None, params=None):
        self.calls.append((method, path, body))
        if method == "GET" and path.endswith("/jobs"):
            return {"jobs": list(self.jobs.values())}
        if method == "POST" and path.endswith("/jobs"):
            job = {**body, "id": f"id-{body['name']}"}
            self.jobs[body["name"]] = job
            return job
        return {}


def test_names_carry_the_project_prefix():
    assert all(j["name"].startswith("rsingh-mule-acct-") for j in JOBS)
    assert MODEL["name"].startswith("rsingh-mule-acct-")
    assert set(JOB_VARIABLES.values()) <= set(BY_NAME)


def test_pip_installing_job_has_at_least_8_gb():
    assert BY_NAME[SYNC_JOB]["memory"] >= 8


def test_ensure_jobs_creates_missing_and_resizes_existing():
    small = {**BY_NAME[SYNC_JOB], "id": "sync-1", "memory": 2}
    wb = FakeWorkbench([small])
    ids = setup_cai.ensure_jobs(wb, {"id": "p1"}, dry_run=False)
    assert ids[SYNC_JOB] == "sync-1"
    assert ("PATCH", "/projects/p1/jobs/sync-1", {"cpu": small["cpu"], "memory": BY_NAME[SYNC_JOB]["memory"]}) \
        in wb.calls
    created = [b for m, p, b in wb.calls if m == "POST"]
    assert [b["name"] for b in created] == [j["name"] for j in JOBS if j["name"] != SYNC_JOB]
    assert all(b["arguments"] == "" and b["runtime_identifier"] == setup_cai.RUNTIME for b in created)


def test_ensure_jobs_dry_run_writes_nothing():
    wb = FakeWorkbench()
    setup_cai.ensure_jobs(wb, {"id": "p1"}, dry_run=True)
    assert all(m == "GET" for m, _, _ in wb.calls)


def test_install_requirements_only_when_the_file_changes(tmp_path):
    (tmp_path / "requirements.txt").write_text("pandas\n")
    runs = []
    assert sync_code.install_requirements(tmp_path, pip=lambda: runs.append(1)) is True
    assert sync_code.install_requirements(tmp_path, pip=lambda: runs.append(1)) is False
    (tmp_path / "requirements.txt").write_text("pandas\nnumpy\n")
    assert sync_code.install_requirements(tmp_path, pip=lambda: runs.append(1)) is True
    assert len(runs) == 2


class FakeAirflow:
    def __init__(self, existing):
        self.vars = dict(existing)
        self.calls = []

    def __call__(self, method, path, body=None):
        self.calls.append((method, path))
        key = path.rsplit("/", 1)[-1]
        if method == "GET":
            if key not in self.vars:
                raise urllib.error.HTTPError(path, 404, "not found", {}, None)
            return {"value": self.vars[key]}
        self.vars[body["key"]] = body["value"]
        return {}


def test_airflow_variables_only_touch_own_keys():
    fake = FakeAirflow({"MULE_CAI_HOST": "https://old", "MULE_CAI_API_KEY": "k", "OTHER_PROJECT_KEY": "x"})
    av.apply(fake, {"MULE_CAI_HOST": "https://new", "MULE_CAI_API_KEY": "k", "MULE_CAI_JOB_ID": "j"}, dry_run=False)
    assert fake.vars == {"MULE_CAI_HOST": "https://new", "MULE_CAI_API_KEY": "k", "MULE_CAI_JOB_ID": "j",
                         "OTHER_PROJECT_KEY": "x"}
    assert not any("OTHER_PROJECT_KEY" in p for _, p in fake.calls)
    with pytest.raises(ValueError):
        av.apply(fake, {"SPEND_CAI_HOST": "x"}, dry_run=False)


def test_dag_reads_every_job_variable_and_registers_paused():
    for var in JOB_VARIABLES:
        assert f'"{var}"' in DAG
    assert "is_paused_upon_creation=True" in DAG
    assert DAG.index("sync >> score") > 0


def test_dag_spark_job_names_match_deploy_script():
    script = (ROOT / "cde" / "scripts" / "deploy_jobs.sh").read_text()
    deployed = set(re.findall(r'create_job "\$\{JOB_PREFIX\}-([a-z-]+)"', script))
    assert set(re.findall(r'job_name=f"\{JOB_PREFIX\}-([a-z-]+)"', DAG)) == deployed and len(deployed) == 5


@pytest.mark.parametrize("script", sorted((ROOT / "cai" / "jobs").glob("*.py")) + [ROOT / "cai/model/predict.py"])
def test_cai_scripts_survive_the_jupyter_job_wrapper(script):
    tree = ast.parse(script.read_text())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)]
    assert not any(c.func.attr == "parse_args" for c in calls), "use parse_known_args"
    for c in calls:
        if c.func.attr == "exit" and c.args and isinstance(c.args[0], ast.Constant):
            assert c.args[0].value != 0, "sys.exit(0) is reported as a failure"
    module_level = [n for n in tree.body if not isinstance(n, (ast.FunctionDef, ast.ClassDef))]
    assert not any(isinstance(n, ast.Name) and n.id == "__file__" for m in module_level for n in ast.walk(m))


def test_output_columns_avoid_impala_reserved_words_and_ddl_quotes_them():
    assert not {c.lower() for cols in TABLE_COLUMNS.values() for c, _ in cols} & RESERVED
    s = ImpalaStorage({})
    sql = []
    s.execute = sql.append
    s.ensure_table("mule_model_run")
    create = next(q for q in sql if q.startswith("CREATE TABLE"))
    assert all(f"`{c}`" in create for c, _ in TABLE_COLUMNS["mule_model_run"])

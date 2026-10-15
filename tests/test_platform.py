"""scripts/platform_ctl.py and config/platform.json, offline: the shape, the guards, and that every
action stays inside this project's names (fake CAI API and cde CLI)."""
import ast
import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("platform_ctl", REPO / "scripts" / "platform_ctl.py")
plat = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(plat)


def test_no_script_shadows_the_standard_library():
    # `python scripts/x.py` puts scripts/ first on sys.path: a scripts/platform.py breaks `import uuid`
    clash = {p.stem for p in (REPO / "scripts").glob("*.py")} & set(sys.stdlib_module_names)
    assert not clash, f"rename {sorted(clash)}"


def test_config_layers_and_orders():
    cfg = json.loads((REPO / "config" / "platform.json").read_text())
    assert sorted(cfg["stop_order"]) == sorted(cfg["start_order"]) == ["cai", "cde", "cdw"]
    assert cfg["stop_order"] == cfg["start_order"][::-1]
    assert set(plat.PROJECT) | set(plat.SHARED) == set(cfg["stop_order"])
    p = cfg["project"]
    assert p["cde_job_prefix"] == "rsingh-mule-acct-"
    assert all(j.startswith(p["cde_job_prefix"]) for j in p["cde_dag_jobs"])
    assert all(j.startswith(tuple(p["cai_job_prefixes"])) for j in p["cai_scheduled_jobs"])
    assert p["cai_models"][0].startswith(p["cai_job_prefixes"][0])


def test_config_names_match_the_deploy_code():
    from ci import cai_jobs
    p = json.loads((REPO / "config" / "platform.json").read_text())["project"]
    assert p["cai_project"] == cai_jobs.CAI_PROJECT_NAME
    assert set(p["cai_apps"]) == {cai_jobs.APP["name"], cai_jobs.DATAVIZ["name"]}
    assert p["cai_models"] == [cai_jobs.MODEL["name"]]
    assert all(j["name"].startswith(tuple(p["cai_job_prefixes"])) for j in cai_jobs.JOBS)
    assert f'DAG_JOB_NAME:-{p["cde_dag_jobs"][0]}' in (REPO / "cde" / "scripts" / "deploy_dag.sh").read_text()


def test_every_dag_has_run_guards():
    for dag in (REPO / "cde" / "dags").glob("*.py"):
        tree = ast.parse(dag.read_text())
        dag_kw = {k.arg: k.value for n in ast.walk(tree)
                  if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "DAG" for k in n.keywords}
        assert {"max_active_runs", "dagrun_timeout"} <= set(dag_kw), dag.name
        assert ast.literal_eval(dag_kw["max_active_runs"]) == 1, dag.name
        assert '"execution_timeout"' in dag.read_text(), f"{dag.name}: no per-task execution_timeout in default_args"


def test_platform_scope_needs_consent_and_layers_are_checked():
    with pytest.raises(SystemExit, match="--yes-shared"):
        plat.main(["stop", "--scope", "platform"])
    with pytest.raises(SystemExit, match="unknown layers"):
        plat.main(["status", "--only", "nifi"])


class FakeCai(plat.Cai):
    """The real filters (apps, jobs, deployments) over canned API answers."""

    def __init__(self, app_status="APPLICATION_RUNNING", dep_status="deployed"):
        self.pid, self.calls = "P", []
        self.app_status, self.dep_status = app_status, dep_status

    def __call__(self, method, path, **kw):
        self.calls.append((method, path, kw.get("json")))
        if path.endswith("/applications"):
            return {"applications": [
                {"id": "a1", "name": "Mule Investigator Console", "status": self.app_status, "cpu": 2, "memory": 4},
                {"id": "a2", "name": "Mule Data Visualization", "status": "APPLICATION_FAILED", "cpu": 2, "memory": 8},
                {"id": "a3", "name": "Someone else", "status": "APPLICATION_RUNNING"}]}
        if path.endswith("/jobs"):
            return {"jobs": [{"id": "js", "name": "rsingh-mule-acct-daily-score", "schedule": "", "paused": False},
                             {"id": "jx", "name": "rsingh-mule-other-job", "schedule": "", "paused": False}]}
        if path.endswith("/runs"):
            return {"job_runs": [{"id": "r1", "status": "ENGINE_RUNNING"}]}
        if path.endswith("/models"):
            return {"models": [{"id": "m1", "name": "rsingh-mule-acct-scorer"}, {"id": "m2", "name": "not-ours"}]}
        if path.endswith("/builds"):
            return {"model_builds": [{"id": "b1"}]}
        if path.endswith("/deployments"):
            return {"model_deployments": [{"id": "d1", "status": self.dep_status, "created_at": "2026-09-29"}]}
        return {}


def test_project_stop_touches_only_this_project(monkeypatch):
    fake = FakeCai()
    monkeypatch.setattr(plat, "Cai", lambda: fake)
    monkeypatch.setattr(plat, "DRY", False)
    plat.cai_project("stop")
    acted = [(m, p) for m, p, _ in fake.calls if m != "GET"]
    assert acted == [("POST", "/projects/P/applications/a1:stop"),
                     ("POST", "/projects/P/models/m1/builds/b1/deployments/d1:stop"),
                     ("POST", "/projects/P/jobs/js/runs/r1:stop")]
    assert all("m2" not in p and "jx" not in p and "a3" not in p for _, p, _ in fake.calls)


def test_project_start_restarts_newest_deployment_and_apps(monkeypatch):
    fake = FakeCai(app_status="APPLICATION_STOPPED", dep_status="stopped")
    monkeypatch.setattr(plat, "Cai", lambda: fake)
    monkeypatch.setattr(plat, "DRY", False)
    plat.cai_project("start")
    acted = [(m, p) for m, p, _ in fake.calls if m != "GET"]
    assert acted == [("POST", "/projects/P/models/m1/builds/b1/deployments/d1:restart"),
                     ("POST", "/projects/P/applications/a1:restart"),
                     ("POST", "/projects/P/applications/a2:restart")]


def fake_cde_calls(monkeypatch, paused: bool):
    calls = []

    def fake_cde(*args):
        calls.append(args)
        if args[:2] == ("run", "list"):
            return [{"id": 1, "job": "rsingh-mule-acct-orchestration", "type": "airflow", "status": "running"},
                    {"id": 2, "job": "rsingh-mule-acct-generate-bronze", "type": "spark", "status": "starting"},
                    {"id": 3, "job": "rsingh-mule-acct2-other", "type": "spark", "status": "running"}]
        if args[:2] == ("job", "list"):
            return [{"name": "rsingh-mule-acct-orchestration", "schedule": {"enabled": True, "paused": paused}},
                    {"name": "rsingh-mule-acctx-orchestration", "schedule": {"enabled": True, "paused": paused}},
                    {"name": "rsingh-mule-acct-build-silver", "schedule": {"enabled": False}}]
        return ""

    monkeypatch.setattr(plat, "cde", fake_cde)
    monkeypatch.setattr(plat, "DRY", False)
    return calls


def test_cde_stop_pauses_then_kills_spark_before_orchestration(monkeypatch):
    calls = fake_cde_calls(monkeypatch, paused=False)
    plat.cde_project("stop")
    acted = [a for a in calls if a[:2] not in (("run", "list"), ("job", "list"))]
    assert acted == [("job", "schedule", "pause", "--name", "rsingh-mule-acct-orchestration"),
                     ("run", "kill", "--id", "2"), ("run", "kill", "--id", "1")]


def test_cde_start_unpauses_only_this_dag(monkeypatch):
    calls = fake_cde_calls(monkeypatch, paused=True)
    plat.cde_project("start")
    acted = [a for a in calls if a[:2] not in (("run", "list"), ("job", "list"))]
    assert acted == [("job", "schedule", "unpause", "--name", "rsingh-mule-acct-orchestration")]


def test_dry_run_acts_on_nothing(monkeypatch):
    calls = fake_cde_calls(monkeypatch, paused=False)
    monkeypatch.setattr(plat, "DRY", True)
    plat.cde_project("stop")
    assert all(a[:2] in (("run", "list"), ("job", "list")) for a in calls)

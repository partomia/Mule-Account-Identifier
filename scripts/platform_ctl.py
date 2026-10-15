#!/usr/bin/env python3
"""
Stop, start or show the demo across its layers, in dependency order (config/platform.json).

Two scopes:
  project   (default) this demo's workloads only: the CAI applications (Mule Investigator Console,
            Mule Data Visualization), the rsingh-mule-acct-scorer model deployment and any running
            rsingh-mule-acct-* CAI job runs; the rsingh-mule-acct-orchestration Airflow schedule and
            running or starting rsingh-mule-acct-* CDE runs. Iceberg tables, the CDE repository and
            python-env, and the Airflow Variables stay as they are.
  platform  the project scope plus the environment's shared services: the CDE virtual cluster,
            the CAI workspace and the CDW Impala virtual warehouse. Sibling demos use them, so
            this needs --yes-shared.

Stop runs in stop_order (CDE first: its DAG starts the CAI jobs sync-code and daily-score; then
CAI; then CDW), start in start_order (CDW first: the app, endpoint and scoring job read Impala;
then CAI; then CDE), each layer's service before its workloads on start and after them on stop.
--only limits the layers. The CAI jobs have no schedule of their own (the DAG starts them).
Unpausing the DAG runs its latest closed interval at once.

The CAI API key comes from the git-ignored .env (MULE_CAI_HOST, MULE_CAI_API_KEY), read here and
never printed; `cde` and `cdp` use their own CLI profiles (~/.cde, ~/.cdp).

  python scripts/platform_ctl.py status [--scope platform]
  python scripts/platform_ctl.py stop  [--only cai,cde] [--dry-run]
  python scripts/platform_ctl.py start [--dry-run]
  python scripts/platform_ctl.py stop --scope platform --yes-shared
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "config" / "platform.json").read_text())
PRJ, PLT = CFG["project"], CFG["platform"]
LAYERS = tuple(CFG["stop_order"])
DRY = False
ACTS = 0
ACTIVE_CAI = ("ENGINE_SCHEDULING", "ENGINE_STARTING", "ENGINE_RUNNING")
APP_UP = ("APPLICATION_RUNNING", "APPLICATION_STARTING")
DEPLOYMENT_UP = ("deployed", "deploying", "pending")


def say(msg: str) -> None:
    print(msg, flush=True)


def act(what: str, fn, *a, **kw):
    global ACTS
    ACTS += 1
    if DRY:
        say(f"  would {what}")
        return None
    say(f"  {what}")
    return fn(*a, **kw)


def wait_for(what: str, fn, ok: set, timeout: int = 2400, poll: int = 20) -> str:
    if DRY:
        return ""
    t0, last = time.time(), None
    while time.time() - t0 < timeout:
        st = str(fn())
        if st != last:
            say(f"    {what}: {st} ({time.time() - t0:.0f} s)")
            last = st
        if st.lower() in {s.lower() for s in ok}:
            return st
        time.sleep(poll)
    raise SystemExit(f"{what}: still {last} after {timeout} s")


def dotenv() -> dict:
    path = ROOT / ".env"
    if not path.is_file():
        raise SystemExit(".env not found (copy .env.example and fill in MULE_CAI_HOST, MULE_CAI_API_KEY)")
    out = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip("'\"")
    return out


# ------------------------------------------------------------------ CLIs
def run_cli(cmd: list[str], read: bool, tries: int = 5) -> str:
    """Reads are retried (the CDE API drops connections with EOF); actions never are."""
    for attempt in range(1, (tries if read else 1) + 1):
        out = subprocess.run(cmd, capture_output=True, text=True)
        if not out.returncode:
            return out.stdout
        if attempt < (tries if read else 1):
            time.sleep(5 * attempt)
    raise RuntimeError(f"{Path(cmd[0]).name} {' '.join(cmd[1:4])}: {out.stderr.strip()[:300]}")


def cdp(*args: str) -> dict:
    exe = os.environ.get("MULE_CDP_CLI") or shutil.which("cdp") or next(iter(glob.glob(
        os.path.expanduser("~/Library/Python/*/bin/cdp"))), None)
    if not exe:
        raise SystemExit("cdp CLI not found: pip install cdpcli, or set MULE_CDP_CLI")
    out = run_cli([exe, *args], read=args[1].startswith(("list-", "describe-")))
    return json.loads(out) if out.strip() else {}


def cde(*args: str):
    out = run_cli(["cde", *args], read=args[1] in ("list", "describe"))
    return json.loads(out) if out.strip().startswith(("{", "[")) else out


class Cai:
    def __init__(self):
        env = dotenv()
        missing = [k for k in ("MULE_CAI_HOST", "MULE_CAI_API_KEY") if not env.get(k)]
        if missing:
            raise SystemExit(f".env has no {', '.join(missing)}")
        host = env["MULE_CAI_HOST"].rstrip("/")
        self.base = (host if host.startswith("http") else f"https://{host}") + "/api/v2"
        self.h = {"Authorization": f"Bearer {env['MULE_CAI_API_KEY']}"}
        found = self("GET", "/projects", params={"search_filter": json.dumps({"name": PRJ["cai_project"]}),
                                                 "page_size": 100})
        self.pid = next(p["id"] for p in found["projects"] if p["name"] == PRJ["cai_project"])

    def __call__(self, method: str, path: str, **kw) -> dict:
        for attempt in range(1, 6 if method == "GET" else 2):   # GETs retried on drops and 5xx, never on 4xx
            try:
                r = requests.request(method, self.base + path, headers=self.h, timeout=60, **kw)
                if r.status_code < 500 or method != "GET" or attempt == 5:
                    break
            except requests.RequestException:
                if method != "GET" or attempt == 5:
                    raise
            time.sleep(5 * attempt)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path}: HTTP {r.status_code} {r.text[:200]}")
        return r.json() if r.text.strip() else {}

    def p(self, path: str) -> str:
        return f"/projects/{self.pid}{path}"

    def apps(self) -> list[dict]:
        return [a for a in self("GET", self.p("/applications"), params={"page_size": 100})["applications"]
                if a["name"] in PRJ["cai_apps"]]

    def jobs(self) -> list[dict]:
        return [j for j in self("GET", self.p("/jobs"), params={"page_size": 200})["jobs"]
                if j["name"].startswith(tuple(PRJ["cai_job_prefixes"]))]

    def active_runs(self, job: dict) -> list[dict]:
        runs = self("GET", self.p(f"/jobs/{job['id']}/runs"),
                    params={"page_size": 20, "sort": "-created_at"}).get("job_runs", [])
        return [r for r in runs if r["status"] in ACTIVE_CAI]

    def deployments(self) -> list[tuple[dict, dict, dict]]:
        """(model, build, deployment) for every deployment of the project's models, newest first."""
        out = []
        for m in self("GET", self.p("/models"), params={"page_size": 100})["models"]:
            if m["name"] not in PRJ["cai_models"]:
                continue
            for b in self("GET", self.p(f"/models/{m['id']}/builds"), params={"page_size": 100})["model_builds"]:
                for d in self("GET", self.p(f"/models/{m['id']}/builds/{b['id']}/deployments"),
                              params={"page_size": 100})["model_deployments"]:
                    out.append((m, b, d))
        return sorted(out, key=lambda t: t[2]["created_at"], reverse=True)


# ------------------------------------------------------------------ project workloads
def cai_project(action: str) -> None:
    c = Cai()
    deps = c.deployments()
    jobs = c.jobs()
    runs = {j["id"]: c.active_runs(j) for j in jobs}
    apps = c.apps()
    if action == "status":
        for a in apps:
            say(f"  app {a['name']}: {a['status']} ({a['cpu']} vCPU / {a['memory']} GB)")
        for name in sorted(set(PRJ["cai_apps"]) - {a["name"] for a in apps}):
            say(f"  app {name}: not found")
        for name in PRJ["cai_models"]:
            mine = [(b, d) for m, b, d in deps if m["name"] == name]
            live = [(b, d) for b, d in mine if d["status"] == "deployed"]
            say(f"  model {name}: " + (f"deployed (build {live[0][0]['id'][:8]})" if live else
                                       f"not deployed (newest deployment {mine[0][1]['id'][:8]} {mine[0][1]['status']})"
                                       if mine else "no deployment"))
        for j in jobs:
            if j.get("schedule") or runs[j["id"]]:
                say(f"  job {j['name']}: schedule {j.get('schedule') or '-'}{' PAUSED' if j.get('paused') else ''}"
                    f"{', ' + str(len(runs[j['id']])) + ' active run(s)' if runs[j['id']] else ''}")
        if not any(j.get("schedule") for j in jobs):
            say(f"  {len(jobs)} job(s), none scheduled in CAI (the DAG starts them)")
        if not any(runs.values()):
            say("  no active job runs")
        return
    if action == "stop":
        for a in apps:
            if a["status"] in APP_UP:
                act(f"stop application {a['name']}", c, "POST", c.p(f"/applications/{a['id']}:stop"))
        for m, b, d in deps:
            if d["status"] in DEPLOYMENT_UP:
                act(f"stop model {m['name']} deployment {d['id'][:8]}", c, "POST",
                    c.p(f"/models/{m['id']}/builds/{b['id']}/deployments/{d['id']}:stop"))
        for j in jobs:
            if j["name"] in PRJ["cai_scheduled_jobs"] and j.get("schedule") and not j.get("paused"):
                act(f"pause schedule of {j['name']}", c, "PATCH", c.p(f"/jobs/{j['id']}"), json={"paused": True})
            for r in runs[j["id"]]:
                act(f"stop job run {j['name']} {r['id']}", c, "POST", c.p(f"/jobs/{j['id']}/runs/{r['id']}:stop"))
        return
    # start: schedules, the newest deployment of each model, then the applications
    for j in jobs:
        if j["name"] in PRJ["cai_scheduled_jobs"] and j.get("paused"):
            act(f"resume schedule of {j['name']}", c, "PATCH", c.p(f"/jobs/{j['id']}"), json={"paused": False})
    for name in PRJ["cai_models"]:
        mine = [(m, b, d) for m, b, d in deps if m["name"] == name]
        if mine and not any(d["status"] in DEPLOYMENT_UP for _, _, d in mine):
            m, b, d = mine[0]
            act(f"restart model {name} deployment {d['id'][:8]} (build {b['id'][:8]})", c, "POST",
                c.p(f"/models/{m['id']}/builds/{b['id']}/deployments/{d['id']}:restart"))
    for a in apps:
        if a["status"] not in APP_UP:
            act(f"start application {a['name']} (now {a['status']})", c, "POST",
                c.p(f"/applications/{a['id']}:restart"))
    say("  the endpoint (4 / 16) and both apps (2 / 4, 2 / 8) share the per-user quota with the DAG's "
        "daily-score job (4 / 16): stop the apps if the job sits in scheduling")


def cde_project(action: str) -> None:
    prefix = PRJ["cde_job_prefix"]
    runs = [r for r in cde("run", "list", "--filter", "status[in]running,starting",
                           "--filter", f"job[rlike]^{prefix}") or [] if r["job"].startswith(prefix)]
    jobs = [j for j in cde("job", "list", "--filter", f"name[rlike]^{prefix}") or [] if j["name"].startswith(prefix)]
    scheduled = [j for j in jobs if (j.get("schedule") or {}).get("enabled")]
    if action == "status":
        say("  active runs: " + (", ".join(f"{r['job']} #{r['id']} {r['status']}" for r in runs) or "none"))
        for j in scheduled:
            s = j["schedule"]
            say(f"  schedule {j['name']} ({s.get('cronExpression')} UTC): "
                f"{'paused' if s.get('paused') else 'active, next ' + str(s.get('nextExecution'))}")
        for name in sorted(set(PRJ["cde_dag_jobs"]) - {j["name"] for j in jobs}):
            say(f"  DAG job {name}: not found")
        return
    if action == "stop":
        for j in scheduled:
            if not j["schedule"].get("paused"):
                act(f"pause CDE schedule {j['name']}", cde, "job", "schedule", "pause", "--name", j["name"])
        # Spark children first, so an orchestration run does not retry them
        for r in sorted(runs, key=lambda r: r["type"] == "airflow"):
            act(f"kill CDE run {r['job']} #{r['id']} ({r['type']}, {r['status']})", cde, "run", "kill",
                "--id", str(r["id"]))
        return
    for j in scheduled:
        if j["schedule"].get("paused"):
            act(f"unpause CDE schedule {j['name']} (runs the latest closed interval at once)",
                cde, "job", "schedule", "unpause", "--name", j["name"])


PROJECT = {"cai": cai_project, "cde": cde_project}


# ------------------------------------------------------------------ shared services
def cdw(action: str) -> None:
    env_crn = cdp("environments", "describe-environment", "--environment-name", CFG["environment"])["environment"]["crn"]
    cid = next(c["id"] for c in cdp("dw", "list-clusters")["clusters"] if c["environmentCrn"] == env_crn)
    vws = {v["name"]: v for v in cdp("dw", "list-vws", "--cluster-id", cid)["vws"]}
    for name in PLT["cdw_vws"]:
        vw = vws[name]
        status = lambda v=vw: cdp("dw", "describe-vw", "--cluster-id", cid, "--vw-id", v["id"])["vw"]["status"]  # noqa: E731
        if action == "status":
            say(f"  CDW virtual warehouse {name}: {vw['status']}")
        elif action == "stop" and vw["status"].lower() not in ("stopped", "suspended"):
            act(f"suspend CDW virtual warehouse {name}", cdp, "dw", "suspend-vw", "--cluster-id", cid, "--vw-id", vw["id"])
            wait_for(name, status, {"Stopped", "Suspended"})
        elif action == "start" and vw["status"].lower() != "running":
            act(f"start CDW virtual warehouse {name}", cdp, "dw", "start-vw", "--cluster-id", cid, "--vw-id", vw["id"])
            wait_for(name, status, {"Running"})


def cde_vc(action: str) -> None:
    svc = next(s for s in cdp("de", "list-services")["services"]
               if s["name"] == PLT["cde_service"] and s["status"] == "ClusterCreationCompleted")
    vc = next(v for v in cdp("de", "list-vcs", "--cluster-id", svc["clusterId"])["vcs"] if v["vcName"] == PLT["cde_vc"])
    ids = ("--cluster-id", svc["clusterId"], "--vc-id", vc["vcId"])
    status = lambda: cdp("de", "describe-vc", *ids)["vc"]["status"]  # noqa: E731
    st = vc["status"]
    if action == "status":
        say(f"  CDE virtual cluster {PLT['cde_vc']}: {st}")
    elif action == "stop" and "suspend" not in st.lower():
        act(f"suspend CDE virtual cluster {PLT['cde_vc']}", cdp, "de", "suspend-vc", *ids)
        wait_for(PLT["cde_vc"], status, {"AppSuspended", "Suspended"})
    elif action == "start" and "suspend" in st.lower():
        act(f"resume CDE virtual cluster {PLT['cde_vc']}", cdp, "de", "resume-vc", *ids)
        wait_for(PLT["cde_vc"], status, {"AppInstalled", "Running"})


def cai_workspace(action: str) -> None:
    def ws():
        return next(w for w in cdp("ml", "list-workspaces")["workspaces"]
                    if w["instanceName"] == PLT["cai_workspace"] and w["environmentName"] == CFG["environment"])
    w = ws()
    st = w["instanceStatus"]
    if action == "status":
        say(f"  CAI workspace {PLT['cai_workspace']}: {st}")
    elif action == "stop" and "suspend" not in st:
        act(f"suspend CAI workspace {PLT['cai_workspace']}", cdp, "ml", "suspend-workspace", "--workspace-crn", w["crn"])
        wait_for(PLT["cai_workspace"], lambda: ws()["instanceStatus"], {"suspend:finished"}, timeout=3600)
    elif action == "start" and "suspend" in st:
        act(f"resume CAI workspace {PLT['cai_workspace']}", cdp, "ml", "resume-workspace", "--workspace-crn", w["crn"])
        wait_for(PLT["cai_workspace"], lambda: ws()["instanceStatus"], {"resume:finished", "installation:finished"},
                 timeout=3600)


SHARED = {"cdw": cdw, "cde": cde_vc, "cai": cai_workspace}


def layer(name: str, action: str, scope: str) -> None:
    steps = []
    if action == "start" and scope == "platform":
        steps.append(SHARED[name])
    if name in PROJECT:
        steps.append(PROJECT[name])
    if action != "start" and scope == "platform":
        steps.append(SHARED[name])
    if not steps:
        return
    say(f"[{name}]")
    before = ACTS
    for step in steps:
        try:
            step(action)
        except Exception as e:  # noqa: BLE001  one layer's failure is reported; the others still run
            say(f"  ERROR {type(e).__name__}: {e}")
            if action != "status":
                raise
    if action != "status" and ACTS == before:
        say(f"  nothing to {action}")


def main(argv: list[str] | None = None) -> int:
    global DRY
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["status", "stop", "start"])
    ap.add_argument("--scope", choices=["project", "platform"], default="project")
    ap.add_argument("--only", default="", help=f"comma-separated layers from {', '.join(LAYERS)}")
    ap.add_argument("--yes-shared", action="store_true", help="required to stop or start the shared services")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    DRY = a.dry_run
    if a.scope == "platform" and a.action != "status" and not (a.yes_shared or a.dry_run):
        raise SystemExit("--scope platform stops/starts services other demos share: add --yes-shared")
    only = {x.strip() for x in a.only.split(",") if x.strip()}
    if only - set(LAYERS):
        raise SystemExit(f"unknown layers {sorted(only - set(LAYERS))}")
    order = CFG["start_order"] if a.action == "start" else CFG["stop_order"]
    if DRY:
        say(f"dry run: {a.action} --scope {a.scope}, nothing is changed")
    for name in order:
        if not only or name in only:
            layer(name, a.action, a.scope)
    return 0


if __name__ == "__main__":
    sys.exit(main())

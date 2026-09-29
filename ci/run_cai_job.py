#!/usr/bin/env python3
"""
Start one CAI job of this project by name and wait for it (API v2).

  set -a; source .env; set +a
  python ci/run_cai_job.py rsingh-mule-acct-sync-code
  python ci/run_cai_job.py rsingh-mule-acct-daily-score --env MULE_RUN_DATE=2026-09-28

A job run ignores arguments; --env KEY=VALUE goes into the run's environment.
Exit code 0 when the run succeeded. API v2 has no run-log endpoint: check the
job's output in the CAI UI, or the tables it wrote.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ci.cai_api import BAD, OK, Workbench, find_project, job_ids, status_of  # noqa: E402
from ci.cai_jobs import CAI_PROJECT_NAME  # noqa: E402


def run_job(wb, project_id: str, job_id: str, env: dict, poll_s: float = 30, deadline_s: float = 120 * 60) -> str:
    run = wb("POST", f"/projects/{project_id}/jobs/{job_id}/runs", body={"environment": env})
    print(f"started run {run['id']} with environment {sorted(env)}", flush=True)
    t0, last = time.time(), None
    while time.time() - t0 < deadline_s:
        time.sleep(poll_s)
        status = status_of(wb("GET", f"/projects/{project_id}/jobs/{job_id}/runs/{run['id']}"))
        if status != last:
            print(f"{time.strftime('%H:%M:%S')} +{time.time() - t0:.0f}s {status}", flush=True)
            last = status
        if status in OK | BAD:
            return status
    return "timedout"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("job")
    p.add_argument("--env", action="append", default=[], metavar="KEY=VALUE")
    p.add_argument("--deadline-min", type=float, default=120)
    args, _ = p.parse_known_args()
    wb = Workbench(os.environ["MULE_CAI_HOST"], os.environ["MULE_CAI_API_KEY"])
    project = find_project(wb, CAI_PROJECT_NAME)
    if not project:
        raise SystemExit(f"CAI project {CAI_PROJECT_NAME} not found: run ci/setup_cai.py first")
    ids = job_ids(wb, project["id"])
    if args.job not in ids:
        raise SystemExit(f"job {args.job} not in {sorted(ids)}")
    env = dict(kv.split("=", 1) for kv in args.env)
    status = run_job(wb, project["id"], ids[args.job], env, deadline_s=args.deadline_min * 60)
    print(f"{args.job}: {status}")
    return 0 if status in OK else 1


if __name__ == "__main__":
    sys.exit(main())

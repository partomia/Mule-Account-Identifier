#!/usr/bin/env python3
"""
CAI Job: daily mule-account scoring.

  1. Holdout: context from older labelled weeks, test on the latest labelled
     weeks; prints capture / precision and lift over the rules-only baseline.
  2. KPI gate (config/policy.yaml): capture at top 1%, precision at top 0.2%,
     lift over rules. Failing it does not stop the job early - yesterday's
     alert queue stays live and today's run is still recorded, with
     gate_passed = false - but the job exits non-zero so the Airflow DAG (and
     a CLI run) shows red.
  3. Scores today's active book, tiers and explains the alerts, rolls them up
     to rings, writes gold.mule_{alerts,rings,holdout,model_run}.
  4. Logs the run's params and KPIs to MLflow (CAI Experiments) when mlflow is
     available; a missing mlflow does not fail the job.

Jobs > New Job: script cai/jobs/daily_score.py, Python 3.11 runtime, GPU
profile if available. Airflow passes the run date and trigger through the
environment (MULE_RUN_DATE, MULE_TRIGGERED_BY), since a job run ignores
arguments.

  python cai/jobs/daily_score.py --dry-run                  # reads, scores, writes nothing
  python cai/jobs/daily_score.py --backend parquet --stub   # offline smoke run, no checkpoint

Exit codes: 0 gate passed (or holdout skipped / --ignore-gate); 1 gate failed.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

from mule.config import settings  # noqa: E402
from mule.pipeline import run_daily  # noqa: E402
from mule.storage import get_storage  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

MLFLOW_PARAMS = ("run_id", "model_family", "model_id", "model_version", "device", "snapshot_date", "context_rows",
                 "context_mules", "context_from", "context_to", "book_rate_source", "triggered_by",
                 "holdout_test_from", "holdout_test_to")
MLFLOW_METRICS = ("scored_accounts", "alerts_t1", "alerts_t2", "alerts_t3", "context_mule_rate", "book_mule_rate",
                  "holdout_auc", "holdout_pr_auc", "capture_top1", "capture_top5", "precision_top02",
                  "precision_top1", "rules_capture_top1", "rules_precision_top1", "lift_over_rules", "duration_s")


def log_to_mlflow(model_run: dict) -> None:
    cfg = settings()["tracking"]
    if not cfg["enabled"]:
        return
    try:
        import mlflow
    except ImportError:
        logger.warning("mlflow not available - skipping experiment tracking")
        return
    mlflow.set_experiment(cfg["experiment"])
    with mlflow.start_run(run_name=model_run["run_id"]):
        mlflow.log_params({k: model_run.get(k) for k in MLFLOW_PARAMS})
        mlflow.log_metrics({k: float(v) for k in MLFLOW_METRICS if (v := model_run.get(k)) is not None})
        mlflow.set_tags({"gate_passed": model_run.get("gate_passed"), "alerts_published": model_run.get(
            "alerts_published")})
        mlflow.log_text(model_run.get("gate_detail") or "", "gate_detail.txt")
    logger.info("logged run %s to MLflow experiment %s", model_run["run_id"], cfg["experiment"])


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-date", default=os.environ.get("MULE_RUN_DATE") or None,
                   help="snapshot date to score, YYYY-MM-DD (default: latest snapshot in gold)")
    p.add_argument("--backend", default=None, help="impala | parquet (default: config)")
    p.add_argument("--family", default=None, help="auto | mitra | tabicl | stub (default: config)")
    p.add_argument("--no-holdout", action="store_true")
    p.add_argument("--stub", action="store_true", help="logistic regression stand-in instead of Mitra / TabICL")
    p.add_argument("--dry-run", action="store_true", help="do not write any table")
    p.add_argument("--publish-on-fail", action="store_true", help="publish today's alerts even if the gate fails")
    p.add_argument("--ignore-gate", action="store_true", help="exit 0 even if the KPI gate fails")
    p.add_argument("--triggered-by", default=os.environ.get("MULE_TRIGGERED_BY") or "cai-job")
    args, _ = p.parse_known_args()  # a Jupyter-kernel job runtime adds -f <kernel.json>

    run_date = datetime.strptime(args.run_date, "%Y-%m-%d").date() if args.run_date else None
    family = "stub" if args.stub else args.family
    out = run_daily(get_storage(args.backend), family=family, run_date=run_date, triggered_by=args.triggered_by,
                    write=not args.dry_run, run_holdout=not args.no_holdout, publish_on_fail=args.publish_on_fail)
    s = out["summary"]
    print(f"\nrun {s['run_id']} ({s['run_date']}): {s['scored_accounts']} accounts scored, tiers {s['tiers']}, "
         f"{s['rings']} rings with an alert")
    if s.get("capture_top1") is not None:
        print(f"holdout AUC {s['holdout_auc']:.3f} | top 1% of the book catches {s['capture_top1']:.0%} of mules "
             f"(rules alone {s['rules_capture_top1']:.0%}), lift {s['lift_over_rules']:.2f}x | precision top 0.2% "
             f"{s['precision_top02']:.1%}, top 1% {s['precision_top1']:.1%}")
    for line in s["gate_detail"]:
        print(f"gate: {line}")
    print(f"alerts published: {s['alerts_published']}")

    if not args.dry_run and "mule_model_run" in out:
        log_to_mlflow(out["mule_model_run"].iloc[0].to_dict())

    if not s["gate_passed"] and not args.ignore_gate:
        return 1
    return 0


if __name__ == "__main__":
    # CAI Jobs run a script inside a Jupyter kernel wrapper (not a plain python
    # subprocess): any SystemExit, even sys.exit(0), is caught as an unhandled
    # exception there and the Job engine reports it as a failure regardless of
    # the code (confirmed live: gate PASS, alerts published, then "Engine
    # exited with status 1"). Only exit explicitly on real failure; falling
    # off the end of the script on success reports correctly either way.
    _rc = main()
    if _rc:
        sys.exit(_rc)

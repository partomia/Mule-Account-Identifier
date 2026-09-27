#!/usr/bin/env python3
"""
One-off: run the daily scoring for the last N weekly (Friday) snapshots,
oldest first, so the app and mule_model_run have a history from day one. Each
run only uses labels known on its own run date (mule.pipeline.run_daily), so
the history is what the job would have produced on those days.

  python cai/jobs/backfill_history.py --weeks 8
  python cai/jobs/backfill_history.py --weeks 4 --backend parquet --stub
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

from mule.pipeline import run_daily  # noqa: E402
from mule.storage import get_storage  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--weeks", type=int, default=8)
    p.add_argument("--backend", default=None)
    p.add_argument("--family", default=None)
    p.add_argument("--stub", action="store_true")
    args, _ = p.parse_known_args()  # a Jupyter-kernel job runtime adds -f <kernel.json>

    storage = get_storage(args.backend)
    snap = storage.snapshot_id("mule_features")
    latest = storage.latest_snapshot_date(snap)
    labelled = storage.labelled_dates(snap)
    window_from = labelled[0] if labelled else latest
    # the latest snapshots are unlabelled but still scoreable; take them from the full date list
    all_dates = sorted(set(storage.features(date_from=window_from, date_to=latest, snapshot_id=snap)["snapshot_date"]))
    fridays = sorted({d for d in all_dates if d.weekday() == 4})
    dates = fridays[-args.weeks:]
    family = "stub" if args.stub else args.family

    for d in dates:
        out = run_daily(storage, family=family, run_date=d, triggered_by="backfill", save_context_file=False)
        s = out["summary"]
        print(f"{d}: {s['scored_accounts']} accounts, tiers {s['tiers']}, holdout AUC {s.get('holdout_auc')}, "
             f"top-1% capture {s.get('capture_top1')}, gate_passed {s['gate_passed']}")


if __name__ == "__main__":
    main()

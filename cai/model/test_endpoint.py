#!/usr/bin/env python3
"""
Try the scorer with a demo account, then the same account after a new device
login, a mobile/VPA change and near-total pass-through (the live what-if from
the demo).

  python cai/model/test_endpoint.py --print-request      # JSON for the model's Test tab
  python cai/model/test_endpoint.py --local              # in-process, context from models/
  python cai/model/test_endpoint.py --local --stub       # no checkpoint needed
  python cai/model/test_endpoint.py                      # the deployed endpoint (MULE_ENDPOINT_*)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

EXAMPLE = {
    "accounts": [{
        "account_id": "ACC-04821193", "account_age_days": 210, "min_kyc_flag": 1, "dormant_reactivated_flag": 0,
        "inflow_to_declared_income_30d": 3.2, "distinct_senders_7d": 6, "distinct_receivers_7d": 2,
        "pass_through_ratio_7d": 0.3, "median_hold_hours_30d": 18.0, "night_txn_share_30d": 0.12,
        "round_amount_share_30d": 0.2, "new_device_logins_30d": 0, "vpa_or_mobile_changes_30d": 0,
        "accounts_on_same_device": 2, "cifs_sharing_mobile": 1, "person_cluster_size": 2, "ring_size": 2,
        "hops_to_known_mule": 2, "credits_from_complainants_30d": 0,
    }],
    "what_if": {"new_device_logins_30d": 2, "vpa_or_mobile_changes_30d": 1, "pass_through_ratio_7d": 0.92},
}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--print-request", action="store_true")
    p.add_argument("--local", action="store_true", help="score in-process instead of calling the endpoint")
    p.add_argument("--stub", action="store_true", help="with --local: logistic stand-in instead of Mitra / TabICL")
    p.add_argument("--context", default="file", help="with --local: impala | file | auto")
    args, _ = p.parse_known_args()  # a Jupyter-kernel job runtime adds -f <kernel.json>

    if args.print_request:
        print(json.dumps(EXAMPLE, indent=2))
        return
    t0 = time.time()
    if args.local:
        from mule.model import StubClassifier, new_classifier
        from mule.scoring import load_scorer, score

        clf, meta = load_scorer(args.context, factory=StubClassifier if args.stub else new_classifier)
        t1 = time.time()
        resp = score(EXAMPLE, clf, meta)
        print(f"context built in {t1 - t0:.1f}s, request scored in {time.time() - t1:.2f}s")
    else:
        from mule.client import call_endpoint, endpoint_configured

        if not endpoint_configured():
            sys.exit("MULE_ENDPOINT_URL is not set (or use --local)")
        resp = call_endpoint(EXAMPLE)
        print(f"endpoint answered in {time.time() - t0:.2f}s")
    print(json.dumps(resp, indent=2))


if __name__ == "__main__":
    main()

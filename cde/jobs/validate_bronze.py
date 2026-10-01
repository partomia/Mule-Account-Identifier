"""
Stage 2 - Validate (bronze gate)

The bronze layer of cde/jobs/dq_check.py: records every check in
<prefix>_ref.dq_results, then exits non-zero on a critical failure so the
Airflow DAG stops before anything reaches silver / gold (and therefore before
anything reaches an investigator). Re-sent duplicate records are a warning
only: silver removes them.

Critical checks (unchanged from the gate before results were recorded):
  * every extract is non-empty and has no null keys; nothing dated after the as-of date
  * account_id unique in cbs_accounts; product and branch codes known in ref
  * every account belongs to a known CIF, every transaction to a known account,
    every session to a known CIF (at most 0.1% orphans; a warning at 0.05%)
  * transaction amounts positive, direction CR / DR (same limits)
  * no missing day in upi_transactions between the first day and the as-of date
  * PAN well formed in kyc_onboarding.pan, and a raw PAN appears in no other
    column of any extract (identity documents stay in their governed column)
New: every extract's batch_as_of is the as-of date (critical); re-sent duplicates
at most 1% and row counts within -5% / +10% of the previous load (warnings).

Usage:
  spark-submit validate_bronze.py [--db-prefix P] [--as-of YYYY-MM-DD] [--pipeline-run ID]
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dq_check  # noqa: E402


def main(argv=None, spark=None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    dq_check.main(["--layer", "bronze", *argv], spark=spark)


if __name__ == "__main__":
    main()

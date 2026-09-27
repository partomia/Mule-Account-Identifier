"""Column definitions for the tables written from CAI.

Gold outputs of the daily job are keyed by run_date: a rerun for the same day
replaces that day only, so yesterday's alert queue stays queryable.
bronze.investigator_decisions is appended to by the app and read by the CDE
silver job: a CONFIRMED_MULE decision becomes a label (and a known mule for
the graph) in tomorrow's features.
"""

OUTPUT_TABLES = {
    "mule_alerts": [
        ("run_date", "DATE"), ("run_id", "STRING"), ("snapshot_date", "DATE"), ("account_id", "STRING"),
        ("cif", "STRING"), ("branch_code", "STRING"), ("product_code", "STRING"), ("person_cluster_id", "STRING"),
        ("ring_id", "STRING"), ("ring_size", "INT"), ("hops_to_known_mule", "INT"),
        ("p_mule", "DOUBLE"), ("p_mule_adj", "DOUBLE"), ("risk_rank", "INT"), ("risk_pct", "DOUBLE"),
        ("tier", "STRING"), ("action", "STRING"), ("reason_codes", "STRING"), ("reasons", "STRING"),
    ],
    "mule_rings": [
        ("run_date", "DATE"), ("run_id", "STRING"), ("ring_id", "STRING"), ("ring_size", "INT"),
        ("accounts_scored", "INT"), ("accounts_alerted", "INT"), ("top_tier", "STRING"),
        ("max_p_mule_adj", "DOUBLE"), ("sum_p_mule_adj", "DOUBLE"), ("min_hops_to_known_mule", "INT"),
        ("top_reason_codes", "STRING"),
    ],
    "mule_holdout": [
        ("run_date", "DATE"), ("run_id", "STRING"), ("band", "STRING"), ("upto_pct", "DOUBLE"),
        ("accounts", "DOUBLE"), ("mules", "DOUBLE"), ("mule_rate", "DOUBLE"), ("mean_p_mule", "DOUBLE"),
        ("cum_capture", "DOUBLE"), ("rules_cum_capture", "DOUBLE"),
    ],
    "mule_model_run": [
        ("run_date", "DATE"), ("run_id", "STRING"), ("run_ts", "TIMESTAMP"), ("model_family", "STRING"),
        ("model_id", "STRING"), ("model_version", "STRING"), ("device", "STRING"), ("snapshot_date", "DATE"),
        ("scored_accounts", "INT"), ("alerts_t1", "INT"), ("alerts_t2", "INT"), ("alerts_t3", "INT"),
        ("t1_cutoff", "DOUBLE"), ("t2_cutoff", "DOUBLE"), ("t3_cutoff", "DOUBLE"),
        ("context_rows", "INT"), ("context_mules", "INT"), ("context_negatives", "INT"),
        ("context_from", "DATE"), ("context_to", "DATE"), ("context_mule_rate", "DOUBLE"),
        ("book_mule_rate", "DOUBLE"), ("book_rate_source", "STRING"),
        ("source_table", "STRING"), ("source_snapshot_id", "STRING"),
        ("holdout_test_from", "DATE"), ("holdout_test_to", "DATE"), ("holdout_context_rows", "INT"),
        ("holdout_rows", "INT"), ("holdout_mules", "INT"), ("holdout_mule_rate", "DOUBLE"),
        ("holdout_auc", "DOUBLE"), ("holdout_pr_auc", "DOUBLE"),
        ("capture_top1", "DOUBLE"), ("capture_top5", "DOUBLE"), ("precision_top02", "DOUBLE"),
        ("precision_top1", "DOUBLE"), ("rules_capture_top1", "DOUBLE"), ("rules_precision_top1", "DOUBLE"),
        ("lift_over_rules", "DOUBLE"), ("gate_passed", "BOOLEAN"), ("gate_detail", "STRING"),
        ("alerts_published", "BOOLEAN"), ("policy_json", "STRING"), ("triggered_by", "STRING"),
        ("duration_s", "DOUBLE"),
    ],
}

# Appended by the app (maker-checker); read by cde/jobs/build_silver.py.
INVESTIGATOR_DECISIONS = [
    ("decision_id", "STRING"), ("run_date", "DATE"), ("account_id", "STRING"), ("cif", "STRING"),
    ("decision", "STRING"), ("action", "STRING"), ("maker", "STRING"), ("checker", "STRING"),
    ("notes", "STRING"), ("decided_at", "TIMESTAMP"), ("ingested_at", "TIMESTAMP"),
]

DECISIONS = ["CONFIRMED_MULE", "FALSE_POSITIVE", "NEEDS_MORE_INFO"]

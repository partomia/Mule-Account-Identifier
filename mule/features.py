"""Model inputs: the numeric columns of gold.mule_features.

Must match FEATURES / LABEL in cde/jobs/build_gold_features.py (the CDE jobs
are self-contained, so the list lives in both places; a test checks they agree).
"""

FEATURES = [
    # account profile
    "account_age_days",
    "min_kyc_flag",                    # 1 = OTP-based minimum KYC
    "dormant_reactivated_flag",        # 1 = dormant account reactivated in the last 90 days
    # money movement
    "inflow_to_declared_income_30d",   # credits in 30 days / declared monthly income
    "distinct_senders_7d",
    "distinct_receivers_7d",
    "pass_through_ratio_7d",           # share of credited INR sent out within 24 hours
    "median_hold_hours_30d",           # hours from a credit to the next debit (capped at 720)
    "night_txn_share_30d",             # 23:00-05:00
    "round_amount_share_30d",          # credits in multiples of INR 1,000
    # digital channel
    "new_device_logins_30d",
    "vpa_or_mobile_changes_30d",
    # identity graph
    "accounts_on_same_device",         # accounts behind the busiest device this customer used (incl. own)
    "cifs_sharing_mobile",             # customers with this customer's mobile (incl. self)
    "person_cluster_size",             # customers linked by device / mobile / PAN / address
    "ring_size",                       # customers in the wider component incl. money-flow links
    # network proximity (only reports known on the snapshot date)
    "hops_to_known_mule",              # 1, 2, or 3 = 3+ or not connected
    "credits_from_complainants_30d",   # credits from payers who filed a 1930 / NCRP complaint
]
LABEL = "is_mule_90d"
KEYS = ["account_id", "snapshot_date"]
INFO = ["cif", "branch_code", "product_code", "person_cluster_id", "ring_id"]
COLUMNS = [*KEYS, *INFO, *FEATURES, LABEL]

# Features an investigator or the what-if tab can reason about in plain language.
NETWORK_FEATURES = ["accounts_on_same_device", "cifs_sharing_mobile", "person_cluster_size", "ring_size",
                    "hops_to_known_mule", "credits_from_complainants_30d"]

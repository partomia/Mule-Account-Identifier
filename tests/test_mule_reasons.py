"""Alert tiers (percentile cut-offs) and plain-language reason codes (business rules)."""

import pandas as pd
import pytest

from mule.reasons import NO_TIER, assign_tiers, fired_rules, reasons, rules_score

TIERS = [
    {"code": "T1_FREEZE_REVIEW", "upto_pct": 0.002, "action": "freeze"},
    {"code": "T2_HOLD_MONITOR", "upto_pct": 0.010, "action": "hold"},
    {"code": "T3_WATCHLIST", "upto_pct": 0.030, "action": "watch"},
]

RULES = {
    "shared_device_accounts": 3, "shared_mobile_cifs": 2, "pass_through_ratio": 0.8, "hold_hours": 6,
    "inflow_multiple": 5, "distinct_senders_7d": 15, "new_account_days": 60, "night_share": 0.3,
    "device_or_mobile_changes": 1, "ring_size": 5,
}

FEATURE_DEFAULTS = {
    "hops_to_known_mule": 3, "credits_from_complainants_30d": 0, "pass_through_ratio_7d": 0.1,
    "median_hold_hours_30d": 100, "ring_size": 1, "accounts_on_same_device": 1, "cifs_sharing_mobile": 1,
    "new_device_logins_30d": 0, "vpa_or_mobile_changes_30d": 0, "inflow_to_declared_income_30d": 1.0,
    "distinct_senders_7d": 1, "dormant_reactivated_flag": 0, "account_age_days": 1000, "min_kyc_flag": 0,
    "night_txn_share_30d": 0.0,
}


def _row(**overrides):
    return {**FEATURE_DEFAULTS, **overrides}


def test_assign_tiers_boundaries():
    pct = pd.Series([0.001, 0.002, 0.005, 0.02, 0.05])
    tier, action = assign_tiers(pct, TIERS)
    assert list(tier) == ["T1_FREEZE_REVIEW", "T1_FREEZE_REVIEW", "T2_HOLD_MONITOR", "T3_WATCHLIST", NO_TIER]
    assert action.iloc[-1] == ""
    assert action.iloc[0] == "freeze"


def test_fired_rules_clean_account_fires_nothing():
    df = pd.DataFrame([_row()])
    fired = fired_rules(df, RULES)
    assert not fired.any(axis=None)


def test_fired_rules_flags_each_signal():
    df = pd.DataFrame([
        _row(hops_to_known_mule=1),
        _row(credits_from_complainants_30d=2),
        _row(pass_through_ratio_7d=0.9),
        _row(ring_size=6),
        _row(accounts_on_same_device=4),
    ])
    fired = fired_rules(df, RULES)
    assert fired.loc[0, "NEAR_KNOWN_MULE"]
    assert fired.loc[1, "COMPLAINANT_CREDITS"]
    assert fired.loc[2, "PASS_THROUGH"]
    assert fired.loc[3, "RING"]
    assert fired.loc[4, "SHARED_DEVICE"]
    # nothing else spuriously fires on an otherwise-clean row
    assert fired.loc[0].sum() == 1
    assert fired.loc[4].sum() == 1


def test_rules_score_orders_by_number_of_flags():
    clean = pd.DataFrame([_row()])
    flagged = pd.DataFrame([_row(hops_to_known_mule=1, ring_size=6, accounts_on_same_device=4)])
    assert rules_score(flagged, RULES)[0] > rules_score(clean, RULES)[0]


def test_reasons_text_matches_fired_codes():
    df = pd.DataFrame([_row(ring_size=6, hops_to_known_mule=1)])
    codes, texts = reasons(df, RULES, top=4)
    fired_codes = codes.iloc[0].split(",")
    assert "NEAR_KNOWN_MULE" in fired_codes
    assert "RING" in fired_codes
    assert "hop(s)" in texts.iloc[0]


def test_reasons_caps_at_top_n():
    df = pd.DataFrame([_row(hops_to_known_mule=1, credits_from_complainants_30d=2, pass_through_ratio_7d=0.9,
                             ring_size=6, accounts_on_same_device=4, cifs_sharing_mobile=3)])
    codes, _ = reasons(df, RULES, top=2)
    assert len(codes.iloc[0].split(",")) == 2

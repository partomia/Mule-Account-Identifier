"""Alert tiers and plain-language reason codes.

Tiers are percentiles of p_mule_adj across the whole scored book, with
cut-offs from config/policy.yaml (fraud-risk settings, not model settings), so
they come from the daily run rather than from a single-account request.

Reason codes are business rules on the inputs, not an explanation of the
model's score: they tell an investigator where to look first. The same rules,
counted, are the rules-only baseline the holdout compares the model against.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from mule.config import policy

NO_TIER = "NONE"


def tiers_from_policy() -> list[dict]:
    return policy()["tiers"]


def rules_from_policy() -> dict:
    return policy()["rules"]


def assign_tiers(risk_pct: pd.Series, tiers: list[dict]) -> tuple[pd.Series, pd.Series]:
    """risk_pct in (0, 1], small = riskiest. Returns (tier, action); NONE below the last tier."""
    pct = risk_pct.to_numpy()
    codes = np.full(len(pct), NO_TIER, dtype=object)
    actions = np.full(len(pct), "", dtype=object)
    for t in reversed(tiers):
        mask = pct <= float(t["upto_pct"])
        codes[mask] = t["code"]
        actions[mask] = t.get("action", "")
    return pd.Series(codes, index=risk_pct.index), pd.Series(actions, index=risk_pct.index)


# (code, text) in the order an investigator should read them.
REASONS = [
    ("NEAR_KNOWN_MULE", "{hops} hop(s) from a reported / frozen mule"),
    ("COMPLAINANT_CREDITS", "{credits_from_complainants_30d} credit(s) from 1930 / NCRP complainants in 30d"),
    ("PASS_THROUGH", "forwards {pass_pct}% of credits within 24h"),
    ("FAST_HOLD", "median {median_hold_hours_30d:.1f}h from credit to next debit"),
    ("RING", "in a linked ring of {ring_size} customers"),
    ("SHARED_DEVICE", "device shared by {accounts_on_same_device} accounts"),
    ("SHARED_MOBILE", "mobile shared by {cifs_sharing_mobile} customers"),
    ("DEVICE_OR_MOBILE_CHANGE", "{changes} new device / mobile / VPA change(s) in 30d"),
    ("INFLOW_VS_INCOME", "30d credits {inflow_to_declared_income_30d:.1f}x declared monthly income"),
    ("MANY_SENDERS", "{distinct_senders_7d} distinct senders in 7d"),
    ("DORMANT_REACTIVATED", "dormant account reactivated in last 90d"),
    ("NEW_ACCOUNT", "account {account_age_days} days old"),
    ("MIN_KYC", "minimum-KYC (OTP) account"),
    ("NIGHT_ACTIVITY", "{night_pct}% of transactions 23:00-05:00"),
]
TEXT = dict(REASONS)


def fired_rules(df: pd.DataFrame, rules: dict | None = None) -> pd.DataFrame:
    """Boolean frame: one column per reason code."""
    r = rules or rules_from_policy()
    f = lambda c: df[c].astype(float)  # noqa: E731
    return pd.DataFrame({
        "NEAR_KNOWN_MULE": f("hops_to_known_mule") <= 2,
        "COMPLAINANT_CREDITS": f("credits_from_complainants_30d") >= 1,
        "PASS_THROUGH": f("pass_through_ratio_7d") >= float(r["pass_through_ratio"]),
        "FAST_HOLD": (f("median_hold_hours_30d") <= float(r["hold_hours"])),
        "RING": f("ring_size") >= float(r["ring_size"]),
        "SHARED_DEVICE": f("accounts_on_same_device") >= float(r["shared_device_accounts"]),
        "SHARED_MOBILE": f("cifs_sharing_mobile") >= float(r["shared_mobile_cifs"]),
        "DEVICE_OR_MOBILE_CHANGE": (f("new_device_logins_30d") + f("vpa_or_mobile_changes_30d"))
                                   >= float(r["device_or_mobile_changes"]),
        "INFLOW_VS_INCOME": f("inflow_to_declared_income_30d") >= float(r["inflow_multiple"]),
        "MANY_SENDERS": f("distinct_senders_7d") >= float(r["distinct_senders_7d"]),
        "DORMANT_REACTIVATED": f("dormant_reactivated_flag") >= 1,
        "NEW_ACCOUNT": f("account_age_days") <= float(r["new_account_days"]),
        "MIN_KYC": f("min_kyc_flag") >= 1,
        "NIGHT_ACTIVITY": f("night_txn_share_30d") >= float(r["night_share"]),
    }, index=df.index)


def rules_score(df: pd.DataFrame, rules: dict | None = None) -> np.ndarray:
    """Rules-only baseline: how many red-flag rules fire. Ties are broken by the
    strongest single flags (proximity to a known mule, then pass-through)."""
    fired = fired_rules(df, rules)
    tie = (3 - df["hops_to_known_mule"].astype(float).clip(0, 3)) / 10 \
        + df["pass_through_ratio_7d"].astype(float).clip(0, 1) / 100
    return fired.sum(axis=1).to_numpy(dtype=float) + tie.to_numpy(dtype=float)


def reasons(df: pd.DataFrame, rules: dict | None = None, top: int = 4) -> tuple[pd.Series, pd.Series]:
    """(reason_codes, reasons): up to `top` fired rules per row, as codes and as text."""
    fired = fired_rules(df, rules)
    codes_out, text_out = [], []
    for idx, row in df.iterrows():
        codes = [c for c, _ in REASONS if fired.at[idx, c]][:top]
        vals = {k: row[k] for k in row.index}
        vals.update(hops=int(row["hops_to_known_mule"]),
                    pass_pct=int(round(100 * float(row["pass_through_ratio_7d"]))),
                    night_pct=int(round(100 * float(row["night_txn_share_30d"]))),
                    changes=int(row["new_device_logins_30d"] + row["vpa_or_mobile_changes_30d"]))
        for k in ("credits_from_complainants_30d", "ring_size", "accounts_on_same_device", "cifs_sharing_mobile",
                  "distinct_senders_7d", "account_age_days"):
            vals[k] = int(row[k])
        codes_out.append(",".join(codes))
        text_out.append("; ".join(TEXT[c].format(**vals) for c in codes))
    return pd.Series(codes_out, index=df.index), pd.Series(text_out, index=df.index)

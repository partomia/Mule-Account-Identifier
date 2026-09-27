"""Holdout check and KPI gate: score the latest labelled weeks with a context
built only from what was known then.

The test set is the latest `test_weeks` labelled snapshots. Context rows come
from snapshots at least `gap_days` before the first test snapshot, so every
context label was observed before the test date, as if the model had been
deployed on that day.

Mules are rare (well under 1% of the book), so the test set is every mule in
the test weeks plus a hash sample of the rest; each sampled non-mule carries
weight 1 / sample share, and every metric below is weighted back to the full
book. "Top 1%" means the riskiest 1% of the book, not of the sample.

The gate is the workshop's "accuracy lies" lesson: at this mule rate a model
that flags nobody is >99.9% accurate, so it measures capture (share of actual
mules in the top of the queue), precision of the queue, and lift over the
rules the bank already runs.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from mule.features import LABEL

BANDS = [("top 0.2%", 0.002), ("0.2-1%", 0.01), ("1-3%", 0.03), ("3-5%", 0.05), ("5-10%", 0.10),
         ("10-25%", 0.25), ("rest", 1.0)]


def test_window(labelled_dates: list[date], test_weeks: int, gap_days: int) -> tuple[date, date, date]:
    """(test_from, test_to, context_to) from the distinct labelled snapshot dates."""
    dates = sorted(set(labelled_dates))
    if len(dates) < test_weeks + 1:
        raise ValueError(f"need more than {test_weeks} labelled snapshots, have {len(dates)}")
    test = dates[-test_weeks:]
    return test[0], test[-1], test[0] - timedelta(days=gap_days)


def _top_mask(score: np.ndarray, weight: np.ndarray, share: float) -> np.ndarray:
    """Rows in the riskiest `share` of the weighted population (ties broken by row order)."""
    order = np.argsort(-score, kind="stable")
    cum = np.cumsum(weight[order])
    k = max(1, int(np.searchsorted(cum, share * cum[-1], side="right")))
    mask = np.zeros(len(score), dtype=bool)
    mask[order[:k]] = True
    return mask


def capture_at(y, score, share: float, weight=None) -> float | None:
    """Share of actual mules in the top `share` of the book by score."""
    y, score = np.asarray(y, dtype=float), np.asarray(score, dtype=float)
    w = np.ones(len(y)) if weight is None else np.asarray(weight, dtype=float)
    if (y * w).sum() <= 0:
        return None
    top = _top_mask(score, w, share)
    return float((y * w)[top].sum() / (y * w).sum())


def precision_at(y, score, share: float, weight=None) -> float | None:
    """Share of the top `share` of the book that are actual mules."""
    y, score = np.asarray(y, dtype=float), np.asarray(score, dtype=float)
    w = np.ones(len(y)) if weight is None else np.asarray(weight, dtype=float)
    if len(y) == 0:
        return None
    top = _top_mask(score, w, share)
    return float((y * w)[top].sum() / w[top].sum())


def auc(y, p, weight=None) -> float | None:
    from sklearn.metrics import roc_auc_score

    y = np.asarray(y, dtype=int)
    if y.min() == y.max():
        return None
    return float(roc_auc_score(y, p, sample_weight=weight))


def pr_auc(y, p, weight=None) -> float | None:
    from sklearn.metrics import average_precision_score

    y = np.asarray(y, dtype=int)
    if y.max() == 0:
        return None
    return float(average_precision_score(y, p, sample_weight=weight))


def summarise(test: pd.DataFrame, p: np.ndarray, rules: np.ndarray) -> dict:
    y = test[LABEL].to_numpy(dtype=int)
    w = test["weight"].to_numpy(dtype=float)
    r = lambda x: None if x is None else round(x, 4)  # noqa: E731
    cap, rcap = capture_at(y, p, 0.01, w), capture_at(y, rules, 0.01, w)
    return {
        "holdout_rows": int(len(test)),
        "holdout_mules": int(y.sum()),
        "holdout_mule_rate": r(float((y * w).sum() / w.sum())) if len(y) else None,
        "holdout_auc": r(auc(y, p, w)),
        "holdout_pr_auc": r(pr_auc(y, p, w)),
        "capture_top1": r(cap),
        "capture_top5": r(capture_at(y, p, 0.05, w)),
        "precision_top02": r(precision_at(y, p, 0.002, w)),
        "precision_top1": r(precision_at(y, p, 0.01, w)),
        "rules_capture_top1": r(rcap),
        "rules_precision_top1": r(precision_at(y, rules, 0.01, w)),
        "lift_over_rules": r(cap / rcap) if cap is not None and rcap else None,
    }


def bands(test: pd.DataFrame, p: np.ndarray, rules: np.ndarray) -> pd.DataFrame:
    """Weighted accounts, mules and cumulative capture by risk band (model and rules-only)."""
    y = test[LABEL].to_numpy(dtype=float)
    w = test["weight"].to_numpy(dtype=float)
    total_w, total_m = w.sum(), max((y * w).sum(), 1e-9)

    def cum_share(score: np.ndarray, upto: float) -> float:
        return float((y * w)[_top_mask(score, w, upto)].sum() / total_m) if upto < 1 else 1.0

    order = np.argsort(-p, kind="stable")
    cum_w = np.cumsum(w[order]) / total_w
    rows, lo = [], 0.0
    for name, upto in BANDS:
        sel = order[(cum_w > lo) & (cum_w <= upto + 1e-12)] if upto < 1 else order[cum_w > lo]
        acc, mules = float(w[sel].sum()), float((y * w)[sel].sum())
        rows.append({"band": name, "upto_pct": upto, "accounts": round(acc, 1), "mules": round(mules, 1),
                     "mule_rate": round(mules / acc, 5) if acc else None,
                     "mean_p_mule": round(float(np.average(p[sel], weights=w[sel])), 5) if acc else None,
                     "cum_capture": round(cum_share(p, upto), 4), "rules_cum_capture": round(cum_share(rules, upto), 4)})
        lo = upto
    return pd.DataFrame(rows)


def gate(summary: dict, cfg: dict) -> tuple[bool, list[str]]:
    """(passed, one line per check). A missing metric fails its check."""
    checks = [("capture_top1", "min_capture_top1pct", "capture in top 1%"),
              ("precision_top02", "min_precision_top02pct", "precision in top 0.2% (T1 queue)"),
              ("lift_over_rules", "min_lift_over_rules", "lift over rules-only at top 1%")]
    lines, ok = [], True
    for key, cfg_key, label in checks:
        if cfg_key not in cfg:
            continue
        v, need = summary.get(key), float(cfg[cfg_key])
        passed = v is not None and v >= need
        ok &= passed
        lines.append(f"{'PASS' if passed else 'FAIL'} {label}: {'n/a' if v is None else f'{v:.3f}'} (min {need:.3f})")
    return ok, lines

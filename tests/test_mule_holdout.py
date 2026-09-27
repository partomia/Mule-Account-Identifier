"""Weighted holdout metrics and the KPI gate."""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from mule import holdout as ho
from mule.features import LABEL


def test_test_window_takes_the_latest_weeks_with_a_gap():
    dates = [date(2026, 1, 2), date(2026, 1, 9), date(2026, 1, 16), date(2026, 1, 23), date(2026, 1, 30)]
    test_from, test_to, ctx_to = ho.test_window(dates, test_weeks=2, gap_days=7)
    assert (test_from, test_to) == (date(2026, 1, 23), date(2026, 1, 30))
    assert ctx_to == date(2026, 1, 16)


def test_test_window_needs_enough_history():
    with pytest.raises(ValueError):
        ho.test_window([date(2026, 1, 2), date(2026, 1, 9)], test_weeks=4, gap_days=7)


def test_capture_and_precision_perfect_ranking():
    y = np.array([1, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    score = np.array([10, 1, 2, 3, 4, 5, 6, 7, 8, 9])  # the one mule ranks highest
    assert ho.capture_at(y, score, 0.1) == 1.0
    assert ho.precision_at(y, score, 0.1) == 1.0


def test_capture_at_is_none_with_no_positives():
    y = np.zeros(10)
    score = np.arange(10)
    assert ho.capture_at(y, score, 0.1) is None


def test_capture_at_respects_weights():
    # a heavily-upweighted mule ranked at the very top is captured even by a tiny share...
    y = np.array([1, 0, 0, 0])
    score = np.array([4.0, 1.0, 2.0, 3.0])
    w = np.array([100.0, 1.0, 1.0, 1.0])
    assert ho.capture_at(y, score, 0.1, weight=w) == 1.0
    # ...but ranked at the very bottom, no share short of the entire book reaches it,
    # because "top share%" is a prefix of the score order regardless of weight.
    score_bottom = np.array([1.0, 2.0, 3.0, 4.0])
    assert ho.capture_at(y, score_bottom, 0.99, weight=w) == 0.0
    assert ho.capture_at(y, score_bottom, 1.0, weight=w) == 1.0


def test_auc_and_pr_auc_none_on_single_class():
    y = np.zeros(10)
    p = np.random.RandomState(0).rand(10)
    assert ho.auc(y, p) is None
    assert ho.pr_auc(y, p) is None


def test_summarise_and_bands_shapes():
    rng = np.random.RandomState(0)
    n = 500
    y = (rng.rand(n) < 0.02).astype(int)
    p = np.clip(y * 0.7 + rng.rand(n) * 0.3, 0, 1)
    rules = rng.rand(n) * 3
    test = pd.DataFrame({LABEL: y, "weight": np.where(y == 1, 1.0, 1 / 0.05)})
    summary = ho.summarise(test, p, rules)
    assert summary["holdout_rows"] == n
    assert summary["holdout_mules"] == int(y.sum())
    assert 0.0 <= summary["capture_top1"] <= 1.0

    bands = ho.bands(test, p, rules)
    assert list(bands["band"]) == [b for b, _ in ho.BANDS]
    assert bands["cum_capture"].iloc[-1] == pytest.approx(1.0)


def test_gate_passes_and_fails():
    summary = {"capture_top1": 0.5, "precision_top02": 0.2, "lift_over_rules": 1.5}
    cfg = {"min_capture_top1pct": 0.4, "min_precision_top02pct": 0.1, "min_lift_over_rules": 1.2}
    ok, lines = ho.gate(summary, cfg)
    assert ok and len(lines) == 3

    cfg_strict = {**cfg, "min_capture_top1pct": 0.9}
    ok, lines = ho.gate(summary, cfg_strict)
    assert not ok
    assert any(line.startswith("FAIL") for line in lines)


def test_gate_fails_closed_on_missing_metric():
    ok, lines = ho.gate({"capture_top1": None}, {"min_capture_top1pct": 0.4})
    assert not ok
    assert "n/a" in lines[0]


def test_gate_ignores_checks_without_a_configured_threshold():
    ok, lines = ho.gate({"capture_top1": 0.5}, {})
    assert ok and lines == []

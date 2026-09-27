"""End-to-end daily run against the parquet backend with the stub model:
context selection, holdout + gate, scoring, tiering, reasons, ring rollup,
and the replace-by-run_date write path.

Tier counts only depend on the size of the scored book and the tier cut-offs
in config/policy.yaml (percentile of rank, not of score), so they are
asserted exactly; the model's own ranking is not.
"""

import copy
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from mule import pipeline as pl
from mule.config import policy as real_policy
from mule.features import COLUMNS, FEATURES, LABEL
from mule.reasons import NO_TIER
from mule.storage import ParquetStorage

N_WEEKS = 10          # weeks 0..8 labelled, week 9 the book to score
N_ACCOUNTS = 200
MULES_PER_WEEK = 6    # 3% of the book


def tiny_policy():
    pol = copy.deepcopy(real_policy())
    pol["label"]["horizon_days"] = 0
    pol["context"].update(lookback_weeks=8, max_mules=50, max_mules_cpu=50, negatives_per_mule=3,
                           book_mule_rate=0.03)
    pol["holdout"].update(test_weeks=1, gap_days=7, negative_sample=1.0)
    pol["gate"] = {"min_capture_top1pct": 0.0, "min_precision_top02pct": 0.0}  # drop the lift check: rcap can be 0
    return pol


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(pl, "load_policy", tiny_policy)
    d0 = date(2026, 1, 2)
    rows = []
    for week in range(N_WEEKS):
        snap = d0 + timedelta(weeks=week)
        labelled = week < N_WEEKS - 1
        for i in range(N_ACCOUNTS):
            row = {c: 0 for c in COLUMNS}
            row.update(account_id=f"acct-{i}", snapshot_date=snap, cif=f"cif-{i}", branch_code="B1",
                       product_code="SAVINGS", person_cluster_id=f"pc-{i}", ring_id=f"ring-{i}", ring_size=1,
                       hops_to_known_mule=3)
            for f in FEATURES:
                row[f] = float((i * 37 + week * 11) % 97) / 97.0
            row[LABEL] = (1 if labelled and i < MULES_PER_WEEK else (None if not labelled else 0))
            rows.append(row)
    df = pd.DataFrame(rows)
    s = ParquetStorage(tmp_path)
    s.dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(s._path("mule_features"), index=False)
    return s


def test_run_daily_end_to_end(storage):
    out = pl.run_daily(storage, family="stub", triggered_by="test", write=True, run_holdout=True,
                       save_context_file=False)

    assert out["summary"]["scored_accounts"] == N_ACCOUNTS
    assert out["summary"]["gate_passed"] is True
    assert out["summary"]["alerts_published"] is True

    # tier counts are a pure function of book size (200) and the real policy cut-offs
    # (0.2% / 1% / 3%): rank <= 0.4 -> none reach T1; ranks 1-2 -> T2; ranks 3-6 -> T3.
    assert len(out["alerts_all"]) == 6
    assert out["summary"]["tiers"] == {"T2_HOLD_MONITOR": 2, "T3_WATCHLIST": 4}
    assert set(out["alerts_all"]["tier"]) == {"T2_HOLD_MONITOR", "T3_WATCHLIST"}
    assert (out["alerts_all"]["reason_codes"] != "").all()  # every alerted account gets at least a rank-based tier

    run_date = storage.latest_snapshot_date()
    written = storage.read("mule_model_run", where_run_date=run_date)
    assert len(written) == 1
    assert written.iloc[0]["scored_accounts"] == N_ACCOUNTS
    assert written.iloc[0]["model_family"] == "stub"

    alerts = storage.read("mule_alerts", where_run_date=run_date)
    assert len(alerts) == 6

    holdout = storage.read("mule_holdout", where_run_date=run_date)
    assert len(holdout) > 0
    assert holdout["accounts"].sum() == pytest.approx(N_ACCOUNTS, abs=1)


def test_gate_failure_withholds_alerts_unless_published_on_fail(storage, monkeypatch):
    strict = tiny_policy()
    strict["gate"] = {"min_capture_top1pct": 1.1}  # impossible to satisfy -> always fails
    monkeypatch.setattr(pl, "load_policy", lambda: strict)

    out = pl.run_daily(storage, family="stub", write=True, run_holdout=True, save_context_file=False)
    assert out["summary"]["gate_passed"] is False
    assert out["summary"]["alerts_published"] is False
    run_date = storage.latest_snapshot_date()
    with pytest.raises(FileNotFoundError):
        storage.read("mule_alerts", where_run_date=run_date)

    out2 = pl.run_daily(storage, family="stub", write=True, run_holdout=True, save_context_file=False,
                        publish_on_fail=True)
    assert out2["summary"]["alerts_published"] is True
    alerts = storage.read("mule_alerts", where_run_date=run_date)
    assert len(alerts) == 6


def test_run_daily_requires_both_classes_in_context(storage, monkeypatch):
    def no_mules():
        pol = tiny_policy()
        pol["context"]["max_mules"] = 0
        return pol

    monkeypatch.setattr(pl, "load_policy", no_mules)
    with pytest.raises(ValueError):
        pl.run_daily(storage, family="stub", write=False, run_holdout=False, save_context_file=False)

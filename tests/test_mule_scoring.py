"""The endpoint's request handling: validation, what-if, and tier assignment
from a daily run's recorded p_mule_adj cut-offs (there is no book to rank
against for a single ad-hoc request)."""

import copy
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from mule.features import FEATURES
from mule.model import StubClassifier, build_model
from mule.reasons import NO_TIER
from mule.scoring import score, tier_from_cutoffs, validate

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "cai" / "model"))
from test_endpoint import EXAMPLE  # noqa: E402

META = {"model_id": "stub", "context_mule_rate": 0.5, "book_mule_rate": 0.5,
        "t1_cutoff": 0.9, "t2_cutoff": 0.7, "t3_cutoff": 0.5}


@pytest.fixture(scope="module")
def clf():
    # centred on the EXAMPLE account's realistic scale (StandardScaler on a fixture
    # of pure U(0,1) features would saturate the stub on account_age_days=210 etc.)
    rng = np.random.RandomState(0)
    base = EXAMPLE["accounts"][0]
    n = 300
    df = pd.DataFrame({f: rng.uniform(0, 2 * base[f] + 1, n) for f in FEATURES})
    df["pass_through_ratio_7d"] = rng.rand(n)  # the one feature that drives the label
    df["is_mule_90d"] = (df["pass_through_ratio_7d"] > 0.5).astype(int)
    return build_model(df, StubClassifier)


def test_example_request_scores(clf):
    resp = score(EXAMPLE, clf, META)
    s = resp["scores"][0]
    assert s["account_id"] == "ACC-04821193"
    assert 0 <= s["p_mule"] <= 1 and 0 <= s["p_mule_adj"] <= 1
    assert s["tier"] in {"T1_FREEZE_REVIEW", "T2_HOLD_MONITOR", "T3_WATCHLIST", NO_TIER}
    assert resp["model"]["model_id"] == "stub"
    # the what-if raises pass-through to 0.92, the only signal in the synthetic fixture
    assert s["p_mule_what_if"] > s["p_mule"]


def test_no_what_if_fields_without_what_if(clf):
    req = {"accounts": EXAMPLE["accounts"]}
    assert "p_mule_what_if" not in score(req, clf, META)["scores"][0]


def test_reason_codes_reflect_the_inputs(clf):
    s = score(EXAMPLE, clf, META)["scores"][0]
    assert "PASS_THROUGH" not in s["reason_codes"]          # 0.55 is below the 0.8 rule threshold
    assert "PASS_THROUGH" in s["reason_codes_what_if"]       # 0.92 fires it


@pytest.mark.parametrize("mutate, message", [
    (lambda r: r.pop("accounts"), "send"),
    (lambda r: r["accounts"][0].pop("ring_size"), "missing fields"),
    (lambda r: r["accounts"][0].update(ring_size="x"), "numbers"),
    (lambda r: r["accounts"][0].update(ring_size=float("nan")), "finite"),
    (lambda r: r.update(what_if={"not_a_feature": 1}), "unknown"),
])
def test_validation_errors(mutate, message):
    req = copy.deepcopy(EXAMPLE)
    mutate(req)
    err = validate(req)
    assert err and message in err


def test_example_has_every_feature():
    assert set(FEATURES) <= set(EXAMPLE["accounts"][0])


def test_tier_from_cutoffs_checks_tightest_first():
    tiers = [{"code": "T1_FREEZE_REVIEW"}, {"code": "T2_HOLD_MONITOR"}, {"code": "T3_WATCHLIST"}]
    cutoffs = {"T1_FREEZE_REVIEW": 0.9, "T2_HOLD_MONITOR": 0.7, "T3_WATCHLIST": 0.5}
    assert tier_from_cutoffs(0.95, cutoffs, tiers)[0] == "T1_FREEZE_REVIEW"
    assert tier_from_cutoffs(0.75, cutoffs, tiers)[0] == "T2_HOLD_MONITOR"
    assert tier_from_cutoffs(0.55, cutoffs, tiers)[0] == "T3_WATCHLIST"
    assert tier_from_cutoffs(0.1, cutoffs, tiers)[0] == NO_TIER


def test_tier_from_cutoffs_skips_a_missing_cutoff():
    tiers = [{"code": "T1_FREEZE_REVIEW"}, {"code": "T2_HOLD_MONITOR"}]
    cutoffs = {"T1_FREEZE_REVIEW": None, "T2_HOLD_MONITOR": 0.7}
    assert tier_from_cutoffs(0.95, cutoffs, tiers)[0] == "T2_HOLD_MONITOR"


class _FakeImpalaStorage:
    """A pandas-native mule_model_run row, the shape ImpalaStorage.read() returns
    (numeric columns as numpy floats, not the JSON-deserialised floats/strings the
    file backend already gave us) - the impala context path only ever ran for
    real for the first time on live CDW, where it hit exactly this bug."""

    def read(self, key):
        assert key == "mule_model_run"
        return pd.DataFrame([{
            "run_id": "20260925-abc123", "run_date": pd.Timestamp("2026-09-25"),
            "run_ts": pd.Timestamp("2026-09-25 21:23:00"), "context_from": pd.Timestamp("2025-12-26"),
            "context_to": pd.Timestamp("2026-06-26"), "source_snapshot_id": "591936169293103544",
            "context_mules": 2000, "context_negatives": 6000, "context_mule_rate": 0.25,
            "book_mule_rate": 0.0006970776360641925, "t1_cutoff": 0.001728, "t2_cutoff": 0.000249,
            "t3_cutoff": 0.000132,
        }])

    def context(self, *, date_from, date_to, max_mules, negatives_per_mule, snapshot_id):
        row = {c: 0.0 for c in FEATURES}
        row.update(account_id="ACC-1", snapshot_date=date_to, cif="CIF-1", branch_code="B1",
                   product_code="SAVINGS", person_cluster_id="PC-1", ring_id="R-1", is_mule_90d=1.0)
        return pd.DataFrame([row])


def test_load_context_impala_keeps_rates_and_cutoffs_numeric(monkeypatch):
    """Regression: load_context's impala branch once ran every mule_model_run
    field through str(), including context_mule_rate / book_mule_rate / the
    tier cut-offs - prior_correct() then got a string and TypeError'd deep
    inside odds_ratio's `0.0 < v < 1.0`. Only caught by a live Impala run
    (the file-backed context path never had this bug, since json.loads
    already gives back real floats)."""
    import mule.storage as storage_mod
    from mule.scoring import load_context

    monkeypatch.setattr(storage_mod, "get_storage", lambda backend: _FakeImpalaStorage())
    ctx, meta = load_context("impala")
    assert len(ctx) == 1
    assert isinstance(meta["context_mule_rate"], float) and meta["context_mule_rate"] == 0.25
    assert isinstance(meta["book_mule_rate"], float)
    assert isinstance(meta["t1_cutoff"], float)
    assert meta["run_id"] == "20260925-abc123"       # ids/dates still come back as strings
    assert meta["context_from"] == "2025-12-26 00:00:00"

    clf = build_model(pd.DataFrame([{**{f: 0.0 for f in FEATURES}, "is_mule_90d": 0},
                                    {**{f: 1.0 for f in FEATURES}, "is_mule_90d": 1}]), StubClassifier)
    resp = score(EXAMPLE, clf, meta)   # would have raised the TypeError above the fix
    assert 0 <= resp["scores"][0]["p_mule_adj"] <= 1

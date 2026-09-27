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

"""Model family resolution and the stub classifier used by tests / CI."""

import numpy as np
import pandas as pd
import pytest

from mule import model as m
from mule.features import FEATURES, LABEL


def test_resolve_family_rejects_unknown():
    with pytest.raises(ValueError):
        m.resolve_family("not-a-family")


def test_resolve_family_explicit_passthrough():
    assert m.resolve_family("stub") == "stub"
    assert m.resolve_family("tabicl") == "tabicl"


def test_resolve_family_auto_depends_on_cuda(monkeypatch):
    monkeypatch.setattr(m, "has_cuda", lambda: True)
    assert m.resolve_family("auto") == "mitra"
    monkeypatch.setattr(m, "has_cuda", lambda: False)
    assert m.resolve_family("auto") == "tabicl"


def test_stub_model_id_and_version():
    assert m.model_id("stub") == "sklearn/logistic-regression-stub"
    assert m.model_version("stub").startswith("scikit-learn")


def _synthetic(n=200, seed=0):
    rng = np.random.RandomState(seed)
    df = pd.DataFrame({f: rng.rand(n) for f in FEATURES})
    # a clean linear signal on one feature so the stub can actually separate classes
    df[LABEL] = (df[FEATURES[0]] > 0.5).astype(int)
    return df


def test_build_model_and_predict_mule_roundtrip():
    df = _synthetic()
    clf = m.build_model(df, factory=lambda **kw: m.StubClassifier())
    p = m.predict_mule(clf, df, batch=64)
    assert p.shape == (len(df),)
    assert ((p >= 0) & (p <= 1)).all()
    # the stub should recover a strong linear signal reasonably well
    from sklearn.metrics import roc_auc_score
    assert roc_auc_score(df[LABEL], p) > 0.9


def test_predict_mule_batches_do_not_change_the_result():
    df = _synthetic()
    clf = m.build_model(df, factory=lambda **kw: m.StubClassifier())
    whole = m.predict_mule(clf, df, batch=10_000)
    batched = m.predict_mule(clf, df, batch=7)
    assert np.allclose(whole, batched)


def test_device_name_falls_back_to_device_string():
    clf = m.StubClassifier()
    assert m.device_name(clf) == "cpu"

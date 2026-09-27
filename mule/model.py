"""Tabular foundation model setup: Mitra-v2 on a GPU, TabICL v2 on CPU.

Both learn in context: fit() only stores the labelled rows, the learning
happens inside predict_proba. So "the model" is the pinned weights plus the
context rows, and both are recorded for every run.

  mitra   autogluon/mitra-classifier-2 through AutoGluon's sklearn interface,
          with fine-tuning off (pure in-context). Needs CUDA to be usable:
          ~5 rows/s on CPU at a 2,000-row context (measured).
  tabicl  TabICL v2 (pinned checkpoint), fast enough on CPU for the daily book.
  stub    logistic regression, for tests, CI and a light app host.

model.family: auto picks mitra when CUDA is available, else tabicl.
"""

from __future__ import annotations

import logging
from importlib.metadata import PackageNotFoundError, version

import numpy as np
import pandas as pd

from mule.config import policy, settings
from mule.features import FEATURES, LABEL

logger = logging.getLogger(__name__)

FAMILIES = ("auto", "mitra", "tabicl", "stub")
TABICL_REPO = "jingang/TabICL"
MITRA_MAX_CONTEXT = 8192


def has_cuda() -> bool:
    try:
        import torch

        return torch.cuda.is_available()
    except ImportError:
        return False


def resolve_family(family: str | None = None) -> str:
    family = (family or settings()["model"]["family"]).lower()
    if family not in FAMILIES:
        raise ValueError(f"model.family must be one of {FAMILIES}, got {family!r}")
    if family == "auto":
        return "mitra" if has_cuda() else "tabicl"
    return family


def device() -> str:
    return settings()["model"]["device"] or ("cuda" if has_cuda() else "cpu")


def max_mules() -> int:
    """Context mules: the full budget on a GPU, a quarter of it on CPU (TabICL scoring cost grows with context)."""
    ctx = policy()["context"]
    n = int(ctx["max_mules"])
    return n if has_cuda() else int(ctx.get("max_mules_cpu", n // 4))


def _pkg(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def model_id(family: str | None = None) -> str:
    fam = resolve_family(family)
    cfg = settings()["model"]
    if fam == "mitra":
        return cfg["hf_model"]
    if fam == "tabicl":
        return f"{TABICL_REPO}/{cfg['tabicl_checkpoint']}"
    return "sklearn/logistic-regression-stub"


def model_version(family: str | None = None) -> str | None:
    fam = resolve_family(family)
    if fam == "mitra":
        return f"autogluon.tabular {_pkg('autogluon.tabular')}"
    if fam == "tabicl":
        return f"tabicl {_pkg('tabicl')}"
    return f"scikit-learn {_pkg('scikit-learn')}"


class MitraInContext:
    """Mitra-v2 as a plain in-context classifier: fit stores the context,
    predict_proba conditions on it. No fine-tuning and no validation split,
    so every context row is used and the result does not depend on a split."""

    def __init__(self, **_):
        from autogluon.tabular.models.mitra.sklearn_interface import MitraClassifier

        cfg = settings()["model"]
        self.device_ = device()
        self._clf = MitraClassifier(model_type="Tab2D", n_estimators=int(cfg["n_estimators"]), device=self.device_,
                                    fine_tune=False, hf_model=cfg["hf_model"], seed=0, verbose=False)

    def fit(self, X, y):
        X = pd.DataFrame(X).reset_index(drop=True)
        if len(X) > MITRA_MAX_CONTEXT:
            raise ValueError(f"Mitra supports at most {MITRA_MAX_CONTEXT} context rows, got {len(X)}")
        self._clf.fit(X, pd.Series(np.asarray(y, dtype=int)))
        return self

    def predict_proba(self, X):
        return np.asarray(self._clf.predict_proba(pd.DataFrame(X).reset_index(drop=True)), dtype=float)


def _tabicl(kv_cache: bool | str = False):
    from tabicl import TabICLClassifier

    cfg = settings()["model"]
    kwargs = dict(n_estimators=int(cfg["tabicl_n_estimators"]), checkpoint_version=cfg["tabicl_checkpoint"],
                  random_state=42, kv_cache=kv_cache)
    if cfg["device"]:
        kwargs["device"] = cfg["device"]
    return TabICLClassifier(**kwargs)


class StubClassifier:
    """Stand-in with the fit / predict_proba interface, for tests and quick
    smoke runs without model weights: logistic regression on standardised
    features."""

    device_ = "cpu"

    def __init__(self, **_):
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler

        self._clf = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000, C=0.5))

    def fit(self, X, y):
        self._clf.fit(np.asarray(X, dtype=float), np.asarray(y, dtype=int))
        return self

    def predict_proba(self, X):
        return self._clf.predict_proba(np.asarray(X, dtype=float))


def new_classifier(family: str | None = None, kv_cache: bool | str = False):
    fam = resolve_family(family)
    if fam == "mitra":
        return MitraInContext()
    if fam == "tabicl":
        return _tabicl(kv_cache)
    return StubClassifier()


def factory_for(family: str | None = None):
    """A no-argument-compatible factory for the given family, for run_daily / load_scorer."""
    fam = resolve_family(family)
    return lambda **kw: new_classifier(fam, **kw)


def build_model(ctx: pd.DataFrame, factory=new_classifier, **kwargs):
    """Fit on the context rows: stores them, no training."""
    clf = factory(**kwargs)
    clf.fit(ctx[FEATURES].astype(float), ctx[LABEL].astype(int))
    return clf


def predict_mule(clf, df: pd.DataFrame, batch: int | None = None) -> np.ndarray:
    """P(mule) against the context's class balance, scored in batches to bound GPU memory."""
    batch = batch or int(settings()["model"]["batch_rows"])
    X = df[FEATURES].astype(float)
    out = [np.asarray(clf.predict_proba(X.iloc[i:i + batch]))[:, 1] for i in range(0, len(X), batch)]
    return np.concatenate(out) if out else np.array([])


def device_name(clf) -> str:
    dev = getattr(clf, "device_", None) or getattr(clf, "device", None)
    if dev is not None and not isinstance(dev, str):
        dev = getattr(dev, "type", str(dev))
    return str(dev or device())

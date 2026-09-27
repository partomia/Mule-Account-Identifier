"""Request handling shared by the CAI model endpoint and any in-process caller.

Request:
  {"accounts": [{"account_id": "ACC-1", <18 features from mule.features.FEATURES>}],
   "what_if": {"new_device_logins_30d": 2, "pass_through_ratio_7d": 0.9}}   # optional
Response:
  {"scores": [{"account_id", "p_mule", "p_mule_adj", "tier", "action",
               "reason_codes", "reasons",
               "p_mule_what_if", "p_mule_adj_what_if", "tier_what_if", "action_what_if",
               "reason_codes_what_if", "reasons_what_if"}],   # what-if fields only if sent
   "model": {"model_id", "run_id", "context_rows", "context_to", "source_snapshot_id"}}

Tiers here come from the p_mule_adj cut-offs the latest daily run recorded
(mule.pipeline.run_daily), not from a percentile rank: a single request has no
book to rank against. A tier with no accounts on the day the cut-off was
recorded has no cut-off and is treated as unreachable for that request.
"""

from __future__ import annotations

import json
import logging
import math

import pandas as pd

from mule.calibrate import prior_correct
from mule.features import COLUMNS, FEATURES
from mule.model import build_model, model_id, new_classifier, predict_mule
from mule.reasons import NO_TIER, reasons, tiers_from_policy

logger = logging.getLogger(__name__)

MAX_ACCOUNTS = 1000
META_KEYS = ("run_id", "run_date", "context_from", "context_to", "source_snapshot_id", "context_mule_rate",
             "book_mule_rate", "t1_cutoff", "t2_cutoff", "t3_cutoff")


def validate(req: dict) -> str | None:
    accounts = req.get("accounts") if isinstance(req, dict) else None
    if not accounts or not isinstance(accounts, list):
        return "send {'accounts': [ {...}, ... ]}"
    if len(accounts) > MAX_ACCOUNTS:
        return f"at most {MAX_ACCOUNTS} accounts per request"
    for i, acct in enumerate(accounts):
        if not isinstance(acct, dict):
            return f"accounts[{i}] must be an object"
        missing = [f for f in FEATURES if f not in acct]
        if missing:
            return f"accounts[{i}] ({acct.get('account_id', '?')}): missing fields {missing}"
        try:
            if any(not math.isfinite(float(acct[f])) for f in FEATURES):
                return f"accounts[{i}]: feature values must be finite numbers"
        except (TypeError, ValueError):
            return f"accounts[{i}]: feature values must be numbers"
    what_if = req.get("what_if")
    if what_if is not None:
        if not isinstance(what_if, dict) or not what_if:
            return "what_if must be an object of feature overrides"
        unknown = [k for k in what_if if k not in FEATURES]
        if unknown:
            return f"what_if: unknown features {unknown}"
        try:
            [float(v) for v in what_if.values()]
        except (TypeError, ValueError):
            return "what_if values must be numbers"
    return None


def _frame(accounts: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(accounts)
    if "account_id" not in df:
        df["account_id"] = [f"account-{i}" for i in range(len(df))]
    df[FEATURES] = df[FEATURES].astype(float)
    return df


def _cutoffs(meta: dict) -> dict:
    return {"T1_FREEZE_REVIEW": meta.get("t1_cutoff"), "T2_HOLD_MONITOR": meta.get("t2_cutoff"),
            "T3_WATCHLIST": meta.get("t3_cutoff")}


def tier_from_cutoffs(p_adj: float, cutoffs: dict, tiers: list[dict]) -> tuple[str, str]:
    """Highest tier whose recorded cut-off this probability reaches (T1 checked
    first: cut-offs fall T1 >= T2 >= T3, so the first match is the tightest)."""
    for t in tiers:
        c = cutoffs.get(t["code"])
        if c is not None and p_adj >= c:
            return t["code"], t.get("action", "")
    return NO_TIER, ""


def _score_one(df: pd.DataFrame, clf, ctx_rate: float | None, book_rate: float | None) -> tuple:
    p = predict_mule(clf, df)
    p_adj = prior_correct(p, ctx_rate, book_rate) if ctx_rate and book_rate else p
    codes, texts = reasons(df)
    return p, p_adj, codes, texts


def score(req: dict, clf, meta: dict | None = None) -> dict:
    meta = meta or {"model_id": model_id()}
    err = validate(req)
    if err:
        return {"error": err}
    df = _frame(req["accounts"])
    tiers, cutoffs = tiers_from_policy(), _cutoffs(meta)
    ctx_rate, book_rate = meta.get("context_mule_rate"), meta.get("book_mule_rate")
    p, p_adj, codes, texts = _score_one(df, clf, ctx_rate, book_rate)

    what_if = req.get("what_if")
    if what_if:
        alt = df.copy()
        for k, v in what_if.items():
            alt[k] = float(v)
        p_wi, p_adj_wi, codes_wi, texts_wi = _score_one(alt, clf, ctx_rate, book_rate)

    scores = []
    for i, row in enumerate(df.itertuples()):
        tier, action = tier_from_cutoffs(float(p_adj[i]), cutoffs, tiers)
        s = {"account_id": str(row.account_id), "p_mule": round(float(p[i]), 5),
             "p_mule_adj": round(float(p_adj[i]), 6), "tier": tier, "action": action,
             "reason_codes": codes.iloc[i], "reasons": texts.iloc[i]}
        if what_if:
            tier_wi, action_wi = tier_from_cutoffs(float(p_adj_wi[i]), cutoffs, tiers)
            s.update(p_mule_what_if=round(float(p_wi[i]), 5), p_mule_adj_what_if=round(float(p_adj_wi[i]), 6),
                     tier_what_if=tier_wi, action_what_if=action_wi, reason_codes_what_if=codes_wi.iloc[i],
                     reasons_what_if=texts_wi.iloc[i])
        scores.append(s)
    return {"scores": scores, "model": {k: meta.get(k) for k in
                                        ("model_id", "run_id", "context_rows", "context_to", "source_snapshot_id")}}


def load_context(source: str = "auto") -> tuple[pd.DataFrame, dict]:
    """Context for the endpoint: rebuilt from gold exactly as the latest daily
    run selected it (same Iceberg snapshot, window and row counts), or the
    parquet file that run saved.

    source: impala | file | auto (impala if credentials are set, else file).
    """
    from mule.config import policy, settings
    from mule.pipeline import CONTEXT_FILE

    imp = settings()["impala"]
    if source == "impala" or (source == "auto" and imp["user"] and imp["password"]):
        from mule.storage import get_storage

        storage = get_storage("impala")
        runs = storage.read("mule_model_run")
        if runs.empty:
            raise RuntimeError("no daily run in mule_model_run yet: run cai/jobs/daily_score.py first")
        run = runs.sort_values(["run_date", "run_ts"]).iloc[-1]  # backfills write older dates later
        n_mules = int(run["context_mules"]) if run["context_mules"] else 0
        neg_per_mule = (round(int(run["context_negatives"]) / n_mules) if n_mules
                        else int(policy()["context"]["negatives_per_mule"]))
        ctx = storage.context(date_from=pd.Timestamp(run["context_from"]).date(),
                              date_to=pd.Timestamp(run["context_to"]).date(), max_mules=n_mules,
                              negatives_per_mule=neg_per_mule, snapshot_id=run["source_snapshot_id"] or None)
        meta = {k: (str(run[k]) if run[k] is not None and not pd.isna(run[k]) else None) for k in META_KEYS}
        meta["source"] = "impala"
    else:
        if not CONTEXT_FILE.exists():
            raise RuntimeError(f"{CONTEXT_FILE} not found: run cai/jobs/daily_score.py first")
        ctx = pd.read_parquet(CONTEXT_FILE)
        meta_file = CONTEXT_FILE.with_suffix(".json")
        raw = json.loads(meta_file.read_text()) if meta_file.exists() else {}
        meta = {k: raw.get(k) for k in META_KEYS}
        meta["source"] = "file"
    meta.update(model_id=model_id(), context_rows=len(ctx))
    logger.info("endpoint context: %d rows from %s (run %s)", len(ctx), meta["source"], meta.get("run_id"))
    return ctx[COLUMNS], meta


def load_scorer(source: str = "auto", factory=new_classifier, kv_cache: bool | str | None = None):
    """(classifier fitted on the context, meta). With kv_cache the context is
    encoded once at startup, so each request only pays for its own rows
    (TabICL only; ignored by Mitra and the stub)."""
    from mule.config import settings

    ctx, meta = load_context(source)
    if kv_cache is None:
        kv_cache = settings()["model"]["endpoint_kv_cache"] or False
    kwargs = {"kv_cache": kv_cache} if factory is new_classifier else {}
    return build_model(ctx, factory, **kwargs), meta

"""Daily scoring run: holdout + KPI gate, context selection, score today's
book, tier and explain the alerts, roll them up to rings.

A run for run_date D only uses labels known on D: snapshots up to D - 90 days
(the label horizon). That keeps backfills of past run dates honest, and for a
normal daily run it is simply every matured label.

Context: every labelled mule in the lookback window (newest first, up to
max_mules) plus negatives_per_mule hash-sampled non-mules per mule. The model
sees an enriched class balance, so its probabilities are prior-corrected to
the book's measured mule rate (mule.calibrate); ranking is unchanged.

All reads are pinned to one Iceberg snapshot of gold.mule_features (Impala
backend), recorded in mule_model_run with the context window and counts, so
the endpoint and any audit can rebuild the exact context later.

If the gate fails, the model run and holdout are still written (the evidence),
but today's alerts and rings are not published unless publish_on_fail is set:
yesterday's queue stays in place and the job exits non-zero.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from mule import holdout as ho
from mule.calibrate import prior_correct
from mule.config import ROOT, table
from mule.config import policy as load_policy
from mule.features import COLUMNS, FEATURES, LABEL
from mule.model import (build_model, device_name, max_mules, model_id, model_version, new_classifier,
                        predict_mule, resolve_family)
from mule.reasons import NO_TIER, assign_tiers, reasons, rules_score, tiers_from_policy

logger = logging.getLogger(__name__)

CONTEXT_FILE = ROOT / "models" / "mule_context.parquet"
ALERT_INFO = ["snapshot_date", "account_id", "cif", "branch_code", "product_code", "person_cluster_id", "ring_id",
              "ring_size", "hops_to_known_mule"]


def select_context(storage, *, date_to: date, lookback_weeks: int, n_mules: int, negatives_per_mule: int,
                   snapshot_id: str | None) -> tuple[pd.DataFrame, date]:
    date_from = date_to - timedelta(weeks=lookback_weeks)
    ctx = storage.context(date_from=date_from, date_to=date_to, max_mules=n_mules,
                          negatives_per_mule=negatives_per_mule, snapshot_id=snapshot_id)
    if ctx.empty or ctx[LABEL].nunique() < 2:
        raise ValueError(f"context {date_from}..{date_to} needs both mules and non-mules, got {len(ctx)} rows")
    return ctx, date_from


def save_context(ctx: pd.DataFrame, meta: dict, path: Path | None = None) -> None:
    path = path or CONTEXT_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    ctx[COLUMNS].to_parquet(path, index=False)
    path.with_suffix(".json").write_text(json.dumps(meta, default=str, indent=2))
    logger.info("saved %d context rows to %s", len(ctx), path)


def tier_book(book: pd.DataFrame, p: np.ndarray, p_adj: np.ndarray, tiers: list[dict] | None = None) -> pd.DataFrame:
    """Every scored account with its rank, percentile and tier (NONE below the last tier)."""
    tiers = tiers or tiers_from_policy()
    out = book[ALERT_INFO].copy().reset_index(drop=True)
    out["p_mule"] = np.round(p, 5)
    out["p_mule_adj"] = np.round(p_adj, 6)
    order = np.lexsort((out["account_id"].to_numpy(), -out["p_mule_adj"].to_numpy()))
    out = out.iloc[order].reset_index(drop=True)
    out["risk_rank"] = np.arange(1, len(out) + 1)
    out["risk_pct"] = np.round(out["risk_rank"] / max(len(out), 1), 6)
    out["tier"], out["action"] = assign_tiers(out["risk_pct"], tiers)
    return out


def ring_rollup(scored: pd.DataFrame) -> pd.DataFrame:
    """Rings (2+ customers) with at least one alerted account."""
    s = scored[scored["ring_size"] >= 2].copy()
    s["alerted"] = s["tier"] != NO_TIER
    s["tier_rank"] = s["tier"].where(s["alerted"], "~")        # tier codes sort T1 < T2 < T3 < ~
    keep = s.groupby("ring_id")["alerted"].transform("any")
    s = s[keep]
    if s.empty:
        return pd.DataFrame(columns=["ring_id", "ring_size", "accounts_scored", "accounts_alerted", "top_tier",
                                     "max_p_mule_adj", "sum_p_mule_adj", "min_hops_to_known_mule",
                                     "top_reason_codes"])

    def top_codes(codes: pd.Series) -> str:
        c = pd.Series([x for v in codes.dropna() for x in v.split(",") if x]).value_counts()
        return ",".join(c.index[:3])

    g = s.groupby("ring_id").agg(ring_size=("ring_size", "max"), accounts_scored=("account_id", "size"),
                                 accounts_alerted=("alerted", "sum"), top_tier=("tier_rank", "min"),
                                 max_p_mule_adj=("p_mule_adj", "max"), sum_p_mule_adj=("p_mule_adj", "sum"),
                                 min_hops_to_known_mule=("hops_to_known_mule", "min"),
                                 top_reason_codes=("reason_codes", top_codes)).reset_index()
    g["sum_p_mule_adj"] = g["sum_p_mule_adj"].round(5)
    return g.sort_values(["sum_p_mule_adj", "ring_id"], ascending=[False, True]).reset_index(drop=True)


def run_daily(storage, factory=None, run_date: date | None = None, triggered_by: str = "manual",
              write: bool = True, run_holdout: bool = True, save_context_file: bool = True,
              publish_on_fail: bool = False, family: str | None = None) -> dict:
    t0 = time.time()
    pol = load_policy()
    fam = resolve_family(family)
    factory = factory or (lambda **kw: new_classifier(fam, **kw))
    horizon = int(pol["label"]["horizon_days"])
    c = pol["context"]
    lookback, k_neg = int(c["lookback_weeks"]), int(c["negatives_per_mule"])
    n_mules = max_mules() if fam != "stub" else int(c["max_mules"])

    storage.refresh("mule_features")
    snap_id = storage.snapshot_id("mule_features")
    latest = storage.latest_snapshot_date(snap_id)
    run_date = run_date or latest
    known_to = run_date - timedelta(days=horizon)
    labelled = [d for d in storage.labelled_dates(snap_id) if d <= known_to]
    if not labelled:
        raise ValueError(f"no labelled snapshots on or before {known_to}")
    run_id = f"{run_date:%Y%m%d}-{uuid.uuid4().hex[:8]}"
    logger.info("run %s: model %s (%s), scoring snapshot %s, labels known to %s, gold snapshot %s, "
                "context up to %d mules x (1 + %d)", run_id, model_id(fam), fam, run_date, labelled[-1], snap_id,
                n_mules, k_neg)

    summary, band_df = {}, pd.DataFrame()
    test_from = test_to = hctx_rows = None
    passed, gate_lines = True, ["holdout skipped"]
    if run_holdout:
        h = pol["holdout"]
        test_from, test_to, hctx_to = ho.test_window(labelled, int(h["test_weeks"]), int(h["gap_days"]))
        test = storage.holdout_sample(date_from=test_from, date_to=test_to,
                                      negative_share=float(h["negative_sample"]), snapshot_id=snap_id)
        hctx, _ = select_context(storage, date_to=hctx_to, lookback_weeks=lookback, n_mules=n_mules,
                                 negatives_per_mule=k_neg, snapshot_id=snap_id)
        hctx_rows = len(hctx)
        t1 = time.time()
        p_test = predict_mule(build_model(hctx, factory), test)
        r_test = rules_score(test)
        summary, band_df = ho.summarise(test, p_test, r_test), ho.bands(test, p_test, r_test)
        passed, gate_lines = ho.gate(summary, pol["gate"])
        pct = lambda k: 100 * (summary[k] or 0)  # noqa: E731
        logger.info("holdout %s..%s: %d rows (%d mules, book rate %.3f%%), AUC %.3f, PR-AUC %.3f, scored in %.0fs | "
                    "top 1%% of the book catches %.0f%% of mules (rules alone %.0f%%), top 5%% %.0f%%; precision "
                    "top 0.2%% %.1f%%, top 1%% %.1f%%", test_from, test_to, len(test), summary["holdout_mules"],
                    pct("holdout_mule_rate"), summary["holdout_auc"] or 0, summary["holdout_pr_auc"] or 0,
                    time.time() - t1, pct("capture_top1"), pct("rules_capture_top1"), pct("capture_top5"),
                    pct("precision_top02"), pct("precision_top1"))
        for line in gate_lines:
            logger.info("gate: %s", line)

    ctx, ctx_from = select_context(storage, date_to=labelled[-1], lookback_weeks=lookback, n_mules=n_mules,
                                   negatives_per_mule=k_neg, snapshot_id=snap_id)
    ctx_rate = float(ctx[LABEL].mean())
    book_rate = storage.labelled_rate(date_from=ctx_from, date_to=labelled[-1], snapshot_id=snap_id)
    rate_source = "measured"
    if not book_rate or not 0 < book_rate < 1:
        book_rate, rate_source = float(c["book_mule_rate"]), "policy fallback"
    n_ctx_mules = int(ctx[LABEL].sum())
    ctx_meta = {"run_id": run_id, "run_date": run_date, "model_family": fam, "model_id": model_id(fam),
                "context_from": ctx_from, "context_to": labelled[-1], "context_rows": len(ctx),
                "context_mules": n_ctx_mules, "context_negatives": len(ctx) - n_ctx_mules,
                "context_mule_rate": ctx_rate, "book_mule_rate": book_rate,
                "source_table": table("mule_features"), "source_snapshot_id": snap_id, "features": FEATURES}
    if save_context_file:
        save_context(ctx, ctx_meta)
    clf = build_model(ctx, factory)

    book = storage.features(date_from=run_date, date_to=run_date, snapshot_id=snap_id)
    if book.empty:
        raise ValueError(f"no rows for snapshot {run_date} (run dates must be snapshot dates)")
    t1 = time.time()
    p = predict_mule(clf, book)
    p_adj = prior_correct(p, ctx_rate, book_rate)
    scored = tier_book(book, p, p_adj)
    alerted = scored["tier"] != NO_TIER
    feats = book.set_index("account_id").loc[scored.loc[alerted, "account_id"]].reset_index()
    codes, texts = reasons(feats)
    scored["reason_codes"], scored["reasons"] = "", ""
    scored.loc[alerted, "reason_codes"] = codes.to_numpy()
    scored.loc[alerted, "reasons"] = texts.to_numpy()
    alerts = scored[alerted].reset_index(drop=True)
    rings = ring_rollup(scored)
    counts = alerts["tier"].value_counts().to_dict()
    cutoffs = alerts.groupby("tier")["p_mule_adj"].min().to_dict()
    logger.info("scored %d accounts in %.0fs: %s; %d rings with alerts; context %d rows (%d mules, %.1f%%), "
                "book mule rate %.4f%% (%s)", len(scored), time.time() - t1, counts, len(rings), len(ctx),
                n_ctx_mules, 100 * ctx_rate, 100 * book_rate, rate_source)

    tier_codes = [t["code"] for t in tiers_from_policy()]
    publish = passed or publish_on_fail
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    model_run = pd.DataFrame([{
        "run_date": run_date, "run_id": run_id, "run_ts": now, "model_family": fam, "model_id": model_id(fam),
        "model_version": model_version(fam), "device": device_name(clf), "snapshot_date": run_date,
        "scored_accounts": len(scored),
        **{f"alerts_t{i + 1}": int(counts.get(code, 0)) for i, code in enumerate(tier_codes[:3])},
        **{f"t{i + 1}_cutoff": cutoffs.get(code) for i, code in enumerate(tier_codes[:3])},
        "context_rows": len(ctx), "context_mules": n_ctx_mules, "context_negatives": len(ctx) - n_ctx_mules,
        "context_from": ctx_from, "context_to": labelled[-1], "context_mule_rate": round(ctx_rate, 5),
        "book_mule_rate": round(book_rate, 7), "book_rate_source": rate_source,
        "source_table": table("mule_features"), "source_snapshot_id": snap_id,
        "holdout_test_from": test_from, "holdout_test_to": test_to, "holdout_context_rows": hctx_rows,
        **{k: summary.get(k) for k in (
            "holdout_rows", "holdout_mules", "holdout_mule_rate", "holdout_auc", "holdout_pr_auc", "capture_top1",
            "capture_top5", "precision_top02", "precision_top1", "rules_capture_top1", "rules_precision_top1",
            "lift_over_rules")},
        "gate_passed": passed, "gate_detail": " | ".join(gate_lines), "alerts_published": publish,
        "policy_json": json.dumps(pol, sort_keys=True), "triggered_by": triggered_by,
        "duration_s": round(time.time() - t0, 1),
    }])
    out = {"mule_model_run": model_run}
    if not band_df.empty:
        out["mule_holdout"] = band_df.assign(run_date=run_date, run_id=run_id)
    if publish:
        out["mule_alerts"] = alerts.assign(run_date=run_date, run_id=run_id)
        out["mule_rings"] = rings.assign(run_date=run_date, run_id=run_id)
    else:
        logger.warning("KPI gate failed: today's alerts are NOT published (yesterday's queue stays)")
    if write:
        for key, df in out.items():
            storage.replace_run(key, df, run_date)
    out["alerts_all"] = alerts
    out["summary"] = {**summary, "run_id": run_id, "run_date": run_date, "scored_accounts": len(scored),
                      "tiers": counts, "rings": len(rings), "context_rows": len(ctx), "context_mules": n_ctx_mules,
                      "book_mule_rate": book_rate, "gate_passed": passed, "gate_detail": gate_lines,
                      "alerts_published": publish, "model_family": fam, "model_id": model_id(fam)}
    return out

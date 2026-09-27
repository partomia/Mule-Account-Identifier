# Build plan — Mule Account Identifier on Cloudera

Link five source systems (KYC onboarding, core banking, UPI switch, digital
banking sessions, fraud complaints / intel) on the CIF and on shared secondary
identifiers into one identity graph in Iceberg, score every active account for
the chance of being a mule with the **Mitra-v2** tabular foundation model
(in-context, no training loop), and hand investigators a daily alert queue with
plain-language reasons, ring evidence and a recommended preventive action
(debit freeze pending re-KYC, hold + enhanced monitoring, watchlist).

Demo of a governed data + scoring pipeline on synthetic data, not a validated
fraud model. It ranks accounts for human review; it never freezes one on its own.

## Platform mapping

Same CDP environment as `partomia/Collections-Delinquency-Roll-Forward-Prediction`
and `partomia/ALM-IRRBB-CASA-Behavioural-Forecasting` (same CDE vcluster, CDW
Impala VW, CAI workbench). Adds MLflow tracking, a KPI gate and GitHub Actions CI
from `partomia/Cloudera-AI-MLOps-Workshop-Iceberg`.

| Layer | Service | What runs there |
|---|---|---|
| Ingest + medallion | CDE (Spark 3, Iceberg v2) | generate 5 bronze sources, validate, silver (keys standardised + hashed) |
| Entity resolution | CDE Spark | identity edges + connected components (person clusters, rings), point-in-time |
| Gold | CDE Spark | account x weekly snapshot, 18 features + 90-day label, MERGE per load |
| Orchestration | CDE Airflow | daily DAG chains the Spark jobs, then triggers the CAI scoring job (API v2) |
| Scoring + KPI gate | CAI Job | holdout, gate, score the book, prior correction, tiers, reasons, rings |
| Experiment tracking | CAI Experiments (MLflow) | params, KPIs, context file per daily run |
| On-demand / what-if | CAI Model Deployment | `predict.py`, score an account, what-if on graph features |
| SQL + reports | CDW Impala (Hue) | alert reports, ring reports, Iceberg time travel |
| Investigator console | CAI Application (Streamlit) | queue, linked identity, ring graph, what-if, trust, decisions, lineage |
| CI | GitHub Actions | pytest + small end-to-end run with the gate on every push |

Names:

- Databases: `rsingh_mule_acct_{bronze,silver,gold,ref}` (prefix in `config/mule.yaml`,
  `--db-prefix` / `DB_PREFIX` on the CDE side).
- Python package `mule/`, env var prefix `MULE_<SECTION>_<KEY>`.
- CDE resources: `rsingh-mule-acct-*` (repository `rsingh-mule-acct-pipeline`,
  DAG job `rsingh-mule-acct-orchestration`).
- CAI project `mule-account-identifier`; job `mule-daily-score`; model `mule-scorer`;
  application `Mule Investigator Console`; MLflow experiment `mule-account-identifier`.

## Data flow

```
CDE  generate_mule_bronze  -> bronze.{kyc_onboarding, cbs_accounts, upi_transactions,
                                      digital_sessions, fraud_reports}, ref.{branch_map, product_map}
CDE  validate_bronze       -> gate (fails the DAG; --inject-bad-data on the generator fails it on cue)
CDE  build_silver          -> silver.{customer, account, txn, session, report} + silver.identity_edges
CDE  build_identity_graph  -> silver.identity_clusters (snapshot x CIF: person_cluster_id, ring_id, sizes)
CDE  build_gold_features   -> gold.mule_features (MERGE, one Iceberg snapshot per load)
CAI  daily_score           -> gold.{mule_alerts, mule_rings, mule_holdout, mule_model_run} (keyed by run_date)
CAI  app (decisions)       -> bronze.investigator_decisions -> silver -> tomorrow's labels
```

## Decisions (where this plan differs from the design doc, and why)

The design doc is kept for the story, sources, table set, features, tiers,
gate and runbook. These changes fix correctness or run-time risks found while
reviewing it against the two recent repos and the Mitra-v2 / AutoGluon 1.6 docs.

1. **Point-in-time identity graph (leakage fix).** The doc builds the graph once
   from all history, so a snapshot in 2025 would "see" CIFs and shared devices
   that appear later, and the holdout would look better than reality. Every
   edge carries `first_seen` (the date both CIFs first held the identifier);
   connected components run once over `(snapshot_date, cif)` nodes using only
   edges with `first_seen <= snapshot_date`. One Spark job, no loop per week.
2. **Hub suppression.** Identifiers shared by more than `max_hub_cifs` (default 25)
   CIFs are not edges: branch tablets, office Wi-Fi devices, merchant and
   aggregator VPAs. Money-flow ring edges are person-to-person only. Without this,
   a 12M-transaction graph collapses into one giant component and label
   propagation needs many iterations.
3. **Scoring population = active, not yet caught.** Each snapshot keeps accounts
   with any credit or debit in the last 30 days that are not already frozen or
   reported. Already-known mules are excluded from scoring and from the label
   population, so the model is not credited for "finding" accounts the bank
   already froze. This also cuts gold rows and scoring time by ~2-3x.
4. **Mitra without AutoGluon's hidden 20% split.** `TabularPredictor.fit` holds out
   20% for validation by default, which would drop a fifth of the ~2,000 context
   mules. The wrapper uses AutoGluon's `MitraClassifier` (sklearn interface):
   `fit` stores the whole context and `predict_proba` attends to all of it (up to
   8,192 support rows). Its default weights are Mitra v1, so `hf_model` is always
   set to `autogluon/mitra-classifier-2`.
5. **Mitra needs the GPU; TabICL covers CPU (measured in phase 0).** On a laptop
   CPU, Mitra-v2 scored 16 / 9 / 5 rows/s at 500 / 1,000 / 2,000 context rows
   (AUC 0.94-0.95 on a synthetic check) and a single-row predict took ~26 s: a
   ~160k-account book would take hours and the endpoint would time out. Apple MPS
   was ~30x faster but bfloat16 gave AUC 0.5 at one size, so only CUDA counts.
   `model.family: auto` = Mitra-v2 on CUDA, TabICL v2 (already proven on this
   cluster, fast on CPU) otherwise; the family and checkpoint are recorded for
   every run. `stub` (logistic regression) for tests and CI.
6. **Holdout on a weighted sample.** All holdout positives plus a random share of
   negatives, with inverse sampling weights, so capture at top 1% / 5% and alert
   precision are unbiased while the holdout scores ~40k rows instead of ~600k.
7. **Book mule rate measured, not hard-coded.** The prior correction uses the
   labelled mule rate of the scored population over the context window
   (`book_mule_rate` in policy is only the fallback), recorded in `mule_model_run`.
8. **Requirements: no CPU-only torch index in `requirements.txt`.** The doc's
   `--extra-index-url .../whl/cpu` would install CPU torch on GPU sessions. It is
   kept only for Docker / CI files.
9. **`REFRESH` before reading gold**, then read the snapshot id, so the job never
   scores a stale snapshot (the risk flagged in the ALM review).
10. **CI runs the real pipeline small.** GitHub Actions runs pytest, then the CDE
    jobs on local Spark + Iceberg at ~5,000 customers, then the daily job with the
    stub model and the gate. No committed data file; the generator is deterministic.
11. Alerts, rings, holdout and model run are kept per run date (DELETE + INSERT
    on a rerun), only alert-tier accounts are written (not the whole book), and
    the endpoint rebuilds the context of the latest gated run with
    `FOR SYSTEM_VERSION AS OF`, as in Collections.

## Method (demo policy, see `config/policy.yaml`)

- **Label** `is_mule_90d` = 1 if the account is confirmed as a mule within 90 days
  after the snapshot (fraud report followed by a freeze, Suspect Registry match,
  or an investigator CONFIRMED_MULE); an investigator FALSE_POSITIVE sets it to 0.
  NULL until 90 days have passed.
- **Snapshots**: every Friday from 2 May 2025 plus the as-of date (today's book).
- **Features (18)**: account profile (age, min-KYC, dormant reactivated), money
  movement (inflow / declared income, distinct senders / receivers 7d,
  pass-through ratio 7d, median hold hours, night share, round-amount share),
  digital (new device logins, VPA / mobile changes), identity graph (accounts on
  same device, CIFs sharing mobile, person cluster size, ring size), network
  proximity (hops to a known mule, credits from complainants 30d). All network
  features use only reports known on the snapshot date.
- **Context**: every labelled mule in the last 26 weeks (up to 2,000) plus 3x as
  many non-mules; prior correction `p_adj` back to the book rate.
- **Gate** (workshop "accuracy lies" lesson): capture at top 1%, alert precision,
  and lift over a rules-only baseline (shared device + pass-through + distance to
  a reported account). Fail = model run recorded with `gate_passed = false`,
  yesterday's alerts stay live, job exits non-zero so Airflow shows red.
- **Tiers** by percentile of `p_adj` across the book: T1 freeze review (top 0.2%),
  T2 hold + monitor (to 1%), T3 watchlist (to 3%); single-account endpoint calls
  use the `p_adj` cut-offs the daily run recorded.
- **Reason codes**: business rules on the inputs, not an explanation of the score.

Synthetic data: ~200,000 customers / 260,000 accounts, extracts from April 2025,
~17M UPI transaction legs by September 2026, ~0.8% mule accounts in rings of 3-25, three mule patterns
(recruited dormant, rented min-KYC with shared device, synthetic identity with a
reused PAN hash / mobile) and legitimate look-alikes (students, gig workers,
small traders, families sharing a phone). Prefix-stable history; 0.02% re-sent
duplicates for silver to remove. Scale is a flag, so tests and CI run small.

## Phases

- [x] **0. Scaffold + model spike** — layout, config, requirements (AutoGluon 1.6.3,
  torch 2.10), venv; full-context `MitraClassifier` path confirmed; CPU too slow for
  Mitra (see decision 5), so `family: auto`.
- [ ] **1. Bronze** — generator for the five sources + ref tables, planted rings and
  look-alikes, prefix-stable, `--inject-bad-data`. Unit tests on the pure-Python simulator.
- [ ] **2. Validate + silver** — gate (null keys, orphans, raw PAN regex in hash
  columns, missing days); dedupe, key standardisation, salted SHA-256, identity
  edges with `first_seen` and hub suppression.
- [ ] **3. Identity graph + gold** — point-in-time connected components, 18 features,
  90-day label, MERGE. Checked locally against an independent pandas computation
  on a small book.
- [ ] **4. Core logic** (`mule/`) — config, features contract, model wrappers
  (Mitra / TabICL / stub), calibration, reasons + tiers, holdout + gate, ring
  subgraph, MLflow tracking, Impala / parquet storage, daily pipeline, scoring. Unit tests.
- [ ] **5. CAI jobs + endpoint** — `daily_score.py` (gate exit code, MLflow),
  `backfill_history.py`, `predict.py` with what-if, `test_endpoint.py`.
- [ ] **6. Investigator console** — 7 tabs (queue, linked identity, ring view,
  what-if, holdout & trust, decisions, lineage), CAI launcher, Dockerfile,
  headless app test.
- [ ] **7. Orchestration + CI + docs** — daily Airflow DAG, CDE deploy / backfill
  scripts, GitHub Actions workflow, Hue SQL, README, demo runbook.
- [ ] **8. On Cloudera** — CDE jobs on the vcluster, CDW checks; then with Ravi:
  CAI project, job, model deployment, application, Airflow variables.

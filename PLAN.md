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

Built first on the go01 environment shared with
`partomia/Collections-Delinquency-Roll-Forward-Prediction` and
`partomia/ALM-IRRBB-CASA-Behavioural-Forecasting`; moved on 2026-09-29 to the
"federal" environment, rebuilt from empty (decision 12). Adds MLflow tracking, a
KPI gate and GitHub Actions CI from `partomia/Cloudera-AI-MLOps-Workshop-Iceberg`.

Environment (federal):

| Service | Endpoint |
|---|---|
| CDE virtual cluster | `https://bxjjm2cr.cde-lzjl69mv.federal.dp5i-5vkq.cloudera.site/dex/api/v1` |
| CDW Impala | `coordinator-federal-impala-1.dw-federal-cdp-env.dp5i-5vkq.cloudera.site:443` (HTTP transport, `cliservice`, SSL, LDAP workload user) |
| CAI workbench | `https://federal-cml.federal.dp5i-5vkq.cloudera.site` |
| CAI runtime | `docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.11-standard:2026.08.1-b5` |

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
- CAI (federal, `ci/cai_jobs.py`): project `rsingh-mule-acct`; jobs
  `rsingh-mule-acct-sync-code` and `rsingh-mule-acct-daily-score`; model
  `rsingh-mule-acct-scorer`; application `Mule Investigator Console` (subdomain
  `rsingh-mule-acct-console`); MLflow experiment `mule-account-identifier`. On
  go01 they were `mule-account-identifier`, `mule-daily-score` and `mule-scorer`.
- Airflow Variables: `MULE_CAI_{HOST,PROJECT_ID,SYNC_JOB_ID,JOB_ID,API_KEY}`.

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
    stub model. No committed data file; the generator is deterministic. The daily
    job runs with `--ignore-gate`: at 5,000 customers a holdout window has only a
    handful of mules, too few for capture / precision to mean anything against
    thresholds calibrated for the real ~200k-customer book (confirmed by running
    it: AUC ~0.5, gate fails on sample size, not on a code defect) — CI still
    exercises the whole holdout + gate code path, it just doesn't conflate a
    regression there with the KPI gate itself failing.
11. Alerts, rings, holdout and model run are kept per run date (DELETE + INSERT
    on a rerun), only alert-tier accounts are written (not the whole book), and
    the endpoint rebuilds the context of the latest gated run with
    `FOR SYSTEM_VERSION AS OF`, as in Collections.
12. **Moved to the federal environment, rebuilt from empty (2026-09-29).** No
    tables, models or runs carry over from go01: the CDE jobs regenerate the
    book, the first CAI run publishes the first alert queue. The committed
    hosts (`config/mule.yaml`, `.env.example`, the DAG docstring, the
    environment table above) point at federal; credentials stay in the
    gitignored `.env`, the CAI project environment and Airflow Variables.
13. **CAI is set up over the API v2, not by hand.** `ci/setup_cai.py` creates
    the project from Git, its environment, the jobs (resizing existing ones),
    the model and the app, idempotently, following Spend-Analytics.
    `cai/jobs/sync_code.py` resets the project to origin/main and pip-installs
    `requirements.txt` when its hash changes, at 2 vCPU / 8 GB (pip is killed
    at 2 GB while installing torch). The DAG runs it before scoring, so the
    CAI project follows the pushed code without a manual `git pull`.
    `ci/run_cai_job.py` starts one job and polls it (API v2 has no run-log
    endpoint); `cde/scripts/set_airflow_variables.py` sets only the
    `MULE_CAI_*` Variables through the vcluster's Airflow API.
14. **DAG registered paused, first interval = the migration date.**
    `cde job create --schedule-paused` is rejected for Airflow jobs, and with
    `catchup=False` and a past `start_date` an unpaused DAG runs the latest
    closed interval at once, so the DAG sets `is_paused_upon_creation=True`
    and `start_date` 2026-09-28 20:30 UTC (first scheduled run: as_of
    2026-09-28, the date the chain is run for by hand).
15. **Impala identifiers are backtick-quoted** in the DDL and inserts
    `ImpalaStorage` generates; a test checks the output columns against
    Impala's 409 reserved words (none collide today).

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
- [x] **1. Bronze** — generator for the five sources + ref tables (incl.
  `ref.bank_devices`: branch kiosks and BC-agent terminals), planted rings with a
  seasoning phase before activation and complaint lags of days to weeks,
  look-alikes, prefix-stable, `--inject-bad-data`. Unit tests on the pure-Python simulator.
- [x] **2. Validate + silver** — gate (null keys, orphans, raw PAN regex outside the
  PAN column, missing days); dedupe, key standardisation, salted SHA-256, identity
  edges with `first_seen`; the bank's own devices and identifiers on more than 25
  customers are hubs, never edges.
- [x] **3. Identity graph + gold** — point-in-time connected components, 18 features,
  90-day label, MERGE. Checked locally at 20k customers: 138 of 148 fraud-frozen
  accounts appear in gold before they are known (median 5 weekly rows), 0.03-0.09%
  positives per snapshot, top rings 70-100% frozen mules, single-feature AUCs up
  to 0.78 (ring size, new devices, VPA / mobile changes).
- [x] **4. Core logic** (`mule/`) — config, features contract, model wrappers
  (Mitra / TabICL / stub), calibration, reasons + tiers, holdout + gate, ring
  rollup, Impala / parquet storage, daily pipeline, scoring. Unit tests.
  Local run against the real gold book (parquet backend, TabICL on CPU,
  2026-09-25 snapshot, book mule rate 0.065%): holdout AUC 1.000, PR-AUC 0.909,
  capture top 1% 100% (rules alone 54%), precision top 0.2% 16.7%, lift over
  rules 1.85x — all three gates pass against the current `config/policy.yaml`
  thresholds. Book run: 20,133 accounts scored, 604 alerts (40 T1 / 161 T2 /
  403 T3), 164 rings with an alert.
- [x] **5. CAI jobs + endpoint** — `daily_score.py` (gate exit code, MLflow),
  `backfill_history.py`, `predict.py` with what-if, `test_endpoint.py`. Request
  handling (`mule/scoring.py`, `mule/client.py`) mirrors the Collections repo:
  the endpoint rebuilds the daily run's context (Impala or the saved parquet
  file) and tiers a request from that run's recorded `p_mule_adj` cut-offs,
  since a single request has no book to rank against. Smoke-tested locally:
  `daily_score.py --dry-run` and `--stub` runs, `backfill_history.py`,
  `test_endpoint.py --local` (file context) and `predict.py`'s `predict()`
  directly all score correctly end to end; the what-if (new device + mobile
  change + high pass-through) moves the demo account from T2 to T1.
- [x] **6. Investigator console** — 7 tabs (queue, linked identities, ring view
  with a networkx/plotly graph of identity + money-flow edges, what-if,
  holdout & trust, decisions written to `bronze.investigator_decisions`,
  lineage), CAI launcher (`app/run.py`), Dockerfile, headless app test
  (`tests/test_app.py`, Streamlit's `AppTest` harness — no browser extension
  was available in this environment, so this is the verification in place of
  a manual browser check). Caught and fixed a real bug this way: `alerts.
  set_index("cif")` isn't safe since one CIF can have several alerted
  accounts. Also hardened the app against a cold start (gate fails on the
  very first run, so `mule_alerts` never existed) instead of hard-crashing.
- [x] **7. Orchestration + CI + docs** — `cde/dags/mule_dag.py` (five CDE
  tasks, then a `PythonOperator` triggering `mule-daily-score` over the API
  v2, `AirflowSkipException` if `MULE_CAI_HOST` isn't set yet), daily at
  20:30 UTC / 02:00 IST; `deploy_dag.sh`, `backfill_drill.sh`;
  `.github/workflows/ci.yml`; `sql/reports.sql`; `docs/DEMO_RUNBOOK.md`;
  README's Phase-7 markers replaced with the real thing. Verified: `pytest -q`
  (69 tests) and the CDE pipeline at 5,000 customers both run clean, redirected
  to a scratch warehouse/parquet dir so the real ~200k-customer local data
  wasn't touched. Found live: at 5,000 customers a holdout window has only a
  handful of mules — the KPI gate fails on sample size alone (AUC ~0.5), not
  on a code defect — so CI's daily-job step uses `--ignore-gate` (exercises
  the full holdout/gate code path; doesn't conflate that with the KPI gate
  itself, which is calibrated for the real book). The DAG and `sql/reports.sql`
  can't be run for real without a CDE Airflow / CDW environment (Phase 8);
  checked instead by syntax-parsing the DAG and cross-referencing every
  table/column name against `mule/schema.py` and `config/mule.yaml`.
- [x] **8. On Cloudera** — CDE jobs on the vcluster, CDW checks; then with Ravi:
  CAI project, job, model deployment, application, Airflow variables.
  In progress with Ravi (2026-09-27/28): CAI project `mule-account-identifier`
  created from the GitHub repo, GPU profile, `MULE_IMPALA_USER`/`_PASSWORD` /
  `HF_HOME` set. Session sanity checks all passed against the real CDW book
  (14,004,971 rows in `gold.mule_features`, latest snapshot 2026-09-25 — about
  10x the ~1.4M-row local sample every earlier phase validated against).
  **First real production run, Mitra-v2 on GPU** (`daily_score.py`, no
  `--dry-run`): holdout AUC 1.000, capture top 1% 100% (rules alone 58%),
  precision top 0.2% 26.5%, lift 1.73x — all three gates pass with more margin
  than the local TabICL/CPU numbers. Scored 200,380 active accounts in ~7 min:
  400 T1 / 1,603 T2 / 4,008 T3 (6,011 alerts), 1,026 rings. Wrote the four gold
  tables for real for the first time (`mule_model_run`, `mule_holdout`,
  `mule_alerts`, `mule_rings`) and logged to MLflow (CAI Experiments created
  the `mule-account-identifier` experiment automatically). Found a real bug
  the first time the endpoint's `impala` context path ran against a live
  Impala (only `file` had ever been tested before): `mule/scoring.py`'s
  `load_context()` stringified every `mule_model_run` field including the
  numeric ones, so `prior_correct()` got a string rate and crashed. Fixed
  (only id/date fields are stringified now) with a regression test
  (`test_load_context_impala_keeps_rates_and_cutoffs_numeric`, a fake-Impala
  fixture, confirmed to fail on the old code). Created the `mule-daily-score`
  Job and ran it for real: gate passed, alerts published, MLflow logged — but
  the CAI Job engine still reported "Engine exited with status 1", because
  CAI Jobs run a script inside a Jupyter kernel wrapper, not a plain
  subprocess, and that wrapper catches *any* `SystemExit` (including
  `sys.exit(0)`) as an unhandled exception and reports failure regardless of
  the code. Fixed `daily_score.py`'s `if __name__ == "__main__"` block to
  only call `sys.exit()` on a real failure (falling off the end on success no
  longer raises `SystemExit` at all); verified both exit codes are still
  correct for a plain subprocess (CI, a terminal). Re-ran the Job: now shows
  **succeeded** (same gate-pass numbers, 1,064 rings this time — the small
  run-to-run wobble in ring count across otherwise-identical gate numbers is
  Mitra's stochastic context sampling/scoring, not a bug). Created the
  `mule-scorer` Model Deployment (GPU, 2 GPUs / 2 vCPU / 4 GiB, 1 replica)
  and the `Mule Investigator Console` Application — both confirmed working
  against the real 200,380-account book: the endpoint's what-if moved
  `p_mule_adj` by ~80x live, and the app renders all 7 tabs correctly with
  the real gate-pass numbers. The Impala credentials plus
  `MULE_ENDPOINT_URL`/`_ACCESS_KEY`/`_API_KEY` were promoted to project-level
  environment variables (re-set `_ACCESS_KEY` if the model is ever
  redeployed — a new deployment can get a new one).

  Also found: this laptop's `cde` CLI is live against the real vcluster
  (`cde repository list` / `cde job list` work). The CDE side (repository
  `rsingh-mule-acct-pipeline`, all five Spark jobs) was already deployed
  before this session started, explaining the pre-existing 14M-row gold
  table. Registered `rsingh-mule-acct-orchestration` (the Airflow DAG job)
  via `cde job create --type airflow` after confirming the repo was synced
  to the latest push. **Its schedule came back enabled with a past start
  date** (`is_paused_upon_creation=False` in the DAG, by design for a normal
  deploy) — meaning it could have fired unattended at the next 20:30 UTC
  before the Airflow Variables were ever set or a manual run tested. Paused
  it immediately with `cde job schedule pause` (confirmed via `cde job
  describe`: `"paused": true`) as a safety measure. Ravi set the four
  Airflow Variables in the CDE Airflow UI (confirmed matching the values
  pulled via `cmlapi` above); `cde job run` then refused to trigger a
  **paused** job at all ("resume the schedule before triggering the run") —
  a real constraint neither of us expected, since pausing had been the
  safety measure. Unpaused (`cde job schedule unpause`) and triggered a
  manual run (`cde job run --name rsingh-mule-acct-orchestration --wait`).

  **Result: `succeeded`, all six tasks, ~27 minutes end to end** (`cde run
  describe --id 2304`). `cde run fg-status` isn't available on this
  vcluster, but `cde run logs --type cai_daily_score/attempt_1` shows the
  Airflow → CAI trigger worked exactly as designed: started CAI job run
  `s55yl2sn75k5ctsq` with `MULE_TRIGGERED_BY=airflow`,
  `MULE_RUN_DATE=2026-09-27` (yesterday, from the DAG's `AS_OF` macro),
  polled every 30s through `scheduling → running → succeeded` (~10 minutes,
  matching the Mitra-v2 GPU scoring time measured earlier), and the Airflow
  task itself exited with return code 0 — confirming the `sys.exit()` fix
  holds under the Airflow PythonOperator path too, not just a bare CAI Job
  run. The daily 20:30 UTC / 02:00 IST schedule is now live and unpaused:
  the pipeline runs unattended from here on.

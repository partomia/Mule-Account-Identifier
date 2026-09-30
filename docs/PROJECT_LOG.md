# Project Log

Chronological record of the move to the federal environment, with measured
numbers only. Times are IST (UTC+5:30) unless marked UTC; CDE and Impala
report UTC. Earlier history (phases 0-8 on go01) is in `CONTEXT.md`; names,
decisions and the phase checklist are in `PLAN.md`.

## 2026-09-29: move to federal

### Step 1: hosts

- `config/mule.yaml` (Impala host), `.env.example` (CAI host, endpoint URL
  format), the DAG docstring, `docs/DEMO_RUNBOOK.md` and a new environment
  table in `PLAN.md` point at federal (decision 12).
- Nothing from go01 carries over: federal starts with no `rsingh_mule_acct_*`
  databases, no `rsingh-mule-acct-*` CDE resources and no CAI project.
- Before the move: `pytest -q` 70 passed.
- Federal Impala answers as `impalad version 4.5.0.2025.0.21.3-3`; no
  `rsingh_mule*` databases. The CAI API v2 key works (78 runtimes, including
  the Python 3.11 standard 2026.08.1-b5 runtime).

### Step 2: GitHub

- `gh secret list`: no secrets, so no `CAI_URL`. Mule's CI runs no CAI chain
  (pytest, local Spark at 5,000 customers, stub scoring), so a push cannot
  reach any workbench. The repo is public, so the CDE repository needs no Git
  credential.

### Step 3: CDE

- `./cde/scripts/deploy_jobs.sh` (18:20:58-18:22:13 UTC, 76 s): repository
  `rsingh-mule-acct-pipeline`, python-env `rsingh-mule-acct-python-env` and
  the five Spark jobs. The only other resources on the vcluster are
  Spend-Analytics' (`rsingh-spend-anl-*`), untouched.
- No job uses `mapInArrow` or pandas UDFs, so the python-env needs no pyarrow.

### Step 4: CAI code

- New: `ci/cai_jobs.py` (names, sizes, runtime), `ci/cai_api.py`,
  `ci/setup_cai.py`, `ci/run_cai_job.py`, `cai/jobs/sync_code.py`,
  `cde/scripts/set_airflow_variables.py` (decision 13).
- DAG: `cai_sync_code` before `cai_daily_score`, `is_paused_upon_creation=True`,
  `start_date` 2026-09-28 20:30 UTC (decision 14).
- `ImpalaStorage` backtick-quotes generated identifiers (decision 15).
- `tests/test_cai_setup.py` (14 tests): job sizes and prefixes, create/resize,
  requirements hash, Airflow keys limited to `MULE_CAI_*`, DAG variables and
  job names, the Jupyter-wrapper rules for every CAI script, reserved words.
  `pytest -q`: 84 passed.
- Pushed `fbf7bca` and `3643968`. `ci/setup_cai.py --skip-serving` (7 s):
  project `rsingh-mule-acct` (`fizl-vpsd-g4je-vch4`), jobs
  `rsingh-mule-acct-sync-code` (`k18a-gx0s-wurr-ss0j`) and
  `rsingh-mule-acct-daily-score` (`f9pk-yhdd-c6hs-905f`), project environment
  `HF_HOME`, `MULE_IMPALA_USER`, `MULE_IMPALA_PASSWORD`.
- First sync-code run `ys9wpoq7xh3ip5nj`: succeeded in 682 s (the one-time
  pip install at 2 vCPU / 8 GB). `models/.requirements.sha256`, read back with
  `POST /files/<path>:download`, matches `requirements.txt` (`4d06acc970e0`).

### Step 5: data chain for as_of 2026-09-28

- generate-bronze failed twice, run 3 (18:34 UTC) and run 16 (19:48 UTC),
  each within 28 s and with no logs at all ("object not found").
  - Job size was 4 initial executors of 8 cores / 12 GB (the go01 size). The
    two other projects on the vcluster run 2 initial of 4 cores / 8 GB.
  - `deploy_jobs.sh` now defaults to executors of 4 cores / 8 GB, 1 minimum,
    2 initial, 16 maximum (overridable). Redeployed in 175 s. Committed
    `cfa613d`.
- The chain waits for other projects' Spark runs before each job (Airflow
  runs hold no executors, so they are not waited for) and polls
  `cde run describe`. Every step verified in Impala:

  | Run | Job | Wall | Checked in Impala |
  |---|---|---|---|
  | 19 | generate-bronze | 183 s | kyc 196,125; cbs_accounts 246,867; upi 17,175,915; sessions 3,742,570; fraud_reports 19,547; ref 60 / 5 / 420; batch_as_of 2026-09-28; 0 null keys |
  | 25 | validate-bronze | 122 s | the bronze gate passed |
  | 26 | build-silver | 224 s | customer 196,088; account 246,867 (unique); txn 17,172,596 (3,319 duplicates removed); session 3,741,774; report 21,077; identity links 846,995, hubs 414, edges 49,641 |
  | 27 | build-identity-graph | 245 s | graph_edges 6,082,570; identity_clusters 2,725,800; latest snapshot 2026-09-28; largest ring 81, largest person cluster 25 |
  | 28 | build-gold-features | 184 s | mule_features 14,206,374 rows, 75 snapshots, 11,439,586 labelled, 7,495 mule rows; 0 duplicate or null keys; book 201,404 accounts on 2026-09-28 |

- All tables are Iceberg with one snapshot each (a fresh build).
- Mule has no separate silver or gold DQ job; the Impala checks above stand
  in for them.
- CAI scoring, `rsingh-mule-acct-daily-score` with `MULE_RUN_DATE=2026-09-28`:
  - At 8 vCPU / 32 GB, run `uanmoctcojxuevt5` stayed in "scheduling" for
    14 min and was stopped. Spend-Analytics' largest jobs here are 4 / 16.
  - Resized to 4 vCPU / 16 GB (`40ffbc3`). Run `mx3dtqj730s040v9` was
    scheduled in 91 s and succeeded in 1,273 s.
  - `mule_model_run` (read in Impala): run `20260928-cd07a805`, TabICL
    (`tabicl-classifier-v2-20260212.ckpt`) on CPU, 1,148.9 s of scoring,
    context 2,000 rows (500 mules) from 2025-12-26 to 2026-06-26, source
    snapshot `5480612185225799219`.
  - Holdout: 39,556 rows, 414 mules, AUC 0.9996, PR-AUC 0.446, capture top
    1% 1.000 (rules 0.580), precision top 0.2% 0.256, lift 1.725. Gate PASS
    on all three checks; alerts published.
  - Book: 201,404 accounts scored; 402 T1 / 1,612 T2 / 4,028 T3 (6,042
    alerts); 1,002 rings. Cut-offs T1 0.0818, T2 0.000007, T3 0.0.
  - The go01 run on Mitra-v2 with a GPU (2026-09-27 book) gave the same
    capture and lift, and precision top 0.2% 0.264.

### Step 6: model, app, Airflow, DAG

- `ci/setup_cai.py` with `MULE_ENDPOINT_API_KEY` set to the CAI API v2 key,
  as in Spend-Analytics:
  - Model `rsingh-mule-acct-scorer` (`6df21e26-d25d-4dec-9f6f-1e6f5a5e2dc9`):
    built at 21:31 UTC, deployed at 21:34 (created at 21:22).
  - `MULE_ENDPOINT_URL`, `_ACCESS_KEY` and `_API_KEY` are in the project
    environment.
  - Application `Mule Investigator Console` (`10w4-874e-ezlo-f22u`,
    subdomain `rsingh-mule-acct-console`) reached APPLICATION_RUNNING. Its
    URL redirects to the CAI login.
- `test_endpoint.py` against the deployment: 14.1 s and 13.5 s per call.
  It serves run `20260928-cd07a805`. The what-if moves `p_mule_adj` from
  0.000079 to 0.0027, staying T2.
- The app ran headless (Streamlit `AppTest`) from the laptop against
  federal Impala and the endpoint, in 267 s:
  - All 7 tabs ran with no exception.
  - Metrics: 201,404 scored, 6,042 alerts, 1,002 rings, capture top 1%
    100.0%.
- `cde/scripts/set_airflow_variables.py` created the five `MULE_CAI_*`
  Variables. No other key was read or written.
- `deploy_dag.sh` registered `rsingh-mule-acct-orchestration`. The Airflow
  API shows:
  - `is_paused` true, 0 DAG runs, 7 tasks;
  - next logical date 2026-09-28 20:30 UTC with interval end 2026-09-29
    20:30. That end has passed, so unpausing runs as_of 2026-09-28 at once.

### First DAG run (2026-09-30)

- Pushed `a24b873`, synced the CDE repository to it, and unpaused the DAG at
  00:39:33 UTC with no other Spark run active. DAG run 50 (the closed
  interval, as_of 2026-09-28) started at once and succeeded in 2,678 s:

  | Run | Task | Wall |
  |---|---|---|
  | 51 | generate-bronze | 375 s |
  | 52 | validate-bronze | 117 s |
  | 53 | build-silver | 241 s |
  | 54 | build-identity-graph | 264 s |
  | 55 | build-gold-features | 202 s |
  | - | cai_sync_code + cai_daily_score | 01:01:01 to 01:24:12 UTC, ~23 min |

- `mule_model_run` has one row for 2026-09-28: run `20260928-2672b8ce`,
  `triggered_by` airflow, TabICL on CPU, 1,067.9 s of scoring. It replaced
  the manual run `20260928-cd07a805`, and the results are the same: 201,404
  scored, 402 / 1,612 / 4,028 alerts, AUC 0.9996, capture top 1% 1.0,
  precision top 0.2% 0.2559, lift 1.725, gate PASS.
- `mule_alerts` has 6,042 rows for 2026-09-28. The gold MERGE kept
  14,206,374 rows and added a second Iceberg snapshot (`7351637737303755153`).
- Next: the scheduled run at 2026-09-30 20:30 UTC (as_of 2026-09-29).

### Analytics queries (2026-09-30)

- Added queries 9 to 21 to `sql/reports.sql` and ran each on federal Impala
  against the 2026-09-28 run; each took 1.6 to 5.4 s.
- Measured on that data:
  - Top branch: Surat Branch 1 at 38.9 alerts per 1,000 active accounts.
  - Alert rate: min-KYC savings 9.60% and min-KYC BSBD 7.69%, against
    1.56% to 3.88% for full-KYC products.
  - Mule rate by hops to a known mule: 1 hop 30.947% (3,004 of 9,707),
    2 hops 5.355%, further or none 0.038%.
  - Pass-through of at least 90%: mule rate 2.377%, against 0.031% to 0.192%
    for the lower bands.
  - UPI in the last 30 days: T1 accounts forwarded 96% of credits
    (₹0.74 Cr in, ₹0.71 Cr out); accounts not alerted forwarded 25%.
  - Holdout: the top 0.2% holds 96.4% of mules, against 49.0% for rules.
  - Days from opening to first report (median): BSBD 34, savings 2,119.
- `DESCRIBE HISTORY` on gold `mule_features` shows 2 snapshots. Time travel
  to the first one gives the same 14,206,374 rows, because the DAG run
  re-wrote the manual run's data.

### Step 7: GitHub secrets

- Not needed: Mule's CI has no GitHub-to-CAI chain, so no workflow reads
  CAI secrets. `gh secret list` stays empty.

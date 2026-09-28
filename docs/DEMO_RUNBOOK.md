# Demo runbook

Two parts: **Setup** (one-time, what was actually run to get this live on
Cloudera) and **Demo** (the ~12-minute walkthrough). Every command in Setup
is copy-paste from the real Phase 8 session — hostnames, project/job IDs and
measured numbers are the real ones for `mule-account-identifier`, not
placeholders. Secrets (`MULE_IMPALA_PASSWORD`, any API key) are never
written out; grab those from your own `User Settings` / `.env` each time.

## Setup (one-time)

### 1. CDE: repository, five Spark jobs, Airflow DAG

Already automated; running these again is idempotent (`deploy_jobs.sh`
re-syncs the repo and recreates the jobs, `deploy_dag.sh` re-registers the
DAG).

```bash
./cde/scripts/deploy_jobs.sh
./cde/scripts/deploy_dag.sh
```

Verify what's registered:

```bash
cde repository describe --name rsingh-mule-acct-pipeline
cde job list | python3 -c "import json,sys; [print(j['name'], j.get('type')) for j in json.load(sys.stdin)]"
```

Real output for this project: repository `rsingh-mule-acct-pipeline` status
`ready`; jobs `rsingh-mule-acct-{generate-bronze,validate-bronze,build-silver,
build-identity-graph,build-gold-features}` (type `spark`) and
`rsingh-mule-acct-orchestration` (type `airflow`).

### 2. CAI project

Projects > New Project: name `mule-account-identifier`, Git URL
`https://github.com/partomia/Mule-Account-Identifier`, Python 3.11, GPU
profile if available. Project Settings > Advanced > Environment Variables:

| Variable | Value |
|---|---|
| `HF_HOME` | `/home/cdsw/.hf_cache` |
| `MULE_IMPALA_USER` | your workload username |
| `MULE_IMPALA_PASSWORD` | your workload password — type it directly into the UI, never into a file that gets committed |

### 3. Session sanity checks

Open a Session (Python 3.11, same GPU profile), then:

```bash
git pull
pip3 install -r requirements.txt

python -c "import torch; print('cuda', torch.cuda.is_available())"

python -c "from mule.storage import get_storage; print(get_storage('impala').query( \
  'SELECT COUNT(*) n, MAX(snapshot_date) d FROM rsingh_mule_acct_gold.mule_features'))"

# first run downloads the model checkpoint from Hugging Face; reads + scores, writes nothing
python cai/jobs/daily_score.py --dry-run
```

Real results, this environment: `cuda True` (GPU available, so `family:
auto` resolves to **Mitra-v2**, not the TabICL/CPU path used for local
laptop validation); `gold.mule_features` had **14,004,971 rows**, latest
snapshot `2026-09-25`. The dry run: holdout AUC 1.000, capture top 1% 100%
(rules alone 58%), precision top 0.2% 26.5%, lift 1.73×, all three gates
PASS; 200,380 active accounts scored in ~7 minutes.

### 4. CAI Job: `mule-daily-score`

Jobs > New Job: name `mule-daily-score`, script `cai/jobs/daily_score.py`,
arguments empty, Python 3.11, GPU profile, schedule **Manual** (Airflow
triggers it, not the CAI scheduler). Run it once for real (writes the gold
tables and logs to MLflow):

```bash
python cai/jobs/daily_score.py
```

Get the IDs Airflow needs, from a session terminal (single line, paste
as-is — a multi-line paste can pick up stray indentation and break Python):

```bash
python3 -c "import os, cmlapi; c = cmlapi.default_client(); pid = os.environ['CDSW_PROJECT_ID']; print('MULE_CAI_HOST       =', 'https://' + os.environ['CDSW_DOMAIN']); print('MULE_CAI_PROJECT_ID =', pid); [print('MULE_CAI_JOB_ID     =', j.id, '(' + j.name + ')') for j in c.list_jobs(pid).jobs]"
```

Real values for this project:

```
MULE_CAI_HOST       = https://ml-dbfc64d1-783.go01-dem.ylcu-atmi.cloudera.site
MULE_CAI_PROJECT_ID = jxyu-tt5i-g9s7-93jd
MULE_CAI_JOB_ID     = g9qt-ic8o-e7lr-moxt
```

### 5. Model Deployment: `mule-scorer`

Model Deployments > New Model: name `mule-scorer`, file
`cai/model/predict.py`, function `predict`, Python 3.11, GPU profile, 1
replica, authentication **on**. Example input for the Test tab:

```bash
python cai/model/test_endpoint.py --print-request
```

Real result once deployed (1 replica, 2 GPUs, 2 vCPU, 4 GiB): a Test-tab
call returns real scores + a what-if comparison built from the latest Job
run's context (8,000 rows). Confirm the replica actually has GPU resources
via **Deployments** tab (not just Overview, which can show a stale `0 GPU`
reading right after a build).

### 6. Application: `Mule Investigator Console`

Applications > New Application: name `Mule Investigator Console`, script
`app/run.py`, Python 3.11, 2 vCPU / 4 GiB. Environment variables — copy
these from the `mule-scorer` deployment's own Overview page (its sample
curl command), don't hand-type them:

| Variable | Where to find it |
|---|---|
| `MULE_ENDPOINT_URL` | the `-X POST https://modelservice.../model` URL in the sample curl |
| `MULE_ENDPOINT_ACCESS_KEY` | the `accessKey` value in that same sample curl |
| `MULE_ENDPOINT_API_KEY` | User Settings > API Keys > create a **Model API key** (different from the CAI API v2 key in step 7) |

`MULE_IMPALA_USER`/`_PASSWORD` are inherited from the project, no need to
reset them on the application.

### 7. Airflow Variables

CDE Airflow UI > Admin > Variables — set the four values from step 4, plus
a CAI **API v2** key (User Settings > API Keys — a different key from the
application's Model API key):

- `MULE_CAI_HOST`
- `MULE_CAI_PROJECT_ID`
- `MULE_CAI_JOB_ID`
- `MULE_CAI_API_KEY`

### 8. First end-to-end run

A newly-registered DAG job comes with its schedule enabled but its start
date in the past, so it's live before you've finished configuring it —
**pause it immediately after creation** (`--schedule-enabled false` does
*not* pause it; only this does):

```bash
cde job schedule pause --name rsingh-mule-acct-orchestration
```

Once the Airflow Variables are set, CDE won't manually trigger a *paused*
job at all (`"job ... is paused, resume the schedule before triggering the
run"`), so unpause it — which is the real end state anyway, since it also
arms the daily 20:30 UTC / 02:00 IST schedule — then trigger a manual run:

```bash
cde job schedule unpause --name rsingh-mule-acct-orchestration
cde job run --name rsingh-mule-acct-orchestration --wait
```

Verify:

```bash
cde run describe --id <run-id>                                       # overall status, start/end time
cde run logs --id <run-id> --type cai_daily_score/attempt_1 --follow=false   # confirms the Airflow -> CAI trigger worked
```

Real result: Run 2304, `succeeded`, all six tasks, **27 minutes**
(`03:09:08Z` → `03:35:52Z`). The `cai_daily_score` task log showed the
trigger working exactly as designed: started CAI job run `s55yl2sn75k5ctsq`
with `MULE_TRIGGERED_BY=airflow`, `MULE_RUN_DATE=2026-09-27`, polled every
30s through `scheduling → running → succeeded` (~10 minutes), task exited 0.

## Before the demo (30 minutes ahead)

- CDE Job Runs / Airflow UI: today's 02:00 IST DAG run succeeded (all six
  tasks green, including `validate_bronze`).
- App Lineage tab: today's run with `triggered_by = airflow`, plus 4+
  backfilled run dates (`cai/jobs/backfill_history.py --weeks 8`).
- Model `mule-scorer` restarted after today's run; its Test tab shows
  today's `run_date`.
- Open the app and run one Hue query 5 minutes before: the Impala virtual
  warehouse auto-suspends and the first query after a pause can take
  minutes.
- Hue open on `sql/reports.sql`; the Airflow UI open on the DAG grid.
- Showing CDE live? Trigger the DAG ~30 minutes before — the real run above
  took 27 minutes end to end.

## 1. The question (1 min)

Fraudsters recruit or rent bank accounts to receive and forward stolen money
before a victim's complaint catches up. At well under 0.1% of the book, an
investigation team can't review every account — which ones get a debit
freeze today, which get held and watched, and which just go on a watchlist?

## 2. The pipeline (2 min): CDE Airflow UI

- DAG `mule_account_identifier_pipeline`, daily: KYC / core banking / UPI /
  digital session / fraud-report extracts → `validate_bronze` (the hard
  gate: null keys, orphans, a raw PAN outside its governed column, missing
  days) → silver (dedupe, salted-hash keys, identity edges) → identity
  graph (point-in-time person clusters and money-flow rings, hub
  identifiers like branch kiosks excluded) → gold features (Iceberg MERGE)
  → CAI scoring job (triggered over the API v2, polled to completion).
- A `validate_bronze` failure stops the DAG before silver: yesterday's
  alert queue stands, nothing bad reaches an investigator.
- The gold table has one row per active account per weekly snapshot, 18
  numeric features, and the label "confirmed mule within 90 days" fills in
  as it matures: each load is one Iceberg snapshot.

## 3. Alert queue (2 min): app, first tab

- Accounts scored, alert count and share of the book, rings with an alert,
  the measured book mule rate (real run, 2026-09-25 snapshot, Mitra-v2 on
  GPU: **200,380 accounts scored, 6,011 alerts — 400 T1 / 1,603 T2 / 4,008
  T3 — across 1,000+ rings, book mule rate 0.065%**). "Alerts by tier" bar
  chart and a filterable, rank-ordered queue.
- Point at the reasons column ("2 hop(s) from a reported mule; minimum-KYC
  (OTP) account"): business rules on the inputs, what the investigator
  opens the case with — not an explanation of the model's score.

## 4. Linked identities and ring view (2 min): app, next two tabs

- Pick a T1 account in Linked identities: the graph shows every other
  customer sharing a non-hub device, mobile, PAN hash or address hash, with
  whether they're alerted this run too.
- Switch to Ring view for the same account's ring: identity links plus
  own-bank money-flow edges (transfers, shared counterparties), point in
  time as of this run's snapshot. This is the evidence behind "ring of
  3-25 accounts" in the synthetic design — a rented min-KYC account usually
  sits one hop from an already-frozen one.

## 5. Can we trust it? (2 min): Holdout & trust tab

- The talking point: "the top 1% of the book catches X% of the mules that
  actually matured, against rules alone" (real run: **100% capture at the
  top 1%, vs. 58% for rules alone — a 1.73× lift**; precision at the T1
  queue (top 0.2%) is 26.5%, holdout AUC 1.000).
- Why the gate reads capture/precision/lift instead of accuracy: at well
  under 0.1% mule rate, a model that flags nobody is "99.9% accurate."
- The holdout is honest: context ends 90 days before the test weeks, as if
  the model had been deployed then.

## 6. Live what-if (2 min): What-if tab

- Pick an account near the T2/T1 boundary. Score it as is, then simulate a
  new device login, a mobile/VPA change, and pass-through jumping to 92%:
  `p_mule_adj` typically moves by well over an order of magnitude live,
  scored by the CAI model endpoint (or in-app if the endpoint isn't
  configured — the tab says which). **Whether it crosses into a higher
  tier depends on that day's cut-offs** (Mitra's scoring has some run-to-run
  variability, and cut-offs are recomputed fresh from each day's book) — the
  probability jump is the reliable part of the story, an exact tier flip is
  not guaranteed on every single run.
- Mitra-v2 / TabICL have no training step: `fit` stores the labelled
  context and learning happens at prediction time. Restarting the model
  after a new daily run is how it picks up fresh context, not a retraining
  job.

## 7. Close the loop (1 min): Decisions tab

- Record `CONFIRMED_MULE` (or `FALSE_POSITIVE`) with a checker and a note.
  It's maker-checker: nothing here freezes an account by itself. It lands
  in `bronze.investigator_decisions`; tomorrow's silver load turns a
  `CONFIRMED_MULE` into a label (and a known mule for the graph) with no
  retraining cycle.

## 8. Audit and time travel (1 min): Lineage tab + Hue

- Lineage table: model family/checkpoint, context window, and the Iceberg
  snapshot id of gold each run read.
- In Hue: `DESCRIBE HISTORY` on `mule_features`, then query 8 in
  `sql/reports.sql` with `FOR SYSTEM_VERSION AS OF <source_snapshot_id>` to
  rebuild the exact context a past run learned from. Query 6 checks whether
  a past run's alerts held up against labels that have since matured.

## Honest framing (close)

Synthetic data, demo pipeline, not a validated fraud model. It ranks
accounts for human review; it never freezes one on its own — every tier is
a recommended action that goes through the maker-checker decision flow. The
context holds account and identity rows, so it stays inside the governed
project with the same masking policies as the gold table.

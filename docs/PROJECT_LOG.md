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

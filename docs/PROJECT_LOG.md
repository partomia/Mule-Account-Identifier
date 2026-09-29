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

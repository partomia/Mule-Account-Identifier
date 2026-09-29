"""
The CAI side of this project, read by ci/setup_cai.py, ci/run_cai_job.py and
cde/scripts/set_airflow_variables.py. Pure Python (no cmlapi).

The jobs have no CAI dependencies: the nightly DAG (cde/dags/mule_dag.py)
starts sync-code, then daily-score, through the CAI API v2. A job run ignores
arguments, so settings travel in the run's environment.
"""
from __future__ import annotations

CAI_PROJECT_NAME = "rsingh-mule-acct"
GIT_URL = "https://github.com/partomia/Mule-Account-Identifier"
RUNTIME = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.11-standard:2026.08.1-b5"

SYNC_JOB = "rsingh-mule-acct-sync-code"
SCORE_JOB = "rsingh-mule-acct-daily-score"
JOBS = [
    # sync-code pip-installs torch + CUDA wheels on a change of requirements.txt: killed below 8 GB
    {"name": SYNC_JOB, "script": "cai/jobs/sync_code.py", "cpu": 2, "memory": 8},
    {"name": SCORE_JOB, "script": "cai/jobs/daily_score.py", "cpu": 8, "memory": 32},
]
BY_NAME = {j["name"]: j for j in JOBS}

MODEL = {"name": "rsingh-mule-acct-scorer", "file": "cai/model/predict.py", "cpu": 4, "memory": 16,
         "description": "Mule probability, tier and reasons for accounts on demand, with what-if"}
APP = {"name": "Mule Investigator Console", "subdomain": "rsingh-mule-acct-console", "script": "app/run.py",
       "cpu": 2, "memory": 4, "description": "Streamlit console over the published mule alert queue"}

# Airflow Variable -> CAI job name (the DAG reads these)
JOB_VARIABLES = {"MULE_CAI_SYNC_JOB_ID": SYNC_JOB, "MULE_CAI_JOB_ID": SCORE_JOB}

"""
Airflow DAG (CDE): daily mule-account scoring pipeline.

  generate_mule_bronze -> validate_bronze -> build_silver -> build_identity_graph -> build_gold_features   (CDE Spark)
    -> cai_daily_score                                                                                     (CAI Job via API v2)

validate_bronze is the one hard gate (null keys, orphans, raw PAN outside the
PAN column, missing days): a failure stops the DAG before silver, so nothing
reaches an investigator. silver / build_identity_graph / build_gold_features
take no --as-of of their own (they derive it from MAX(batch_as_of) already in
the data), so only the first task needs its arguments overridden.

The CAI step triggers the Cloudera AI job `cai/jobs/daily_score.py` and waits
for it, so one DAG run goes from the overnight extracts to a scored,
gate-checked alert queue. It needs these Airflow Variables (CDE Airflow UI >
Admin > Variables):
  MULE_CAI_HOST        https://federal-cml.federal.dp5i-5vkq.cloudera.site  (CAI workbench URL)
  MULE_CAI_PROJECT_ID  project id (from the project URL or API)
  MULE_CAI_JOB_ID      id of the daily scoring job (mule-daily-score)
  MULE_CAI_API_KEY     CAI API v2 key (User settings > API keys)
If MULE_CAI_HOST is not set the CAI step is skipped, so the Spark part can be
tested on its own before Phase 8 wires up the CAI side.

Scheduled daily at 20:30 UTC (02:00 IST): the run loads and scores the
business day just closed. Manual trigger (Trigger DAG w/ config):
{"as_of": "2026-09-25"}; empty = yesterday.

Job names must match cde/scripts/deploy_jobs.sh exactly (CDEJobRunOperator
fails with 404 "job not found" otherwise).
"""

import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

JOB_PREFIX = "rsingh-mule-acct"
DB_PREFIX = "rsingh_mule_acct"
# Scheduled runs: the day before the interval end; manual runs: the as_of param (empty = yesterday).
AS_OF = ("{{ params.as_of or ((data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') "
         "if dag_run.run_type == 'scheduled' else (macros.datetime.utcnow() - macros.timedelta(days=1))"
         ".strftime('%Y-%m-%d')) }}")
TERMINAL_OK = {"succeeded"}
TERMINAL_BAD = {"failed", "stopped", "timedout"}
DAILY = "30 20 * * *"


def trigger_cai_job(as_of: str, **_):
    host = Variable.get("MULE_CAI_HOST", default_var="").rstrip("/")
    if not host:
        raise AirflowSkipException("MULE_CAI_HOST not set: skipping the CAI scoring step")
    project = Variable.get("MULE_CAI_PROJECT_ID")
    job = Variable.get("MULE_CAI_JOB_ID")
    headers = {"Authorization": f"Bearer {Variable.get('MULE_CAI_API_KEY')}", "Content-Type": "application/json"}
    # A job run ignores "arguments" (the job's own are used); the environment map is applied.
    env = {"MULE_TRIGGERED_BY": "airflow", "MULE_RUN_DATE": as_of}
    url = f"{host}/api/v2/projects/{project}/jobs/{job}/runs"
    resp = requests.post(url, json={"environment": env}, headers=headers, timeout=60)
    resp.raise_for_status()
    run_id = resp.json()["id"]
    print(f"Started CAI job run {run_id} with environment: {env}")

    deadline = time.time() + 60 * 60
    while time.time() < deadline:
        time.sleep(30)
        r = requests.get(f"{url}/{run_id}", headers=headers, timeout=60)
        r.raise_for_status()
        status = str(r.json().get("status", "")).lower().replace("engine_", "")
        print(f"CAI run {run_id}: {status}")
        if status in TERMINAL_OK:
            return run_id
        if status in TERMINAL_BAD:
            raise AirflowException(f"CAI job run {run_id} ended with status {status}")
    raise AirflowException(f"CAI job run {run_id} did not finish within 60 minutes")


default_args = {
    "owner": "mule-account-identifier",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mule_account_identifier_pipeline",
    description="Bronze -> validate -> silver -> identity graph -> gold (CDE) -> mule scoring (CAI)",
    default_args=default_args,
    schedule_interval=DAILY,
    # in the past so manual triggers run; the first interval closes 20:30 UTC / 02:00 IST, so
    # deploying today fires no unwanted run
    start_date=datetime(2026, 9, 27, 20, 30),
    catchup=False,
    is_paused_upon_creation=False,
    params={"as_of": ""},
    tags=["mule", "fraud", "iceberg", "mitra", "tabicl"],
) as dag:

    generate = CDEJobRunOperator(
        task_id="generate_mule_bronze",
        job_name=f"{JOB_PREFIX}-generate-bronze",
        # run-time args replace the job's own args, so repeat --db-prefix
        overrides={"spark": {"args": ["--db-prefix", DB_PREFIX, "--as-of", AS_OF]}},
        wait=True,
    )
    validate = CDEJobRunOperator(task_id="validate_bronze", job_name=f"{JOB_PREFIX}-validate-bronze", wait=True)
    silver = CDEJobRunOperator(task_id="build_silver", job_name=f"{JOB_PREFIX}-build-silver", wait=True)
    graph = CDEJobRunOperator(task_id="build_identity_graph", job_name=f"{JOB_PREFIX}-build-identity-graph",
                              wait=True)
    gold = CDEJobRunOperator(task_id="build_gold_features", job_name=f"{JOB_PREFIX}-build-gold-features", wait=True)
    score = PythonOperator(
        task_id="cai_daily_score",
        python_callable=trigger_cai_job,
        op_kwargs={"as_of": AS_OF},
        retries=0,
    )

    generate >> validate >> silver >> graph >> gold >> score

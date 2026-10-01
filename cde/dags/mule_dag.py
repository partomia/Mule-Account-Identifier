"""
Airflow DAG (CDE): daily mule-account scoring pipeline.

  generate_mule_bronze -> validate_bronze -> build_silver -> build_identity_graph -> dq_silver
    -> build_gold_features -> dq_gold                                                   (CDE Spark)
    -> cai_sync_code -> cai_daily_score -> dq_publish                                   (CAI Jobs via API v2, then CDE)

Data quality gates: validate_bronze and the dq_* tasks (job rsingh-mule-acct-dq-check,
cde/jobs/dq_check.py --layer bronze|silver|gold|publish) append every check to
rsingh_mule_acct_ref.dq_results with pipeline_run = the Airflow run_id; a
critical failure fails the task and stops the DAG (bronze: before silver, so
nothing reaches an investigator), a failed warning is recorded only. A retried
task records its checks again under a new run_id; the report views read the
latest run_id per (pipeline_run, layer). silver / build_identity_graph /
build_gold_features take no --as-of of their own (they derive it from
MAX(batch_as_of) already in the data).

The CAI steps start two Cloudera AI jobs one after the other and wait for
each: sync-code brings the CAI project to origin/main (and installs a changed
requirements.txt), then `cai/jobs/daily_score.py` scores, so one DAG run goes
from the overnight extracts to a scored, gate-checked alert queue. They need
these Airflow Variables, set by cde/scripts/set_airflow_variables.py:
  MULE_CAI_HOST         https://federal-cml.federal.dp5i-5vkq.cloudera.site  (CAI workbench URL)
  MULE_CAI_PROJECT_ID   project id of rsingh-mule-acct
  MULE_CAI_SYNC_JOB_ID  id of rsingh-mule-acct-sync-code
  MULE_CAI_JOB_ID       id of rsingh-mule-acct-daily-score
  MULE_CAI_API_KEY      CAI API v2 key (User settings > API keys)
If MULE_CAI_HOST is not set the CAI steps are skipped, so the Spark part can be
tested on its own.

Scheduled daily at 20:30 UTC (02:00 IST): the run loads and scores the
business day just closed. Registered paused (is_paused_upon_creation): with
catchup=False an unpaused DAG runs the latest closed interval at once. Manual
trigger (Trigger DAG w/ config): {"as_of": "2026-09-25"}; empty = yesterday.

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
RUN = "{{ run_id }}"
TERMINAL_OK = {"succeeded"}
TERMINAL_BAD = {"failed", "stopped", "timedout"}
DAILY = "30 20 * * *"


def trigger_cai_job(job_variable: str, env: dict, deadline_min: int, **_):
    host = Variable.get("MULE_CAI_HOST", default_var="").rstrip("/")
    if not host:
        raise AirflowSkipException("MULE_CAI_HOST not set: skipping the CAI steps")
    project = Variable.get("MULE_CAI_PROJECT_ID")
    job = Variable.get(job_variable)
    headers = {"Authorization": f"Bearer {Variable.get('MULE_CAI_API_KEY')}", "Content-Type": "application/json"}
    # A job run ignores "arguments" (the job's own are used); the environment map is applied.
    url = f"{host}/api/v2/projects/{project}/jobs/{job}/runs"
    resp = requests.post(url, json={"environment": env}, headers=headers, timeout=60)
    resp.raise_for_status()
    run_id = resp.json()["id"]
    print(f"Started CAI job run {run_id} ({job_variable}) with environment: {env}")

    deadline = time.time() + deadline_min * 60
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
    raise AirflowException(f"CAI job run {run_id} did not finish within {deadline_min} minutes")


def dq_task(task_id: str, layer: str) -> CDEJobRunOperator:
    return CDEJobRunOperator(
        task_id=task_id,
        job_name=f"{JOB_PREFIX}-dq-check",
        overrides={"spark": {"args": ["--db-prefix", DB_PREFIX, "--layer", layer, "--as-of", AS_OF,
                                      "--pipeline-run", RUN]}},
        wait=True,
    )


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
    # the first interval closes 2026-09-29 20:30 UTC and scores as_of 2026-09-28
    start_date=datetime(2026, 9, 28, 20, 30),
    catchup=False,
    is_paused_upon_creation=True,
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
    validate = CDEJobRunOperator(
        task_id="validate_bronze",
        job_name=f"{JOB_PREFIX}-validate-bronze",
        overrides={"spark": {"args": ["--db-prefix", DB_PREFIX, "--as-of", AS_OF, "--pipeline-run", RUN]}},
        wait=True,
    )
    silver = CDEJobRunOperator(task_id="build_silver", job_name=f"{JOB_PREFIX}-build-silver", wait=True)
    graph = CDEJobRunOperator(task_id="build_identity_graph", job_name=f"{JOB_PREFIX}-build-identity-graph",
                              wait=True)
    dq_silver = dq_task("dq_silver", "silver")
    gold = CDEJobRunOperator(task_id="build_gold_features", job_name=f"{JOB_PREFIX}-build-gold-features", wait=True)
    dq_gold = dq_task("dq_gold", "gold")
    sync = PythonOperator(
        task_id="cai_sync_code",
        python_callable=trigger_cai_job,
        op_kwargs={"job_variable": "MULE_CAI_SYNC_JOB_ID", "env": {}, "deadline_min": 45},
    )
    score = PythonOperator(
        task_id="cai_daily_score",
        python_callable=trigger_cai_job,
        op_kwargs={"job_variable": "MULE_CAI_JOB_ID",
                   "env": {"MULE_TRIGGERED_BY": "airflow", "MULE_RUN_DATE": AS_OF}, "deadline_min": 120},
        retries=0,
    )
    dq_publish = dq_task("dq_publish", "publish")

    generate >> validate >> silver >> graph >> dq_silver >> gold >> dq_gold >> sync >> score >> dq_publish

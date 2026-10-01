#!/usr/bin/env bash
# Create/sync the CDE Repository for this GitHub repo and (re)create the six
# Spark jobs, each reading its application file straight from the repo.
# validate-bronze and dq-check record their checks in <prefix>_ref.dq_results.
#
# After a code change: git push, then either re-run this script or just
#   cde repository sync --name rsingh-mule-acct-pipeline
#
# Private repo? Store a GitHub PAT as a CDE credential first (the flag takes
# the credential NAME, not the token):
#   cde credential create --name my-github-pat --type basic --username <github-user>
#   GIT_CREDENTIAL=my-github-pat ./cde/scripts/deploy_jobs.sh
#
# Resources: a 4-core / 8 GB driver and executors of 4 cores / 8 GB, starting
# at 2 and scaling to 16 (up to 64 task slots). At ~200k customers the
# generator runs ~120 partitions and the graph job's label propagation
# shuffles ~10M (snapshot, customer) labels per iteration; the vcluster
# default (1 core / 1 GB) is far too small for either.
# The vcluster's YuniKorn queue must fit the driver plus the initial executors
# up front (PySpark adds 40% memory overhead). On go01, 8 initial executors of
# 16 GB were rejected and 4 of 8 cores / 12 GB fitted; on the shared federal
# vcluster that size failed twice within 30 s with no logs, so the defaults
# match the other projects there. Override with EXECUTOR_CORES,
# EXECUTOR_MEMORY, MIN_EXECUTORS, INITIAL_EXECUTORS, MAX_EXECUTORS.

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/partomia/Mule-Account-Identifier}"
REPO_BRANCH="${REPO_BRANCH:-main}"
REPO_NAME="${REPO_NAME:-rsingh-mule-acct-pipeline}"
PYTHON_ENV="${PYTHON_ENV:-rsingh-mule-acct-python-env}"
JOB_PREFIX="${JOB_PREFIX:-rsingh-mule-acct}"
DB_PREFIX="${DB_PREFIX:-rsingh_mule_acct}"
REQUIREMENTS="$(cd "$(dirname "$0")/.." && pwd)/resources/requirements.txt"
RESOURCES=(--driver-cores 4 --driver-memory 8g
           --executor-cores "${EXECUTOR_CORES:-4}" --executor-memory "${EXECUTOR_MEMORY:-8g}"
           --min-executors "${MIN_EXECUTORS:-1}" --initial-executors "${INITIAL_EXECUTORS:-2}"
           --max-executors "${MAX_EXECUTORS:-16}"
           --conf spark.sql.shuffle.partitions=256
           --conf spark.sql.adaptive.enabled=true
           --conf spark.sql.adaptive.coalescePartitions.enabled=true
           --conf spark.driver.maxResultSize=4g)

echo "==> Repository: ${REPO_NAME}"
if cde repository describe --name "${REPO_NAME}" &>/dev/null; then
  echo "    exists, syncing ${REPO_BRANCH}"
else
  create_args=(--name "${REPO_NAME}" --url "${REPO_URL}" --branch "${REPO_BRANCH}")
  [[ -n "${GIT_CREDENTIAL:-}" ]] && create_args+=(--credential "${GIT_CREDENTIAL}")
  cde repository create "${create_args[@]}"
fi
cde repository sync --name "${REPO_NAME}"

echo "==> Python environment resource: ${PYTHON_ENV}"
cde resource create --name "${PYTHON_ENV}" --type python-env 2>/dev/null || true
cde resource upload --name "${PYTHON_ENV}" --local-path "${REQUIREMENTS}"
echo "    building (1-3 min); jobs fail fast until it is ready:"
for _ in $(seq 1 30); do
  status="$(cde resource describe --name "${PYTHON_ENV}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))")"
  echo "    status: ${status}"
  [[ "${status}" == "ready" ]] && break
  [[ "${status}" == "failed" ]] && { echo "python-env build failed"; exit 1; }
  sleep 20
done

create_job() {
  local name=$1 file=$2
  shift 2
  if cde job describe --name "${name}" &>/dev/null; then
    cde job delete --name "${name}"
  fi
  echo "==> Creating job ${name} (${file})"
  cde job create --name "${name}" --type spark \
    --mount-1-resource "${REPO_NAME}" \
    --application-file "${file}" \
    --python-env-resource-name "${PYTHON_ENV}" \
    "${RESOURCES[@]}" \
    --arg=--db-prefix --arg="${DB_PREFIX}" "$@"
}

create_job "${JOB_PREFIX}-generate-bronze"       "cde/jobs/generate_mule_bronze.py"
create_job "${JOB_PREFIX}-validate-bronze"       "cde/jobs/validate_bronze.py"
create_job "${JOB_PREFIX}-build-silver"          "cde/jobs/build_silver.py"
create_job "${JOB_PREFIX}-build-identity-graph"  "cde/jobs/build_identity_graph.py"
create_job "${JOB_PREFIX}-build-gold-features"   "cde/jobs/build_gold_features.py"
create_job "${JOB_PREFIX}-dq-check"              "cde/jobs/dq_check.py" --arg=--layer --arg=silver

echo ""
echo "Jobs deployed from ${REPO_NAME}. Run them in order:"
for j in generate-bronze validate-bronze build-silver build-identity-graph dq-check build-gold-features; do
  echo "  cde job run --name ${JOB_PREFIX}-${j} --wait"
done
echo "  (dq-check runs --layer silver; for gold / publish override its args:"
echo "   cde job run --name ${JOB_PREFIX}-dq-check --arg --db-prefix --arg ${DB_PREFIX} --arg --layer --arg gold --wait)"
echo "Then register the DAG: ./cde/scripts/deploy_dag.sh"

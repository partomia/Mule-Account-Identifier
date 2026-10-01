# Mule dashboards (Cloudera Data Visualization)

Two dashboards built as code and imported into a Cloudera Data Visualization application in
this project's CAI workspace:

- **Mule Investigation Command Centre** (five sheets): the published pipeline output for
  investigators, fraud ops and model risk.
- **Mule Data Health** (four sheets): every data quality check the pipeline records after
  each layer, for the data engineers who run it.

Nothing in the pipeline reads the dashboards or their views; they only read.

| Layer | What | Where |
|---|---|---|
| Check results | every check of `cde/jobs/dq_check.py` after bronze, silver, gold and the CAI publish, one row per check and run | `rsingh_mule_acct_ref.dq_results` |
| Views | 8 flat reporting views, one per dataset | `sql/dataviz_views.sql` -> `rsingh_mule_acct_report` |
| Application | "Mule Data Visualization", runtime `runtimedataviz:8.1.7-b36`, 2 vCPU / 8 GB, subdomain `rsingh-mule-acct-dataviz` | `ci/cai_jobs.py` `DATAVIZ`, `ci/setup_cai.py --dataviz` |
| Connection | `rsingh-mule-acct-impala`: impyla, CDW `federal-impala-1` public endpoint, port 443, HTTP `cliservice`, TLS, LDAP as the workload user | created by `dataviz/build_dashboard.py` |
| Datasets, visuals, dashboards | 8 datasets, 53 visuals (34 + 19), 9 sheets (5 + 4), fixed UUIDs and keys | `dataviz/build_dashboard.py` `DASHBOARDS` -> `dataviz/mule_dashboards.json` |

## Command Centre sheets

- **Alert queue** (investigators, fraud-ops head): alerts on today's queue, freeze reviews
  (T1), rings touched, expected mules on the queue (sum of `p_mule_adj`), alerts one hop
  from a known mule; alerts by region and tier; top 10 branches; alerts per reason code
  (every code on an alert, not only the first); alerts by product and KYC type; the
  morning's top 25 accounts with tier, action, reasons, ring size and hops.
- **Rings and network**: rings with alerts, the largest alerted ring, alerted accounts in
  rings; 90-day mule rate by distance to a known mule and by pass-through band; the top 15
  rings by total risk (`ring_rank` in `v_rings`).
- **Trends**: active book and 90-day mule rate per snapshot by region; mules per snapshot by
  product; published alerts per run date by tier (one bar group per daily run).
- **Model trust** (model risk): holdout AUC, capture in the top 1% for the model and for the
  rules alone, precision of the T1 band (top 0.2%), lift over rules; the holdout capture
  curve and mule rate by risk band; every daily run with the KPI gate.
- **Data quality**: the latest recorded run of each layer (`v_dq`, `is_latest = 1`): checks,
  failed checks, critical failures; passed and failed by layer; the failed checks (empty
  when healthy); every check with its rule, limit (`note`), expected and actual.

## Data Health sheets

The checks (`cde/jobs/dq_check.py`, plain PySpark) run as `validate_bronze` and the DAG tasks
`dq_silver`, `dq_gold`, `dq_publish`. Each is critical (the task fails and the DAG stops:
yesterday's alert queue stays) or a warning (recorded only). Rate limits come in two tiers:
critical at the limit and a warning at half of it. `pipeline_run` is the Airflow `run_id`
(`scheduled__...`, `manual__...`) or `manual-<as_of>` for a run by hand.

- **Health now**: checks in the latest run, pass rate %, critical failures, warnings failed,
  near misses (passed, but some rows were unexpected), rows under check; checks by layer and
  category (volume, completeness, uniqueness, validity, timeliness, reconciliation, rate
  limit, privacy, leakage guard); checks by category and severity; a scorecard per table.
- **Trends**: pass rate by as-of date and layer; passed and failed per pipeline run; bronze
  rows per table per load; every rate check against its limit (% of rows).
- **Pipeline runs** (`v_dq_run`, one row per pipeline run and layer): minutes after the
  bronze gate at which each later gate ran; rows checked per run and layer; every gate run
  with its counts.
- **Check details**: the near misses; failed checks over all runs (empty when healthy); the
  full catalogue of the latest run with category, rule, column, observed value and limit.

`v_dq` keeps only the latest `run_id` of each (`pipeline_run`, layer), so a retried task is
not counted twice, and adds `run_type`, `run_label`, `layer_order` / `layer_label`,
`category`, `near_miss`, `row_count` (from the `row count` checks), `rate_pct` / `limit_pct`
and `is_latest` (the latest pipeline run of each layer). The volume checks (rows against the
previous load, -5% / +10%) need a previous load in `dq_results`, so they appear from the
second recorded as-of date on.

## Build or rebuild

```bash
set -a; source .env; set +a
python ci/setup_cai.py --dataviz --dry-run          # then without --dry-run: the application
python scripts/run_impala_sql.py sql/dataviz_views.sql
# open the application once in a browser as the workload user: CAI then gives the user the
# vizcml-rw group, which is System Admin in Data Visualization (needed to export and import)
python dataviz/build_dashboard.py                   # connection if missing, file, import
python dataviz/build_dashboard.py --verify
```

The script authenticates through the CAI proxy with the CAI API key, so no Data
Visualization API key is needed. The import matches artefacts by UUID: a rerun updates both
dashboards in place. The Command Centre's UUIDs and keys (dashboard 7000, visuals
7201-7234, datasets 7100-7106) predate Data Health and are kept; Data Health uses dashboard
7001, visuals 7401-, the `DQ runs` dataset 7107 and UUIDs under `data-health`. `--verify`
sends every visual's query of both dashboards through the Data API, so it checks Data
Visualization's own connection to Impala, not the laptop's, and recomputes every KPI tile
directly in Impala to compare. A ParseException on a new view column means the import did
not happen; rerun it.

To move the dashboards to another Data Visualization instance (for example a CDW one), import
`dataviz/mule_dashboards.json` there (Data -> Import Visual Artifacts) and pick a connection
that can read `rsingh_mule_acct_report`. The CDW instance's own connections use a
cluster-internal host (port 28000) that CAI cannot reach, which is why this one is separate.

## Notes

- A CAI project hosts one Data Visualization application; its metadata (SQLite) lives in
  `/home/cdsw/.arc` of the project.
- Table visuals sort by a dimension only, so "top N" tables filter on a rank column
  (`risk_rank`, `ring_rank`) instead of sorting by a measure.
- In Impala `LIKE`, `_` matches any character: `v_dq` tests `manual-%` before `manual__%`.
- The built-in `vizapps_admin` account has a default password: change it in the
  application (Gear -> Users) before sharing the URL.
- The connection stores the workload password inside Data Visualization; rotate it there
  (Data -> connection -> Edit) when the password changes.

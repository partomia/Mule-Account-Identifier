# Mule Investigation Command Centre (Cloudera Data Visualization)

A five-sheet reporting dashboard over the published pipeline output, built as code and
imported into a Cloudera Data Visualization application in this project's CAI workspace.
Nothing in the pipeline reads it; it only reads.

| Layer | What | Where |
|---|---|---|
| Views | 7 flat reporting views, one per dataset | `sql/dataviz_views.sql` -> `rsingh_mule_acct_report` |
| Application | "Mule Data Visualization", runtime `runtimedataviz:8.1.7-b36`, 2 vCPU / 8 GB, subdomain `rsingh-mule-acct-dataviz` | `ci/cai_jobs.py` `DATAVIZ`, `ci/setup_cai.py --dataviz` |
| Connection | `rsingh-mule-acct-impala`: impyla, CDW `federal-impala-1` public endpoint, port 443, HTTP `cliservice`, TLS, LDAP as the workload user | created by `dataviz/build_dashboard.py` |
| Datasets, visuals, dashboard | 7 datasets, 34 visuals, 5 sheets, fixed UUIDs | `dataviz/build_dashboard.py` -> `dataviz/mule_command_centre.json` |

## Sheets

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
- **Data quality**: checks, failed checks, critical failures; passed and failed by layer; the
  failed checks (empty when healthy); every check. `v_dq` computes the checks live on each
  query: bronze-to-silver row reconciliation after the silver rules (dedupe, positive
  amounts, known accounts), transactions of unknown accounts and unresolved complaints
  against the 0.1% / 1% limits, null keys, future-dated rows, the gold merge key, and the
  latest run row against the rows in `mule_features` and `mule_alerts` per tier.

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
Visualization API key is needed. The import matches artefacts by UUID: a rerun updates the
dashboard in place. `--verify` sends every visual's query through the Data API, so it checks
Data Visualization's own connection to Impala, not the laptop's, and recomputes every KPI
tile directly in Impala to compare.

To move the dashboard to another Data Visualization instance (for example a CDW one), import
`dataviz/mule_command_centre.json` there (Data -> Import Visual Artifacts) and pick a
connection that can read `rsingh_mule_acct_report`. The CDW instance's own connections use a
cluster-internal host (port 28000) that CAI cannot reach, which is why this one is separate.

## Notes

- A CAI project hosts one Data Visualization application; its metadata (SQLite) lives in
  `/home/cdsw/.arc` of the project.
- Table visuals sort by a dimension only, so "top N" tables filter on a rank column
  (`risk_rank`, `ring_rank`) instead of sorting by a measure.
- The built-in `vizapps_admin` account has a default password: change it in the
  application (Gear -> Users) before sharing the URL.
- The connection stores the workload password inside Data Visualization; rotate it there
  (Data -> connection -> Edit) when the password changes.

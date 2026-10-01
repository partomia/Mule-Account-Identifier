-- Reporting views for the Cloudera Data Visualization dashboard "Mule Investigation Command
-- Centre" (docs/DATAVIZ.md). One flat view per dataset, so every visual is a drag-and-drop on
-- one table. Views only: nothing is copied, each visual reads the latest published output, and
-- nothing in the pipeline reads them. Impala has no CREATE OR REPLACE VIEW, so each is dropped
-- and recreated; datasets refer to them by name. Re-runnable:
--   python scripts/run_impala_sql.py sql/dataviz_views.sql

CREATE DATABASE IF NOT EXISTS rsingh_mule_acct_report;

-- Dataset "Alerts": every published alert, with the branch's city and region, the product, the
-- main (first) reason and the account's features on the scored snapshot.
DROP VIEW IF EXISTS rsingh_mule_acct_report.v_alerts;
CREATE VIEW rsingh_mule_acct_report.v_alerts AS
SELECT a.run_date,
       CASE WHEN a.run_date = m.latest THEN 1 ELSE 0 END                             AS is_latest,
       a.run_id, a.risk_rank, a.account_id, a.cif, a.tier, a.action,
       a.branch_code, b.branch_name, b.city, b.state, b.region,
       a.product_code, p.product_name,
       CASE WHEN f.min_kyc_flag = 1 THEN 'Min-KYC (OTP)' ELSE 'Full KYC' END         AS kyc_type,
       COALESCE(NULLIF(split_part(a.reason_codes, ',', 1), ''), 'MODEL_ONLY')         AS main_reason,
       a.reason_codes, a.reasons, a.ring_id, a.ring_size,
       CASE WHEN a.ring_size > 1 THEN 1 ELSE 0 END                                    AS in_ring,
       a.hops_to_known_mule,
       CASE WHEN a.hops_to_known_mule <= 1 THEN 1 ELSE 0 END                          AS near_known_mule,
       a.p_mule, a.p_mule_adj, a.risk_pct,
       f.account_age_days, f.pass_through_ratio_7d, f.median_hold_hours_30d, f.distinct_senders_7d,
       f.new_device_logins_30d, f.accounts_on_same_device, f.cifs_sharing_mobile
FROM rsingh_mule_acct_gold.mule_alerts a
CROSS JOIN (SELECT MAX(run_date) AS latest FROM rsingh_mule_acct_gold.mule_alerts) m
LEFT JOIN rsingh_mule_acct_ref.branch_map b ON b.branch_code = a.branch_code
LEFT JOIN rsingh_mule_acct_ref.product_map p ON p.product_code = a.product_code
LEFT JOIN rsingh_mule_acct_gold.mule_features f
  ON f.account_id = a.account_id AND f.snapshot_date = a.snapshot_date;

-- Dataset "Alert reasons": one row per alert and reason code (an alert has up to three), so
-- the frequency of every code is a plain count. The code list is mule/reasons.py RULES.
DROP VIEW IF EXISTS rsingh_mule_acct_report.v_alert_reasons;
CREATE VIEW rsingh_mule_acct_report.v_alert_reasons AS
SELECT a.run_date,
       CASE WHEN a.run_date = m.latest THEN 1 ELSE 0 END AS is_latest,
       a.account_id, a.tier, b.region, c.reason_code
FROM rsingh_mule_acct_gold.mule_alerts a
CROSS JOIN (SELECT MAX(run_date) AS latest FROM rsingh_mule_acct_gold.mule_alerts) m
LEFT JOIN rsingh_mule_acct_ref.branch_map b ON b.branch_code = a.branch_code
JOIN (SELECT 'NEAR_KNOWN_MULE' AS reason_code UNION ALL SELECT 'COMPLAINANT_CREDITS'
      UNION ALL SELECT 'PASS_THROUGH' UNION ALL SELECT 'FAST_HOLD' UNION ALL SELECT 'RING'
      UNION ALL SELECT 'SHARED_DEVICE' UNION ALL SELECT 'SHARED_MOBILE'
      UNION ALL SELECT 'DEVICE_OR_MOBILE_CHANGE' UNION ALL SELECT 'INFLOW_VS_INCOME'
      UNION ALL SELECT 'MANY_SENDERS' UNION ALL SELECT 'DORMANT_REACTIVATED'
      UNION ALL SELECT 'NEW_ACCOUNT' UNION ALL SELECT 'MIN_KYC' UNION ALL SELECT 'NIGHT_ACTIVITY') c
  ON concat(',', a.reason_codes, ',') LIKE concat('%,', c.reason_code, ',%');

-- Dataset "Rings": every published ring report, ranked by total risk within its run.
DROP VIEW IF EXISTS rsingh_mule_acct_report.v_rings;
CREATE VIEW rsingh_mule_acct_report.v_rings AS
SELECT r.run_date,
       CASE WHEN r.run_date = m.latest THEN 1 ELSE 0 END AS is_latest,
       ROW_NUMBER() OVER (PARTITION BY r.run_date ORDER BY r.sum_p_mule_adj DESC, r.ring_id) AS ring_rank,
       r.ring_id, r.ring_size, r.accounts_scored, r.accounts_alerted,
       r.accounts_alerted / r.accounts_scored            AS alerted_share,
       r.top_tier, r.max_p_mule_adj, r.sum_p_mule_adj AS ring_risk,
       r.min_hops_to_known_mule, r.top_reason_codes
FROM rsingh_mule_acct_gold.mule_rings r
CROSS JOIN (SELECT MAX(run_date) AS latest FROM rsingh_mule_acct_gold.mule_rings) m;

-- Dataset "Book weekly": the active book per snapshot, region, product, KYC type, distance to a
-- known mule and pass-through band. Rates are sum(mules) / sum(labelled) in the visual; the
-- label is NULL until 90 days after the snapshot.
DROP VIEW IF EXISTS rsingh_mule_acct_report.v_book_weekly;
CREATE VIEW rsingh_mule_acct_report.v_book_weekly AS
SELECT f.snapshot_date, b.region, p.product_name,
       CASE WHEN f.min_kyc_flag = 1 THEN 'Min-KYC (OTP)' ELSE 'Full KYC' END AS kyc_type,
       CASE WHEN f.hops_to_known_mule <= 1 THEN '1 hop'
            WHEN f.hops_to_known_mule = 2 THEN '2 hops' ELSE '3+ hops / none' END AS hops_band,
       CASE WHEN f.pass_through_ratio_7d >= 0.9 THEN 'a. 90% or more'
            WHEN f.pass_through_ratio_7d >= 0.7 THEN 'b. 70-90%'
            WHEN f.pass_through_ratio_7d >= 0.4 THEN 'c. 40-70%'
            WHEN f.pass_through_ratio_7d > 0 THEN 'd. under 40%'
            ELSE 'e. none' END                                                AS pass_through_band,
       COUNT(*)                                                               AS accounts,
       COUNT(f.is_mule_90d)                                                   AS labelled,
       SUM(f.is_mule_90d)                                                     AS mules
FROM rsingh_mule_acct_gold.mule_features f
LEFT JOIN rsingh_mule_acct_ref.branch_map b ON b.branch_code = f.branch_code
LEFT JOIN rsingh_mule_acct_ref.product_map p ON p.product_code = f.product_code
GROUP BY 1, 2, 3, 4, 5, 6;

-- Dataset "Model runs": one row per daily run, the trust numbers and the KPI gate.
DROP VIEW IF EXISTS rsingh_mule_acct_report.v_model_run;
CREATE VIEW rsingh_mule_acct_report.v_model_run AS
SELECT r.run_date, CASE WHEN r.run_ts = m.latest THEN 1 ELSE 0 END AS is_latest,
       r.run_id, r.run_ts, r.model_family, r.device, r.triggered_by, r.scored_accounts,
       r.alerts_t1, r.alerts_t2, r.alerts_t3, r.alerts_t1 + r.alerts_t2 + r.alerts_t3 AS alerts_total,
       r.book_mule_rate, r.holdout_auc, r.holdout_pr_auc, r.capture_top1, r.capture_top5,
       r.precision_top02, r.precision_top1, r.rules_capture_top1, r.rules_precision_top1,
       r.lift_over_rules, r.t1_cutoff,
       CAST(r.gate_passed AS INT) AS gate_passed, CAST(r.alerts_published AS INT) AS alerts_published,
       r.gate_detail, r.duration_s
FROM rsingh_mule_acct_gold.mule_model_run r
CROSS JOIN (SELECT MAX(run_ts) AS latest FROM rsingh_mule_acct_gold.mule_model_run) m;

-- Dataset "Holdout": the capture curve by risk band, model against the rules-only score.
-- band_label is numbered so the bands sort in rank order.
DROP VIEW IF EXISTS rsingh_mule_acct_report.v_holdout;
CREATE VIEW rsingh_mule_acct_report.v_holdout AS
SELECT h.run_date, CASE WHEN h.run_id = m.run_id THEN 1 ELSE 0 END AS is_latest,
       concat(CAST(ROW_NUMBER() OVER (PARTITION BY h.run_id ORDER BY h.upto_pct) AS STRING), '. ', h.band)
                                                                    AS band_label,
       h.upto_pct, CAST(h.accounts AS INT) AS accounts, CAST(h.mules AS INT) AS mules,
       h.mule_rate, h.mean_p_mule, h.cum_capture AS model_cum_capture, h.rules_cum_capture
FROM rsingh_mule_acct_gold.mule_holdout h
CROSS JOIN (SELECT run_id FROM rsingh_mule_acct_gold.mule_model_run
            ORDER BY run_ts DESC LIMIT 1) m;

-- Dataset "Data quality": every recorded check of cde/jobs/dq_check.py (rsingh_mule_acct_ref.dq_results).
-- A retried DAG task records its layer again under a new run_id; only the latest run_id of each
-- (pipeline_run, layer) is kept. is_latest flags the latest pipeline run of each layer. A near miss
-- passed but found unexpected rows (re-sent duplicates, orphans under the limit). expected / actual:
-- the limit and the observed number of the rule (count rules: 0 and the bad rows; rates: the
-- maximum rate and the rate; reconciliations: the source count and the count). In LIKE, _ is a
-- wildcard, so 'manual-%' (a run by hand) is tested before Airflow's 'manual__%'.
DROP VIEW IF EXISTS rsingh_mule_acct_report.v_dq;
CREATE VIEW rsingh_mule_acct_report.v_dq AS
SELECT d.pipeline_run,
       CASE WHEN d.pipeline_run LIKE 'scheduled__%' THEN 'nightly'
            WHEN d.pipeline_run LIKE 'manual-%' THEN 'by hand'
            WHEN d.pipeline_run LIKE 'manual__%' THEN 'triggered' ELSE 'other' END           AS run_type,
       concat(CAST(d.as_of AS STRING), ' ',
              CASE WHEN d.pipeline_run LIKE 'scheduled__%' THEN 'nightly'
                   WHEN d.pipeline_run LIKE 'manual-%' THEN 'by hand'
                   WHEN d.pipeline_run LIKE 'manual__%' THEN 'triggered' ELSE 'other' END,
              ' ', from_timestamp(d.started_ts, 'HH:mm'))                                   AS run_label,
       d.run_id, d.run_ts, d.as_of, d.layer,
       CASE d.layer WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 WHEN 'gold' THEN 3 WHEN 'publish' THEN 4
            ELSE 9 END                                                                      AS layer_order,
       concat(CASE d.layer WHEN 'bronze' THEN '1' WHEN 'silver' THEN '2' WHEN 'gold' THEN '3'
                           WHEN 'publish' THEN '4' ELSE '9' END, '. ', d.layer)            AS layer_label,
       d.table_name, d.check_name, d.expectation_type AS rule,
       CASE WHEN d.check_name LIKE '%raw PAN%' OR d.check_name LIKE '%SHA-256%' THEN 'Privacy'
            WHEN d.check_name LIKE '%(leakage)%' THEN 'Leakage guard'
            WHEN d.expectation_type IN ('row_count', 'volume_change') THEN 'Volume'
            WHEN d.expectation_type = 'not_null' THEN 'Completeness'
            WHEN d.expectation_type = 'unique' THEN 'Uniqueness'
            WHEN d.expectation_type = 'reconciliation_equal' THEN 'Reconciliation'
            WHEN d.expectation_type = 'rate_at_most' THEN 'Rate limit'
            WHEN d.expectation_type = 'not_after_as_of' THEN 'Timeliness'
            ELSE 'Validity' END                                                             AS category,
       d.column_name, d.severity,
       CAST(d.success AS INT) AS passed, 1 - CAST(d.success AS INT) AS failed,
       CASE WHEN d.success AND d.unexpected_count > 0 THEN 1 ELSE 0 END                    AS near_miss,
       d.observed_value, d.actual, d.expected, d.actual - d.expected AS diff,
       CASE WHEN d.expectation_type = 'rate_at_most' THEN 100 * d.actual END               AS rate_pct,
       CASE WHEN d.expectation_type = 'rate_at_most' THEN 100 * d.expected END             AS limit_pct,
       d.unexpected_count, d.unexpected_pct, d.element_count,
       CASE WHEN d.expectation_type = 'row_count' AND d.check_name = 'row count'
            THEN CAST(d.observed_value AS BIGINT) END                                       AS row_count,
       d.kwargs AS note, d.table_snapshot_id,
       CASE WHEN d.pipeline_run = l.pipeline_run THEN 1 ELSE 0 END                         AS is_latest
FROM (
  SELECT r.*,
         CASE WHEN r.expectation_type IN ('row_count', 'reconciliation_equal', 'rate_at_most', 'volume_change')
              THEN CAST(r.observed_value AS DOUBLE) ELSE CAST(r.unexpected_count AS DOUBLE) END AS actual,
         CASE r.expectation_type
              WHEN 'row_count' THEN CAST(get_json_object(r.kwargs, '$.min_value') AS DOUBLE)
              WHEN 'reconciliation_equal' THEN CAST(get_json_object(r.kwargs, '$.expected') AS DOUBLE)
              WHEN 'rate_at_most' THEN CAST(get_json_object(r.kwargs, '$.max_rate') AS DOUBLE)
              WHEN 'volume_change' THEN CAST(NULL AS DOUBLE)
              ELSE CAST(0 AS DOUBLE) END                                                     AS expected,
         MAX(r.run_ts) OVER (PARTITION BY r.pipeline_run, r.layer) AS last_ts,
         MIN(r.run_ts) OVER (PARTITION BY r.pipeline_run) AS started_ts
  FROM rsingh_mule_acct_ref.dq_results r
) d
LEFT JOIN (
  SELECT layer, pipeline_run
  FROM (SELECT layer, pipeline_run,
               ROW_NUMBER() OVER (PARTITION BY layer ORDER BY MAX(run_ts) DESC) AS rn
        FROM rsingh_mule_acct_ref.dq_results GROUP BY layer, pipeline_run) x
  WHERE rn = 1
) l ON l.layer = d.layer
WHERE d.run_ts = d.last_ts;

-- Dataset "DQ runs": one row per pipeline run and layer: when each gate ran relative to the
-- run's bronze gate, and what it checked.
DROP VIEW IF EXISTS rsingh_mule_acct_report.v_dq_run;
CREATE VIEW rsingh_mule_acct_report.v_dq_run AS
SELECT pipeline_run, run_type, run_label, as_of, layer, layer_order, layer_label, MAX(is_latest) AS is_latest,
       MAX(run_ts)                                                   AS checked_at,
       ROUND((UNIX_TIMESTAMP(MAX(run_ts))
              - UNIX_TIMESTAMP(MIN(MIN(run_ts)) OVER (PARTITION BY pipeline_run))) / 60, 1) AS minutes_after_bronze,
       COUNT(*)                                                      AS checks,
       SUM(passed)                                                   AS passed,
       SUM(failed)                                                   AS failed,
       SUM(CASE WHEN severity = 'critical' THEN failed ELSE 0 END)   AS critical_failed,
       SUM(CASE WHEN severity = 'warning' THEN failed ELSE 0 END)    AS warnings_failed,
       SUM(near_miss)                                                AS near_misses,
       COUNT(DISTINCT table_name)                                    AS table_count,
       SUM(row_count)                                                AS rows_checked
FROM rsingh_mule_acct_report.v_dq
GROUP BY pipeline_run, run_type, run_label, as_of, layer, layer_order, layer_label;

-- Checks: the numbers the dashboard's KPI tiles should show

SELECT COUNT(*) AS alerts, SUM(CASE WHEN tier = 'T1_FREEZE_REVIEW' THEN 1 ELSE 0 END) AS t1_freeze,
       COUNT(DISTINCT CASE WHEN in_ring = 1 THEN ring_id END) AS rings_touched,
       ROUND(SUM(p_mule_adj), 1) AS expected_mules, SUM(near_known_mule) AS near_known_mule
FROM rsingh_mule_acct_report.v_alerts WHERE is_latest = 1;

SELECT reason_code, COUNT(*) AS alerts FROM rsingh_mule_acct_report.v_alert_reasons
WHERE is_latest = 1 GROUP BY reason_code ORDER BY alerts DESC;

SELECT COUNT(*) AS rings, SUM(CASE WHEN accounts_alerted > 0 THEN 1 ELSE 0 END) AS rings_alerted,
       MAX(CASE WHEN accounts_alerted > 0 THEN ring_size END) AS largest_alerted
FROM rsingh_mule_acct_report.v_rings WHERE is_latest = 1;

SELECT hops_band, SUM(labelled) AS labelled, SUM(mules) AS mules,
       ROUND(100 * SUM(mules) / SUM(labelled), 3) AS mule_rate_pct
FROM rsingh_mule_acct_report.v_book_weekly GROUP BY hops_band ORDER BY hops_band;

SELECT run_date, holdout_auc, capture_top1, rules_capture_top1, precision_top02, lift_over_rules, gate_passed
FROM rsingh_mule_acct_report.v_model_run ORDER BY run_date;

SELECT band_label, model_cum_capture, rules_cum_capture FROM rsingh_mule_acct_report.v_holdout
WHERE is_latest = 1 ORDER BY band_label;

SELECT COUNT(*) AS checks, ROUND(100 * SUM(passed) / COUNT(*), 2) AS pass_rate_pct,
       SUM(CASE WHEN severity = 'critical' THEN failed ELSE 0 END) AS critical_failed,
       SUM(CASE WHEN severity = 'warning' THEN failed ELSE 0 END) AS warnings_failed,
       SUM(near_miss) AS near_misses, SUM(row_count) AS rows_checked
FROM rsingh_mule_acct_report.v_dq WHERE is_latest = 1;

SELECT layer_label, category, COUNT(*) AS checks, SUM(failed) AS failed, SUM(near_miss) AS near_misses
FROM rsingh_mule_acct_report.v_dq WHERE is_latest = 1 GROUP BY layer_label, category
ORDER BY layer_label, category;

SELECT layer_label, table_name, check_name, severity, observed_value, unexpected_count
FROM rsingh_mule_acct_report.v_dq WHERE is_latest = 1 AND (failed = 1 OR near_miss = 1)
ORDER BY layer_label, table_name;

SELECT run_label, layer_label, checked_at, minutes_after_bronze, checks, failed, near_misses, table_count,
       rows_checked
FROM rsingh_mule_acct_report.v_dq_run ORDER BY checked_at;

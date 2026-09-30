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

-- Dataset "Data quality": reconciliation and integrity checks, computed live on every query.
-- rule 'equal' passes when actual = expected; 'at most' when actual <= expected, where expected
-- is the 0.1% limit of cde/jobs/validate_bronze.py MAX_BAD_RATE. Silver drops transactions of
-- unknown accounts and complaints it cannot resolve, so those are counted, not treated as loss.
DROP VIEW IF EXISTS rsingh_mule_acct_report.v_dq;
CREATE VIEW rsingh_mule_acct_report.v_dq AS
SELECT layer, table_name, check_name, severity, rule, expected, actual, actual - expected AS diff,
       CASE WHEN (rule = 'equal' AND actual = expected) OR (rule = 'at most' AND actual <= expected)
            THEN 1 ELSE 0 END AS passed,
       CASE WHEN (rule = 'equal' AND actual = expected) OR (rule = 'at most' AND actual <= expected)
            THEN 0 ELSE 1 END AS failed,
       note
FROM (
  SELECT 'silver' AS layer, 'customer' AS table_name, 'rows = distinct bronze cif' AS check_name,
         'critical' AS severity, 'equal' AS rule, x.n AS expected, y.n AS actual,
         'kyc_onboarding deduplicated on cif' AS note
  FROM (SELECT COUNT(DISTINCT cif) n FROM rsingh_mule_acct_bronze.kyc_onboarding) x
  CROSS JOIN (SELECT COUNT(*) n FROM rsingh_mule_acct_silver.customer) y
  UNION ALL
  SELECT 'silver', 'account', 'rows = distinct bronze account_id', 'critical', 'equal', x.n, y.n,
         'cbs_accounts deduplicated on account_id'
  FROM (SELECT COUNT(DISTINCT account_id) n FROM rsingh_mule_acct_bronze.cbs_accounts) x
  CROSS JOIN (SELECT COUNT(*) n FROM rsingh_mule_acct_silver.account) y
  UNION ALL
  SELECT 'silver', 'txn', 'rows = distinct positive bronze txn_id of known accounts', 'critical', 'equal',
         x.n, y.n, 'upi_transactions deduplicated on txn_id, amount > 0, account in cbs_accounts'
  FROM (SELECT COUNT(DISTINCT t.txn_id) n FROM rsingh_mule_acct_bronze.upi_transactions t
        LEFT SEMI JOIN rsingh_mule_acct_bronze.cbs_accounts a ON a.account_id = t.account_id
        WHERE t.amount > 0) x
  CROSS JOIN (SELECT COUNT(*) n FROM rsingh_mule_acct_silver.txn) y
  UNION ALL
  SELECT 'bronze', 'upi_transactions', 'transactions of unknown accounts (dropped by silver)', 'warning',
         'at most', CAST(FLOOR(x.total * 0.001) AS BIGINT), x.orphans, 'limit 0.1% of rows'
  FROM (SELECT COUNT(*) total,
               SUM(CASE WHEN a.account_id IS NULL THEN 1 ELSE 0 END) orphans
        FROM rsingh_mule_acct_bronze.upi_transactions t
        LEFT JOIN (SELECT DISTINCT account_id FROM rsingh_mule_acct_bronze.cbs_accounts) a
          ON a.account_id = t.account_id) x
  UNION ALL
  SELECT 'bronze', 'upi_transactions', 'null txn_id or account_id', 'critical', 'equal', 0,
         SUM(CASE WHEN txn_id IS NULL OR account_id IS NULL THEN 1 ELSE 0 END), 'keys must be present'
  FROM rsingh_mule_acct_bronze.upi_transactions
  UNION ALL
  SELECT 'bronze', 'upi_transactions', 'transactions after the batch as-of date', 'critical', 'equal', 0,
         SUM(CASE WHEN to_date(txn_ts) > batch_as_of THEN 1 ELSE 0 END), 'no future-dated rows'
  FROM rsingh_mule_acct_bronze.upi_transactions
  UNION ALL
  SELECT 'silver', 'report', 'report_ids not in bronze', 'critical', 'equal', 0, COUNT(*),
         'every resolved complaint comes from fraud_reports'
  FROM rsingh_mule_acct_silver.report r
  LEFT ANTI JOIN rsingh_mule_acct_bronze.fraud_reports b ON b.report_id = r.report_id
  UNION ALL
  SELECT 'silver', 'report', 'complaints not resolved to an account', 'warning', 'at most',
         CAST(FLOOR(x.n * 0.01) AS BIGINT), x.n - y.n, 'limit 1% of complaints; one complaint can resolve to several accounts'
  FROM (SELECT COUNT(DISTINCT report_id) n FROM rsingh_mule_acct_bronze.fraud_reports) x
  CROSS JOIN (SELECT COUNT(DISTINCT report_id) n FROM rsingh_mule_acct_silver.report) y
  UNION ALL
  SELECT 'gold', 'mule_features', 'duplicate (account_id, snapshot_date)', 'critical', 'equal', 0,
         COUNT(*) - COUNT(DISTINCT concat(account_id, '|', CAST(snapshot_date AS STRING))), 'merge key is unique'
  FROM rsingh_mule_acct_gold.mule_features
  UNION ALL
  SELECT 'gold', 'mule_features', 'latest snapshot rows = scored accounts of the latest run', 'critical',
         'equal', r.scored_accounts, f.n, 'mule_model_run.scored_accounts'
  FROM (SELECT scored_accounts, snapshot_date FROM rsingh_mule_acct_gold.mule_model_run
        ORDER BY run_ts DESC LIMIT 1) r
  JOIN (SELECT snapshot_date, COUNT(*) n FROM rsingh_mule_acct_gold.mule_features GROUP BY snapshot_date) f
    ON f.snapshot_date = r.snapshot_date
  UNION ALL
  SELECT 'gold', 'mule_alerts', concat('latest run ', e.tier, ' rows = the run row'), 'critical', 'equal',
         e.expected, NVL(a.n, 0), 'mule_model_run.alerts_t1 / t2 / t3 of the same run_id'
  FROM (SELECT run_id, 'T1_FREEZE_REVIEW' AS tier, alerts_t1 AS expected FROM
          (SELECT * FROM rsingh_mule_acct_gold.mule_model_run ORDER BY run_ts DESC LIMIT 1) r1
        UNION ALL SELECT run_id, 'T2_HOLD_MONITOR', alerts_t2 FROM
          (SELECT * FROM rsingh_mule_acct_gold.mule_model_run ORDER BY run_ts DESC LIMIT 1) r2
        UNION ALL SELECT run_id, 'T3_WATCHLIST', alerts_t3 FROM
          (SELECT * FROM rsingh_mule_acct_gold.mule_model_run ORDER BY run_ts DESC LIMIT 1) r3) e
  LEFT JOIN (SELECT run_id, tier, COUNT(*) n FROM rsingh_mule_acct_gold.mule_alerts GROUP BY run_id, tier) a
    ON a.run_id = e.run_id AND a.tier = e.tier
) c;

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

SELECT layer, table_name, check_name, rule, expected, actual, passed
FROM rsingh_mule_acct_report.v_dq ORDER BY layer, table_name, check_name;

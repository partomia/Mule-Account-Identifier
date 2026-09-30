-- Mule Account Identifier: report queries for Hue (CDW Impala).
-- Tables written by Spark (CDE) or the CAI job may need a metadata refresh first.
INVALIDATE METADATA rsingh_mule_acct_gold.mule_features;
REFRESH rsingh_mule_acct_gold.mule_alerts;
REFRESH rsingh_mule_acct_gold.mule_rings;
REFRESH rsingh_mule_acct_gold.mule_model_run;

-- 1. Today's alert counts and mean risk by tier
WITH latest AS (SELECT MAX(run_date) AS d FROM rsingh_mule_acct_gold.mule_alerts)
SELECT tier,
       COUNT(*)                    AS accounts,
       ROUND(AVG(p_mule_adj), 5)   AS mean_p_mule_adj,
       COUNT(DISTINCT ring_id)     AS distinct_rings
FROM rsingh_mule_acct_gold.mule_alerts a, latest
WHERE a.run_date = latest.d
GROUP BY tier
ORDER BY MIN(risk_rank);

-- 2. The morning's top 25 alerts with the reasons an investigator opens with
SELECT risk_rank, account_id, cif, branch_code, product_code, tier, action, p_mule_adj, ring_size,
       hops_to_known_mule, reasons
FROM rsingh_mule_acct_gold.mule_alerts
WHERE run_date = (SELECT MAX(run_date) FROM rsingh_mule_acct_gold.mule_alerts)
ORDER BY risk_rank
LIMIT 25;

-- 3. Weekly active book size and measured mule rate (NULL until labels mature)
SELECT snapshot_date,
       COUNT(*)                          AS active_accounts,
       SUM(is_mule_90d)                  AS confirmed_mules,
       ROUND(AVG(is_mule_90d), 5)        AS mule_rate
FROM rsingh_mule_acct_gold.mule_features
GROUP BY snapshot_date
ORDER BY snapshot_date DESC
LIMIT 20;

-- 4. Ring report: today's rings ranked by total risk
SELECT ring_id, ring_size, accounts_scored, accounts_alerted, top_tier, max_p_mule_adj, sum_p_mule_adj,
       min_hops_to_known_mule, top_reason_codes
FROM rsingh_mule_acct_gold.mule_rings
WHERE run_date = (SELECT MAX(run_date) FROM rsingh_mule_acct_gold.mule_rings)
ORDER BY sum_p_mule_adj DESC
LIMIT 25;

-- 5. Trust number and lineage for every daily run
SELECT run_date, run_id, run_ts, model_family, model_id, device, scored_accounts,
       alerts_t1, alerts_t2, alerts_t3, context_rows, context_from, context_to, book_mule_rate,
       holdout_auc, capture_top1, precision_top02, lift_over_rules, gate_passed, alerts_published,
       source_snapshot_id, triggered_by
FROM rsingh_mule_acct_gold.mule_model_run
ORDER BY run_date DESC;

-- 6. Did a past run's alerts hold up? Join the alert queue to the labels that
-- have matured since (is_mule_90d is NULL until 90 days after the snapshot)
SELECT al.run_date, al.tier,
       COUNT(*)                          AS accounts,
       SUM(f.is_mule_90d)                AS confirmed_mules,
       ROUND(AVG(f.is_mule_90d), 3)      AS confirm_rate,
       ROUND(AVG(al.p_mule_adj), 5)      AS mean_p_mule_adj
FROM rsingh_mule_acct_gold.mule_alerts al
JOIN rsingh_mule_acct_gold.mule_features f
  ON f.account_id = al.account_id AND f.snapshot_date = al.snapshot_date
WHERE f.is_mule_90d IS NOT NULL
GROUP BY al.run_date, al.tier
ORDER BY al.run_date, MIN(al.risk_rank);

-- 7. Investigator decisions (maker-checker), fed into tomorrow's labels by the CDE silver job
SELECT decided_at, account_id, cif, decision, action, maker, checker, notes
FROM rsingh_mule_acct_bronze.investigator_decisions
ORDER BY decided_at DESC
LIMIT 50;

-- 8. Iceberg time travel: the exact context a past run learned from
DESCRIBE HISTORY rsingh_mule_acct_gold.mule_features;

-- use mule_model_run.source_snapshot_id, context_from, context_to and context_rows of the run
SELECT COUNT(*) AS context_rows, SUM(is_mule_90d) AS context_mules,
       ROUND(AVG(is_mule_90d), 3) AS context_mule_rate
FROM (
  SELECT is_mule_90d
  FROM rsingh_mule_acct_gold.mule_features FOR SYSTEM_VERSION AS OF 1234567890123456789
  WHERE is_mule_90d IS NOT NULL
    AND snapshot_date BETWEEN DATE '2025-12-26' AND DATE '2026-06-26'
  ORDER BY snapshot_date DESC, account_id
  LIMIT 2000
) ctx;

-- labels maturing: how a snapshot's rows looked before the latest load (needs a day of table history)
SELECT snapshot_date, COUNT(*) AS rows_, COUNT(is_mule_90d) AS labelled
FROM rsingh_mule_acct_gold.mule_features FOR SYSTEM_TIME AS OF now() - INTERVAL 1 DAYS
GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 6;

-- Analytics views for Data Explorer (Hue). 13 and 14 use matured labels only
-- (is_mule_90d IS NOT NULL). Refresh the Spark-written silver tables too:
REFRESH rsingh_mule_acct_gold.mule_holdout;
REFRESH rsingh_mule_acct_silver.txn;
REFRESH rsingh_mule_acct_silver.report;
REFRESH rsingh_mule_acct_silver.account;

-- 9. Morning briefing: today's run in one row
WITH r AS (SELECT * FROM rsingh_mule_acct_gold.mule_model_run
           WHERE run_ts = (SELECT MAX(run_ts) FROM rsingh_mule_acct_gold.mule_model_run))
SELECT run_date, model_family, scored_accounts,
       alerts_t1, alerts_t2, alerts_t3, alerts_t1 + alerts_t2 + alerts_t3 AS total_alerts,
       ROUND(100 * (alerts_t1 + alerts_t2 + alerts_t3) / scored_accounts, 2) AS pct_of_book_alerted,
       ROUND(100 * book_mule_rate, 3) AS book_mule_rate_pct,
       ROUND(100 * capture_top1, 1) AS model_capture_top1_pct,
       ROUND(100 * rules_capture_top1, 1) AS rules_capture_top1_pct,
       ROUND(lift_over_rules, 2) AS lift_over_rules,
       ROUND(100 * precision_top02, 1) AS t1_precision_pct,
       gate_passed, triggered_by
FROM r;

-- 10. Where are the mules? Alert hotspots by region and branch (alerts per 1,000 active accounts)
WITH d AS (SELECT MAX(run_date) AS rd FROM rsingh_mule_acct_gold.mule_alerts),
book AS (SELECT branch_code, COUNT(*) AS active_accounts
         FROM rsingh_mule_acct_gold.mule_features
         WHERE snapshot_date = (SELECT rd FROM d) GROUP BY branch_code),
al AS (SELECT branch_code, COUNT(*) AS alerts,
              SUM(CASE WHEN tier = 'T1_FREEZE_REVIEW' THEN 1 ELSE 0 END) AS t1
       FROM rsingh_mule_acct_gold.mule_alerts WHERE run_date = (SELECT rd FROM d) GROUP BY branch_code)
SELECT b.region, b.state, b.city, b.branch_name, k.active_accounts, NVL(a.alerts, 0) AS alerts, NVL(a.t1, 0) AS t1_freeze,
       ROUND(1000 * NVL(a.alerts, 0) / k.active_accounts, 1) AS alerts_per_1000
FROM book k
JOIN rsingh_mule_acct_ref.branch_map b ON b.branch_code = k.branch_code
LEFT JOIN al a ON a.branch_code = k.branch_code
ORDER BY alerts_per_1000 DESC
LIMIT 15;

-- 11. Which products and KYC types carry the risk? Alert rate by product x KYC
WITH d AS (SELECT MAX(run_date) AS rd FROM rsingh_mule_acct_gold.mule_alerts)
SELECT p.product_name,
       CASE WHEN f.min_kyc_flag = 1 THEN 'Min-KYC (OTP)' ELSE 'Full KYC' END AS kyc,
       COUNT(*) AS active_accounts,
       SUM(CASE WHEN a.account_id IS NOT NULL THEN 1 ELSE 0 END) AS alerts,
       ROUND(100 * SUM(CASE WHEN a.account_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS alert_rate_pct,
       SUM(CASE WHEN a.tier = 'T1_FREEZE_REVIEW' THEN 1 ELSE 0 END) AS t1_freeze
FROM rsingh_mule_acct_gold.mule_features f
JOIN rsingh_mule_acct_ref.product_map p ON p.product_code = f.product_code
LEFT JOIN (SELECT account_id, tier FROM rsingh_mule_acct_gold.mule_alerts
           WHERE run_date = (SELECT rd FROM d)) a ON a.account_id = f.account_id
WHERE f.snapshot_date = (SELECT rd FROM d)
GROUP BY 1, 2
ORDER BY alert_rate_pct DESC;

-- 12. Why are accounts flagged? Reason codes on today's T1 freeze-review queue
WITH t1 AS (SELECT reason_codes FROM rsingh_mule_acct_gold.mule_alerts
            WHERE run_date = (SELECT MAX(run_date) FROM rsingh_mule_acct_gold.mule_alerts)
              AND tier = 'T1_FREEZE_REVIEW'),
codes AS (
  SELECT 'NEAR_KNOWN_MULE' AS code UNION ALL SELECT 'PASS_THROUGH' UNION ALL SELECT 'SHARED_DEVICE'
  UNION ALL SELECT 'SHARED_MOBILE' UNION ALL SELECT 'DEVICE_OR_MOBILE_CHANGE' UNION ALL SELECT 'MIN_KYC'
  UNION ALL SELECT 'NEW_ACCOUNT' UNION ALL SELECT 'DORMANT_REACTIVATED' UNION ALL SELECT 'MANY_SENDERS'
  UNION ALL SELECT 'FAST_HOLD' UNION ALL SELECT 'NIGHT_ACTIVITY' UNION ALL SELECT 'INFLOW_VS_INCOME'
  UNION ALL SELECT 'RING' UNION ALL SELECT 'COMPLAINANT_CREDITS')
SELECT c.code, COUNT(*) AS t1_accounts,
       ROUND(100 * COUNT(*) / (SELECT COUNT(*) FROM t1), 1) AS pct_of_t1
FROM t1 JOIN codes c ON concat(',', t1.reason_codes, ',') LIKE concat('%,', c.code, ',%')
GROUP BY c.code
ORDER BY t1_accounts DESC;

-- 13. Network proximity: how the confirmed-mule rate climbs as accounts get closer to a known mule
SELECT CASE hops_to_known_mule WHEN 1 THEN '1 hop' WHEN 2 THEN '2 hops' ELSE '3+ hops / none' END AS distance_to_known_mule,
       COUNT(*) AS labelled_rows,
       SUM(CAST(is_mule_90d AS INT)) AS became_mule_within_90d,
       ROUND(100 * AVG(CAST(is_mule_90d AS DOUBLE)), 3) AS mule_rate_pct
FROM rsingh_mule_acct_gold.mule_features
WHERE is_mule_90d IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- 14. Pass-through behaviour: money forwarded within 24h vs the chance of being a mule
SELECT CASE WHEN pass_through_ratio_7d >= 0.9 THEN 'a. >= 90%'
            WHEN pass_through_ratio_7d >= 0.7 THEN 'b. 70-90%'
            WHEN pass_through_ratio_7d >= 0.4 THEN 'c. 40-70%'
            WHEN pass_through_ratio_7d > 0 THEN 'd. 0-40%'
            ELSE 'e. none' END AS pass_through_band,
       COUNT(*) AS labelled_rows,
       SUM(CAST(is_mule_90d AS INT)) AS mules,
       ROUND(100 * AVG(CAST(is_mule_90d AS DOUBLE)), 3) AS mule_rate_pct,
       ROUND(AVG(median_hold_hours_30d), 1) AS avg_hold_hours
FROM rsingh_mule_acct_gold.mule_features
WHERE is_mule_90d IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- 15. Rings to investigate first: today's riskiest rings and how much of each is already alerted
SELECT ring_id, ring_size, accounts_scored, accounts_alerted,
       ROUND(100 * accounts_alerted / accounts_scored, 0) AS pct_alerted,
       top_tier, ROUND(max_p_mule_adj, 4) AS max_p_mule_adj, ROUND(sum_p_mule_adj, 4) AS ring_risk,
       min_hops_to_known_mule, top_reason_codes
FROM rsingh_mule_acct_gold.mule_rings
WHERE run_date = (SELECT MAX(run_date) FROM rsingh_mule_acct_gold.mule_rings)
ORDER BY sum_p_mule_adj DESC
LIMIT 15;

-- 16. Money at risk: UPI credits and onward debits through alerted accounts in the last 30 days
WITH d AS (SELECT MAX(run_date) AS rd FROM rsingh_mule_acct_gold.mule_alerts),
al AS (SELECT account_id, tier FROM rsingh_mule_acct_gold.mule_alerts WHERE run_date = (SELECT rd FROM d)),
t AS (SELECT account_id, direction, amount FROM rsingh_mule_acct_silver.txn
      WHERE txn_date > date_sub((SELECT rd FROM d), 30) AND txn_date <= (SELECT rd FROM d))
SELECT NVL(al.tier, 'Not alerted') AS tier,
       COUNT(DISTINCT t.account_id) AS accounts,
       ROUND(SUM(CASE WHEN t.direction = 'CR' THEN t.amount ELSE 0 END) / 1e7, 2) AS credits_inr_crore,
       ROUND(SUM(CASE WHEN t.direction = 'DR' THEN t.amount ELSE 0 END) / 1e7, 2) AS debits_inr_crore,
       ROUND(100 * SUM(CASE WHEN t.direction = 'DR' THEN t.amount ELSE 0 END)
                 / SUM(CASE WHEN t.direction = 'CR' THEN t.amount ELSE 0 END), 0) AS pct_forwarded,
       ROUND(SUM(CASE WHEN t.direction = 'CR' THEN t.amount ELSE 0 END) / COUNT(DISTINCT t.account_id), 0) AS avg_credit_per_account_inr
FROM t LEFT JOIN al ON al.account_id = t.account_id
GROUP BY 1
ORDER BY 1;

-- 17. Can we trust the ranking? Holdout capture curve, model vs rules-only
SELECT band, upto_pct, accounts, mules, ROUND(100 * mule_rate, 2) AS mule_rate_pct,
       ROUND(100 * cum_capture, 1) AS model_cum_capture_pct,
       ROUND(100 * rules_cum_capture, 1) AS rules_cum_capture_pct
FROM rsingh_mule_acct_gold.mule_holdout
WHERE run_id = (SELECT run_id FROM rsingh_mule_acct_gold.mule_model_run
                WHERE run_ts = (SELECT MAX(run_ts) FROM rsingh_mule_acct_gold.mule_model_run))
ORDER BY upto_pct;

-- 18. Fraud complaints trend: monthly complaints and amounts by category
SELECT trunc(report_date, 'MM') AS month, category,
       COUNT(*) AS complaints, COUNT(DISTINCT account_id) AS accounts_reported,
       ROUND(SUM(amount) / 1e5, 1) AS amount_inr_lakh
FROM rsingh_mule_acct_silver.report
WHERE report_date >= add_months(now(), -12)
GROUP BY 1, 2
ORDER BY 1 DESC, complaints DESC
LIMIT 40;

-- 19. Mule lifecycle: how many days from account opening to first fraud report, by product
SELECT p.product_name,
       COUNT(*) AS reported_accounts,
       CAST(APPX_MEDIAN(datediff(r.first_report, a.open_date)) AS INT) AS median_days_open_to_report,
       SUM(CASE WHEN a.reactivated_on IS NOT NULL THEN 1 ELSE 0 END) AS were_dormant_reactivated,
       SUM(CASE WHEN a.freeze_date IS NOT NULL THEN 1 ELSE 0 END) AS frozen
FROM (SELECT account_id, MIN(report_date) AS first_report FROM rsingh_mule_acct_silver.report
      WHERE account_id IS NOT NULL GROUP BY account_id) r
JOIN rsingh_mule_acct_silver.account a ON a.account_id = r.account_id
JOIN rsingh_mule_acct_ref.product_map p ON p.product_code = a.product_code
GROUP BY 1
ORDER BY reported_accounts DESC;

-- 20. Iceberg snapshots of the gold feature table (copy the oldest snapshot_id into 21)
DESCRIBE HISTORY rsingh_mule_acct_gold.mule_features;

-- 21. Iceberg time travel: gold as of its first snapshot vs now
SELECT 'now' AS version, COUNT(*) AS rows_, COUNT(is_mule_90d) AS labelled, MAX(snapshot_date) AS latest_snapshot
FROM rsingh_mule_acct_gold.mule_features
UNION ALL
SELECT 'first snapshot', COUNT(*), COUNT(is_mule_90d), MAX(snapshot_date)
FROM rsingh_mule_acct_gold.mule_features FOR SYSTEM_VERSION AS OF 1234567890123456789;

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

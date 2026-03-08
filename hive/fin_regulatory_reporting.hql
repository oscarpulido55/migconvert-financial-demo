-- ==============================================================================
-- Module: fin_regulatory_reporting.hql
-- Description: Identifies patterns indicative of money laundering or fraud
-- to generate Suspicious Activity Report (SAR) candidates.
-- Uses advanced windowing and pattern matching techniques in Hive.
-- ==============================================================================

SET hive.execution.engine=tez;
SET hive.vectorized.execution.enabled=true;

CREATE DATABASE IF NOT EXISTS fin_reports;

-- Produce a report showing Structuring (Smurfing)
-- e.g., multiple transactions aggregating to > $10k in a short window
CREATE TABLE IF NOT EXISTS fin_reports.sar_structuring_candidates (
    customer_id STRING,
    customer_segment STRING,
    kyc_risk_rating STRING,
    sar_trigger_date DATE,
    total_trx_count INT,
    total_amount_usd DECIMAL(18, 2),
    min_trx_amount DECIMAL(18, 2),
    max_trx_amount DECIMAL(18, 2),
    is_escalated BOOLEAN,
    report_generation_ts TIMESTAMP
) PARTITIONED BY (report_month STRING)
STORED AS PARQUET;

-- Insert logic focusing on a specific month (parameterized)
INSERT OVERWRITE TABLE fin_reports.sar_structuring_candidates PARTITION (report_month)
WITH daily_cash_aggregates AS (
    -- Aggregate daily cash transactions per customer
    SELECT 
        c.customer_id,
        c.customer_segment,
        c.risk_rating AS kyc_risk_rating,
        t.trx_date,
        COUNT(t.trx_uuid) AS daily_tx_cnt,
        SUM(t.normalized_usd_amount) AS daily_amount_usd,
        MIN(t.normalized_usd_amount) AS min_tx,
        MAX(t.normalized_usd_amount) AS max_tx
    FROM fin_core.fact_transactions t
    JOIN fin_core.dim_customers c ON t.customer_id = c.customer_id
    WHERE t.transaction_type IN ('CASH_DEPOSIT', 'WIRE_TRANSFER_IN')
      AND SUBSTR(CAST(t.trx_date AS STRING), 1, 7) = '${hiveconf:REPORT_MONTH}'
    GROUP BY 
        c.customer_id,
        c.customer_segment,
        c.risk_rating,
        t.trx_date
),
rolling_window_aggregates AS (
    -- Apply a 3-day rolling window to find clusters of transactions
    SELECT 
        customer_id,
        customer_segment,
        kyc_risk_rating,
        trx_date,
        daily_tx_cnt,
        daily_amount_usd,
        min_tx,
        max_tx,
        SUM(daily_amount_usd) OVER (
            PARTITION BY customer_id 
            ORDER BY trx_date 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3day_amount,
        SUM(daily_tx_cnt) OVER (
            PARTITION BY customer_id 
            ORDER BY trx_date 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3day_tx_cnt,
        -- Calculate velocity (amount per transaction ratio)
        LAG(daily_amount_usd) OVER (
            PARTITION BY customer_id 
            ORDER BY trx_date
        ) AS prev_day_amount
    FROM daily_cash_aggregates
)
-- Filter for patterns that evade the standard $10k reporting limit
SELECT 
    customer_id,
    customer_segment,
    kyc_risk_rating,
    trx_date AS sar_trigger_date,
    rolling_3day_tx_cnt AS total_trx_count,
    rolling_3day_amount AS total_amount_usd,
    min_tx AS min_trx_amount,
    max_tx AS max_trx_amount,
    -- Auto-escalate high risk customers
    CASE 
        WHEN kyc_risk_rating IN ('HIGH', 'VERY_HIGH') THEN true
        WHEN rolling_3day_amount > 25000 THEN true
        ELSE false
    END AS is_escalated,
    CURRENT_TIMESTAMP() AS report_generation_ts,
    '${hiveconf:REPORT_MONTH}' AS report_month
FROM rolling_window_aggregates
WHERE rolling_3day_amount >= 9500.00 -- Just under the 10k threshold
  AND rolling_3day_amount <= 10500.00
  AND rolling_3day_tx_cnt >= 3
  -- Avoid capturing standard large single corporate deposits
  AND max_tx < 9000.0;

-- Additional compliance reporting logic could follow
-- e.g. finding high velocity circular fund movements via graph-like self-joins:
-- SELECT t1.source, t1.dest, t2.dest ... FROM fact_transactions t1 JOIN fact_transactions t2 ON t1.dest = t2.source and t1.trx_timestamp < t2.trx_timestamp...

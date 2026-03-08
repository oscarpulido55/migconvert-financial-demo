-- ==============================================================================
-- Module: fin_monthly_depository_averages.hql
-- Description: Aggregates daily balances into monthly averages. Computes complex
-- metrics like variance, moving averages, and dynamically tiers the customers 
-- based on their 3-month trailing depository volume.
-- ==============================================================================

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.cbo.enable=true;

CREATE DATABASE IF NOT EXISTS fin_mart;

CREATE TABLE IF NOT EXISTS fin_mart.monthly_account_metrics (
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    report_month STRING,
    avg_daily_balance DECIMAL(18,4),
    max_daily_balance DECIMAL(18,4),
    balance_stddev DECIMAL(18,4),
    trailing_3m_avg_balance DECIMAL(18,4),
    volume_tier STRING
) PARTITIONED BY (processing_year STRING)
STORED AS ORC;

-- Sub-query factoring with CTEs to calculate monthly basic stats
INSERT OVERWRITE TABLE fin_mart.monthly_account_metrics PARTITION (processing_year)
WITH monthly_stats AS (
    SELECT 
        account_id,
        customer_id,
        account_type,
        -- Generate YYYY-MM
        SUBSTR(CAST(balance_date AS STRING), 1, 7) AS report_month,
        SUBSTR(CAST(balance_date AS STRING), 1, 4) AS processing_year,
        AVG(ending_balance) AS avg_daily_balance,
        MAX(ending_balance) AS max_daily_balance,
        STDDEV_SAMP(ending_balance) AS balance_stddev
    FROM fin_core.fact_daily_balances
    GROUP BY 
        account_id,
        customer_id,
        account_type,
        SUBSTR(CAST(balance_date AS STRING), 1, 7),
        SUBSTR(CAST(balance_date AS STRING), 1, 4)
),
trailing_stats AS (
    SELECT 
        account_id,
        customer_id,
        account_type,
        report_month,
        avg_daily_balance,
        max_daily_balance,
        balance_stddev,
        processing_year,
        -- Use Window Function to calculate average over the previous 2 months plus current
        AVG(avg_daily_balance) OVER (
            PARTITION BY account_id
            ORDER BY report_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS trailing_3m_avg_balance
    FROM monthly_stats
)
SELECT 
    account_id,
    customer_id,
    account_type,
    report_month,
    avg_daily_balance,
    max_daily_balance,
    balance_stddev,
    trailing_3m_avg_balance,
    -- Case expression to assign a tier
    CASE 
        WHEN trailing_3m_avg_balance >= 250000 THEN 'TITANIUM'
        WHEN trailing_3m_avg_balance >= 100000 THEN 'PLATINUM'
        WHEN trailing_3m_avg_balance >= 25000 THEN 'GOLD'
        WHEN trailing_3m_avg_balance >= 5000 THEN 'SILVER'
        ELSE 'STANDARD'
    END AS volume_tier,
    processing_year
FROM trailing_stats
WHERE report_month = '${hiveconf:TARGET_MONTH}';

-- ==============================================================================
-- Module: fin_daily_balances.hql
-- Description: Calculates End Of Day (EOD) balances for all banking accounts.
-- Consumes the daily transaction fact table and applies it to the previous
-- day's balance snapshot.
-- ==============================================================================

-- Ensure target database exists
CREATE SCHEMA IF NOT EXISTS fin_core; -- Converted CREATE DATABASE to CREATE SCHEMA for BigQuery

-- 1. Create the Daily Balance Snapshot Table if it doesn't exist
CREATE TABLE IF NOT EXISTS `fin_core.fact_daily_balances` (
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    open_date DATE,
    currency_code STRING,
    beginning_balance NUMERIC(18, 4), -- Converted DECIMAL to NUMERIC
    total_credits NUMERIC(18, 4), -- Converted DECIMAL to NUMERIC
    total_debits NUMERIC(18, 4), -- Converted DECIMAL to NUMERIC
    ending_balance NUMERIC(18, 4), -- Converted DECIMAL to NUMERIC
    interest_accrued NUMERIC(18, 4), -- Converted DECIMAL to NUMERIC
    is_overdrawn BOOL, -- Converted BOOLEAN to BOOL
    etl_timestamp TIMESTAMP
)
OPTIONS(
    description = 'Stores the End of Day balances for all accounts' -- Converted table COMMENT to OPTIONS(description)
)
PARTITION BY balance_date -- Converted PARTITIONED BY to PARTITION BY
CLUSTER BY region_code; -- Converted secondary partition column to CLUSTER BY (BigQuery partitions by one column, then clusters)
-- Removed STORED AS ORC and TBLPROPERTIES as they are Hive-specific

-- 2. Temporary table to hold today's net movements per account
-- In BigQuery, CREATE TEMPORARY TABLE creates a session-scoped table without needing a dataset qualifier.
-- Assuming the temp table should reside logically within the fin_core dataset if persistent, or be a session temp.
-- Changed `default.tmp_daily_movements_stg` to `fin_core.tmp_daily_movements_stg` for consistency.
DROP TABLE IF EXISTS `fin_core.tmp_daily_movements_stg`;

CREATE TEMPORARY TABLE `fin_core.tmp_daily_movements_stg` AS
WITH credited AS (
    SELECT
        destination_account_id AS account_id,
        SUM(amount_base_currency) AS total_credits
    FROM `fin_core.fact_transactions` -- Added backticks for table name
    WHERE trx_date = '2024-01-01'
      AND transaction_type NOT IN ('FEE', 'REVERSAL_DEBIT')
    GROUP BY destination_account_id
),
debited AS (
    SELECT
        source_account_id AS account_id,
        SUM(amount_base_currency) AS total_debits
    FROM `fin_core.fact_transactions` -- Added backticks for table name
    WHERE trx_date = '2024-01-01'
      AND transaction_type NOT IN ('REVERSAL_CREDIT')
    GROUP BY source_account_id
)
SELECT
    COALESCE(c.account_id, d.account_id) AS account_id,
    COALESCE(c.total_credits, CAST(0.0 AS NUMERIC)) AS total_credits, -- Casted 0.0 to NUMERIC
    COALESCE(d.total_debits, CAST(0.0 AS NUMERIC)) AS total_debits -- Casted 0.0 to NUMERIC
FROM credited c
FULL OUTER JOIN debited d ON c.account_id = d.account_id;

-- 3. Calculate End of Day Balances and Overwrite the partition
-- Hive's INSERT OVERWRITE PARTITION is equivalent to a DELETE followed by an INSERT INTO in BigQuery
-- for specific partitions.
DELETE FROM `fin_core.fact_daily_balances`
WHERE balance_date = '2024-01-01';

INSERT INTO `fin_core.fact_daily_balances` (
    account_id,
    customer_id,
    account_type,
    open_date,
    currency_code,
    beginning_balance,
    total_credits,
    total_debits,
    ending_balance,
    interest_accrued,
    is_overdrawn,
    etl_timestamp,
    balance_date,
    region_code
)
SELECT
    a.account_id,
    a.customer_id,
    a.account_type,
    a.open_date,
    a.currency_code,

    -- Beginning Balance is the previous day's ending balance, or 0 if new
    COALESCE(prev.ending_balance, CAST(0.0 AS NUMERIC)) AS beginning_balance, -- Casted 0.0 to NUMERIC

    -- Daily Movements
    COALESCE(m.total_credits, CAST(0.0 AS NUMERIC)) AS total_credits, -- Casted 0.0 to NUMERIC
    COALESCE(m.total_debits, CAST(0.0 AS NUMERIC)) AS total_debits, -- Casted 0.0 to NUMERIC

    -- Ending Balance Calculation
    (COALESCE(prev.ending_balance, CAST(0.0 AS NUMERIC)) + COALESCE(m.total_credits, CAST(0.0 AS NUMERIC)) - COALESCE(m.total_debits, CAST(0.0 AS NUMERIC))) AS ending_balance,

    -- Simple interest calculation (mock logic for demo: 0.05% APY / 365)
    CASE
        WHEN a.account_type = 'SAVINGS' AND (COALESCE(prev.ending_balance, CAST(0.0 AS NUMERIC)) + COALESCE(m.total_credits, CAST(0.0 AS NUMERIC)) - COALESCE(m.total_debits, CAST(0.0 AS NUMERIC))) > 0
        THEN (COALESCE(prev.ending_balance, CAST(0.0 AS NUMERIC)) + COALESCE(m.total_credits, CAST(0.0 AS NUMERIC)) - COALESCE(m.total_debits, CAST(0.0 AS NUMERIC))) * (CAST(0.05 AS NUMERIC) / 365) -- Casted 0.0, 0.05 to NUMERIC
        ELSE CAST(0.0 AS NUMERIC) -- Casted 0.0 to NUMERIC
    END AS interest_accrued,

    -- Overdrawn flag
    CASE
        WHEN (COALESCE(prev.ending_balance, CAST(0.0 AS NUMERIC)) + COALESCE(m.total_credits, CAST(0.0 AS NUMERIC)) - COALESCE(m.total_debits, CAST(0.0 AS NUMERIC))) < 0
        THEN TRUE -- Converted true to TRUE
        ELSE FALSE -- Converted false to FALSE
    END AS is_overdrawn,

    CURRENT_TIMESTAMP() AS etl_timestamp,

    -- Partition columns
    CAST('2024-01-01' AS DATE) AS balance_date,
    COALESCE(a.region_code, 'UN') AS region_code

FROM `fin_core.dim_accounts` a -- Added backticks for table name
-- Join with previous day's balance
LEFT JOIN `fin_core.fact_daily_balances` prev -- Added backticks for table name
    ON a.account_id = prev.account_id
    AND prev.balance_date = DATE_SUB(CAST('2024-01-01' AS DATE), INTERVAL 1 DAY) -- Converted DATE_SUB syntax
-- Join with today's movements
LEFT JOIN `fin_core.tmp_daily_movements_stg` m -- Added backticks for table name
    ON a.account_id = m.account_id
WHERE a.status IN ('OPEN', 'FROZEN', 'DORMANT');

-- Clean up
DROP TABLE IF EXISTS `fin_core.tmp_daily_movements_stg`;

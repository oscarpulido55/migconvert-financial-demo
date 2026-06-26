-- ==============================================================================
-- Module: fin_daily_balances.hql
-- Description: Calculates End Of Day (EOD) balances for all banking accounts.
-- Consumes the daily transaction fact table and applies it to the previous
-- day's balance snapshot.
-- ==============================================================================

-- Ensure target dataset exists (BigQuery uses datasets instead of databases)
CREATE SCHEMA IF NOT EXISTS fin_core;

-- 1. Create the Daily Balance Snapshot Table if it doesn't exist
CREATE TABLE IF NOT EXISTS fin_core.fact_daily_balances (
    account_id STRING OPTIONS(description='Unique identifier for the account'),
    customer_id STRING OPTIONS(description='Identifier for the account owner'),
    account_type STRING OPTIONS(description='Type of account (CHECKING, SAVINGS, LOAN)'),
    open_date DATE OPTIONS(description='Date the account was opened'),
    currency_code STRING OPTIONS(description='Base currency of the account'),
    beginning_balance NUMERIC(18, 4) OPTIONS(description='Balance at the start of the day'), -- DECIMAL(18, 4) in Hive maps to NUMERIC(18, 4) in BigQuery
    total_credits NUMERIC(18, 4) OPTIONS(description='Total value of incoming funds'),
    total_debits NUMERIC(18, 4) OPTIONS(description='Total value of outgoing funds'),
    ending_balance NUMERIC(18, 4) OPTIONS(description='Balance at the end of the day'),
    interest_accrued NUMERIC(18, 4) OPTIONS(description='Daily interest accrued'),
    is_overdrawn BOOL OPTIONS(description='Flag indicating if the account is in negative balance'), -- BOOLEAN in Hive maps to BOOL in BigQuery
    etl_timestamp TIMESTAMP
)
OPTIONS(
    description='Stores the End of Day balances for all accounts'
)
PARTITION BY balance_date -- BigQuery partitioning is based on DATE, TIMESTAMP, or INT columns.
CLUSTER BY region_code; -- Hive's PARTITIONED BY (..., region_code STRING) becomes CLUSTER BY in BigQuery for STRING columns if used for organization within partitions.
-- STORED AS ORC is not applicable in BigQuery.
-- TBLPROPERTIES ('transactional'='true') is not applicable in BigQuery.


-- 2. Temporary table to hold today's net movements per account
DROP TABLE IF EXISTS tmp_daily_movements_stg; -- BigQuery temporary tables are session-scoped and implicitly associated with the project's default dataset if no dataset is specified, and they are automatically deleted at the end of the session.

CREATE TEMPORARY TABLE tmp_daily_movements_stg AS -- BigQuery CREATE TEMPORARY TABLE is supported, `default` is not needed.
WITH credited AS (
    SELECT
        destination_account_id AS account_id,
        SUM(amount_base_currency) AS total_credits
    FROM fin_core.fact_transactions
    WHERE trx_date = DATE '2024-01-01' -- Hive's 'YYYY-MM-DD' for DATE literals maps to DATE 'YYYY-MM-DD' in BigQuery
      AND transaction_type NOT IN ('FEE', 'REVERSAL_DEBIT')
    GROUP BY destination_account_id
),
debited AS (
    SELECT
        source_account_id AS account_id,
        SUM(amount_base_currency) AS total_debits
    FROM fin_core.fact_transactions
    WHERE trx_date = DATE '2024-01-01'
      AND transaction_type NOT IN ('REVERSAL_CREDIT')
    GROUP BY source_account_id
)
SELECT
    COALESCE(c.account_id, d.account_id) AS account_id,
    COALESCE(c.total_credits, 0.0) AS total_credits,
    COALESCE(d.total_debits, 0.0) AS total_debits
FROM credited c
FULL OUTER JOIN debited d ON c.account_id = d.account_id;

-- 3. Calculate End of Day Balances and Insert Overwrite the partition
-- To achieve "INSERT OVERWRITE PARTITION" functionality in BigQuery for dynamically determined partitions (based on balance_date and region_code from the SELECT query),
-- it's common practice to first delete existing data for the relevant partitions and then insert the new data.
DELETE FROM fin_core.fact_daily_balances WHERE balance_date = DATE '2024-01-01'; -- Deletes data for the specific date partition
-- If `region_code` was also a partitioning key in Hive that needs overwrite per region for the day, a MERGE statement would be more robust.
-- For a full dynamic partition overwrite as in Hive, a common BigQuery pattern involves creating a temporary table of the new data,
-- deleting the relevant partitions from the target, then inserting from the temporary table.

INSERT INTO fin_core.fact_daily_balances (
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
    COALESCE(prev.ending_balance, 0.0) AS beginning_balance,

    -- Daily Movements
    COALESCE(m.total_credits, 0.0) AS total_credits,
    COALESCE(m.total_debits, 0.0) AS total_debits,

    -- Ending Balance Calculation
    (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) AS ending_balance,

    -- Simple interest calculation (mock logic for demo: 0.05% APY / 365)
    CASE
        WHEN a.account_type = 'SAVINGS' AND (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) > 0
        THEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) * (0.05 / 365)
        ELSE 0.0
    END AS interest_accrued,

    -- Overdrawn flag
    CASE
        WHEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) < 0
        THEN TRUE -- Hive uses TRUE/FALSE, BigQuery uses TRUE/FALSE or 1/0 for boolean. TRUE is explicit.
        ELSE FALSE
    END AS is_overdrawn,

    CURRENT_TIMESTAMP() AS etl_timestamp,

    -- Partition columns
    DATE '2024-01-01' AS balance_date, -- Hive's CAST('YYYY-MM-DD' AS DATE) maps to DATE 'YYYY-MM-DD'
    COALESCE(a.region_code, 'UN') AS region_code

FROM fin_core.dim_accounts a
-- Join with previous day's balance
LEFT JOIN fin_core.fact_daily_balances prev
    ON a.account_id = prev.account_id
    AND prev.balance_date = DATE_SUB(DATE '2024-01-01', INTERVAL 1 DAY) -- Hive's DATE_SUB requires conversion to BigQuery's INTERVAL syntax
-- Join with today's movements
LEFT JOIN tmp_daily_movements_stg m -- No `default` dataset for BigQuery temporary table
    ON a.account_id = m.account_id
WHERE a.status IN ('OPEN', 'FROZEN', 'DORMANT');

-- Clean up
DROP TABLE IF EXISTS tmp_daily_movements_stg; -- No `default` dataset for BigQuery temporary table

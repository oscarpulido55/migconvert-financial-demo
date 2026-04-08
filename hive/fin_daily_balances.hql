-- ==============================================================================
-- Module: fin_daily_balances.hql
-- Description: Calculates End Of Day (EOD) balances for all banking accounts.
-- Consumes the daily transaction fact table and applies it to the previous 
-- day's balance snapshot.
-- ==============================================================================

-- Ensure target database exists
CREATE SCHEMA IF NOT EXISTS `fin_core`;

-- 1. Create the Daily Balance Snapshot Table if it doesn't exist
CREATE TABLE IF NOT EXISTS `fin_core.fact_daily_balances` (
    account_id STRING OPTIONS(description='Unique identifier for the account'),
    customer_id STRING OPTIONS(description='Identifier for the account owner'),
    account_type STRING OPTIONS(description='Type of account (CHECKING, SAVINGS, LOAN)'),
    open_date DATE OPTIONS(description='Date the account was opened'),
    currency_code STRING OPTIONS(description='Base currency of the account'),
    beginning_balance NUMERIC(18, 4) OPTIONS(description='Balance at the start of the day'),
    total_credits NUMERIC(18, 4) OPTIONS(description='Total value of incoming funds'),
    total_debits NUMERIC(18, 4) OPTIONS(description='Total value of outgoing funds'),
    ending_balance NUMERIC(18, 4) OPTIONS(description='Balance at the end of the day'),
    interest_accrued NUMERIC(18, 4) OPTIONS(description='Daily interest accrued'),
    is_overdrawn BOOL OPTIONS(description='Flag indicating if the account is in negative balance'),
    etl_timestamp TIMESTAMP OPTIONS(description='Timestamp of the ETL process')
)
PARTITION BY balance_date
CLUSTER BY region_code
OPTIONS(
    description='Stores the End of Day balances for all accounts'
);


-- 2. Temporary table to hold today's net movements per account
DROP TEMPORARY TABLE IF EXISTS tmp_daily_movements_stg;

CREATE TEMPORARY TABLE tmp_daily_movements_stg AS
WITH credited AS (
    SELECT 
        destination_account_id AS account_id,
        SUM(amount_base_currency) AS total_credits
    FROM `fin_core.fact_transactions`
    WHERE trx_date = DATE '2024-01-01'
      AND transaction_type NOT IN ('FEE', 'REVERSAL_DEBIT')
    GROUP BY destination_account_id
),
debited AS (
    SELECT 
        source_account_id AS account_id,
        SUM(amount_base_currency) AS total_debits
    FROM `fin_core.fact_transactions`
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
        THEN true 
        ELSE false 
    END AS is_overdrawn,
    
    CURRENT_TIMESTAMP() AS etl_timestamp,
    
    -- Partition columns
    DATE '2024-01-01' AS balance_date,
    COALESCE(a.region_code, 'UN') AS region_code
    
FROM `fin_core.dim_accounts` a
-- Join with previous day's balance
LEFT JOIN `fin_core.fact_daily_balances` prev 
    ON a.account_id = prev.account_id 
    AND prev.balance_date = DATE_SUB(DATE '2024-01-01', INTERVAL 1 DAY)
-- Join with today's movements
LEFT JOIN tmp_daily_movements_stg m 
    ON a.account_id = m.account_id
WHERE a.status IN ('OPEN', 'FROZEN', 'DORMANT');

-- Clean up
DROP TEMPORARY TABLE IF EXISTS tmp_daily_movements_stg;
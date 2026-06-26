-- ==============================================================================
-- Module: fin_daily_balances.hql
-- Description: Calculates End Of Day (EOD) balances for all banking accounts.
-- Consumes the daily transaction fact table and applies it to the previous
-- day's balance snapshot.
-- ==============================================================================

-- 1. Create the Daily Balance Snapshot Table if it doesn't exist
CREATE TABLE IF NOT EXISTS `migconvert-at-next26.fin_core.fact_daily_balances` (
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
    etl_timestamp TIMESTAMP,
    balance_date DATE OPTIONS(description='Date of the balance snapshot'),
    region_code STRING OPTIONS(description='Region code for the account')
)
PARTITION BY balance_date
CLUSTER BY region_code
OPTIONS(
    description='Stores the End of Day balances for all accounts'
);

-- 2. Delete existing data for the target date before inserting (equivalent to Hive's INSERT OVERWRITE PARTITION)
DELETE FROM `migconvert-at-next26.fin_core.fact_daily_balances`
WHERE balance_date = DATE '2024-01-01';

-- 3. Calculate End of Day Balances and Insert into the table
INSERT INTO `migconvert-at-next26.fin_core.fact_daily_balances` (
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
WITH tmp_daily_movements_stg AS (
    WITH credited AS (
        SELECT
            destination_account_id AS account_id,
            SUM(amount_base_currency) AS total_credits
        FROM `migconvert-at-next26.fin_core.fact_transactions`
        WHERE trx_date = DATE '2024-01-01'
          AND transaction_type NOT IN ('FEE', 'REVERSAL_DEBIT')
        GROUP BY destination_account_id
    ),
    debited AS (
        SELECT
            source_account_id AS account_id,
            SUM(amount_base_currency) AS total_debits
        FROM `migconvert-at-next26.fin_core.fact_transactions`
        WHERE trx_date = DATE '2024-01-01'
          AND transaction_type NOT IN ('REVERSAL_CREDIT')
        GROUP BY source_account_id
    )
    SELECT
        COALESCE(c.account_id, d.account_id) AS account_id,
        COALESCE(c.total_credits, 0.0) AS total_credits,
        COALESCE(d.total_debits, 0.0) AS total_debits
    FROM credited c
    FULL OUTER JOIN debited d ON c.account_id = d.account_id
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
        THEN TRUE
        ELSE FALSE
    END AS is_overdrawn,

    CURRENT_TIMESTAMP() AS etl_timestamp,

    -- Partition columns
    DATE '2024-01-01' AS balance_date,
    COALESCE(a.region_code, 'UN') AS region_code

FROM `migconvert-at-next26.fin_core.dim_accounts` AS a
-- Join with previous day's balance
LEFT JOIN `migconvert-at-next26.fin_core.fact_daily_balances` AS prev
    ON a.account_id = prev.account_id
    AND prev.balance_date = DATE_SUB(DATE '2024-01-01', INTERVAL 1 DAY)
-- Join with today's movements
LEFT JOIN tmp_daily_movements_stg AS m
    ON a.account_id = m.account_id
WHERE a.status IN ('OPEN', 'FROZEN', 'DORMANT');

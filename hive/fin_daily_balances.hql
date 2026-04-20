-- ==============================================================================
-- Module: fin_daily_balances.hql (Converted to BigQuery SQL)
-- Description: Calculates End Of Day (EOD) balances for all banking accounts.
-- Consumes the daily transaction fact table and applies it to the previous 
-- day's balance snapshot.
-- ==============================================================================

-- Ensure target dataset exists (BigQuery uses schemas/datasets instead of databases)
CREATE SCHEMA IF NOT EXISTS fin_core;

-- 1. Create the Daily Balance Snapshot Table if it doesn't exist
CREATE TABLE IF NOT EXISTS fin_core.fact_daily_balances (
    account_id STRING OPTIONS(description='Unique identifier for the account'),
    customer_id STRING OPTIONS(description='Identifier for the account owner'),
    account_type STRING OPTIONS(description='Type of account (CHECKING, SAVINGS, LOAN)'),
    open_date DATE OPTIONS(description='Date the account was opened'),
    currency_code STRING OPTIONS(description='Base currency of the account'),
    beginning_balance NUMERIC(18, 4) OPTIONS(description='Balance at the start of the day'), -- Converted DECIMAL to NUMERIC
    total_credits NUMERIC(18, 4) OPTIONS(description='Total value of incoming funds'),     -- Converted DECIMAL to NUMERIC
    total_debits NUMERIC(18, 4) OPTIONS(description='Total value of outgoing funds'),      -- Converted DECIMAL to NUMERIC
    ending_balance NUMERIC(18, 4) OPTIONS(description='Balance at the end of the day'),    -- Converted DECIMAL to NUMERIC
    interest_accrued NUMERIC(18, 4) OPTIONS(description='Daily interest accrued'),         -- Converted DECIMAL to NUMERIC
    is_overdrawn BOOL OPTIONS(description='Flag indicating if the account is in negative balance'), -- Converted BOOLEAN to BOOL
    etl_timestamp TIMESTAMP OPTIONS(description='Timestamp of ETL process')
) 
OPTIONS(description='Stores the End of Day balances for all accounts')
PARTITION BY balance_date  -- BigQuery partitions by a single DATE/TIMESTAMP/INTEGER column
CLUSTER BY region_code;    -- BigQuery uses CLUSTER BY for secondary sorting/co-location, replacing multi-column partitioning
-- Removed STORED AS ORC and TBLPROPERTIES as they are Hive/HDFS specific.


-- 2. Temporary table to hold today's net movements per account
-- (This logic will be embedded as CTEs within the MERGE statement below for BigQuery idiomacy)
-- Removed DROP TABLE IF EXISTS default.tmp_daily_movements_stg; as temporary tables are integrated into CTEs
-- Removed CREATE TEMPORARY TABLE default.tmp_daily_movements_stg AS; as it's integrated into CTEs


-- 3. Calculate End of Day Balances and Insert Overwrite the partition
-- Converted from INSERT OVERWRITE TABLE ... PARTITION to MERGE statement for BigQuery's equivalent functionality
MERGE INTO fin_core.fact_daily_balances AS target
USING (
    WITH credited AS (
        SELECT 
            destination_account_id AS account_id,
            SUM(amount_base_currency) AS total_credits
        FROM fin_core.fact_transactions
        WHERE trx_date = '2024-01-01'
          AND transaction_type NOT IN ('FEE', 'REVERSAL_DEBIT')
        GROUP BY destination_account_id
    ),
    debited AS (
        SELECT 
            source_account_id AS account_id,
            SUM(amount_base_currency) AS total_debits
        FROM fin_core.fact_transactions
        WHERE trx_date = '2024-01-01'
          AND transaction_type NOT IN ('REVERSAL_CREDIT')
        GROUP BY source_account_id
    ),
    tmp_daily_movements_stg AS ( -- Renamed from default.tmp_daily_movements_stg to fit CTE structure
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
            THEN true 
            ELSE false 
        END AS is_overdrawn,
        
        CURRENT_TIMESTAMP() AS etl_timestamp,
        
        -- Partition and Cluster columns for the MERGE source
        CAST('2024-01-01' AS DATE) AS balance_date,
        COALESCE(a.region_code, 'UN') AS region_code
        
    FROM fin_core.dim_accounts a
    -- Join with previous day's balance
    LEFT JOIN fin_core.fact_daily_balances prev 
        ON a.account_id = prev.account_id 
        AND prev.balance_date = DATE_SUB(CAST('2024-01-01' AS DATE), INTERVAL 1 DAY) -- Converted DATE_SUB syntax
    -- Join with today's movements (now a CTE)
    LEFT JOIN tmp_daily_movements_stg m 
        ON a.account_id = m.account_id
    WHERE a.status IN ('OPEN', 'FROZEN', 'DORMANT')
) AS source
ON target.account_id = source.account_id
   AND target.balance_date = source.balance_date
   AND target.region_code = source.region_code
WHEN MATCHED AND target.balance_date = source.balance_date THEN -- Match on entire record for update within the partition
    UPDATE SET
        customer_id = source.customer_id,
        account_type = source.account_type,
        open_date = source.open_date,
        currency_code = source.currency_code,
        beginning_balance = source.beginning_balance,
        total_credits = source.total_credits,
        total_debits = source.total_debits,
        ending_balance = source.ending_balance,
        interest_accrued = source.interest_accrued,
        is_overdrawn = source.is_overdrawn,
        etl_timestamp = source.etl_timestamp
WHEN NOT MATCHED BY TARGET THEN -- Insert new accounts or new partitions
    INSERT (account_id, customer_id, account_type, open_date, currency_code, beginning_balance, total_credits, total_debits, ending_balance, interest_accrued, is_overdrawn, etl_timestamp, balance_date, region_code)
    VALUES (source.account_id, source.customer_id, source.account_type, source.open_date, source.currency_code, source.beginning_balance, source.total_credits, source.total_debits, source.ending_balance, source.interest_accrued, source.is_overdrawn, source.etl_timestamp, source.balance_date, source.region_code)
WHEN NOT MATCHED BY SOURCE AND target.balance_date = CAST('2024-01-01' AS DATE) THEN -- Delete records for this specific partition date that are no longer present in source
    DELETE;

-- Clean up -- Removed this line as CTEs are session-scoped and temporary tables are no longer explicitly created/dropped.
-- DROP TABLE IF EXISTS default.tmp_daily_movements_stg;
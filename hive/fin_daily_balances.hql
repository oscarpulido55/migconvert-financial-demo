-- Translation time: 2026-04-22T19:46:06.112852Z
-- Translation job ID: 65c11728-3e8a-4b90-aa9f-bee42d777053
-- Source: gs://migconvert-at-next26-work-bkt/866_SQL_TRANSLATION_ID_input/866_SQL_TRANSLATION_ID.sql
-- Translated from: Hive
-- Translated to: BigQuery

-- 3. Calculate End of Day Balances and Insert Overwrite the partition
DECLARE balance_date__is_null BOOL;
DECLARE balance_date__values ARRAY<DATE>;
-- ==============================================================================
-- Module: fin_daily_balances.hql
-- Description: Calculates End Of Day (EOD) balances for all banking accounts.
-- Consumes the daily transaction fact table and applies it to the previous
-- day's balance snapshot.
-- ==============================================================================
-- Ensure target database exists
CREATE SCHEMA fin_core;
-- 1. Create the Daily Balance Snapshot Table if it doesn't exist
CREATE TABLE IF NOT EXISTS __DEFAULT_DATABASE__.fin_core.fact_daily_balances
(
  account_id STRING OPTIONS(description='Unique identifier for the account'),
  customer_id STRING OPTIONS(description='Identifier for the account owner'),
  account_type STRING OPTIONS(description='Type of account (CHECKING, SAVINGS, LOAN)'),
  open_date DATE OPTIONS(description='Date the account was opened'),
  currency_code STRING OPTIONS(description='Base currency of the account'),
  beginning_balance NUMERIC(33, 4) OPTIONS(description='Balance at the start of the day'),
  total_credits NUMERIC(33, 4) OPTIONS(description='Total value of incoming funds'),
  total_debits NUMERIC(33, 4) OPTIONS(description='Total value of outgoing funds'),
  ending_balance NUMERIC(33, 4) OPTIONS(description='Balance at the end of the day'),
  interest_accrued NUMERIC(33, 4) OPTIONS(description='Daily interest accrued'),
  is_overdrawn BOOL OPTIONS(description='Flag indicating if the account is in negative balance'),
  etl_timestamp DATETIME,
  balance_date DATE,
  region_code STRING
)
PARTITION BY balance_date
CLUSTER BY region_code
OPTIONS(
  description='Stores the End of Day balances for all accounts'
);
-- 2. Temporary table to hold today's net movements per account
DROP TABLE IF EXISTS tmp_daily_movements_stg;
CREATE TEMPORARY TABLE tmp_daily_movements_stg
  AS
    WITH credited AS (
      SELECT
          fact_transactions.destination_account_id AS account_id,
          sum(fact_transactions.amount_base_currency) AS total_credits
        FROM
          __DEFAULT_DATABASE__.fin_core.fact_transactions
        WHERE fact_transactions.trx_date = '2024-01-01'
         AND fact_transactions.transaction_type NOT IN(
          'FEE', 'REVERSAL_DEBIT'
        )
        GROUP BY 1
    ), debited AS (
      SELECT
          fact_transactions.source_account_id AS account_id,
          sum(fact_transactions.amount_base_currency) AS total_debits
        FROM
          __DEFAULT_DATABASE__.fin_core.fact_transactions
        WHERE fact_transactions.trx_date = '2024-01-01'
         AND fact_transactions.transaction_type NOT IN(
          'REVERSAL_CREDIT'
        )
        GROUP BY 1
    )
    SELECT
        coalesce(c.account_id, d.account_id) AS account_id,
        coalesce(c.total_credits, NUMERIC '0.0') AS total_credits,
        coalesce(d.total_debits, NUMERIC '0.0') AS total_debits
      FROM
        credited AS c
        FULL OUTER JOIN debited AS d ON c.account_id = d.account_id
;
CREATE OR REPLACE TEMPORARY TABLE fact_daily_balances__insert
  AS
    SELECT
        a.account_id AS account_id,
        a.customer_id AS customer_id,
        a.account_type AS account_type,
        a.open_date AS open_date,
        a.currency_code AS currency_code,
        -- Beginning Balance is the previous day's ending balance, or 0 if new
        CAST(round(coalesce(prev.ending_balance, NUMERIC '0.0'), 4) as NUMERIC) AS beginning_balance,
        -- Daily Movements
        CAST(round(coalesce(m.total_credits, NUMERIC '0.0'), 4) as NUMERIC) AS total_credits,
        CAST(round(coalesce(m.total_debits, NUMERIC '0.0'), 4) as NUMERIC) AS total_debits,
        -- Ending Balance Calculation
        CAST(round(coalesce(prev.ending_balance, NUMERIC '0.0') + coalesce(m.total_credits, NUMERIC '0.0') - coalesce(m.total_debits, NUMERIC '0.0'), 4) as NUMERIC) AS ending_balance,
        -- Simple interest calculation (mock logic for demo: 0.05% APY / 365)
        CAST(round(CASE
          WHEN a.account_type = 'SAVINGS'
           AND coalesce(prev.ending_balance, NUMERIC '0.0') + coalesce(m.total_credits, NUMERIC '0.0') - coalesce(m.total_debits, NUMERIC '0.0') > 0 THEN (coalesce(prev.ending_balance, NUMERIC '0.0') + coalesce(m.total_credits, NUMERIC '0.0') - coalesce(m.total_debits, NUMERIC '0.0')) * (NUMERIC '0.05' / 365)
          ELSE NUMERIC '0.0'
        END, 4) as NUMERIC) AS interest_accrued,
        -- Overdrawn flag
        CASE
          WHEN coalesce(prev.ending_balance, NUMERIC '0.0') + coalesce(m.total_credits, NUMERIC '0.0') - coalesce(m.total_debits, NUMERIC '0.0') < 0 THEN true
          ELSE false
        END AS is_overdrawn,
        current_datetime() AS etl_timestamp,
        -- Partition columns
        SAFE_CAST(regexp_extract('2024-01-01', '^([^ ]*)') AS DATE) AS balance_date,
        coalesce(a.region_code, 'UN') AS region_code
      FROM
        __DEFAULT_DATABASE__.fin_core.dim_accounts AS a
        -- Join with previous day's balance
        LEFT OUTER JOIN __DEFAULT_DATABASE__.fin_core.fact_daily_balances AS prev ON a.account_id = prev.account_id
         AND prev.balance_date = SAFE_CAST(regexp_extract('2024-01-01', '^([^ ]*)') AS DATE) - 1
        -- Join with today's movements
        LEFT OUTER JOIN tmp_daily_movements_stg AS m ON a.account_id = m.account_id
      WHERE a.status IN(
        'OPEN', 'FROZEN', 'DORMANT'
      )
;
SET (balance_date__values, balance_date__is_null) = (
  SELECT
      STRUCT(array_agg(DISTINCT fact_daily_balances__insert.balance_date IGNORE NULLS) AS balance_date, logical_or(fact_daily_balances__insert.balance_date IS NULL) AS _f1)
    FROM
      fact_daily_balances__insert
);
MERGE INTO __DEFAULT_DATABASE__.fin_core.fact_daily_balances USING (
  SELECT DISTINCT
      fact_daily_balances__insert.balance_date AS balance_date,
      fact_daily_balances__insert.region_code AS region_code
    FROM
      fact_daily_balances__insert
) AS fact_daily_balances__insert
ON (fact_daily_balances.balance_date IN UNNEST(balance_date__values)
 OR balance_date__is_null
 AND fact_daily_balances.balance_date IS NULL)
 AND fact_daily_balances.balance_date IS NOT DISTINCT FROM fact_daily_balances__insert.balance_date
 AND fact_daily_balances.region_code IS NOT DISTINCT FROM fact_daily_balances__insert.region_code
   WHEN MATCHED THEN DELETE 
;
INSERT INTO __DEFAULT_DATABASE__.fin_core.fact_daily_balances (account_id, customer_id, account_type, open_date, currency_code, beginning_balance, total_credits, total_debits, ending_balance, interest_accrued, is_overdrawn, etl_timestamp, balance_date, region_code)
  SELECT
      fact_daily_balances__insert.account_id,
      fact_daily_balances__insert.customer_id,
      fact_daily_balances__insert.account_type,
      fact_daily_balances__insert.open_date,
      fact_daily_balances__insert.currency_code,
      fact_daily_balances__insert.beginning_balance,
      fact_daily_balances__insert.total_credits,
      fact_daily_balances__insert.total_debits,
      fact_daily_balances__insert.ending_balance,
      fact_daily_balances__insert.interest_accrued,
      fact_daily_balances__insert.is_overdrawn,
      fact_daily_balances__insert.etl_timestamp,
      fact_daily_balances__insert.balance_date,
      fact_daily_balances__insert.region_code
    FROM
      fact_daily_balances__insert
;
-- Clean up
DROP TABLE IF EXISTS tmp_daily_movements_stg;

```java
// ==============================================================================
// Module: fin_daily_balances.hql (Converted to fin_daily_balances.java for BigQuery)
// Description: Calculates End Of Day (EOD) balances for all banking accounts.
// Consumes the daily transaction fact table and applies it to the previous
// day's balance snapshot.
// ==============================================================================

// Ensure target dataset exists (BigQuery equivalent of a database).
// Major change: 'DATABASE' is replaced by 'SCHEMA' (an alias for 'DATASET' in BigQuery).
String createSchema = "CREATE SCHEMA IF NOT EXISTS fin_core;";

// 1. Create the Daily Balance Snapshot Table if it doesn't exist
// Original Hive: CREATE TABLE IF NOT EXISTS fin_core.fact_daily_balances (...) PARTITIONED BY (...) STORED AS ORC TBLPROPERTIES (...);
// Major change: Hive's data types (STRING, DECIMAL, BOOLEAN) are mapped to BigQuery's equivalents (STRING, NUMERIC, BOOL).
// Major change: Hive's 'COMMENT' syntax for table and columns is replaced with BigQuery's 'OPTIONS(description=...)'.
// Major change: Hive's `PARTITIONED BY (balance_date DATE, region_code STRING)` is translated to BigQuery's `PARTITION BY balance_date` (date column)
//               and `CLUSTER BY region_code` (string column), as BigQuery partitions directly by date/timestamp/integer columns and clusters by other columns.
// Major change: Hive-specific clauses like `STORED AS ORC` and `TBLPROPERTIES ('transactional'='true')` are removed
//               as BigQuery automatically manages storage and provides ACID properties inherently.
String createFactTable = "CREATE TABLE IF NOT EXISTS fin_core.fact_daily_balances (\n" +
        "    account_id STRING OPTIONS(description='Unique identifier for the account'),\n" +
        "    customer_id STRING OPTIONS(description='Identifier for the account owner'),\n" +
        "    account_type STRING OPTIONS(description='Type of account (CHECKING, SAVINGS, LOAN)'),\n" +
        "    open_date DATE OPTIONS(description='Date the account was opened'),\n" +
        "    currency_code STRING OPTIONS(description='Base currency of the account'),\n" +
        "    beginning_balance NUMERIC(18, 4) OPTIONS(description='Balance at the start of the day'),\n" +
        "    total_credits NUMERIC(18, 4) OPTIONS(description='Total value of incoming funds'),\n" +
        "    total_debits NUMERIC(18, 4) OPTIONS(description='Total value of outgoing funds'),\n" +
        "    ending_balance NUMERIC(18, 4) OPTIONS(description='Balance at the end of the day'),\n" +
        "    interest_accrued NUMERIC(18, 4) OPTIONS(description='Daily interest accrued'),\n" +
        "    is_overdrawn BOOL OPTIONS(description='Flag indicating if the account is in negative balance'),\n" +
        "    etl_timestamp TIMESTAMP,\n" +
        "    balance_date DATE, \n" +
        "    region_code STRING \n" +
        ")\n" +
        "OPTIONS(\n" +
        "    description='Stores the End of Day balances for all accounts'\n" +
        ")\n" +
        "PARTITION BY balance_date\n" +
        "CLUSTER BY region_code;";

// 2. Temporary table to hold today's net movements per account
// Original Hive: DROP TABLE IF EXISTS default.tmp_daily_movements_stg;
String dropTempTable = "DROP TABLE IF EXISTS default.tmp_daily_movements_stg;";

// Original Hive: CREATE TEMPORARY TABLE default.tmp_daily_movements_stg AS WITH ...;
// Minor change: BigQuery supports `CREATE TEMPORARY TABLE ... AS SELECT` with `WITH` clauses similarly to Hive.
// The `default` schema/dataset qualifier is retained for consistency with the original script,
// though BigQuery temporary tables often resolve to a default project/dataset without explicit qualification.
String createTempTable = "CREATE TEMPORARY TABLE default.tmp_daily_movements_stg AS\n" +
        "WITH credited AS (\n" +
        "    SELECT \n" +
        "        destination_account_id AS account_id,\n" +
        "        SUM(amount_base_currency) AS total_credits\n" +
        "    FROM fin_core.fact_transactions\n" +
        "    WHERE trx_date = '2024-01-01'\n" +
        "      AND transaction_type NOT IN ('FEE', 'REVERSAL_DEBIT')\n" +
        "    GROUP BY destination_account_id\n" +
        "),\n" +
        "debited AS (\n" +
        "    SELECT \n" +
        "        source_account_id AS account_id,\n" +
        "        SUM(amount_base_currency) AS total_debits\n" +
        "    FROM fin_core.fact_transactions\n" +
        "    WHERE trx_date = '2024-01-01'\n" +
        "      AND transaction_type NOT IN ('REVERSAL_CREDIT')\n" +
        "    GROUP BY source_account_id\n" +
        ")\n" +
        "SELECT \n" +
        "    COALESCE(c.account_id, d.account_id) AS account_id,\n" +
        "    COALESCE(c.total_credits, 0.0) AS total_credits,\n" +
        "    COALESCE(d.total_debits, 0.0) AS total_debits\n" +
        "FROM credited c\n" +
        "FULL OUTER JOIN debited d ON c.account_id = d.account_id;";

// 3. Calculate End of Day Balances and Insert Overwrite the partition
// Original Hive: INSERT OVERWRITE TABLE fin_core.fact_daily_balances PARTITION (balance_date, region_code) SELECT ...;
// Major change: BigQuery does not have a direct `INSERT OVERWRITE PARTITION` statement like Hive for specific partitions.
//               The equivalent process involves explicitly deleting existing data for the target partition(s)
//               and then inserting new data. Here, it targets the `balance_date` partition.
String deleteOldData = "DELETE FROM fin_core.fact_daily_balances WHERE balance_date = CAST('2024-01-01' AS DATE);";

String insertFactData = "INSERT INTO fin_core.fact_daily_balances (\n" +
        "    account_id, customer_id, account_type, open_date, currency_code,\n" +
        "    beginning_balance, total_credits, total_debits, ending_balance,\n" +
        "    interest_accrued, is_overdrawn, etl_timestamp, balance_date, region_code\n" +
        ")\n" +
        "SELECT \n" +
        "    a.account_id,\n" +
        "    a.customer_id,\n" +
        "    a.account_type,\n" +
        "    a.open_date,\n" +
        "    a.currency_code,\n" +
        "    \n" +
        "    -- Beginning Balance is the previous day's ending balance, or 0 if new\n" +
        "    COALESCE(prev.ending_balance, 0.0) AS beginning_balance,\n" +
        "    \n" +
        "    -- Daily Movements\n" +
        "    COALESCE(m.total_credits, 0.0) AS total_credits,\n" +
        "    COALESCE(m.total_debits, 0.0) AS total_debits,\n" +
        "    \n" +
        "    -- Ending Balance Calculation\n" +
        "    (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) AS ending_balance,\n" +
        "    \n" +
        "    -- Simple interest calculation (mock logic for demo: 0.05% APY / 365)\n" +
        "    CASE \n" +
        "        WHEN a.account_type = 'SAVINGS' AND (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) > 0 \n" +
        "        THEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) * (0.05 / 365)\n" +
        "        ELSE 0.0 \n" +
        "    END AS interest_accrued,\n" +
        "    \n" +
        "    -- Overdrawn flag\n" +
        "    CASE \n" +
        "        WHEN (COALESCE(prev.ending_balance, 0.0) + COALESCE(m.total_credits, 0.0) - COALESCE(m.total_debits, 0.0)) < 0 \n" +
        "        THEN TRUE \n" +
        "        ELSE FALSE \n" +
        "    END AS is_overdrawn,\n" +
        "    \n" +
        "    CURRENT_TIMESTAMP() AS etl_timestamp, -- Hive's CURRENT_TIMESTAMP() is equivalent to BigQuery's CURRENT_TIMESTAMP().\n" +
        "    \n" +
        "    -- Partition columns\n" +
        "    CAST('2024-01-01' AS DATE) AS balance_date,\n" +
        "    COALESCE(a.region_code, 'UN') AS region_code\n" +
        "    \n" +
        "FROM fin_core.dim_accounts a\n" +
        "-- Join with previous day's balance\n" +
        "LEFT JOIN fin_core.fact_daily_balances prev \n" +
        "    ON a.account_id = prev.account_id \n" +
        "    AND prev.balance_date = DATE_SUB(CAST('2024-01-01' AS DATE), INTERVAL 1 DAY) -- Major change: Hive's `DATE_SUB(date, days_int)` maps to BigQuery's `DATE_SUB(date_expression, INTERVAL int64_expression unit)`.\n" +
        "-- Join with today's movements\n" +
        "LEFT JOIN default.tmp_daily_movements_stg m \n" +
        "    ON a.account_id = m.account_id\n" +
        "WHERE a.status IN ('OPEN', 'FROZEN', 'DORMANT');";

// Clean up
String dropTempTableFinal = "DROP TABLE IF EXISTS default.tmp_daily_movements_stg;";
```

-- ==============================================================================
-- Module: fin_customer_mdm_merge.hql
-- Description: Master Data Management (MDM) module performing a Slowly Changing 
-- Dimension (SCD) Type 2 upsert operation purely through Hive SQL using FULL OUTER 
-- JOIN and complex CASE evaluations.
-- ==============================================================================

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Core Target Table for Customers (SCD Type 2)
CREATE TABLE IF NOT EXISTS fin_core.dim_customers_scd2 (
    customer_surrogate_key STRING,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email_address STRING,
    phone_number STRING,
    residential_address STRING,
    marital_status STRING,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOLEAN
) STORED AS ORC;

-- View logic to do an SCD 2 transform over yesterday's active snapshot vs today's delta update
DROP VIEW IF EXISTS default.vw_scd_transform_${hiveconf:PROCESS_DATE_NODASH};

CREATE VIEW default.vw_scd_transform_${hiveconf:PROCESS_DATE_NODASH} AS
WITH active_records AS (
    SELECT * 
    FROM fin_core.dim_customers_scd2 
    WHERE is_active = true
),
incoming_updates AS (
    -- Deduplicate incoming just in case
    SELECT 
        customer_id,
        first_name,
        last_name,
        email_address,
        phone_number,
        residential_address,
        marital_status,
        timestamp AS update_ts
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY timestamp DESC) as rn
        FROM fin_landing.customer_updates
        WHERE to_date(timestamp) = '${hiveconf:PROCESS_DATE}'
    ) t WHERE rn = 1
)

-- FULL OUTER JOIN handles Inserts, Updates, and Unchanged
SELECT 
    COALESCE(i.customer_id, a.customer_id) as customer_id,
    
    -- Case 1: Brand new customer (Insert) OR Updated Customer (Insert new active row)
    CASE WHEN i.customer_id IS NOT NULL THEN i.first_name ELSE a.first_name END AS first_name_new,
    CASE WHEN i.customer_id IS NOT NULL THEN i.last_name ELSE a.last_name END AS last_name_new,
    CASE WHEN i.customer_id IS NOT NULL THEN i.email_address ELSE a.email_address END AS email_address_new,
    CASE WHEN i.customer_id IS NOT NULL THEN i.phone_number ELSE a.phone_number END AS phone_number_new,
    CASE WHEN i.customer_id IS NOT NULL THEN i.residential_address ELSE a.residential_address END AS residential_address_new,
    CASE WHEN i.customer_id IS NOT NULL THEN i.marital_status ELSE a.marital_status END AS marital_status_new,
    
    -- Evaluate if a change actually occurred
    CASE 
        WHEN a.customer_id IS NULL THEN 'INSERT' -- Brand New
        WHEN i.customer_id IS NOT NULL AND (
             COALESCE(i.last_name, '') != COALESCE(a.last_name, '') OR
             COALESCE(i.email_address, '') != COALESCE(a.email_address, '') OR
             COALESCE(i.residential_address, '') != COALESCE(a.residential_address, '') OR
             COALESCE(i.marital_status, '') != COALESCE(a.marital_status, '')
        ) THEN 'UPDATE'
        WHEN i.customer_id IS NULL THEN 'NO CHANGE' -- No update received today
        ELSE 'NO CHANGE' -- Payload arrived but columns are identical
    END as change_type,
    
    -- Retain old record details to age it out
    a.customer_surrogate_key AS old_sk,
    a.first_name AS first_name_old,
    a.last_name AS last_name_old,
    a.email_address AS email_address_old,
    a.phone_number AS phone_number_old,
    a.residential_address AS residential_address_old,
    a.marital_status AS marital_status_old,
    a.effective_start_date AS old_start_date
    
FROM incoming_updates i
FULL OUTER JOIN active_records a ON i.customer_id = a.customer_id;

-- 1. Insert the retired rows (closing the effective_end_date and setting is_active = false)
INSERT INTO TABLE fin_core.dim_customers_scd2
SELECT 
    old_sk,
    customer_id,
    first_name_old,
    last_name_old,
    email_address_old,
    phone_number_old,
    residential_address_old,
    marital_status_old,
    old_start_date,
    CAST('${hiveconf:PROCESS_DATE} 00:00:00' AS TIMESTAMP) AS effective_end_date,
    false AS is_active
FROM default.vw_scd_transform_${hiveconf:PROCESS_DATE_NODASH}
WHERE change_type = 'UPDATE';

-- 2. Insert the completely NEW rows, and the NEW ACTIVE instances of updated rows
INSERT INTO TABLE fin_core.dim_customers_scd2
SELECT 
    -- Generate new Surrogate Key (UUID)
    reflect("java.util.UUID", "randomUUID") AS customer_surrogate_key,
    customer_id,
    first_name_new,
    last_name_new,
    email_address_new,
    phone_number_new,
    residential_address_new,
    marital_status_new,
    CAST('${hiveconf:PROCESS_DATE} 00:00:00' AS TIMESTAMP) AS effective_start_date,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS effective_end_date,
    true AS is_active
FROM default.vw_scd_transform_${hiveconf:PROCESS_DATE_NODASH}
WHERE change_type IN ('INSERT', 'UPDATE');

-- Note: 'NO CHANGE' records are unaffected since we did an INSERT INTO.
-- If maintaining an OVERWRITE pipeline, we'd also insert back the 'NO CHANGE' rows exactly as they were.

DROP VIEW IF EXISTS default.vw_scd_transform_${hiveconf:PROCESS_DATE_NODASH};

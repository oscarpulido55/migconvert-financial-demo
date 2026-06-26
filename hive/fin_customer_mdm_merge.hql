-- ==============================================================================
-- Module: fin_customer_mdm_merge.hql
-- Description: Master Data Management (MDM) module performing a Slowly Changing
-- Dimension (SCD) Type 2 upsert operation purely through Hive SQL using FULL OUTER
-- JOIN and complex CASE evaluations.
-- ==============================================================================

-- Core Target Table for Customers (SCD Type 2)
CREATE TABLE IF NOT EXISTS `your_project_id.fin_core.dim_customers_scd2` (
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
)
PARTITION BY DATE(effective_start_date)
OPTIONS(
    description="Master Data Management (MDM) module performing a Slowly Changing Dimension (SCD) Type 2 upsert operation for Customers"
);

-- View logic to do an SCD 2 transform over yesterday's active snapshot vs today's delta update
DROP VIEW IF EXISTS `your_project_id.default_dataset.vw_scd_transform_stg`;

CREATE VIEW `your_project_id.default_dataset.vw_scd_transform_stg` AS
WITH active_records AS (
    SELECT *
    FROM `your_project_id.fin_core.dim_customers_scd2`
    WHERE is_active = TRUE
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
        FROM `your_project_id.fin_landing.customer_updates`
        WHERE DATE(timestamp) = '2024-01-01'
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
INSERT INTO `your_project_id.fin_core.dim_customers_scd2` (
    customer_surrogate_key,
    customer_id,
    first_name,
    last_name,
    email_address,
    phone_number,
    residential_address,
    marital_status,
    effective_start_date,
    effective_end_date,
    is_active
)
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
    CAST('2024-01-01 00:00:00' AS TIMESTAMP),
    FALSE
FROM `your_project_id.default_dataset.vw_scd_transform_stg`
WHERE change_type = 'UPDATE';

-- 2. Insert the completely NEW rows, and the NEW ACTIVE instances of updated rows
INSERT INTO `your_project_id.fin_core.dim_customers_scd2` (
    customer_surrogate_key,
    customer_id,
    first_name,
    last_name,
    email_address,
    phone_number,
    residential_address,
    marital_status,
    effective_start_date,
    effective_end_date,
    is_active
)
SELECT
    -- Generate new Surrogate Key (UUID)
    GENERATE_UUID(),
    customer_id,
    first_name_new,
    last_name_new,
    email_address_new,
    phone_number_new,
    residential_address_new,
    marital_status_new,
    CAST('2024-01-01 00:00:00' AS TIMESTAMP),
    CAST('9999-12-31 23:59:59' AS TIMESTAMP),
    TRUE
FROM `your_project_id.default_dataset.vw_scd_transform_stg`
WHERE change_type IN ('INSERT', 'UPDATE');

-- Note: 'NO CHANGE' records are unaffected since we did an INSERT INTO.
-- If maintaining an OVERWRITE pipeline, we'd also insert back the 'NO CHANGE' rows exactly as they were.

DROP VIEW IF EXISTS `your_project_id.default_dataset.vw_scd_transform_stg`;

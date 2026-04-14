-- Translation time: 2026-04-14T19:46:34.301500Z
-- Translation job ID: b3689197-f39b-4da0-a085-96b6a235a8b3
-- Source: gs://migconvert-at-next26-work-bkt/735_SQL_TRANSLATION_ID_input/735_SQL_TRANSLATION_ID.sql
-- Translated from: Hive
-- Translated to: BigQuery

-- ==============================================================================
-- Module: fin_customer_mdm_merge.hql
-- Description: Master Data Management (MDM) module performing a Slowly Changing
-- Dimension (SCD) Type 2 upsert operation purely through Hive SQL using FULL OUTER
-- JOIN and complex CASE evaluations.
-- ==============================================================================
-- Core Target Table for Customers (SCD Type 2)
CREATE TABLE IF NOT EXISTS __DEFAULT_DATABASE__.fin_core.dim_customers_scd2
(
  customer_surrogate_key STRING,
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  email_address STRING,
  phone_number STRING,
  residential_address STRING,
  marital_status STRING,
  effective_start_date DATETIME,
  effective_end_date DATETIME,
  is_active BOOL
)
;
-- View logic to do an SCD 2 transform over yesterday's active snapshot vs today's delta update
DROP VIEW IF EXISTS __DEFAULT_DATABASE__.`default`.vw_scd_transform_stg;
CREATE VIEW __DEFAULT_DATABASE__.`default`.vw_scd_transform_stg(customer_id, first_name_new, last_name_new, email_address_new, phone_number_new, residential_address_new, marital_status_new, change_type, old_sk, first_name_old, last_name_old, email_address_old, phone_number_old, residential_address_old, marital_status_old, old_start_date) AS WITH incoming_updates AS (
  -- Deduplicate incoming just in case
  SELECT
      t.customer_id,
      t.first_name,
      t.last_name,
      t.email_address,
      t.phone_number,
      t.residential_address,
      t.marital_status,
      t.timestamp AS update_ts
    FROM
      (
        SELECT
            *,
            row_number() OVER (PARTITION BY customer_updates.customer_id ORDER BY customer_updates.timestamp DESC) AS rn
          FROM
            __DEFAULT_DATABASE__.fin_landing.customer_updates
          WHERE DATE(customer_updates.timestamp) = CAST('2024-01-01' as DATE)
      ) AS t
    WHERE t.rn = 1
), active_records AS (
  SELECT
      dim_customers_scd2.*
    FROM
      __DEFAULT_DATABASE__.fin_core.dim_customers_scd2
    WHERE dim_customers_scd2.is_active = true
)
-- FULL OUTER JOIN handles Inserts, Updates, and Unchanged
SELECT
    coalesce(i.customer_id, a.customer_id) AS customer_id,
    -- Case 1: Brand new customer (Insert) OR Updated Customer (Insert new active row)
    CASE
      WHEN i.customer_id IS NOT NULL THEN i.first_name
      ELSE a.first_name
    END AS first_name_new,
    CASE
      WHEN i.customer_id IS NOT NULL THEN i.last_name
      ELSE a.last_name
    END AS last_name_new,
    CASE
      WHEN i.customer_id IS NOT NULL THEN i.email_address
      ELSE a.email_address
    END AS email_address_new,
    CASE
      WHEN i.customer_id IS NOT NULL THEN i.phone_number
      ELSE a.phone_number
    END AS phone_number_new,
    CASE
      WHEN i.customer_id IS NOT NULL THEN i.residential_address
      ELSE a.residential_address
    END AS residential_address_new,
    CASE
      WHEN i.customer_id IS NOT NULL THEN i.marital_status
      ELSE a.marital_status
    END AS marital_status_new,
    -- Evaluate if a change actually occurred
    CASE
      WHEN a.customer_id IS NULL THEN 'INSERT' /* Brand New*/
      WHEN i.customer_id IS NOT NULL
       AND (coalesce(i.last_name, '') <> coalesce(a.last_name, '')
       OR coalesce(i.email_address, '') <> coalesce(a.email_address, '')
       OR coalesce(i.residential_address, '') <> coalesce(a.residential_address, '')
       OR coalesce(i.marital_status, '') <> coalesce(a.marital_status, '')) THEN 'UPDATE'
      WHEN i.customer_id IS NULL THEN 'NO CHANGE' /* No update received today*/
      ELSE 'NO CHANGE' /* Payload arrived but columns are identical*/
    END AS change_type,
    -- Retain old record details to age it out
    a.customer_surrogate_key AS old_sk,
    a.first_name AS first_name_old,
    a.last_name AS last_name_old,
    a.email_address AS email_address_old,
    a.phone_number AS phone_number_old,
    a.residential_address AS residential_address_old,
    a.marital_status AS marital_status_old,
    a.effective_start_date AS old_start_date
  FROM
    incoming_updates AS i
    FULL OUTER JOIN active_records AS a ON i.customer_id = a.customer_id
;
-- 1. Insert the retired rows (closing the effective_end_date and setting is_active = false)
INSERT INTO __DEFAULT_DATABASE__.fin_core.dim_customers_scd2 (customer_surrogate_key, customer_id, first_name, last_name, email_address, phone_number, residential_address, marital_status, effective_start_date, effective_end_date, is_active)
  SELECT
      vw_scd_transform_stg.old_sk,
      vw_scd_transform_stg.customer_id,
      vw_scd_transform_stg.first_name_old,
      vw_scd_transform_stg.last_name_old,
      vw_scd_transform_stg.email_address_old,
      vw_scd_transform_stg.phone_number_old,
      vw_scd_transform_stg.residential_address_old,
      vw_scd_transform_stg.marital_status_old,
      vw_scd_transform_stg.old_start_date,
      SAFE_CAST('2024-01-01 00:00:00' AS DATETIME) AS effective_end_date,
      false AS is_active
    FROM
      __DEFAULT_DATABASE__.`default`.vw_scd_transform_stg
    WHERE vw_scd_transform_stg.change_type = 'UPDATE'
;
-- 2. Insert the completely NEW rows, and the NEW ACTIVE instances of updated rows
INSERT INTO __DEFAULT_DATABASE__.fin_core.dim_customers_scd2 (customer_surrogate_key, customer_id, first_name, last_name, email_address, phone_number, residential_address, marital_status, effective_start_date, effective_end_date, is_active)
  SELECT
      -- Generate new Surrogate Key (UUID)
      reflect('java.util.UUID', 'randomUUID') AS customer_surrogate_key,
      vw_scd_transform_stg.customer_id,
      vw_scd_transform_stg.first_name_new,
      vw_scd_transform_stg.last_name_new,
      vw_scd_transform_stg.email_address_new,
      vw_scd_transform_stg.phone_number_new,
      vw_scd_transform_stg.residential_address_new,
      vw_scd_transform_stg.marital_status_new,
      SAFE_CAST('2024-01-01 00:00:00' AS DATETIME) AS effective_start_date,
      SAFE_CAST('9999-12-31 23:59:59' AS DATETIME) AS effective_end_date,
      true AS is_active
    FROM
      __DEFAULT_DATABASE__.`default`.vw_scd_transform_stg
    WHERE vw_scd_transform_stg.change_type IN(
      'INSERT', 'UPDATE'
    )
;
-- Note: 'NO CHANGE' records are unaffected since we did an INSERT INTO.
-- If maintaining an OVERWRITE pipeline, we'd also insert back the 'NO CHANGE' rows exactly as they were.
DROP VIEW IF EXISTS __DEFAULT_DATABASE__.`default`.vw_scd_transform_stg;

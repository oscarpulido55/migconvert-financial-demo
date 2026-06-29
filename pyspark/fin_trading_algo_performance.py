from google.cloud import bigquery # Converted from pyspark.sql.SparkSession for BigQuery client.
# pyspark.sql.functions and pyspark.sql.window are removed as their functionality will be expressed directly in SQL.
import datetime
import pytz
import sys

class AlgorithmicTradingPerformance:
    """
    Simulates high-frequency trading performance evaluation.
    It joins order logs (acks, fills, closures) with market data ticks to evaluate
    slippage (PL), opportunity costs, and VWAP (Volume-Weighted Average Price) differences.
    """

    def local_to_utc_time(self, hour, minute, date_str, tz='America/New_York'):
        local_tz = pytz.timezone(tz)
        date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        local_datetime = local_tz.localize(
            datetime.datetime.combine(date_obj, datetime.time(hour, minute))
        )
        return local_datetime.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')

    def execute_pipeline(self, bq_client: bigquery.Client, project_id: str, dataset_id: str, run_date: str):
        # The SparkSession parameter `spark` is replaced by `bq_client` for BigQuery.
        # `project_id` and `dataset_id` are added for explicit BigQuery table references.

        # Define BigQuery table paths using provided project and dataset IDs.
        trading_events_base_table = f"{project_id}.{dataset_id}.trading_events_base"
        parent_orders_table = f"{project_id}.{dataset_id}.parent_orders"
        level1_quotes_table = f"{project_id}.{dataset_id}.level1_quotes"
        market_trades_table = f"{project_id}.{dataset_id}.market_trades"
        # Define output table for BigQuery. The path includes run_date for partitioning.
        output_table_name = "trading_analytics" # Table name without run_date partitioning.

        # Construct a single SQL query using CTEs (Common Table Expressions) for the entire pipeline.
        # This replaces multiple PySpark DataFrame operations and HDFS I/O with a single BigQuery query.
        main_query_sql = f"""
        WITH
        -- 1. Load Core Datasets as initial CTEs
        all_order_events_df AS (
            SELECT * FROM `{trading_events_base_table}`
        ),
        parent_orders_df AS (
            SELECT * FROM `{parent_orders_table}`
        ),

        -- 2. Separate Event Stream into Fills
        fills_df AS (
            SELECT
                *
            FROM
                all_order_events_df
            WHERE
                ((protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
                OR (CAST(SUBSTR(protocol_version, 2) AS INT64) >= 2 AND event_type = 'FILL'))
                -- Converted F.col and .isin/.cast. For 'V2', CAST(SUBSTR(...)) is used for numeric comparison.
        ),
        close_fill_price_df AS (
            SELECT
                order_id AS close_order_id,
                client_id AS close_client_id,
                last_exec_price AS closing_price,
                last_exec_qty AS closing_qty
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY order_id, client_id ORDER BY event_timestamp) as rn
                    -- Converted Window.partitionBy().orderBy().row_number() to SQL.
                FROM fills_df
            )
            WHERE rn = 1
        ),
        fills_agg_df_raw AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS FillStartTime,
                MIN(event_timestamp) AS FirstFillTime,
                SUM(last_exec_qty) AS TotalSharesExecuted,
                COUNT(last_exec_qty) AS NumberOfFills,
                SAFE_DIVIDE(SUM(last_exec_qty * last_exec_price), SUM(last_exec_qty)) AS AverageExecutionPrice,
                SUM(last_exec_qty * last_exec_price) AS TotalMarketValueExecuted
                -- Converted F.min, F.sum, F.count, F.least, F.col syntax to SQL functions. SAFE_DIVIDE added for robustness.
            FROM fills_df
            GROUP BY order_id, client_id, trade_date
        ),
        fills_agg_df AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                FillStartTime AS fill_FillStartTime,
                FirstFillTime AS fill_FirstFillTime,
                TotalSharesExecuted AS fill_TotalSharesExecuted,
                NumberOfFills AS fill_NumberOfFills,
                AverageExecutionPrice AS fill_AverageExecutionPrice,
                TotalMarketValueExecuted AS fill_TotalMarketValueExecuted
            FROM fills_agg_df_raw
            -- Column prefixing is handled directly in the SELECT clause by aliasing.
        ),

        -- 3. Execution Acknowledgements
        acks_df AS (
            SELECT
                *
            FROM
                all_order_events_df
            WHERE
                status IN ('NEW', 'REPLACED') AND event_type = 'ACK'
        ),
        acks_agg_df_raw AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS AckStartTime
            FROM acks_df
            GROUP BY order_id, client_id, trade_date
        ),
        acks_agg_df AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                AckStartTime AS ack_AckStartTime
            FROM acks_agg_df_raw
            -- Column prefixing handled by aliasing.
        ),

        -- 4. Execution Terminations
        terminations_df AS (
            SELECT
                *
            FROM
                all_order_events_df
            WHERE
                status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')
        ),
        terminations_agg_df_raw AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS ExecutionEndTime
            FROM terminations_df
            GROUP BY order_id, client_id, trade_date
        ),
        terminations_agg_df AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                ExecutionEndTime AS term_ExecutionEndTime
            FROM terminations_agg_df_raw
            -- Column prefixing handled by aliasing.
        ),

        -- 5. Bring it back to Parent Orders
        enriched_orders_base AS (
            SELECT
                p.* EXCEPT(order_id, client_id, trade_date), -- Exclude join keys from parent for re-selection later
                f.fill_FillStartTime,
                f.fill_FirstFillTime,
                f.fill_TotalSharesExecuted,
                f.fill_NumberOfFills,
                f.fill_AverageExecutionPrice,
                f.fill_TotalMarketValueExecuted,
                a.ack_AckStartTime,
                t.term_ExecutionEndTime,
                c.closing_price,
                c.closing_qty,
                p.order_id,
                p.client_id,
                p.trade_date -- Including trade_date from parent for consistency and potential partitioning
            FROM
                parent_orders_df AS p
            LEFT JOIN
                fills_agg_df AS f
                ON p.order_id = f.order_id AND p.client_id = f.client_id
            LEFT JOIN
                acks_agg_df AS a
                ON p.order_id = a.order_id AND p.client_id = a.client_id
            LEFT JOIN
                terminations_agg_df AS t
                ON p.order_id = t.order_id AND p.client_id = t.client_id
            LEFT JOIN
                close_fill_price_df AS c
                ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id
            -- `drop` is implicit by careful selection in SQL.
        ),
        """
        # Python function call for local_to_utc_time
        utc_time_market_open_str = self.local_to_utc_time(9, 30, run_date)
        utc_time_market_close_str = self.local_to_utc_time(16, 00, run_date)

        main_query_sql += f"""
        enriched_orders_df_with_effective_times AS (
            SELECT
                *,
                LEAST(GREATEST(ack_AckStartTime, CAST('{utc_time_market_open_str}' AS TIMESTAMP)), fill_FillStartTime) AS EffectiveStartTime,
                LEAST(term_ExecutionEndTime, CAST('{utc_time_market_close_str}' AS TIMESTAMP)) AS EffectiveEndTime
                -- Converted F.greatest, F.least, F.lit, and .cast to SQL.
            FROM
                enriched_orders_base
        ),

        -- 6. Market Data Tick Metrics (complex time-based joins)
        quotes_df AS (
            SELECT * FROM `{level1_quotes_table}`
        ),
        enriched_orders_df_with_pk AS (
            SELECT
                *,
                GENERATE_UUID() AS order_pk -- Replaced F.monotonically_increasing_id() with GENERATE_UUID() for unique key generation.
                                           -- Note: GENERATE_UUID() produces random UUIDs, not monotonically increasing integers like Spark's function.
            FROM enriched_orders_df_with_effective_times
        ),
        -- The repartition and sortWithinPartitions hints are specific to Spark execution and have no direct BigQuery SQL equivalent,
        -- as BigQuery's optimizer handles data distribution and sorting internally. Removed these lines.

        -- Window buffer calculations for BigQuery SQL
        orders_with_time_buffers AS (
            SELECT
                *,
                TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,
                TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,
                TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,
                TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,
                TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,
                TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower
                -- Converted F.expr with interval arithmetic to BigQuery's TIMESTAMP_SUB and TIMESTAMP_ADD.
            FROM enriched_orders_df_with_pk
        ),

        -- Reimplementing the find_nearest_quote logic using CTEs and window functions in SQL
        open_quotes_candidates AS (
            SELECT
                o.order_pk,
                q.quote_timestamp,
                q.best_bid,
                q.best_ask,
                ABS(TIMESTAMP_DIFF(o.EffectiveStartTime, q.quote_timestamp, MICROSECOND)) AS time_diff
                -- TIMESTAMP_DIFF with MICROSECOND granularity used for time difference.
            FROM orders_with_time_buffers AS o
            JOIN quotes_df AS q
                ON o.ticker = q.ticker
                AND q.quote_timestamp BETWEEN o.Start_lower AND o.EffectiveStartTime
            -- Changed to standard SQL JOIN conditions. hint("merge") removed as it's Spark-specific.
        ),
        open_quotes AS (
            SELECT
                order_pk,
                quote_timestamp AS start_quote_timestamp,
                best_bid AS start_best_bid,
                best_ask AS start_best_ask
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY time_diff, quote_timestamp) AS rn
                    -- Row_number to pick the nearest quote. Added quote_timestamp to ORDER BY for deterministic tie-breaking.
                FROM open_quotes_candidates
            )
            WHERE rn = 1
        ),

        end_quotes_candidates AS (
            SELECT
                o.order_pk,
                q.quote_timestamp,
                q.best_bid,
                q.best_ask,
                ABS(TIMESTAMP_DIFF(o.EffectiveEndTime, q.quote_timestamp, MICROSECOND)) AS time_diff
            FROM orders_with_time_buffers AS o
            JOIN quotes_df AS q
                ON o.ticker = q.ticker
                AND q.quote_timestamp BETWEEN o.End_lower AND o.EffectiveEndTime
        ),
        end_quotes AS (
            SELECT
                order_pk,
                best_bid AS end_best_bid,
                best_ask AS end_best_ask
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY time_diff, quote_timestamp) AS rn
                FROM end_quotes_candidates
            )
            WHERE rn = 1
        ),

        end_1m_quotes_candidates AS (
            SELECT
                o.order_pk,
                q.quote_timestamp,
                q.best_bid,
                q.best_ask,
                ABS(TIMESTAMP_DIFF(o.End_plus1, q.quote_timestamp, MICROSECOND)) AS time_diff
            FROM orders_with_time_buffers AS o
            JOIN quotes_df AS q
                ON o.ticker = q.ticker
                AND q.quote_timestamp BETWEEN o.End_plus1_lower AND o.End_plus1
        ),
        end_1m_quotes AS (
            SELECT
                order_pk,
                best_bid AS end_plus1_best_bid,
                best_ask AS end_plus1_best_ask
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY time_diff, quote_timestamp) AS rn
                FROM end_1m_quotes_candidates
            )
            WHERE rn = 1
        ),

        -- 7. Core VWAP and Financial Performance calculations
        trades_df AS (
            SELECT * FROM `{market_trades_table}`
        ),
        vwap_df AS (
            SELECT
                o.order_id,
                o.client_id,
                SUM(t.trade_size) AS market_interval_volume,
                SAFE_DIVIDE(SUM(t.trade_price * t.trade_size), SUM(t.trade_size)) AS market_interval_vwap
            FROM enriched_orders_df_with_effective_times AS o
            JOIN trades_df AS t
                ON o.ticker = t.ticker
                AND t.trade_timestamp >= o.EffectiveStartTime
                AND t.trade_timestamp <= o.EffectiveEndTime
            GROUP BY o.order_id, o.client_id
        ),

        -- Join everything back
        final_joined_base AS (
            SELECT
                o.* EXCEPT (Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower),
                -- Select all columns from `orders_with_time_buffers` (aliased as `o`),
                -- but exclude the intermediate time buffer columns.
                v.market_interval_volume,
                v.market_interval_vwap,
                oq.start_best_bid,
                oq.start_best_ask,
                oq.start_quote_timestamp,
                eq.end_best_bid,
                eq.end_best_ask,
                e1mq.end_plus1_best_bid,
                e1mq.end_plus1_best_ask
            FROM
                orders_with_time_buffers AS o
            LEFT JOIN
                vwap_df AS v
                ON o.order_id = v.order_id AND o.client_id = v.client_id
            LEFT JOIN
                open_quotes AS oq
                ON o.order_pk = oq.order_pk
            LEFT JOIN
                end_quotes AS eq
                ON o.order_pk = eq.order_pk
            LEFT JOIN
                end_1m_quotes AS e1mq
                ON o.order_pk = e1mq.order_pk
        )
        SELECT
            *,
            -- Calculate complex performance metrics (Slippage, Momentum, Profit/Loss vectors)
            SAFE_DIVIDE(SAFE_ADD(start_best_bid, start_best_ask), 2) AS arrival_mid_price,
            -- Converted F.col and basic arithmetic. SAFE_DIVIDE and SAFE_ADD are BigQuery functions to prevent division/addition by NULL.
            CASE
                WHEN side = 'BUY' THEN SAFE_DIVIDE((fill_AverageExecutionPrice - market_interval_vwap) * 10000, market_interval_vwap)
                WHEN side = 'SELL' THEN SAFE_DIVIDE((market_interval_vwap - fill_AverageExecutionPrice) * 10000, market_interval_vwap)
                ELSE NULL
            END AS slippage_from_vwap_bps,
            CASE
                WHEN side = 'BUY' THEN (SAFE_DIVIDE(SAFE_ADD(start_best_bid, start_best_ask), 2) - fill_AverageExecutionPrice) * fill_TotalSharesExecuted
                WHEN side = 'SELL' THEN (fill_AverageExecutionPrice - SAFE_DIVIDE(SAFE_ADD(start_best_bid, start_best_ask), 2)) * fill_TotalSharesExecuted
                ELSE NULL
            END AS implementation_shortfall_pl,
            CASE
                WHEN side = 'BUY' THEN (SAFE_DIVIDE(SAFE_ADD(end_plus1_best_bid, end_plus1_best_ask), 2) - SAFE_DIVIDE(SAFE_ADD(end_best_bid, end_best_ask), 2)) * fill_TotalSharesExecuted
                WHEN side = 'SELL' THEN (SAFE_DIVIDE(SAFE_ADD(end_best_bid, end_best_ask), 2) - SAFE_DIVIDE(SAFE_ADD(end_plus1_best_bid, end_plus1_best_ask), 2)) * fill_TotalSharesExecuted
                ELSE NULL
            END AS post_trade_1m_momentum,
            CASE
                WHEN side = 'BUY' THEN (requested_shares - fill_TotalSharesExecuted) * (fill_AverageExecutionPrice - closing_price)
                WHEN side = 'SELL' THEN (requested_shares - fill_TotalSharesExecuted) * (closing_price - fill_AverageExecutionPrice)
                ELSE NULL
            END AS opportunity_cost_pl
        FROM
            final_joined_base
        WHERE
            order_pk IS NOT NULL -- Ensures only orders successfully joined with initial metadata are considered.
        ;
        """
        # Configure the BigQuery job to write to the destination table.
        # This replaces final_df.write.parquet(f"hdfs://...")
        job_config = bigquery.QueryJobConfig(
            destination=f"{project_id}.{dataset_id}.{output_table_name}", # Full output table path
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Corresponds to Spark's mode="overwrite"
            time_partitioning=bigquery.TimePartitioning(
                type=bigquery.TimePartitioningType.DAY,
                field="trade_date",  # Assuming `trade_date` from enriched_orders_base is suitable for partitioning.
                                     # This creates a daily partitioned table.
            ),
        )

        # Execute the query.
        query_job = bq_client.query(main_query_sql, job_config=job_config)
        query_job.result()  # Waits for the job to complete.

        print("Performance analysis complete.")

if __name__ == "__main__":
    # Removed SparkSession import and initialization.
    import sys
    import os # Import os to get environment variables

    if len(sys.argv) < 2:
        print("Usage: python script_name.py <run_date>")
        sys.exit(1)

    # Initialize BigQuery client
    # Configure PROJECT_ID and DATASET_ID, e.g., via environment variables or direct assignment
    # Ensure your GOOGLE_APPLICATION_CREDENTIALS environment variable is set or BigQuery default auth is configured.
    project_id = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id") # Replace with your GCP project ID or set env var
    dataset_id = os.environ.get("BQ_DATASET_ID", "trading_dataset")    # Replace with your BigQuery dataset ID or set env var
    bq_client = bigquery.Client(project=project_id) # Initialized BigQuery client instead of SparkSession.

    p = AlgorithmicTradingPerformance()
    # Pass bq_client, project_id, and dataset_id to the execute_pipeline method.
    p.execute_pipeline(bq_client, project_id, dataset_id, sys.argv[1])
    # spark.stop() is removed as there is no explicit client shutdown needed for BigQuery in this context.

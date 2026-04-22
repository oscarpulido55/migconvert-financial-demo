from google.cloud import bigquery
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

    def execute_pipeline(self, client: bigquery.Client, run_date: str):
        
        # 1. Define BigQuery table paths (replacing HDFS paths with BigQuery table references)
        # Please replace `project_id.dataset_id` with your actual GCP project and BigQuery dataset.
        all_order_events_table = "`project_id.dataset_id.trading_events_base`"
        parent_orders_table = "`project_id.dataset_id.parent_orders`"
        level1_quotes_table = "`project_id.dataset_id.level1_quotes`"
        market_trades_table = "`project_id.dataset_id.market_trades`"
        # BigQuery destination table for the final output
        output_table_id = f"project_id.dataset_id.trading_analytics_run_date_{run_date.replace('-', '')}"

        # 1. Load Core Datasets
        # In BigQuery, tables are referenced directly in SQL queries, not loaded into Spark DataFrames.

        # 2. Separate Event Stream into Fills
        # Anonymized protocol filtering conceptually representing status flags
        # Converted `all_order_events_df.filter(...)` to a SQL CTE
        fills_cte_sql = f"""
            SELECT
                *
            FROM
                {all_order_events_table}
            WHERE
                ((protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
                OR (SAFE_CAST(protocol_version AS FLOAT64) >= 2.0 AND event_type = 'FILL'))
        """

        # Get the closing fill price per order
        # Converted Spark DataFrame operations to a SQL CTE using ROW_NUMBER for partitioning
        close_fill_price_cte_sql = f"""
            WITH fills AS ({fills_cte_sql})
            SELECT
                order_id AS close_order_id,
                client_id AS close_client_id,
                last_exec_price AS closing_price,
                last_exec_qty AS closing_qty
            FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER(PARTITION BY order_id, client_id ORDER BY event_timestamp) AS rn
                    FROM
                        fills
                )
            WHERE
                rn = 1
        """

        # Aggregate fills per order
        # Converted `fills_df.groupBy(...).agg(...)` to a SQL CTE.
        # Used SAFE_CAST for division to handle potential non-numeric data or null denominators gracefully.
        fills_agg_cte_sql = f"""
            WITH fills AS ({fills_cte_sql})
            SELECT
                order_id,
                client_id,
                trade_date,
                LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS FillStartTime,
                MIN(event_timestamp) AS FirstFillTime,
                SUM(last_exec_qty) AS TotalSharesExecuted,
                COUNT(last_exec_qty) AS NumberOfFills,
                SUM(SAFE_CAST(last_exec_qty AS BIGNUMERIC) * SAFE_CAST(last_exec_price AS BIGNUMERIC)) / NULLIF(SUM(SAFE_CAST(last_exec_qty AS BIGNUMERIC)), 0) AS AverageExecutionPrice,
                SUM(SAFE_CAST(last_exec_qty AS BIGNUMERIC) * SAFE_CAST(last_exec_price AS BIGNUMERIC)) AS TotalMarketValueExecuted
            FROM
                fills
            GROUP BY
                order_id,
                client_id,
                trade_date
        """
            
        # Prefix columns for joins - this logic is integrated into the final BigQuery SQL CTEs by aliasing.

        # 3. Execution Acknowledgements
        # Converted `acks_df.groupBy(...).agg(...)` to a SQL CTE
        acks_cte_sql = f"""
            SELECT
                order_id,
                client_id,
                trade_date,
                LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS AckStartTime
            FROM
                {all_order_events_table}
            WHERE
                status IN ('NEW', 'REPLACED') AND event_type = 'ACK'
            GROUP BY
                order_id,
                client_id,
                trade_date
        """
            
        # for col_name in acks_agg_df.columns:
        #     if col_name not in ['order_id', 'client_id', 'trade_date']:
        #         acks_agg_df = acks_agg_df.withColumnRenamed(col_name, "ack_" + col_name)
        # Column renaming for 'ack_' prefix is handled during the final join stage in BigQuery SQL.

        # 4. Execution Terminations
        # Converted `terminations_df.groupBy(...).agg(...)` to a SQL CTE
        terminations_cte_sql = f"""
            SELECT
                order_id,
                client_id,
                trade_date,
                GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS ExecutionEndTime
            FROM
                {all_order_events_table}
            WHERE
                status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')
            GROUP BY
                order_id,
                client_id,
                trade_date
        """
            
        # for col_name in terminations_agg_df.columns:
        #     if col_name not in ['order_id', 'client_id', 'trade_date']:
        #         terminations_agg_df = terminations_agg_df.withColumnRenamed(col_name, "term_" + col_name)
        # Column renaming for 'term_' prefix is handled during the final join stage in BigQuery SQL.

        # 5. Bring it back to Parent Orders
        # Converted the series of joins to a BigQuery SQL CTE.
        # `GENERATE_UUID()` is used as an equivalent for Spark's `monotonically_increasing_id()` to get a unique row ID.
        enriched_orders_initial_cte_sql = f"""
            WITH
                fills_cte AS ({fills_cte_sql}),
                close_fill_price_cte AS ({close_fill_price_cte_sql}),
                fills_agg_cte AS ({fills_agg_cte_sql}),
                acks_cte AS ({acks_cte_sql}),
                terminations_cte AS ({terminations_cte_sql})
            SELECT
                p.*,
                f.FillStartTime AS fill_FillStartTime,
                f.FirstFillTime AS fill_FirstFillTime,
                f.TotalSharesExecuted AS fill_TotalSharesExecuted,
                f.NumberOfFills AS fill_NumberOfFills,
                f.AverageExecutionPrice AS fill_AverageExecutionPrice,
                f.TotalMarketValueExecuted AS fill_TotalMarketValueExecuted,
                a.AckStartTime AS ack_AckStartTime,
                t.ExecutionEndTime AS term_ExecutionEndTime,
                c.closing_price,
                c.closing_qty,
                GENERATE_UUID() AS order_pk # BigQuery equivalent for unique row ID for joining with quotes
            FROM
                {parent_orders_table} AS p
            LEFT JOIN
                fills_agg_cte AS f
            ON
                p.order_id = f.order_id AND p.client_id = f.client_id
            LEFT JOIN
                acks_cte AS a
            ON
                p.order_id = a.order_id AND p.client_id = a.client_id
            LEFT JOIN
                terminations_cte AS t
            ON
                p.order_id = t.order_id AND p.client_id = t.client_id
            LEFT JOIN
                close_fill_price_cte AS c
            ON
                p.order_id = c.close_order_id AND p.client_id = c.close_client_id
        """

        utc_time_market_open = self.local_to_utc_time(9, 30, run_date)
        utc_time_market_close = self.local_to_utc_time(16, 00, run_date)

        # Establish effective operating window
        # Converted `enriched_orders_df.withColumn(...)` to BigQuery SQL, using TIMESTAMP literals and functions.
        enriched_orders_with_window_cte_sql = f"""
            WITH enriched_orders_initial AS ({enriched_orders_initial_cte_sql})
            SELECT
                *,
                LEAST(GREATEST(ack_AckStartTime, TIMESTAMP('{utc_time_market_open}')), fill_FillStartTime) AS EffectiveStartTime,
                LEAST(term_ExecutionEndTime, TIMESTAMP('{utc_time_market_close}')) AS EffectiveEndTime
            FROM
                enriched_orders_initial
        """

        # 6. Market Data Tick Metrics (complex time-based joins)
        # Spark's repartition and sortWithinPartitions are hints for distributed processing; BigQuery handles this internally.
        # A CTE for quotes is created with a UUID to serve as a stable row identifier.
        quotes_with_pk_cte_sql = f"""
            SELECT
                *,
                GENERATE_UUID() AS quote_pk # BigQuery equivalent for unique row ID
            FROM
                {level1_quotes_table}
        """

        # Build Window buffers for Quote lookups
        # Converted `enriched_orders_df.withColumn(...)` with interval expressions to BigQuery TIMESTAMP_SUB/ADD.
        enriched_orders_with_quote_windows_cte_sql = f"""
            WITH enriched_orders_with_window AS ({enriched_orders_with_window_cte_sql})
            SELECT
                *,
                TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,
                TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,
                TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,
                TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,
                TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,
                TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower
            FROM
                enriched_orders_with_window
        """

        # Define a closure to reuse logic for looking up the nearest quote
        # In BigQuery, this is implemented using `LEFT JOIN LATERAL` queries within the main SQL query,
        # explicitly defining the lookup logic for each of the three quote lookups.
        
        # Perform lookups for open_quotes (start time)
        # Converted `find_nearest_quote(..., "start")`
        open_quotes_cte_sql = f"""
            WITH
                enriched_orders AS ({enriched_orders_with_quote_windows_cte_sql}),
                quotes_data AS ({quotes_with_pk_cte_sql})
            SELECT
                o.order_pk,
                q.quote_timestamp AS start_quote_timestamp,
                q.best_bid AS start_best_bid,
                q.best_ask AS start_best_ask
            FROM
                enriched_orders AS o
            LEFT JOIN LATERAL
                (
                    SELECT
                        q_sub.*,
                        ABS(TIMESTAMP_DIFF(o.EffectiveStartTime, q_sub.quote_timestamp, MICROSECOND)) AS time_diff_us
                    FROM
                        quotes_data AS q_sub
                    WHERE
                        q_sub.ticker = o.ticker
                        AND q_sub.quote_timestamp BETWEEN o.Start_lower AND o.EffectiveStartTime
                    ORDER BY
                        time_diff_us ASC, q_sub.quote_timestamp ASC
                    LIMIT 1
                ) AS q
            ON TRUE
        """

        # Perform lookups for end_quotes (end time)
        # Converted `find_nearest_quote(..., "end")`
        end_quotes_cte_sql = f"""
            WITH
                enriched_orders AS ({enriched_orders_with_quote_windows_cte_sql}),
                quotes_data AS ({quotes_with_pk_cte_sql})
            SELECT
                o.order_pk,
                q.quote_timestamp AS end_quote_timestamp,
                q.best_bid AS end_best_bid,
                q.best_ask AS end_best_ask
            FROM
                enriched_orders AS o
            LEFT JOIN LATERAL
                (
                    SELECT
                        q_sub.*,
                        ABS(TIMESTAMP_DIFF(o.EffectiveEndTime, q_sub.quote_timestamp, MICROSECOND)) AS time_diff_us
                    FROM
                        quotes_data AS q_sub
                    WHERE
                        q_sub.ticker = o.ticker
                        AND q_sub.quote_timestamp BETWEEN o.End_lower AND o.EffectiveEndTime
                    ORDER BY
                        time_diff_us ASC, q_sub.quote_timestamp ASC
                    LIMIT 1
                ) AS q
            ON TRUE
        """

        # Perform lookups for end_1m_quotes (1 minute after end time)
        # Converted `find_nearest_quote(..., "end_plus1")`
        end_1m_quotes_cte_sql = f"""
            WITH
                enriched_orders AS ({enriched_orders_with_quote_windows_cte_sql}),
                quotes_data AS ({quotes_with_pk_cte_sql})
            SELECT
                o.order_pk,
                q.quote_timestamp AS end_plus1_quote_timestamp,
                q.best_bid AS end_plus1_best_bid,
                q.best_ask AS end_plus1_best_ask
            FROM
                enriched_orders AS o
            LEFT JOIN LATERAL
                (
                    SELECT
                        q_sub.*,
                        ABS(TIMESTAMP_DIFF(o.End_plus1, q_sub.quote_timestamp, MICROSECOND)) AS time_diff_us
                    FROM
                        quotes_data AS q_sub
                    WHERE
                        q_sub.ticker = o.ticker
                        AND q_sub.quote_timestamp BETWEEN o.End_plus1_lower AND o.End_plus1
                    ORDER BY
                        time_diff_us ASC, q_sub.quote_timestamp ASC
                    LIMIT 1
                ) AS q
            ON TRUE
        """
        
        # 7. Core VWAP and Financial Performance calculations
        # VWAP during order existence
        # Converted Spark join and aggregation to a SQL CTE.
        vwap_cte_sql = f"""
            WITH
                enriched_orders_with_window AS ({enriched_orders_with_window_cte_sql})
            SELECT
                eo.order_id,
                eo.client_id,
                SUM(t.trade_size) AS market_interval_volume,
                SUM(SAFE_CAST(t.trade_price AS BIGNUMERIC) * SAFE_CAST(t.trade_size AS BIGNUMERIC)) / NULLIF(SUM(SAFE_CAST(t.trade_size AS BIGNUMERIC)), 0) AS market_interval_vwap
            FROM
                enriched_orders_with_window AS eo
            INNER JOIN
                {market_trades_table} AS t
            ON
                eo.ticker = t.ticker
                AND t.trade_timestamp >= eo.EffectiveStartTime
                AND t.trade_timestamp <= eo.EffectiveEndTime
            GROUP BY
                eo.order_id,
                eo.client_id
        """

        # Join everything back and calculate complex performance metrics
        # All intermediate "DataFrames" are consolidated into a single BigQuery SQL query using CTEs.
        final_query_sql = f"""
            WITH
                fills_cte AS ({fills_cte_sql}),
                close_fill_price_cte AS ({close_fill_price_cte_sql}),
                fills_agg_cte AS ({fills_agg_cte_sql}),
                acks_cte AS ({acks_cte_sql}),
                terminations_cte AS ({terminations_cte_sql}),
                enriched_orders_initial AS ({enriched_orders_initial_cte_sql}),
                enriched_orders_with_window AS ({enriched_orders_with_window_cte_sql}),
                quotes_with_pk_cte AS ({quotes_with_pk_cte_sql}),
                enriched_orders_with_quote_windows AS ({enriched_orders_with_quote_windows_cte_sql}),
                open_quotes AS ({open_quotes_cte_sql}),
                end_quotes AS ({end_quotes_cte_sql}),
                end_1m_quotes AS ({end_1m_quotes_cte_sql}),
                vwap_data AS ({vwap_cte_sql})
            SELECT
                e.* EXCEPT (Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower), # Drop temporary window columns
                v.market_interval_volume,
                v.market_interval_vwap,
                oq.start_best_bid,
                oq.start_best_ask,
                oq.start_quote_timestamp,
                eq.end_best_bid,
                eq.end_best_ask,
                eq1m.end_plus1_best_bid,
                eq1m.end_plus1_best_ask,
                (oq.start_best_bid + oq.start_best_ask) / 2 AS arrival_mid_price,
                # Slippage from VWAP
                CASE
                    WHEN e.side = 'BUY' THEN ((e.fill_AverageExecutionPrice - v.market_interval_vwap) / v.market_interval_vwap) * 10000
                    WHEN e.side = 'SELL' THEN ((v.market_interval_vwap - e.fill_AverageExecutionPrice) / v.market_interval_vwap) * 10000
                    ELSE NULL
                END AS slippage_from_vwap_bps,
                # Slippage from Arrival Mid (Implementation Shortfall)
                CASE
                    WHEN e.side = 'BUY' THEN ( (oq.start_best_bid + oq.start_best_ask) / 2 - e.fill_AverageExecutionPrice ) * e.fill_TotalSharesExecuted
                    WHEN e.side = 'SELL' THEN ( e.fill_AverageExecutionPrice - (oq.start_best_bid + oq.start_best_ask) / 2 ) * e.fill_TotalSharesExecuted
                    ELSE NULL
                END AS implementation_shortfall_pl,
                # Momentum calculations post-trade
                CASE
                    WHEN e.side = 'BUY' THEN ( ((eq1m.end_plus1_best_bid + eq1m.end_plus1_best_ask) / 2) - ((eq.end_best_bid + eq.end_best_ask) / 2) ) * e.fill_TotalSharesExecuted
                    WHEN e.side = 'SELL' THEN ( ((eq.end_best_bid + eq.end_best_ask) / 2) - ((eq1m.end_plus1_best_bid + eq1m.end_plus1_best_ask) / 2) ) * e.fill_TotalSharesExecuted
                    ELSE NULL
                END AS post_trade_1m_momentum,
                # Opportunity Cost
                CASE
                    WHEN e.side = 'BUY' THEN (e.requested_shares - e.fill_TotalSharesExecuted) * (e.fill_AverageExecutionPrice - e.closing_price)
                    WHEN e.side = 'SELL' THEN (e.requested_shares - e.fill_TotalSharesExecuted) * (e.closing_price - e.fill_AverageExecutionPrice)
                    ELSE NULL
                END AS opportunity_cost_pl
            FROM
                enriched_orders_with_quote_windows AS e
            LEFT JOIN
                vwap_data AS v
            ON
                e.order_id = v.order_id AND e.client_id = v.client_id
            LEFT JOIN
                open_quotes AS oq
            ON
                e.order_pk = oq.order_pk
            LEFT JOIN
                end_quotes AS eq
            ON
                e.order_pk = eq.order_pk
            LEFT JOIN
                end_1m_quotes AS eq1m
            ON
                e.order_pk = eq1m.order_pk
        """

        # final_df.write.parquet(f"hdfs://trading_analytics/run_date={run_date}", mode="overwrite")
        # BigQuery equivalent: Execute the final SQL query to create or overwrite the destination table.
        job_config = bigquery.QueryJobConfig()
        job_config.destination = output_table_id
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE # Equivalent to Spark's "overwrite" mode

        print(f"Executing BigQuery job to create/overwrite table: {output_table_id}")
        query_job = client.query(final_query_sql, job_config=job_config)
        query_job.result() # Wait for the BigQuery job to complete
        print("Performance analysis complete.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python your_script.py <run_date> [project_id]")
        sys.exit(1)
    
    # spark = SparkSession.builder.appName("AlgorithmicTradingPerformance").getOrCreate() # Replaced SparkSession initialization
    # Initialize BigQuery client. It typically picks up credentials from environment variables or gcloud CLI.
    # Pass project_id as a command-line argument, or use a default.
    project_id = sys.argv[2] if len(sys.argv) > 2 else "your-gcp-project-id" # Placeholder: Update with your GCP Project ID
    bigquery_client = bigquery.Client(project=project_id)

    p = AlgorithmicTradingPerformance()
    # p.execute_pipeline(spark, sys.argv[1]) # Replaced with BigQuery client
    p.execute_pipeline(bigquery_client, sys.argv[1])
    # spark.stop() # No SparkSession to stop

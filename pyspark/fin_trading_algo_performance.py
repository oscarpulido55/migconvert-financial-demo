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

        # 1. Load Core Datasets
        # Replaced PySpark `spark.read.parquet("hdfs://...")` with BigQuery table references.
        # The entire data processing pipeline is consolidated into a single SQL query using CTEs for efficiency.
        all_order_events_table = "`your_project.your_dataset.trading_events_base`"
        parent_orders_table = "`your_project.your_dataset.parent_orders`"
        quotes_table = "`your_project.your_dataset.level1_quotes`"
        trades_table = "`your_project.your_dataset.market_trades`"

        # Initialize list of CTEs to build the final BigQuery SQL query
        main_query_ctes = []

        # 2. Separate Event Stream into Fills
        # PySpark .filter() equivalent in SQL WHERE clause
        main_query_ctes.append(f"""
            WITH fills_df AS (
                SELECT *
                FROM {all_order_events_table}
                WHERE
                    (protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
                    OR (protocol_version >= 'V2' AND event_type = 'FILL')
            )
        """)

        # Get the closing fill price per order
        # PySpark withColumn, Window, filter, select equivalent in SQL CTE with ROW_NUMBER()
        main_query_ctes.append(f"""
            , close_fill_price_df AS (
                SELECT
                    order_id AS close_order_id,
                    client_id AS close_client_id,
                    last_exec_price AS closing_price,
                    last_exec_qty AS closing_qty
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY order_id, client_id ORDER BY event_timestamp) as rn
                    FROM fills_df
                )
                WHERE rn = 1
            )
        """)

        # Aggregate fills per order
        # PySpark groupBy, agg equivalent in SQL CTE with aggregate functions and column prefixing in SELECT
        main_query_ctes.append(f"""
            , fills_agg_df AS (
                SELECT
                    order_id,
                    client_id,
                    trade_date,
                    LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS fill_FillStartTime,
                    MIN(event_timestamp) AS fill_FirstFillTime,
                    SUM(last_exec_qty) AS fill_TotalSharesExecuted,
                    COUNT(last_exec_qty) AS fill_NumberOfFills,
                    (SUM(last_exec_qty * last_exec_price) / NULLIF(SUM(last_exec_qty), 0)) AS fill_AverageExecutionPrice,
                    SUM(last_exec_qty * last_exec_price) AS fill_TotalMarketValueExecuted
                FROM fills_df
                GROUP BY 1, 2, 3
            )
        """)

        # Prefix columns for joins - handled by explicit aliasing in the SELECT list of fills_agg_df CTE.

        # 3. Execution Acknowledgements
        # PySpark .filter() equivalent in SQL WHERE clause
        main_query_ctes.append(f"""
            , acks_df AS (
                SELECT *
                FROM {all_order_events_table}
                WHERE status IN ('NEW', 'REPLACED') AND event_type = 'ACK'
            )
        """)
        # PySpark groupBy, agg equivalent in SQL CTE with aggregate functions and column prefixing in SELECT
        main_query_ctes.append(f"""
            , acks_agg_df AS (
                SELECT
                    order_id,
                    client_id,
                    trade_date,
                    LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS ack_AckStartTime
                FROM acks_df
                GROUP BY 1, 2, 3
            )
        """)

        # for col_name in acks_agg_df.columns:
        #     if col_name not in ['order_id', 'client_id', 'trade_date']:
        #         acks_agg_df = acks_agg_df.withColumnRenamed(col_name, "ack_" + col_name) # Handled by direct aliasing

        # 4. Execution Terminations
        # PySpark .filter() equivalent in SQL WHERE clause
        main_query_ctes.append(f"""
            , terminations_df AS (
                SELECT *
                FROM {all_order_events_table}
                WHERE status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')
            )
        """)
        # PySpark groupBy, agg equivalent in SQL CTE with aggregate functions and column prefixing in SELECT
        main_query_ctes.append(f"""
            , terminations_agg_df AS (
                SELECT
                    order_id,
                    client_id,
                    trade_date,
                    GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS term_ExecutionEndTime
                FROM terminations_df
                GROUP BY 1, 2, 3
            )
        """)

        # for col_name in terminations_agg_df.columns:
        #     if col_name not in ['order_id', 'client_id', 'trade_date']:
        #         terminations_agg_df = terminations_agg_df.withColumnRenamed(col_name, "term_" + col_name) # Handled by direct aliasing

        # 5. Bring it back to Parent Orders
        # PySpark .join() and .drop() equivalent in SQL CTE with LEFT JOIN and explicit column selection
        main_query_ctes.append(f"""
            , enriched_orders_base AS (
                SELECT p.*,
                    f.fill_FillStartTime,
                    f.fill_FirstFillTime,
                    f.fill_TotalSharesExecuted,
                    f.fill_NumberOfFills,
                    f.fill_AverageExecutionPrice,
                    f.fill_TotalMarketValueExecuted,
                    a.ack_AckStartTime,
                    t.term_ExecutionEndTime,
                    c.closing_price,
                    c.closing_qty
                FROM {parent_orders_table} AS p
                LEFT JOIN fills_agg_df AS f
                    ON p.order_id = f.order_id AND p.client_id = f.client_id
                LEFT JOIN acks_agg_df AS a
                    ON p.order_id = a.order_id AND p.client_id = a.client_id
                LEFT JOIN terminations_agg_df AS t
                    ON p.order_id = t.order_id AND p.client_id = t.client_id
                LEFT JOIN close_fill_price_df AS c
                    ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id
            )
        """)

        utc_time_market_open = self.local_to_utc_time(9, 30, run_date)
        utc_time_market_close = self.local_to_utc_time(16, 00, run_date)

        # Establish effective operating window
        # PySpark withColumn, F.least, F.greatest, F.lit, .cast equivalent in SQL CTE
        main_query_ctes.append(f"""
            , enriched_orders_with_effective_times AS (
                SELECT
                    *,
                    LEAST(
                        GREATEST(ack_AckStartTime, TIMESTAMP('{utc_time_market_open}')),
                        fill_FillStartTime
                    ) AS EffectiveStartTime,
                    LEAST(term_ExecutionEndTime, TIMESTAMP('{utc_time_market_close}')) AS EffectiveEndTime
                FROM enriched_orders_base
            )
        """)

        # 6. Market Data Tick Metrics (complex time-based joins)
        # PySpark repartition, sortWithinPartitions are Spark-specific optimizations, not directly translatable
        # PySpark monotonically_increasing_id is approximated by GENERATE_UUID() for uniqueness
        main_query_ctes.append(f"""
            , enriched_orders_with_pk AS (
                SELECT
                    *,
                    GENERATE_UUID() as order_pk -- Approximates monotonically_increasing_id for a unique identifier.
                FROM enriched_orders_with_effective_times
            )
        """)
        main_query_ctes.append(f"""
            , quotes_with_pk AS (
                SELECT
                    *,
                    GENERATE_UUID() as quote_pk -- Unique identifier for each quote row.
                FROM {quotes_table}
            )
        """)

        # Build Window buffers for Quote lookups
        # PySpark withColumn, F.expr equivalent using BigQuery TIMESTAMP_SUB/ADD
        main_query_ctes.append(f"""
            , enriched_orders_with_time_buffers AS (
                SELECT
                    *,
                    TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,
                    TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,
                    TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,
                    TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,
                    TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,
                    TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower
                FROM enriched_orders_with_pk
            )
        """)

        # Define a closure to reuse logic for looking up the nearest quote
        # This Python function's logic is translated into individual CTEs below for each lookup.
        # This approach ensures all heavy computation stays within BigQuery SQL.

        # Perform lookups for open_quotes
        # PySpark join, withColumn (time_diff), Window (rn), filter, select equivalent in SQL CTE
        main_query_ctes.append(f"""
            , open_quotes AS (
                SELECT
                    order_pk,
                    quote_timestamp AS start_quote_timestamp,
                    best_bid AS start_best_bid,
                    best_ask AS start_best_ask
                FROM (
                    SELECT
                        o.order_pk,
                        q.quote_timestamp,
                        q.best_bid,
                        q.best_ask,
                        ABS(UNIX_MICROS(o.EffectiveStartTime) - UNIX_MICROS(q.quote_timestamp)) AS time_diff,
                        ROW_NUMBER() OVER (PARTITION BY o.order_pk ORDER BY ABS(UNIX_MICROS(o.EffectiveStartTime) - UNIX_MICROS(q.quote_timestamp))) as rn
                    FROM enriched_orders_with_time_buffers AS o
                    INNER JOIN quotes_with_pk AS q
                        ON o.ticker = q.ticker
                        AND q.quote_timestamp BETWEEN o.Start_lower AND o.EffectiveStartTime
                )
                WHERE rn = 1
            )
        """)

        # Perform lookups for end_quotes
        # PySpark join, withColumn (time_diff), Window (rn), filter, select equivalent in SQL CTE
        main_query_ctes.append(f"""
            , end_quotes AS (
                SELECT
                    order_pk,
                    best_bid AS end_best_bid,
                    best_ask AS end_best_ask
                FROM (
                    SELECT
                        o.order_pk,
                        q.best_bid,
                        q.best_ask,
                        ABS(UNIX_MICROS(o.EffectiveEndTime) - UNIX_MICROS(q.quote_timestamp)) AS time_diff,
                        ROW_NUMBER() OVER (PARTITION BY o.order_pk ORDER BY ABS(UNIX_MICROS(o.EffectiveEndTime) - UNIX_MICROS(q.quote_timestamp))) as rn
                    FROM enriched_orders_with_time_buffers AS o
                    INNER JOIN quotes_with_pk AS q
                        ON o.ticker = q.ticker
                        AND q.quote_timestamp BETWEEN o.End_lower AND o.EffectiveEndTime
                )
                WHERE rn = 1
            )
        """)

        # Perform lookups for end_1m_quotes
        # PySpark join, withColumn (time_diff), Window (rn), filter, select equivalent in SQL CTE
        main_query_ctes.append(f"""
            , end_1m_quotes AS (
                SELECT
                    order_pk,
                    best_bid AS end_plus1_best_bid,
                    best_ask AS end_plus1_best_ask
                FROM (
                    SELECT
                        o.order_pk,
                        q.best_bid,
                        q.best_ask,
                        ABS(UNIX_MICROS(o.End_plus1) - UNIX_MICROS(q.quote_timestamp)) AS time_diff,
                        ROW_NUMBER() OVER (PARTITION BY o.order_pk ORDER BY ABS(UNIX_MICROS(o.End_plus1) - UNIX_MICROS(q.quote_timestamp))) as rn
                    FROM enriched_orders_with_time_buffers AS o
                    INNER JOIN quotes_with_pk AS q
                        ON o.ticker = q.ticker
                        AND q.quote_timestamp BETWEEN o.End_plus1_lower AND o.End_plus1
                )
                WHERE rn = 1
            )
        """)

        # 7. Core VWAP and Financial Performance calculations
        # VWAP during order existence
        # PySpark join, groupBy, agg equivalent in SQL CTE
        main_query_ctes.append(f"""
            , vwap_df AS (
                SELECT
                    o.order_id,
                    o.client_id,
                    SUM(t.trade_size) AS market_interval_volume,
                    (SUM(t.trade_price * t.trade_size) / NULLIF(SUM(t.trade_size), 0)) AS market_interval_vwap
                FROM enriched_orders_with_time_buffers AS o
                INNER JOIN {trades_table} AS t
                    ON o.ticker = t.ticker
                    AND t.trade_timestamp >= o.EffectiveStartTime
                    AND t.trade_timestamp <= o.EffectiveEndTime
                GROUP BY 1, 2
            )
        """)

        # Join everything back
        # PySpark .join() and .select() for specific columns equivalent in SQL CTE
        main_query_ctes.append(f"""
            , final_joined_df AS (
                SELECT
                    o.* EXCEPT(Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower), -- Drop buffer columns
                    v.market_interval_volume,
                    v.market_interval_vwap,
                    oq.start_best_bid,
                    oq.start_best_ask,
                    oq.start_quote_timestamp,
                    eq.end_best_bid,
                    eq.end_best_ask,
                    eq1m.end_plus1_best_bid,
                    eq1m.end_plus1_best_ask
                FROM enriched_orders_with_time_buffers AS o
                LEFT JOIN vwap_df AS v USING (order_id, client_id)
                LEFT JOIN open_quotes AS oq USING (order_pk)
                LEFT JOIN end_quotes AS eq USING (order_pk)
                LEFT JOIN end_1m_quotes AS eq1m USING (order_pk)
            )
        """)

        # Calculate complex performance metrics (Slippage, Momentum, Profit/Loss vectors)
        # PySpark withColumn, F.when, F.col equivalent in SQL CTE using CASE statements
        main_query_ctes.append(f"""
            , final_metrics_df AS (
                SELECT
                    *,
                    (start_best_bid + start_best_ask) / 2 AS arrival_mid_price,
                    CASE
                        WHEN side = 'BUY' THEN ((fill_AverageExecutionPrice - market_interval_vwap) / NULLIF(market_interval_vwap, 0)) * 10000
                        WHEN side = 'SELL' THEN ((market_interval_vwap - fill_AverageExecutionPrice) / NULLIF(market_interval_vwap, 0)) * 10000
                        ELSE NULL
                    END AS slippage_from_vwap_bps,
                    CASE
                        WHEN side = 'BUY' THEN ( (start_best_bid + start_best_ask) / 2 - fill_AverageExecutionPrice ) * fill_TotalSharesExecuted
                        WHEN side = 'SELL' THEN ( fill_AverageExecutionPrice - (start_best_bid + start_best_ask) / 2 ) * fill_TotalSharesExecuted
                        ELSE NULL
                    END AS implementation_shortfall_pl,
                    CASE
                        WHEN side = 'BUY' THEN ( ((end_plus1_best_bid + end_plus1_best_ask) / 2) - ((end_best_bid + end_best_ask) / 2) ) * fill_TotalSharesExecuted
                        WHEN side = 'SELL' THEN ( ((end_best_bid + end_best_ask) / 2) - ((end_plus1_best_bid + end_plus1_best_ask) / 2) ) * fill_TotalSharesExecuted
                        ELSE NULL
                    END AS post_trade_1m_momentum,
                    CASE
                        WHEN side = 'BUY' THEN (requested_shares - fill_TotalSharesExecuted) * (fill_AverageExecutionPrice - closing_price)
                        WHEN side = 'SELL' THEN (requested_shares - fill_TotalSharesExecuted) * (closing_price - fill_AverageExecutionPrice)
                        ELSE NULL
                    END AS opportunity_cost_pl
                FROM final_joined_df
            )
        """)

        # Combine all CTEs and the final SELECT statement into a single query
        final_query = "\n".join(main_query_ctes) + "\nSELECT * FROM final_metrics_df"

        # PySpark final_df.write.parquet(...) is replaced by BigQuery `client.query` to a destination table
        destination_table_id = f"your_project.your_dataset.trading_analytics_run_{run_date.replace('-', '_')}"
        job_config = bigquery.QueryJobConfig(destination=destination_table_id, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE) # Overwrite mode

        query_job = client.query(final_query, job_config=job_config) # API request
        query_job.result() # Wait for the job to complete
        print("Performance analysis complete and results written to BigQuery table:", destination_table_id)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        sys.exit(1)

    # Replaced SparkSession initialization with BigQuery client initialization
    client = bigquery.Client()
    p = AlgorithmicTradingPerformance()
    p.execute_pipeline(client, sys.argv[1])
    # spark.stop() # No equivalent for BigQuery client, as it manages connections implicitly.

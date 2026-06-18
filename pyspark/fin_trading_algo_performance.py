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

    def execute_pipeline(self, client: bigquery.Client, run_date: str, project_id: str = "your_gcp_project_id", dataset_id: str = "your_bigquery_dataset_id"):

        # Define source tables as fully qualified BigQuery table references.
        # These replace the HDFS paths from the original PySpark code.
        all_order_events_source = f"`{project_id}.{dataset_id}.trading_events_base`"
        parent_orders_source = f"`{project_id}.{dataset_id}.parent_orders`"
        quotes_source = f"`{project_id}.{dataset_id}.level1_quotes`"
        trades_source = f"`{project_id}.{dataset_id}.market_trades`"

        # Convert local market open/close times to UTC string for embedding into SQL.
        utc_time_market_open_str = self.local_to_utc_time(9, 30, run_date)
        utc_time_market_close_str = self.local_to_utc_time(16, 00, run_date)

        # The entire data processing pipeline is constructed as a single BigQuery SQL query string
        # using Common Table Expressions (CTEs) to mimic PySpark's DataFrame transformations.
        final_query_sql = f"""
            WITH
            # 1. Load Core Datasets (replaced spark.read.parquet with direct table references)
            all_order_events_df AS (SELECT * FROM {all_order_events_source}),
            parent_orders_df AS (SELECT * FROM {parent_orders_source}),
            quotes_df_base AS (SELECT * FROM {quotes_source}),
            trades_df_base AS (SELECT * FROM {trades_source}),

            # 2. Separate Event Stream into Fills (F.col and .filter() translated to SQL WHERE)
            fills_df AS (
                SELECT * FROM all_order_events_df
                WHERE
                    (protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
                    OR (CAST(protocol_version AS BIGNUMERIC) >= 2 AND event_type = 'FILL')
            ),

            # Get the closing fill price per order (Window function and filter translated to SQL)
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
                    FROM fills_df
                )
                WHERE rn = 1
            ),

            # Aggregate fills per order (groupBy and agg functions translated to SQL)
            # Column prefixes applied directly in the SELECT statement.
            fills_agg_df AS (
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
                GROUP BY order_id, client_id, trade_date
            ),

            # 3. Execution Acknowledgements (filter, groupBy, agg translated to SQL)
            acks_df AS (
                SELECT * FROM all_order_events_df
                WHERE status IN ('NEW', 'REPLACED') AND event_type = 'ACK'
            ),
            acks_agg_df AS (
                SELECT
                    order_id,
                    client_id,
                    trade_date,
                    LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS ack_AckStartTime
                FROM acks_df
                GROUP BY order_id, client_id, trade_date
            ),

            # 4. Execution Terminations (filter, groupBy, agg translated to SQL)
            terminations_df AS (
                SELECT * FROM all_order_events_df
                WHERE status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')
            ),
            terminations_agg_df AS (
                SELECT
                    order_id,
                    client_id,
                    trade_date,
                    GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS term_ExecutionEndTime
                FROM terminations_df
                GROUP BY order_id, client_id, trade_date
            ),

            # 5. Bring it back to Parent Orders (Multiple joins translated to SQL JOIN clauses)
            # The .drop() of PySpark is achieved by selectively not including conflicting or redundant join keys.
            enriched_orders_initial AS (
                SELECT
                    p.*,
                    f.fill_FillStartTime, f.fill_FirstFillTime, f.fill_TotalSharesExecuted, f.fill_NumberOfFills, f.fill_AverageExecutionPrice, f.fill_TotalMarketValueExecuted,
                    a.ack_AckStartTime,
                    t.term_ExecutionEndTime,
                    c.closing_price, c.closing_qty
                FROM parent_orders_df AS p
                LEFT JOIN fills_agg_df AS f ON p.order_id = f.order_id AND p.client_id = f.client_id
                LEFT JOIN acks_agg_df AS a ON p.order_id = a.order_id AND p.client_id = a.client_id
                LEFT JOIN terminations_agg_df AS t ON p.order_id = t.order_id AND p.client_id = t.client_id
                LEFT JOIN close_fill_price_df AS c ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id
            ),

            # Establish effective operating window (withColumn with F.least/F.greatest/F.lit translated)
            enriched_orders_df AS (
                SELECT
                    *,
                    LEAST(GREATEST(ack_AckStartTime, TIMESTAMP '{utc_time_market_open_str}'), fill_FillStartTime) AS EffectiveStartTime,
                    LEAST(term_ExecutionEndTime, TIMESTAMP '{utc_time_market_close_str}') AS EffectiveEndTime
                FROM enriched_orders_initial
            ),

            # 6. Market Data Tick Metrics (complex time-based joins)
            # repartition and sortWithinPartitions are PySpark optimizations, removed for BigQuery.
            # F.monotonically_increasing_id() replaced with BigQuery's GENERATE_UUID() for unique row identifiers.
            # F.expr interval logic translated to TIMESTAMP_SUB/TIMESTAMP_ADD.
            enriched_orders_df_with_pk AS (
                SELECT
                    *,
                    GENERATE_UUID() AS order_pk,
                    TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,
                    TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,
                    TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,
                    TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,
                    TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,
                    TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower
                FROM enriched_orders_df
            ),
            quotes_df_with_pk AS (
                SELECT
                    *,
                    GENERATE_UUID() AS quote_pk
                FROM quotes_df_base
            ),

            # Perform lookups for nearest quotes (translated from find_nearest_quote closure logic)
            open_quotes AS (
                SELECT * EXCEPT(rn, time_diff) FROM ( -- Drop internal rank and time difference columns
                    SELECT
                        o.order_pk,
                        q.quote_timestamp AS start_quote_timestamp,
                        q.best_bid AS start_best_bid,
                        q.best_ask AS start_best_ask,
                        ABS(TIMESTAMP_DIFF(o.EffectiveStartTime, q.quote_timestamp, MILLISECOND)) AS time_diff,
                        ROW_NUMBER() OVER (PARTITION BY o.order_pk ORDER BY ABS(TIMESTAMP_DIFF(o.EffectiveStartTime, q.quote_timestamp, MILLISECOND))) AS rn
                    FROM enriched_orders_df_with_pk AS o
                    INNER JOIN quotes_df_with_pk AS q
                        ON o.ticker = q.ticker
                        AND q.quote_timestamp BETWEEN o.Start_lower AND o.EffectiveStartTime
                )
                WHERE rn = 1
            ),

            end_quotes AS (
                SELECT * EXCEPT(rn, time_diff) FROM (
                    SELECT
                        o.order_pk,
                        q.quote_timestamp AS end_quote_timestamp,
                        q.best_bid AS end_best_bid,
                        q.best_ask AS end_best_ask,
                        ABS(TIMESTAMP_DIFF(o.EffectiveEndTime, q.quote_timestamp, MILLISECOND)) AS time_diff,
                        ROW_NUMBER() OVER (PARTITION BY o.order_pk ORDER BY ABS(TIMESTAMP_DIFF(o.EffectiveEndTime, q.quote_timestamp, MILLISECOND))) AS rn
                    FROM enriched_orders_df_with_pk AS o
                    INNER JOIN quotes_df_with_pk AS q
                        ON o.ticker = q.ticker
                        AND q.quote_timestamp BETWEEN o.End_lower AND o.EffectiveEndTime
                )
                WHERE rn = 1
            ),

            end_1m_quotes AS (
                SELECT * EXCEPT(rn, time_diff) FROM (
                    SELECT
                        o.order_pk,
                        q.quote_timestamp AS end_plus1_quote_timestamp,
                        q.best_bid AS end_plus1_best_bid,
                        q.best_ask AS end_plus1_best_ask,
                        ABS(TIMESTAMP_DIFF(o.End_plus1, q.quote_timestamp, MILLISECOND)) AS time_diff,
                        ROW_NUMBER() OVER (PARTITION BY o.order_pk ORDER BY ABS(TIMESTAMP_DIFF(o.End_plus1, q.quote_timestamp, MILLISECOND))) AS rn
                    FROM enriched_orders_df_with_pk AS o
                    INNER JOIN quotes_df_with_pk AS q
                        ON o.ticker = q.ticker
                        AND q.quote_timestamp BETWEEN o.End_plus1_lower AND o.End_plus1
                )
                WHERE rn = 1
            ),

            # 7. Core VWAP and Financial Performance calculations
            # VWAP during order existence (join, groupBy, agg translated to SQL)
            vwap_df AS (
                SELECT
                    o.order_id,
                    o.client_id,
                    SUM(t.trade_size) AS market_interval_volume,
                    (SUM(t.trade_price * t.trade_size) / NULLIF(SUM(t.trade_size), 0)) AS market_interval_vwap
                FROM enriched_orders_df_with_pk AS o
                INNER JOIN {trades_source} AS t
                    ON o.ticker = t.ticker
                    AND t.trade_timestamp >= o.EffectiveStartTime
                    AND t.trade_timestamp <= o.EffectiveEndTime
                GROUP BY o.order_id, o.client_id
            ),

            # Join everything back (final join step from PySpark translated to SQL)
            # EXCEPT clause removes temporary internal columns from `enriched_orders_df_with_pk`.
            final_df_base AS (
                SELECT
                    e.* EXCEPT(Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower, order_pk),
                    v.market_interval_volume,
                    v.market_interval_vwap,
                    oq.start_best_bid,
                    oq.start_best_ask,
                    oq.start_quote_timestamp,
                    eq.end_best_bid,
                    eq.end_best_ask,
                    eq1m.end_plus1_best_bid,
                    eq1m.end_plus1_best_ask
                FROM enriched_orders_df_with_pk AS e
                LEFT JOIN vwap_df AS v ON e.order_id = v.order_id AND e.client_id = v.client_id
                LEFT JOIN open_quotes AS oq ON e.order_pk = oq.order_pk
                LEFT JOIN end_quotes AS eq ON e.order_pk = eq.order_pk
                LEFT JOIN end_1m_quotes AS eq1m ON e.order_pk = eq1m.order_pk
            )

            # Calculate complex performance metrics (F.when translated to SQL CASE statements)
            SELECT
                *,
                (start_best_bid + start_best_ask) / 2 AS arrival_mid_price,
                CASE
                    WHEN side = 'BUY' THEN ((fill_AverageExecutionPrice - market_interval_vwap) / NULLIF(market_interval_vwap, 0)) * 10000
                    WHEN side = 'SELL' THEN ((market_interval_vwap - fill_AverageExecutionPrice) / NULLIF(market_interval_vwap, 0)) * 10000
                    ELSE NULL
                END AS slippage_from_vwap_bps,
                CASE
                    WHEN side = 'BUY' THEN (arrival_mid_price - fill_AverageExecutionPrice) * fill_TotalSharesExecuted
                    WHEN side = 'SELL' THEN (fill_AverageExecutionPrice - arrival_mid_price) * fill_TotalSharesExecuted
                    ELSE NULL
                END AS implementation_shortfall_pl,
                CASE
                    WHEN side = 'BUY' THEN (((end_plus1_best_bid + end_plus1_best_ask) / 2) - ((end_best_bid + end_best_ask) / 2)) * fill_TotalSharesExecuted
                    WHEN side = 'SELL' THEN (((end_best_bid + end_best_ask) / 2) - ((end_plus1_best_bid + end_plus1_best_ask) / 2)) * fill_TotalSharesExecuted
                    ELSE NULL
                END AS post_trade_1m_momentum,
                CASE
                    WHEN side = 'BUY' THEN (requested_shares - fill_TotalSharesExecuted) * (fill_AverageExecutionPrice - closing_price)
                    WHEN side = 'SELL' THEN (requested_shares - fill_TotalSharesExecuted) * (closing_price - fill_AverageExecutionPrice)
                    ELSE NULL
                END AS opportunity_cost_pl
            FROM final_df_base
        """

        # Configure BigQuery job for destination table and write disposition.
        # Replaces df.write.parquet(..., mode="overwrite").
        destination_table_id = f"{project_id}.{dataset_id}.trading_analytics_run_date_{run_date.replace('-', '_')}"
        job_config = bigquery.QueryJobConfig(destination=destination_table_id, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

        # Execute the BigQuery SQL query.
        query_job = client.query(final_query_sql, job_config=job_config)
        query_job.result() # Wait for the query to complete
        print("Performance analysis complete.")

if __name__ == "__main__":
    from google.cloud import bigquery
    import sys
    if len(sys.argv) < 2:
        sys.exit(1)

    # Initialize BigQuery client. It typically auto-detects project_id from environment.
    client = bigquery.Client()
    project_id = client.project # Use client's inferred project ID
    dataset_id = "your_bigquery_dataset_id" # <<< IMPORTANT: Replace with your actual BigQuery dataset ID

    p = AlgorithmicTradingPerformance()
    # Execute the pipeline using the BigQuery client and inferred project/dataset.
    p.execute_pipeline(client, sys.argv[1], project_id, dataset_id)

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

    def execute_pipeline(self, client: bigquery.Client, project_id: str, dataset_id: str, run_date: str):
        # Major Change: HDFS paths are replaced with BigQuery table references.
        all_order_events_table = f"`{project_id}.{dataset_id}.trading_events_base`"
        parent_orders_table = f"`{project_id}.{dataset_id}.parent_orders`"
        quotes_table = f"`{project_id}.{dataset_id}.level1_quotes`"
        trades_table = f"`{project_id}.{dataset_id}.market_trades`"
        target_table_full_path = f"`{project_id}.{dataset_id}.trading_analytics`"

        # UTC market open and close times based on run_date
        utc_time_market_open = self.local_to_utc_time(9, 30, run_date)
        utc_time_market_close = self.local_to_utc_time(16, 00, run_date)

        # Major Change: PySpark DataFrame operations (loading, filtering, aggregations, joins,
        # window functions, and final calculations) are converted into a single BigQuery SQL query
        # using Common Table Expressions (CTEs) for modularity.
        full_bigquery_sql = f"""
            CREATE OR REPLACE TABLE {target_table_full_path}
            PARTITION BY analysis_run_date
            OPTIONS(
                description="Aggregated algorithmic trading performance analytics"
            ) AS
            WITH fills AS (
                SELECT
                    *
                FROM {all_order_events_table}
                WHERE
                    -- Anonymized protocol filtering conceptually representing status flags
                    (
                        (protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
                        -- Compatibility Note: PySpark 'protocol_version >= "V2"' performs string comparison.
                        -- For numeric version comparison (e.g., V1, V2, V10), parsing to INT64 is more robust.
                        OR (SAFE_CAST(SUBSTR(protocol_version, 2) AS INT64) >= 2 AND event_type = 'FILL')
                    )
            ),
            -- Get the closing fill price per order
            close_fill_price AS (
                SELECT
                    order_id AS close_order_id,
                    client_id AS close_client_id,
                    last_exec_price AS closing_price,
                    last_exec_qty AS closing_qty
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY order_id, client_id ORDER BY event_timestamp) AS rn
                    FROM fills
                )
                WHERE rn = 1
            ),
            -- Aggregate fills per order
            fills_agg AS (
                SELECT
                    order_id,
                    client_id,
                    trade_date,
                    LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS fill_FillStartTime,
                    MIN(event_timestamp) AS fill_FirstFillTime,
                    SUM(last_exec_qty) AS fill_TotalSharesExecuted,
                    COUNT(last_exec_qty) AS fill_NumberOfFills,
                    SAFE_DIVIDE(SUM(last_exec_qty * last_exec_price), SUM(last_exec_qty)) AS fill_AverageExecutionPrice,
                    SUM(last_exec_qty * last_exec_price) AS fill_TotalMarketValueExecuted
                FROM fills
                GROUP BY order_id, client_id, trade_date
            ),
            -- Execution Acknowledgements
            acks AS (
                SELECT
                    *
                FROM {all_order_events_table}
                WHERE status IN ('NEW', 'REPLACED') AND event_type = 'ACK'
            ),
            acks_agg AS (
                SELECT
                    order_id,
                    client_id,
                    trade_date,
                    LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS ack_AckStartTime
                FROM acks
                GROUP BY order_id, client_id, trade_date
            ),
            -- Execution Terminations
            terminations AS (
                SELECT
                    *
                FROM {all_order_events_table}
                WHERE status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')
            ),
            terminations_agg AS (
                SELECT
                    order_id,
                    client_id,
                    trade_date,
                    GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS term_ExecutionEndTime
                FROM terminations
                GROUP BY order_id, client_id, trade_date
            ),
            -- Bring it back to Parent Orders
            enriched_orders_initial AS (
                SELECT
                    p.*,
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
                LEFT JOIN fills_agg AS f ON p.order_id = f.order_id AND p.client_id = f.client_id
                LEFT JOIN acks_agg AS a ON p.order_id = a.order_id AND p.client_id = a.client_id
                LEFT JOIN terminations_agg AS t ON p.order_id = t.order_id AND p.client_id = t.client_id
                LEFT JOIN close_fill_price AS c ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id
            ),
            -- Establish effective operating window
            enriched_orders_with_effective_times AS (
                SELECT
                    *,
                    LEAST(
                        GREATEST(ack_AckStartTime, TIMESTAMP('{utc_time_market_open}')),
                        fill_FillStartTime
                    ) AS EffectiveStartTime,
                    LEAST(
                        term_ExecutionEndTime,
                        TIMESTAMP('{utc_time_market_close}')
                    ) AS EffectiveEndTime,
                    -- Major Change: F.monotonically_increasing_id() is replaced by ROW_NUMBER()
                    -- for unique row identification within the query context for subsequent joins.
                    ROW_NUMBER() OVER (ORDER BY order_id, client_id, trade_date, EffectiveStartTime) AS order_pk
                FROM enriched_orders_initial
            ),
            -- Market Data Tick Metrics (complex time-based joins)
            quotes_base AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (ORDER BY quote_timestamp, ticker) AS quote_pk -- Unique ID for quote lookups
                FROM {quotes_table}
            ),
            -- Build Window buffers for Quote lookups
            orders_with_time_buffers AS (
                SELECT
                    *,
                    -- Major Change: F.expr(f"EffectiveStartTime - interval 10 minutes") uses BigQuery TIMESTAMP_SUB.
                    TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,
                    TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,
                    TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,
                    TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,
                    TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,
                    TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower
                FROM enriched_orders_with_effective_times
            ),
            -- Perform nearest quote lookups (equivalent to PySpark find_nearest_quote closure)
            open_quotes_lookup AS (
                SELECT
                    t1.order_pk,
                    t2.best_bid AS start_best_bid,
                    t2.best_ask AS start_best_ask,
                    t2.quote_timestamp AS start_quote_timestamp
                FROM orders_with_time_buffers AS t1
                LEFT JOIN quotes_base AS t2
                    ON t1.ticker = t2.ticker
                    AND t2.quote_timestamp BETWEEN t1.Start_lower AND t1.EffectiveStartTime
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY t1.order_pk
                    ORDER BY ABS(TIMESTAMP_DIFF(t1.EffectiveStartTime, t2.quote_timestamp, MICROSECOND)) ASC
                ) = 1
            ),
            end_quotes_lookup AS (
                SELECT
                    t1.order_pk,
                    t2.best_bid AS end_best_bid,
                    t2.best_ask AS end_best_ask
                FROM orders_with_time_buffers AS t1
                LEFT JOIN quotes_base AS t2
                    ON t1.ticker = t2.ticker
                    AND t2.quote_timestamp BETWEEN t1.End_lower AND t1.EffectiveEndTime
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY t1.order_pk
                    ORDER BY ABS(TIMESTAMP_DIFF(t1.EffectiveEndTime, t2.quote_timestamp, MICROSECOND)) ASC
                ) = 1
            ),
            end_1m_quotes_lookup AS (
                SELECT
                    t1.order_pk,
                    t2.best_bid AS end_plus1_best_bid,
                    t2.best_ask AS end_plus1_best_ask
                FROM orders_with_time_buffers AS t1
                LEFT JOIN quotes_base AS t2
                    ON t1.ticker = t2.ticker
                    AND t2.quote_timestamp BETWEEN t1.End_plus1_lower AND t1.End_plus1
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY t1.order_pk
                    ORDER BY ABS(TIMESTAMP_DIFF(t1.End_plus1, t2.quote_timestamp, MICROSECOND)) ASC
                ) = 1
            ),
            -- Core VWAP calculations
            vwap_calc AS (
                SELECT
                    t1.order_id,
                    t1.client_id,
                    SUM(t2.trade_size) AS market_interval_volume,
                    SAFE_DIVIDE(SUM(t2.trade_price * t2.trade_size), SUM(t2.trade_size)) AS market_interval_vwap
                FROM orders_with_time_buffers AS t1
                INNER JOIN {trades_table} AS t2
                    ON t1.ticker = t2.ticker
                    AND t2.trade_timestamp >= t1.EffectiveStartTime
                    AND t2.trade_timestamp <= t1.EffectiveEndTime
                GROUP BY t1.order_id, t1.client_id
            )
            -- Join everything back and calculate final metrics
            SELECT
                t1.* EXCEPT (Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower), -- Drop PySpark-specific buffer columns
                COALESCE(vwap.market_interval_volume, 0) AS market_interval_volume, -- COALESCE handles potential NULL from LEFT JOIN
                vwap.market_interval_vwap,
                oq.start_best_bid,
                oq.start_best_ask,
                oq.start_quote_timestamp,
                eq.end_best_bid,
                eq.end_best_ask,
                eq_1m.end_plus1_best_bid,
                eq_1m.end_plus1_best_ask,
                SAFE_DIVIDE((oq.start_best_bid + oq.start_best_ask), 2) AS arrival_mid_price,
                -- Slippage from VWAP
                CASE
                    WHEN t1.side = 'BUY' THEN SAFE_DIVIDE((t1.fill_AverageExecutionPrice - vwap.market_interval_vwap), vwap.market_interval_vwap) * 10000
                    WHEN t1.side = 'SELL' THEN SAFE_DIVIDE((vwap.market_interval_vwap - t1.fill_AverageExecutionPrice), vwap.market_interval_vwap) * 10000
                    ELSE NULL
                END AS slippage_from_vwap_bps,
                -- Slippage from Arrival Mid (Implementation Shortfall)
                CASE
                    WHEN t1.side = 'BUY' THEN ( SAFE_DIVIDE((oq.start_best_bid + oq.start_best_ask), 2) - t1.fill_AverageExecutionPrice) * t1.fill_TotalSharesExecuted
                    WHEN t1.side = 'SELL' THEN ( t1.fill_AverageExecutionPrice - SAFE_DIVIDE((oq.start_best_bid + oq.start_best_ask), 2)) * t1.fill_TotalSharesExecuted
                    ELSE NULL
                END AS implementation_shortfall_pl,
                -- Momentum calculations post-trade
                CASE
                    WHEN t1.side = 'BUY' THEN (SAFE_DIVIDE((eq_1m.end_plus1_best_bid + eq_1m.end_plus1_best_ask), 2) - SAFE_DIVIDE((eq.end_best_bid + eq.end_best_ask), 2)) * t1.fill_TotalSharesExecuted
                    WHEN t1.side = 'SELL' THEN (SAFE_DIVIDE((eq.end_best_bid + eq.end_best_ask), 2) - SAFE_DIVIDE((eq_1m.end_plus1_best_bid + eq_1m.end_plus1_best_ask), 2)) * t1.fill_TotalSharesExecuted
                    ELSE NULL
                END AS post_trade_1m_momentum,
                -- Opportunity Cost
                CASE
                    WHEN t1.side = 'BUY' THEN (t1.requested_shares - t1.fill_TotalSharesExecuted) * (t1.fill_AverageExecutionPrice - t1.closing_price)
                    WHEN t1.side = 'SELL' THEN (t1.requested_shares - t1.fill_TotalSharesExecuted) * (t1.closing_price - t1.fill_AverageExecutionPrice)
                    ELSE NULL
                END AS opportunity_cost_pl,
                CAST('{run_date}' AS DATE) AS analysis_run_date
            FROM orders_with_time_buffers AS t1
            LEFT JOIN vwap_calc AS vwap ON t1.order_id = vwap.order_id AND t1.client_id = vwap.client_id
            LEFT JOIN open_quotes_lookup AS oq ON t1.order_pk = oq.order_pk
            LEFT JOIN end_quotes_lookup AS eq ON t1.order_pk = eq.order_pk
            LEFT JOIN end_1m_quotes_lookup AS eq_1m ON t1.order_pk = eq_1m.order_pk
        """
        print(f"Executing BigQuery analysis for run_date: {run_date}...")
        query_job = client.query(full_bigquery_sql)
        query_job.result()  # Waits for the job to complete.
        print("Performance analysis complete.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)

    # Major Change: PySpark SparkSession initialization is replaced by BigQuery client setup.
    # Placeholder values for BigQuery project and dataset IDs.
    bq_project_id = "your_gcp_project_id" # Replace with your GCP Project ID
    bq_dataset_id = "your_bigquery_dataset_id" # Replace with your BigQuery Dataset ID

    bq_client = bigquery.Client(project=bq_project_id)
    p = AlgorithmicTradingPerformance()
    # Pass the BigQuery client and project/dataset IDs.
    p.execute_pipeline(bq_client, bq_project_id, bq_dataset_id, sys.argv[1])
    # BigQuery client does not require an explicit `stop()` call like SparkSession.

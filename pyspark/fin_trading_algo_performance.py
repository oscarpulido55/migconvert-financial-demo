import datetime
import pytz
import sys
from google.cloud import bigquery
from google.cloud.bigquery import Client

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

    def execute_pipeline(self, bigquery_client: Client, run_date: str):
        
        project_id = "your-gcp-project-id" # Placeholder: Replace with your actual GCP Project ID
        dataset_id = "your_dataset_id"     # Placeholder: Replace with your actual BigQuery Dataset ID

        # Map HDFS paths to BigQuery tables
        trading_events_base_table = f"`{project_id}.{dataset_id}.trading_events_base`"
        parent_orders_table = f"`{project_id}.{dataset_id}.parent_orders`"
        level1_quotes_table = f"`{project_id}.{dataset_id}.level1_quotes`"
        market_trades_table = f"`{project_id}.{dataset_id}.market_trades`"

        # Calculate UTC market open/close times using the Python method
        utc_time_market_open = self.local_to_utc_time(9, 30, run_date)
        utc_time_market_close = self.local_to_utc_time(16, 00, run_date)

        # The PySpark DataFrame operations are translated into a single BigQuery SQL query using CTEs.
        # This approach ensures functional equivalence and leverages BigQuery's SQL query engine.
        full_sql_query = f"""
        WITH all_order_events_data AS (
          SELECT * FROM {trading_events_base_table}
        ),
        parent_orders_data AS (
          SELECT * FROM {parent_orders_table}
        ),
        quotes_data AS (
          SELECT * FROM {level1_quotes_table}
        ),
        trades_data AS (
          SELECT * FROM {market_trades_table}
        ),
        
        -- 2. Separate Event Stream into Fills
        -- Anonymized protocol filtering conceptually representing status flags
        fills AS (
          SELECT *
          FROM all_order_events_data
          WHERE
            ((protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
            OR (CAST(SUBSTR(protocol_version, 2) AS INT64) >= 2 AND event_type = 'FILL'))
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
            
        -- 3. Execution Acknowledgements
        acks AS (
          SELECT *
          FROM all_order_events_data
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
            
        -- 4. Execution Terminations
        terminations AS (
          SELECT *
          FROM all_order_events_data
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
        
        -- 5. Bring it back to Parent Orders
        initial_enriched_orders AS (
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
          FROM parent_orders_data AS p
          LEFT JOIN fills_agg AS f
            ON p.order_id = f.order_id AND p.client_id = f.client_id
          LEFT JOIN acks_agg AS a
            ON p.order_id = a.order_id AND p.client_id = a.client_id
          LEFT JOIN terminations_agg AS t
            ON p.order_id = t.order_id AND p.client_id = t.client_id
          LEFT JOIN close_fill_price AS c
            ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id
        ),

        -- Establish effective operating window
        enriched_orders_with_effective_times AS (
          SELECT
            *,
            LEAST(GREATEST(ack_AckStartTime, TIMESTAMP '{utc_time_market_open}'), fill_FillStartTime) AS EffectiveStartTime,
            LEAST(term_ExecutionEndTime, TIMESTAMP '{utc_time_market_close}') AS EffectiveEndTime,
            FARM_FINGERPRINT(TO_JSON_STRING(t)) AS order_pk -- Unique row identifier for later joins, replacing monotonically_increasing_id
          FROM initial_enriched_orders AS t
        ),

        -- 6. Market Data Tick Metrics (complex time-based joins)
        -- `repartition` and `sortWithinPartitions` are Spark-specific optimizations and are not translated to BigQuery SQL client code.

        -- Build Window buffers for Quote lookups
        enriched_orders_with_time_buffers AS (
          SELECT
            *,
            TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,
            TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,
            TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,
            TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,
            TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,
            TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower
          FROM enriched_orders_with_effective_times
        ),

        -- Perform lookups: open_quotes (equivalent to `find_nearest_quote` call)
        open_quotes AS (
          SELECT
            t1.order_pk,
            t2.quote_timestamp AS start_quote_timestamp,
            t2.best_bid AS start_best_bid,
            t2.best_ask AS start_best_ask
          FROM (
            SELECT
              t1_inner.*,
              ABS(TIMESTAMP_DIFF(EffectiveStartTime, quotes_data.quote_timestamp, MICROSECOND)) AS time_diff,
              ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY ABS(TIMESTAMP_DIFF(EffectiveStartTime, quotes_data.quote_timestamp, MICROSECOND))) AS rn
            FROM enriched_orders_with_time_buffers AS t1_inner
            INNER JOIN quotes_data
              ON t1_inner.ticker = quotes_data.ticker
             AND quotes_data.quote_timestamp BETWEEN t1_inner.Start_lower AND t1_inner.EffectiveStartTime
          )
          WHERE rn = 1
        ),

        -- Perform lookups: end_quotes (equivalent to `find_nearest_quote` call)
        end_quotes AS (
          SELECT
            t1.order_pk,
            t2.best_bid AS end_best_bid,
            t2.best_ask AS end_best_ask
          FROM (
            SELECT
              t1_inner.*,
              ABS(TIMESTAMP_DIFF(EffectiveEndTime, quotes_data.quote_timestamp, MICROSECOND)) AS time_diff,
              ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY ABS(TIMESTAMP_DIFF(EffectiveEndTime, quotes_data.quote_timestamp, MICROSECOND))) AS rn
            FROM enriched_orders_with_time_buffers AS t1_inner
            INNER JOIN quotes_data
              ON t1_inner.ticker = quotes_data.ticker
             AND quotes_data.quote_timestamp BETWEEN t1_inner.End_lower AND t1_inner.EffectiveEndTime
          )
          WHERE rn = 1
        ),
        
        -- Perform lookups: end_1m_quotes (equivalent to `find_nearest_quote` call)
        end_1m_quotes AS (
          SELECT
            t1.order_pk,
            t2.best_bid AS end_plus1_best_bid,
            t2.best_ask AS end_plus1_best_ask
          FROM (
            SELECT
              t1_inner.*,
              ABS(TIMESTAMP_DIFF(End_plus1, quotes_data.quote_timestamp, MICROSECOND)) AS time_diff,
              ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY ABS(TIMESTAMP_DIFF(End_plus1, quotes_data.quote_timestamp, MICROSECOND))) AS rn
            FROM enriched_orders_with_time_buffers AS t1_inner
            INNER JOIN quotes_data
              ON t1_inner.ticker = quotes_data.ticker
             AND quotes_data.quote_timestamp BETWEEN t1_inner.End_plus1_lower AND t1_inner.End_plus1
          )
          WHERE rn = 1
        ),
        
        -- 7. Core VWAP and Financial Performance calculations
        -- VWAP during order existence
        vwap_agg AS (
          SELECT
            eo.order_id,
            eo.client_id,
            SUM(td.trade_size) AS market_interval_volume,
            SAFE_DIVIDE(SUM(td.trade_price * td.trade_size), SUM(td.trade_size)) AS market_interval_vwap
          FROM enriched_orders_with_time_buffers AS eo
          INNER JOIN trades_data AS td
            ON eo.ticker = td.ticker
           AND td.trade_timestamp >= eo.EffectiveStartTime
           AND td.trade_timestamp <= eo.EffectiveEndTime
          GROUP BY eo.order_id, eo.client_id
        ),

        -- Join everything back
        intermediate_final_df AS (
          SELECT
            eo.* EXCEPT(Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower), -- Drop temporary buffer columns
            va.market_interval_volume,
            va.market_interval_vwap,
            oq.start_best_bid,
            oq.start_best_ask,
            oq.start_quote_timestamp,
            eq.end_best_bid,
            eq.end_best_ask,
            eq1m.end_plus1_best_bid,
            eq1m.end_plus1_best_ask
          FROM enriched_orders_with_time_buffers AS eo
          LEFT JOIN vwap_agg AS va
            ON eo.order_id = va.order_id AND eo.client_id = va.client_id
          LEFT JOIN open_quotes AS oq
            ON eo.order_pk = oq.order_pk
          LEFT JOIN end_quotes AS eq
            ON eo.order_pk = eq.order_pk
          LEFT JOIN end_1m_quotes AS eq1m
            ON eo.order_pk = eq1m.order_pk
        ),

        -- Calculate complex performance metrics (Slippage, Momentum, Profit/Loss vectors)
        final_calculated_metrics AS (
          SELECT
            *,
            SAFE_DIVIDE((start_best_bid + start_best_ask), 2) AS arrival_mid_price,
            CASE
              WHEN side = 'BUY' THEN SAFE_DIVIDE((fill_AverageExecutionPrice - market_interval_vwap), market_interval_vwap) * 10000
              WHEN side = 'SELL' THEN SAFE_DIVIDE((market_interval_vwap - fill_AverageExecutionPrice), market_interval_vwap) * 10000
              ELSE NULL
            END AS slippage_from_vwap_bps,
            CASE
              WHEN side = 'BUY' THEN (SAFE_DIVIDE((start_best_bid + start_best_ask), 2) - fill_AverageExecutionPrice) * fill_TotalSharesExecuted
              WHEN side = 'SELL' THEN (fill_AverageExecutionPrice - SAFE_DIVIDE((start_best_bid + start_best_ask), 2)) * fill_TotalSharesExecuted
              ELSE NULL
            END AS implementation_shortfall_pl,
            CASE
              WHEN side = 'BUY' THEN (SAFE_DIVIDE((end_plus1_best_bid + end_plus1_best_ask), 2) - SAFE_DIVIDE((end_best_bid + end_best_ask), 2)) * fill_TotalSharesExecuted
              WHEN side = 'SELL' THEN (SAFE_DIVIDE((end_best_bid + end_best_ask), 2) - SAFE_DIVIDE((end_plus1_best_bid + end_plus1_best_ask), 2)) * fill_TotalSharesExecuted
              ELSE NULL
            END AS post_trade_1m_momentum,
            CASE
              WHEN side = 'BUY' THEN (requested_shares - fill_TotalSharesExecuted) * (fill_AverageExecutionPrice - closing_price)
              WHEN side = 'SELL' THEN (requested_shares - fill_TotalSharesExecuted) * (closing_price - fill_AverageExecutionPrice)
              ELSE NULL
            END AS opportunity_cost_pl
          FROM intermediate_final_df
        )
        
        SELECT * FROM final_calculated_metrics
        """
        
        # Write the final result to a BigQuery table, replacing Spark's HDFS write.
        output_table_id = f"{project_id}.{dataset_id}.trading_analytics_run_date_{run_date.replace('-', '_')}"
        job_config = bigquery.QueryJobConfig(destination=output_table_id, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        
        query_job = bigquery_client.query(full_sql_query, job_config=job_config)
        query_job.result() # Wait for the job to complete
        print("Performance analysis complete.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        sys.exit(1)
    
    # Initialize BigQuery client instead of SparkSession
    bigquery_client = bigquery.Client()
    p = AlgorithmicTradingPerformance()
    p.execute_pipeline(bigquery_client, sys.argv[1])
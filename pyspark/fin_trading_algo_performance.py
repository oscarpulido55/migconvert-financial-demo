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

    def execute_pipeline(self, bq_client: bigquery.Client, run_date: str): # Converted SparkSession to BigQuery Client
        
        # Internal helper variables for constructing BigQuery SQL with CTEs
        _cte_definitions = []
        _current_cte_index = 0
        _BQ_PROJECT = "your-gcp-project-id"  # Placeholder: Replace with your actual GCP Project ID
        _BQ_DATASET = "your_bigquery_dataset"      # Placeholder: Replace with your actual BigQuery Dataset ID

        def _get_bq_table_id(table_name):
            return f"`{_BQ_PROJECT}.{_BQ_DATASET}.{table_name}`"

        def _add_cte_to_builder(sql_body, base_name="cte"):
            nonlocal _current_cte_index
            _current_cte_index += 1
            cte_name = f"{base_name}_{_current_cte_index}"
            _cte_definitions.append(f"{cte_name} AS (\n{sql_body}\n)")
            return cte_name
        
        # 1. Load Core Datasets
        # Instead of reading parquet files into a Spark DataFrame, we now refer to BigQuery tables.
        # These variables will hold the names of CTEs or BigQuery table IDs that represent the data at each stage.
        all_order_events_df = _get_bq_table_id("trading_events_base")
        parent_orders_df = _get_bq_table_id("parent_orders")

        # 2. Separate Event Stream into Fills
        # Anonymized protocol filtering conceptually representing status flags
        fills_df = _add_cte_to_builder(f"""
            SELECT *
            FROM {all_order_events_df}
            WHERE
                (
                    (protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
                    OR (protocol_version >= 'V2' AND event_type = 'FILL')
                )
        """, "fills")

        # Get the closing fill price per order
        close_fill_price_df = _add_cte_to_builder(f"""
            SELECT
                order_id AS close_order_id,
                client_id AS close_client_id,
                last_exec_price AS closing_price,
                last_exec_qty AS closing_qty
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY order_id, client_id ORDER BY event_timestamp) as rn
                FROM {fills_df}
            )
            WHERE rn = 1
        """, "close_fill_price")

        # Aggregate fills per order
        fills_agg_df = _add_cte_to_builder(f"""
            SELECT
                order_id,
                client_id,
                trade_date,
                LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS FillStartTime,
                MIN(event_timestamp) AS FirstFillTime,
                SUM(CAST(last_exec_qty AS BIGNUMERIC)) AS TotalSharesExecuted,
                COUNT(last_exec_qty) AS NumberOfFills,
                (SUM(CAST(last_exec_qty AS BIGNUMERIC) * CAST(last_exec_price AS BIGNUMERIC)) / SUM(CAST(last_exec_qty AS BIGNUMERIC))) AS AverageExecutionPrice,
                SUM(CAST(last_exec_qty AS BIGNUMERIC) * CAST(last_exec_price AS BIGNUMERIC)) AS TotalMarketValueExecuted
            FROM {fills_df}
            GROUP BY order_id, client_id, trade_date
        """, "fills_agg")
            
        # Prefix columns for joins - this operation is translated by adding a new CTE with aliased columns
        fills_agg_source_cols = [
            "FillStartTime", "FirstFillTime", "TotalSharesExecuted", "NumberOfFills", 
            "AverageExecutionPrice", "TotalMarketValueExecuted"
        ]
        aliased_fills_cols = ", ".join([f"{col_name} AS fill_{col_name}" for col_name in fills_agg_source_cols])

        fills_agg_df = _add_cte_to_builder(f"""
            SELECT
                order_id,
                client_id,
                trade_date,
                {aliased_fills_cols}
            FROM {fills_agg_df}
        """, "fills_agg_prefixed")

        # 3. Execution Acknowledgements
        acks_df = _add_cte_to_builder(f"""
            SELECT *
            FROM {all_order_events_df}
            WHERE status IN ('NEW', 'REPLACED') AND event_type = 'ACK'
        """, "acks")

        acks_agg_df = _add_cte_to_builder(f"""
            SELECT
                order_id,
                client_id,
                trade_date,
                LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS AckStartTime
            FROM {acks_df}
            GROUP BY order_id, client_id, trade_date
        """, "acks_agg")
            
        # Prefix columns for joins
        acks_agg_source_cols = ["AckStartTime"]
        aliased_acks_cols = ", ".join([f"{col_name} AS ack_{col_name}" for col_name in acks_agg_source_cols])

        acks_agg_df = _add_cte_to_builder(f"""
            SELECT
                order_id,
                client_id,
                trade_date,
                {aliased_acks_cols}
            FROM {acks_agg_df}
        """, "acks_agg_prefixed")

        # 4. Execution Terminations
        terminations_df = _add_cte_to_builder(f"""
            SELECT *
            FROM {all_order_events_df}
            WHERE status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')
        """, "terminations")

        terminations_agg_df = _add_cte_to_builder(f"""
            SELECT
                order_id,
                client_id,
                trade_date,
                GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS ExecutionEndTime
            FROM {terminations_df}
            GROUP BY order_id, client_id, trade_date
        """, "terminations_agg")
            
        # Prefix columns for joins
        terminations_agg_source_cols = ["ExecutionEndTime"]
        aliased_terms_cols = ", ".join([f"{col_name} AS term_{col_name}" for col_name in terminations_agg_source_cols])

        terminations_agg_df = _add_cte_to_builder(f"""
            SELECT
                order_id,
                client_id,
                trade_date,
                {aliased_terms_cols}
            FROM {terminations_agg_df}
        """, "terminations_agg_prefixed")

        # 5. Bring it back to Parent Orders
        enriched_orders_df = _add_cte_to_builder(f"""
            SELECT
                p.* EXCEPT (order_id, client_id), -- BigQuery specific syntax to drop columns from 'p' before re-selecting.
                p.order_id,
                p.client_id,
                f.* EXCEPT (order_id, client_id, trade_date),
                a.* EXCEPT (order_id, client_id, trade_date),
                t.* EXCEPT (order_id, client_id, trade_date),
                c.closing_price,
                c.closing_qty
            FROM {parent_orders_df} AS p
            LEFT JOIN {fills_agg_df} AS f ON p.order_id = f.order_id AND p.client_id = f.client_id
            LEFT JOIN {acks_agg_df} AS a ON p.order_id = a.order_id AND p.client_id = a.client_id
            LEFT JOIN {terminations_agg_df} AS t ON p.order_id = t.order_id AND p.client_id = t.client_id
            LEFT JOIN {close_fill_price_df} AS c ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id
        """, "enriched_orders_base")

        utc_time_market_open = self.local_to_utc_time(9, 30, run_date)
        utc_time_market_close = self.local_to_utc_time(16, 00, run_date)

        # Establish effective operating window
        enriched_orders_df = _add_cte_to_builder(f"""
            SELECT
                *,
                LEAST(GREATEST(ack_AckStartTime, CAST('{utc_time_market_open}' AS TIMESTAMP)), fill_FillStartTime) AS EffectiveStartTime,
                LEAST(term_ExecutionEndTime, CAST('{utc_time_market_close}' AS TIMESTAMP)) AS EffectiveEndTime
            FROM {enriched_orders_df}
        """, "enriched_orders_window")

        # 6. Market Data Tick Metrics (complex time-based joins)
        quotes_df = _get_bq_table_id("level1_quotes")

        # quotes_df = quotes_df.repartition("ticker").sortWithinPartitions("quote_timestamp")
        # enriched_orders_df = enriched_orders_df.repartition("ticker").sortWithinPartitions("EffectiveStartTime")
        # BigQuery handles physical data layout and optimization internally;
        # PySpark-specific repartition/sortWithinPartitions are removed.

        quotes_df_with_pk = _add_cte_to_builder(f"""
            SELECT *, GENERATE_UUID() AS quote_pk FROM {quotes_df}
        """, "quotes_with_pk")
        enriched_orders_df_with_pk = _add_cte_to_builder(f"""
            SELECT *, GENERATE_UUID() AS order_pk FROM {enriched_orders_df}
        """, "enriched_orders_with_pk") # GENERATE_UUID() in BigQuery is functionally equivalent to generate unique IDs.

        # Build Window buffers for Quote lookups
        enriched_orders_df_with_buffers = _add_cte_to_builder(f"""
            SELECT
                *,
                TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,
                TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,
                TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,
                TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,
                TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,
                TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower
            FROM {enriched_orders_df_with_pk}
        """, "enriched_orders_buffers")

        # Define a closure to reuse logic for looking up the nearest quote
        # This PySpark function's logic is converted into repeated BigQuery SQL CTEs directly.

        # Perform lookups: start_quotes
        start_quotes_cte = _add_cte_to_builder(f"""
            SELECT
                t1.order_pk,
                t2.quote_timestamp AS start_quote_timestamp,
                t2.best_bid AS start_best_bid,
                t2.best_ask AS start_best_ask
            FROM {enriched_orders_df_with_buffers} AS t1
            INNER JOIN {quotes_df_with_pk} AS t2
                ON t1.ticker = t2.ticker
                AND t2.quote_timestamp BETWEEN t1.Start_lower AND t1.EffectiveStartTime
            QUALIFY ROW_NUMBER() OVER (PARTITION BY t1.order_pk ORDER BY ABS(TIMESTAMP_DIFF(t1.EffectiveStartTime, t2.quote_timestamp, MILLISECOND))) = 1
        """, "start_quotes_lookup")

        # Perform lookups: end_quotes
        end_quotes_cte = _add_cte_to_builder(f"""
            SELECT
                t1.order_pk,
                t2.best_bid AS end_best_bid,
                t2.best_ask AS end_best_ask
            FROM {enriched_orders_df_with_buffers} AS t1
            INNER JOIN {quotes_df_with_pk} AS t2
                ON t1.ticker = t2.ticker
                AND t2.quote_timestamp BETWEEN t1.End_lower AND t1.EffectiveEndTime
            QUALIFY ROW_NUMBER() OVER (PARTITION BY t1.order_pk ORDER BY ABS(TIMESTAMP_DIFF(t1.EffectiveEndTime, t2.quote_timestamp, MILLISECOND))) = 1
        """, "end_quotes_lookup")

        # Perform lookups: end_1m_quotes
        end_1m_quotes_cte = _add_cte_to_builder(f"""
            SELECT
                t1.order_pk,
                t2.best_bid AS end_plus1_best_bid,
                t2.best_ask AS end_plus1_best_ask
            FROM {enriched_orders_df_with_buffers} AS t1
            INNER JOIN {quotes_df_with_pk} AS t2
                ON t1.ticker = t2.ticker
                AND t2.quote_timestamp BETWEEN t1.End_plus1_lower AND t1.End_plus1
            QUALIFY ROW_NUMBER() OVER (PARTITION BY t1.order_pk ORDER BY ABS(TIMESTAMP_DIFF(t1.End_plus1, t2.quote_timestamp, MILLISECOND))) = 1
        """, "end_1m_quotes_lookup")
        
        # 7. Core VWAP and Financial Performance calculations
        trades_df = _get_bq_table_id("market_trades")
        
        # VWAP during order existence
        vwap_df = _add_cte_to_builder(f"""
            SELECT
                t1.order_id,
                t1.client_id,
                SUM(CAST(t2.trade_size AS BIGNUMERIC)) AS market_interval_volume,
                (SUM(CAST(t2.trade_price AS BIGNUMERIC) * CAST(t2.trade_size AS BIGNUMERIC)) / SUM(CAST(t2.trade_size AS BIGNUMERIC))) AS market_interval_vwap
            FROM {enriched_orders_df_with_buffers} AS t1
            INNER JOIN {trades_df} AS t2
                ON t1.ticker = t2.ticker
                AND t2.trade_timestamp >= t1.EffectiveStartTime
                AND t2.trade_timestamp <= t1.EffectiveEndTime
            GROUP BY t1.order_id, t1.client_id
        """, "vwap")

        # Join everything back
        final_df_base = _add_cte_to_builder(f"""
            SELECT
                e.* EXCEPT (Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower),
                v.market_interval_volume,
                v.market_interval_vwap,
                s.start_best_bid,
                s.start_best_ask,
                s.start_quote_timestamp,
                nd.end_best_bid,
                nd.end_best_ask,
                np1.end_plus1_best_bid,
                np1.end_plus1_best_ask
            FROM {enriched_orders_df_with_buffers} AS e
            LEFT JOIN {vwap_df} AS v USING(order_id, client_id)
            LEFT JOIN {start_quotes_cte} AS s USING(order_pk)
            LEFT JOIN {end_quotes_cte} AS nd USING(order_pk)
            LEFT JOIN {end_1m_quotes_cte} AS np1 USING(order_pk)
        """, "final_df_joined_base")

        # Calculate complex performance metrics (Slippage, Momentum, Profit/Loss vectors)
        final_df_arrival_mid = _add_cte_to_builder(f"""
            SELECT
                *,
                (start_best_bid + start_best_ask) / 2 AS arrival_mid_price
            FROM {final_df_base}
        """, "final_df_arrival_mid")
        
        # Slippage from VWAP
        final_df_slippage_vwap = _add_cte_to_builder(f"""
            SELECT
                *,
                CASE
                    WHEN side = 'BUY' THEN ((CAST(fill_AverageExecutionPrice AS BIGNUMERIC) - CAST(market_interval_vwap AS BIGNUMERIC)) / CAST(market_interval_vwap AS BIGNUMERIC)) * 10000
                    WHEN side = 'SELL' THEN ((CAST(market_interval_vwap AS BIGNUMERIC) - CAST(fill_AverageExecutionPrice AS BIGNUMERIC)) / CAST(market_interval_vwap AS BIGNUMERIC)) * 10000
                    ELSE NULL
                END AS slippage_from_vwap_bps
            FROM {final_df_arrival_mid}
        """, "final_df_slippage_vwap")

        # Slippage from Arrival Mid (Implementation Shortfall)
        final_df_implementation_shortfall = _add_cte_to_builder(f"""
            SELECT
                *,
                CASE
                    WHEN side = 'BUY' THEN (CAST(arrival_mid_price AS BIGNUMERIC) - CAST(fill_AverageExecutionPrice AS BIGNUMERIC)) * CAST(fill_TotalSharesExecuted AS BIGNUMERIC)
                    WHEN side = 'SELL' THEN (CAST(fill_AverageExecutionPrice AS BIGNUMERIC) - CAST(arrival_mid_price AS BIGNUMERIC)) * CAST(fill_TotalSharesExecuted AS BIGNUMERIC)
                    ELSE NULL
                END AS implementation_shortfall_pl
            FROM {final_df_slippage_vwap}
        """, "final_df_implementation_shortfall")
        
        # Momentum calculations post-trade
        final_df_momentum = _add_cte_to_builder(f"""
            SELECT
                *,
                CASE
                    WHEN side = 'BUY' THEN (((CAST(end_plus1_best_bid AS BIGNUMERIC) + CAST(end_plus1_best_ask AS BIGNUMERIC)) / 2) - ((CAST(end_best_bid AS BIGNUMERIC) + CAST(end_best_ask AS BIGNUMERIC)) / 2)) * CAST(fill_TotalSharesExecuted AS BIGNUMERIC)
                    WHEN side = 'SELL' THEN (((CAST(end_best_bid AS BIGNUMERIC) + CAST(end_best_ask AS BIGNUMERIC)) / 2) - ((CAST(end_plus1_best_bid AS BIGNUMERIC) + CAST(end_plus1_best_ask AS BIGNUMERIC)) / 2)) * CAST(fill_TotalSharesExecuted AS BIGNUMERIC)
                    ELSE NULL
                END AS post_trade_1m_momentum
            FROM {final_df_implementation_shortfall}
        """, "final_df_momentum")

        final_df = _add_cte_to_builder(f"""
            SELECT
                *,
                CASE
                    WHEN side = 'BUY' THEN (CAST(requested_shares AS BIGNUMERIC) - CAST(fill_TotalSharesExecuted AS BIGNUMERIC)) * (CAST(fill_AverageExecutionPrice AS BIGNUMERIC) - CAST(closing_price AS BIGNUMERIC))
                    WHEN side = 'SELL' THEN (CAST(requested_shares AS BIGNUMERIC) - CAST(fill_TotalSharesExecuted AS BIGNUMERIC)) * (CAST(closing_price AS BIGNUMERIC) - CAST(fill_AverageExecutionPrice AS BIGNUMERIC))
                    ELSE NULL
                END AS opportunity_cost_pl
            FROM {final_df_momentum}
        """, "final_df_with_opportunity_cost")

        # Assemble the full BigQuery SQL query with all CTEs
        final_bq_query = (
            "WITH\n"
            + ",\n".join(_cte_definitions)
            + f"\nSELECT * FROM {final_df}" # Select from the final CTE
        )
        
        # Instead of writing to HDFS, write the results to a BigQuery table
        destination_table_id = _get_bq_table_id(f"trading_analytics_{run_date.replace('-', '_')}")

        job_config = bigquery.QueryJobConfig(
            destination=destination_table_id,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # PySpark's mode="overwrite"
        )

        query_job = bq_client.query(final_bq_query, job_config=job_config)
        query_job.result() # Waits for the BigQuery job to complete
        print("Performance analysis complete.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python script_name.py <run_date>")
        sys.exit(1)
    
    # Initialize BigQuery client
    # Ensure GOOGLE_APPLICATION_CREDENTIALS environment variable is set or other authentication is configured.
    bq_client = bigquery.Client()
    p = AlgorithmicTradingPerformance()
    p.execute_pipeline(bq_client, sys.argv[1])
    # No explicit client.stop() needed for BigQuery client

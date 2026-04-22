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
        project_id = client.project 
        dataset_id = "your_bigquery_dataset" 
        
        # 1. Load Core Datasets
        all_order_events_table = f"`{project_id}.{dataset_id}.trading_events_base`"
        parent_orders_table = f"`{project_id}.{dataset_id}.parent_orders`"

        # 2. Separate Event Stream into Fills
        fills_df_query = f"""
            SELECT *
            FROM {all_order_events_table}
            WHERE 
                (protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
                OR (SAFE_CAST(REPLACE(protocol_version, 'V', '') AS INT64) >= 2 AND event_type = 'FILL')
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE fills_temp AS {fills_df_query}").result()
        fills_temp_table = "fills_temp"

        # Get the closing fill price per order
        close_fill_price_df_query = f"""
            SELECT
                order_id AS close_order_id,
                client_id AS close_client_id,
                last_exec_price AS closing_price,
                last_exec_qty AS closing_qty
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY order_id, client_id ORDER BY event_timestamp) as rn
                FROM {fills_temp_table}
            )
            WHERE rn = 1
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE close_fill_price_temp AS {close_fill_price_df_query}").result()
        close_fill_price_temp_table = "close_fill_price_temp"

        # Aggregate fills per order
        fills_agg_df_query = f"""
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
            FROM {fills_temp_table}
            GROUP BY order_id, client_id, trade_date
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE fills_agg_temp AS {fills_agg_df_query}").result()
        fills_agg_temp_table = "fills_agg_temp"

        # Prefix columns logic is handled directly in the SQL for fills_agg_df above.

        # 3. Execution Acknowledgements
        acks_df_query = f"""
            SELECT *
            FROM {all_order_events_table}
            WHERE status IN ('NEW', 'REPLACED') AND event_type = 'ACK'
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE acks_temp AS {acks_df_query}").result()
        acks_temp_table = "acks_temp"

        acks_agg_df_query = f"""
            SELECT
                order_id,
                client_id,
                trade_date,
                LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS ack_AckStartTime
            FROM {acks_temp_table}
            GROUP BY order_id, client_id, trade_date
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE acks_agg_temp AS {acks_agg_df_query}").result()
        acks_agg_temp_table = "acks_agg_temp"

        # Prefix columns logic is handled directly in the SQL for acks_agg_df above.

        # 4. Execution Terminations
        terminations_df_query = f"""
            SELECT *
            FROM {all_order_events_table}
            WHERE status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE terminations_temp AS {terminations_df_query}").result()
        terminations_temp_table = "terminations_temp"

        terminations_agg_df_query = f"""
            SELECT
                order_id,
                client_id,
                trade_date,
                GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS term_ExecutionEndTime
            FROM {terminations_temp_table}
            GROUP BY order_id, client_id, trade_date
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE terminations_agg_temp AS {terminations_agg_df_query}").result()
        terminations_agg_temp_table = "terminations_agg_temp"

        # Prefix columns logic is handled directly in the SQL for terminations_agg_df above.

        # 5. Bring it back to Parent Orders
        enriched_orders_base_query = f"""
            SELECT
                p.* EXCEPT(order_id, client_id),
                p.order_id, p.client_id,
                f.* EXCEPT(order_id, client_id, trade_date),
                a.* EXCEPT(order_id, client_id, trade_date),
                t.* EXCEPT(order_id, client_id, trade_date),
                c.closing_price,
                c.closing_qty
            FROM {parent_orders_table} AS p
            LEFT JOIN {fills_agg_temp_table} AS f
                ON p.order_id = f.order_id AND p.client_id = f.client_id
            LEFT JOIN {acks_agg_temp_table} AS a
                ON p.order_id = a.order_id AND p.client_id = a.client_id
            LEFT JOIN {terminations_agg_temp_table} AS t
                ON p.order_id = t.order_id AND p.client_id = t.client_id
            LEFT JOIN {close_fill_price_temp_table} AS c
                ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE enriched_orders_base_temp AS {enriched_orders_base_query}").result()
        enriched_orders_base_temp_table = "enriched_orders_base_temp"

        utc_time_market_open = self.local_to_utc_time(9, 30, run_date)
        utc_time_market_close = self.local_to_utc_time(16, 00, run_date)

        # Establish effective operating window
        enriched_orders_df_query_with_effective_window = f"""
            SELECT
                *,
                LEAST(
                    GREATEST(
                        ack_AckStartTime,
                        TIMESTAMP('{utc_time_market_open}')
                    ),
                    fill_FillStartTime
                ) AS EffectiveStartTime,
                LEAST(
                    term_ExecutionEndTime,
                    TIMESTAMP('{utc_time_market_close}')
                ) AS EffectiveEndTime
            FROM {enriched_orders_base_temp_table}
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE enriched_orders_with_window_temp AS {enriched_orders_df_query_with_effective_window}").result()
        enriched_orders_with_window_temp_table = "enriched_orders_with_window_temp"

        # 6. Market Data Tick Metrics (complex time-based joins)
        quotes_table = f"`{project_id}.{dataset_id}.level1_quotes`"

        # Spark-specific repartition and sortWithinPartitions removed, BigQuery handles optimizations.

        # Add unique IDs using GENERATE_UUID() in BigQuery for later joins.
        quotes_df_with_pk_query = f"""
            SELECT *, GENERATE_UUID() AS quote_pk
            FROM {quotes_table}
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE quotes_with_pk_temp AS {quotes_df_with_pk_query}").result()
        quotes_with_pk_temp_table = "quotes_with_pk_temp"

        enriched_orders_df_with_pk_query = f"""
            SELECT *, GENERATE_UUID() AS order_pk
            FROM {enriched_orders_with_window_temp_table}
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE enriched_orders_with_pk_temp AS {enriched_orders_df_with_pk_query}").result()
        enriched_orders_with_pk_temp_table = "enriched_orders_with_pk_temp"

        # Build Window buffers for Quote lookups
        enriched_orders_df_with_buffers_query = f"""
            SELECT
                *,
                TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,
                TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,
                TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,
                TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,
                TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,
                TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower
            FROM {enriched_orders_with_pk_temp_table}
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE enriched_orders_with_buffers_temp AS {enriched_orders_df_with_buffers_query}").result()
        enriched_orders_with_buffers_temp_table = "enriched_orders_with_buffers_temp"

        # Define a Python helper function to generate the SQL for nearest quote lookups
        def _find_nearest_quote_sql(orders_table_name, quotes_table_name, target_time_col, lower_bound_col, prefix):
            return f"""
                SELECT
                    t1.order_pk,
                    t2.quote_timestamp AS {prefix}_quote_timestamp,
                    t2.best_bid AS {prefix}_best_bid,
                    t2.best_ask AS {prefix}_best_ask
                FROM (
                    SELECT
                        t1.order_pk,
                        t1.ticker,
                        t1.{target_time_col},
                        t2.quote_timestamp,
                        t2.best_bid,
                        t2.best_ask,
                        ROW_NUMBER() OVER (PARTITION BY t1.order_pk ORDER BY ABS(TIMESTAMP_DIFF(t1.{target_time_col}, t2.quote_timestamp, MILLISECOND))) AS rn
                    FROM {orders_table_name} AS t1
                    JOIN {quotes_table_name} AS t2
                        ON t1.ticker = t2.ticker
                        AND t2.quote_timestamp BETWEEN t1.{lower_bound_col} AND t1.{target_time_col}
                ) AS subquery
                WHERE rn = 1
            """

        # Perform lookups
        open_quotes_query = _find_nearest_quote_sql(enriched_orders_with_buffers_temp_table, quotes_with_pk_temp_table, "EffectiveStartTime", "Start_lower", "start")
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE open_quotes_temp AS {open_quotes_query}").result()
        open_quotes_temp_table = "open_quotes_temp"

        end_quotes_query = _find_nearest_quote_sql(enriched_orders_with_buffers_temp_table, quotes_with_pk_temp_table, "EffectiveEndTime", "End_lower", "end")
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE end_quotes_temp AS {end_quotes_query}").result()
        end_quotes_temp_table = "end_quotes_temp"

        end_1m_quotes_query = _find_nearest_quote_sql(enriched_orders_with_buffers_temp_table, quotes_with_pk_temp_table, "End_plus1", "End_plus1_lower", "end_plus1")
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE end_1m_quotes_temp AS {end_1m_quotes_query}").result()
        end_1m_quotes_temp_table = "end_1m_quotes_temp"
        
        # 7. Core VWAP and Financial Performance calculations
        trades_table = f"`{project_id}.{dataset_id}.market_trades`"
        
        # VWAP during order existence
        vwap_df_query = f"""
            SELECT
                t1.order_id,
                t1.client_id,
                SUM(t2.trade_size) AS market_interval_volume,
                SAFE_DIVIDE(SUM(t2.trade_price * t2.trade_size), SUM(t2.trade_size)) AS market_interval_vwap
            FROM {enriched_orders_with_buffers_temp_table} AS t1
            INNER JOIN {trades_table} AS t2
                ON t1.ticker = t2.ticker
                AND t2.trade_timestamp >= t1.EffectiveStartTime
                AND t2.trade_timestamp <= t1.EffectiveEndTime
            GROUP BY t1.order_id, t1.client_id
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE vwap_temp AS {vwap_df_query}").result()
        vwap_temp_table = "vwap_temp"

        # Join everything back
        final_df_base_join_query = f"""
            SELECT
                e.*,
                v.market_interval_volume,
                v.market_interval_vwap,
                oq.start_best_bid,
                oq.start_best_ask,
                oq.start_quote_timestamp,
                eq.end_best_bid,
                eq.end_best_ask,
                e1m.end_plus1_best_bid,
                e1m.end_plus1_best_ask
            FROM {enriched_orders_with_buffers_temp_table} AS e
            LEFT JOIN {vwap_temp_table} AS v
                ON e.order_id = v.order_id AND e.client_id = v.client_id
            LEFT JOIN {open_quotes_temp_table} AS oq
                ON e.order_pk = oq.order_pk
            LEFT JOIN {end_quotes_temp_table} AS eq
                ON e.order_pk = eq.order_pk
            LEFT JOIN {end_1m_quotes_temp_table} AS e1m
                ON e.order_pk = e1m.order_pk
        """
        client.query(f"CREATE OR REPLACE TEMPORARY TABLE final_df_base_temp AS {final_df_base_join_query}").result()
        final_df_base_temp_table = "final_df_base_temp"

        # Calculate complex performance metrics (Slippage, Momentum, Profit/Loss vectors)
        final_metrics_query = f"""
            SELECT
                *,
                (start_best_bid + start_best_ask) / 2 AS arrival_mid_price,
                
                CASE
                    WHEN side = 'BUY' THEN SAFE_DIVIDE((fill_AverageExecutionPrice - market_interval_vwap), market_interval_vwap) * 10000
                    WHEN side = 'SELL' THEN SAFE_DIVIDE((market_interval_vwap - fill_AverageExecutionPrice), market_interval_vwap) * 10000
                    ELSE NULL
                END AS slippage_from_vwap_bps,

                CASE
                    WHEN side = 'BUY' THEN ( (start_best_bid + start_best_ask) / 2 - fill_AverageExecutionPrice) * fill_TotalSharesExecuted
                    WHEN side = 'SELL' THEN (fill_AverageExecutionPrice - ( (start_best_bid + start_best_ask) / 2)) * fill_TotalSharesExecuted
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
            FROM {final_df_base_temp_table}
        """

        output_table_name = f"`{project_id}.{dataset_id}.trading_analytics_run_date_{run_date.replace('-', '_')}`"
        
        final_create_table_query = f"""
            CREATE OR REPLACE TABLE {output_table_name} AS
            {final_metrics_query}
        """
        client.query(final_create_table_query).result()
        print("Performance analysis complete.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        sys.exit(1)
    
    client = bigquery.Client()
    p = AlgorithmicTradingPerformance()
    p.execute_pipeline(client, sys.argv[1])
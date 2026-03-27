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
        
        utc_time_market_open = self.local_to_utc_time(9, 30, run_date)
        utc_time_market_close = self.local_to_utc_time(16, 00, run_date)

        sql_query = f"""
        WITH 
        all_order_events_base AS (
            SELECT * FROM `{project_id}.{dataset_id}.trading_events_base`
        ),
        parent_orders_base AS (
            SELECT * FROM `{project_id}.{dataset_id}.parent_orders`
        ),
        
        -- 2. Separate Event Stream into Fills
        fills_df AS (
            SELECT
                *
            FROM
                all_order_events_base
            WHERE
                (
                    (protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
                    OR
                    (protocol_version >= 'V2' AND event_type = 'FILL')
                )
        ),
        
        -- Get the closing fill price per order
        close_fill_price_df AS (
            SELECT
                order_id AS close_order_id, 
                client_id AS close_client_id, 
                last_exec_price AS closing_price, 
                last_exec_qty AS closing_qty
            FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY order_id, client_id ORDER BY event_timestamp) AS rn
                    FROM
                        fills_df
                )
            WHERE rn = 1
        ),
        
        -- Aggregate fills per order
        fills_agg_df AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS fill_FillStartTime,
                MIN(event_timestamp) AS fill_FirstFillTime,
                SUM(last_exec_qty) AS fill_TotalSharesExecuted,
                COUNT(last_exec_qty) AS fill_NumberOfFills,
                SUM(last_exec_qty * last_exec_price) / SUM(last_exec_qty) AS fill_AverageExecutionPrice,
                SUM(last_exec_qty * last_exec_price) AS fill_TotalMarketValueExecuted
            FROM
                fills_df
            GROUP BY
                order_id,
                client_id,
                trade_date
        ),
        
        -- 3. Execution Acknowledgements
        acks_df AS (
            SELECT *
            FROM all_order_events_base
            WHERE status IN ('NEW', 'REPLACED') AND event_type = 'ACK'
        ),
        acks_agg_df AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS ack_AckStartTime
            FROM
                acks_df
            GROUP BY
                order_id,
                client_id,
                trade_date
        ),
            
        -- 4. Execution Terminations
        terminations_df AS (
            SELECT *
            FROM all_order_events_base
            WHERE status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')
        ),
        terminations_agg_df AS (
            SELECT
                order_id,
                client_id,
                trade_date,
                GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS term_ExecutionEndTime
            FROM
                terminations_df
            GROUP BY
                order_id,
                client_id,
                trade_date
        ),
        
        -- 5. Bring it back to Parent Orders
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
            FROM
                parent_orders_base AS p
            LEFT JOIN
                fills_agg_df AS f ON p.order_id = f.order_id AND p.client_id = f.client_id
            LEFT JOIN
                acks_agg_df AS a ON p.order_id = a.order_id AND p.client_id = a.client_id
            LEFT JOIN
                terminations_agg_df AS t ON p.order_id = t.order_id AND p.client_id = t.client_id
            LEFT JOIN
                close_fill_price_df AS c ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id
        ),
        
        -- Establish effective operating window
        enriched_orders_with_effective_times AS (
            SELECT
                *,
                LEAST(
                    GREATEST(ack_AckStartTime, CAST('{utc_time_market_open}' AS TIMESTAMP)),
                    fill_FillStartTime
                ) AS EffectiveStartTime,
                LEAST(
                    term_ExecutionEndTime,
                    CAST('{utc_time_market_close}' AS TIMESTAMP)
                ) AS EffectiveEndTime
            FROM
                enriched_orders_initial
        ),

        -- 6. Market Data Tick Metrics (complex time-based joins)
        quotes_base AS (
            SELECT *, GENERATE_UUID() AS quote_pk_raw FROM `{project_id}.{dataset_id}.level1_quotes` -- Added _raw suffix to avoid column name clash
        ),
        trades_base AS (
            SELECT * FROM `{project_id}.{dataset_id}.market_trades`
        ),

        -- Add unique identifiers to orders and compute time buffers for joins
        enriched_orders_processed AS (
            SELECT
                *,
                GENERATE_UUID() AS order_pk, -- Replaced F.monotonically_increasing_id()
                TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,
                TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,
                TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,
                TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,
                TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,
                TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower
            FROM
                enriched_orders_with_effective_times
        ),

        -- open_quotes
        open_quotes_candidate AS (
            SELECT
                e.order_pk,
                q.quote_timestamp,
                q.best_bid,
                q.best_ask,
                ABS(TIMESTAMP_DIFF(e.EffectiveStartTime, q.quote_timestamp, MICROSECOND)) AS time_diff_microseconds
            FROM
                enriched_orders_processed AS e
            INNER JOIN
                quotes_base AS q
            ON
                e.ticker = q.ticker AND q.quote_timestamp BETWEEN e.Start_lower AND e.EffectiveStartTime
        ),
        open_quotes AS (
            SELECT
                order_pk,
                quote_timestamp AS start_quote_timestamp,
                best_bid AS start_best_bid,
                best_ask AS start_best_ask
            FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY time_diff_microseconds) AS rn
                    FROM
                        open_quotes_candidate
                )
            WHERE rn = 1
        ),
        
        -- end_quotes
        end_quotes_candidate AS (
            SELECT
                e.order_pk,
                q.quote_timestamp,
                q.best_bid,
                q.best_ask,
                ABS(TIMESTAMP_DIFF(e.EffectiveEndTime, q.quote_timestamp, MICROSECOND)) AS time_diff_microseconds
            FROM
                enriched_orders_processed AS e
            INNER JOIN
                quotes_base AS q
            ON
                e.ticker = q.ticker AND q.quote_timestamp BETWEEN e.End_lower AND e.EffectiveEndTime
        ),
        end_quotes AS (
            SELECT
                order_pk,
                quote_timestamp AS end_quote_timestamp,
                best_bid AS end_best_bid,
                best_ask AS end_best_ask
            FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY time_diff_microseconds) AS rn
                    FROM
                        end_quotes_candidate
                )
            WHERE rn = 1
        ),
        
        -- end_1m_quotes
        end_1m_quotes_candidate AS (
            SELECT
                e.order_pk,
                q.quote_timestamp,
                q.best_bid,
                q.best_ask,
                ABS(TIMESTAMP_DIFF(e.End_plus1, q.quote_timestamp, MICROSECOND)) AS time_diff_microseconds
            FROM
                enriched_orders_processed AS e
            INNER JOIN
                quotes_base AS q
            ON
                e.ticker = q.ticker AND q.quote_timestamp BETWEEN e.End_plus1_lower AND e.End_plus1
        ),
        end_1m_quotes AS (
            SELECT
                order_pk,
                quote_timestamp AS end_plus1_quote_timestamp,
                best_bid AS end_plus1_best_bid,
                best_ask AS end_plus1_best_ask
            FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY time_diff_microseconds) AS rn
                    FROM
                        end_1m_quotes_candidate
                )
            WHERE rn = 1
        ),

        -- 7. Core VWAP and Financial Performance calculations
        vwap_df AS (
            SELECT
                e.order_id,
                e.client_id,
                SUM(t.trade_size) AS market_interval_volume,
                SUM(t.trade_price * t.trade_size) / SUM(t.trade_size) AS market_interval_vwap
            FROM
                enriched_orders_processed AS e
            INNER JOIN
                trades_base AS t
            ON
                e.ticker = t.ticker AND t.trade_timestamp >= e.EffectiveStartTime AND t.trade_timestamp <= e.EffectiveEndTime
            GROUP BY
                e.order_id,
                e.client_id
        ),
        
        -- Join everything back for final calculations
        final_df_base AS (
            SELECT
                e.* EXCEPT (Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower),
                v.market_interval_volume,
                v.market_interval_vwap,
                oq.start_best_bid,
                oq.start_best_ask,
                oq.start_quote_timestamp,
                eq.end_best_bid,
                eq.end_best_ask,
                e1m.end_plus1_best_bid,
                e1m.end_plus1_best_ask
            FROM
                enriched_orders_processed AS e
            LEFT JOIN
                vwap_df AS v ON e.order_id = v.order_id AND e.client_id = v.client_id
            LEFT JOIN
                open_quotes AS oq ON e.order_pk = oq.order_pk
            LEFT JOIN
                end_quotes AS eq ON e.order_pk = eq.order_pk
            LEFT JOIN
                end_1m_quotes AS e1m ON e.order_pk = e1m.order_pk
        )
        SELECT
            *,
            (start_best_bid + start_best_ask) / 2 AS arrival_mid_price,
            
            CASE
                WHEN side = 'BUY' THEN ((fill_AverageExecutionPrice - market_interval_vwap) / market_interval_vwap) * 10000
                WHEN side = 'SELL' THEN ((market_interval_vwap - fill_AverageExecutionPrice) / market_interval_vwap) * 10000
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

        FROM
            final_df_base
        """
        
        destination_table_id_with_date = f"{project_id}.{dataset_id}.trading_analytics_{run_date.replace('-', '_')}"

        job_config = bigquery.QueryJobConfig(
            destination=destination_table_id_with_date,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        query_job = client.query(sql_query, job_config=job_config)
        query_job.result()
        print("Performance analysis complete and data written to BigQuery table: " + destination_table_id_with_date)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python script_name.py <run_date> <project_id> <dataset_id>")
        sys.exit(1)
    
    client = bigquery.Client()
    
    p = AlgorithmicTradingPerformance()
    run_date = sys.argv[1]
    project_id = sys.argv[2]
    dataset_id = sys.argv[3]
    p.execute_pipeline(client, project_id, dataset_id, run_date)
DECLARE run_date STRING DEFAULT '2023-01-01'; -- Placeholder for run_date
DECLARE utc_time_market_open TIMESTAMP;
DECLARE utc_time_market_close TIMESTAMP;

SET utc_time_market_open = TIMESTAMP(FORMAT_DATETIME('%F %T', DATETIME(PARSE_DATE('%F', run_date), TIME '09:30:00'), 'America/New_York'));
SET utc_time_market_close = TIMESTAMP(FORMAT_DATETIME('%F %T', DATETIME(PARSE_DATE('%F', run_date), TIME '16:00:00'), 'America/New_York'));

CREATE OR REPLACE TABLE `your-gcp-project.your_dataset.trading_analytics_${FORMAT_DATE('%Y%m%d', PARSE_DATE('%Y-%m-%d', run_date))}` AS
WITH
all_order_events_df AS (
  SELECT * FROM `your-gcp-project.your_dataset.trading_events_base`
),
parent_orders_df AS (
  SELECT * FROM `your-gcp-project.your_dataset.parent_orders`
),
fills_raw_df AS (
  SELECT
    *
  FROM
    all_order_events_df
  WHERE
    ((protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
      OR (protocol_version >= 'V2' AND event_type = 'FILL'))
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
      ROW_NUMBER() OVER (PARTITION BY order_id, client_id ORDER BY event_timestamp) AS rn
    FROM
      fills_raw_df
  )
  WHERE
    rn = 1
),
fills_agg_df AS (
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
  FROM
    fills_raw_df
  GROUP BY
    order_id,
    client_id,
    trade_date
),
acks_agg_df AS (
  SELECT
    order_id,
    client_id,
    trade_date,
    LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS ack_AckStartTime
  FROM
    all_order_events_df
  WHERE
    status IN ('NEW', 'REPLACED') AND event_type = 'ACK'
  GROUP BY
    order_id,
    client_id,
    trade_date
),
terminations_agg_df AS (
  SELECT
    order_id,
    client_id,
    trade_date,
    GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS term_ExecutionEndTime
  FROM
    all_order_events_df
  WHERE
    status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')
  GROUP BY
    order_id,
    client_id,
    trade_date
),
enriched_orders_pre_window AS (
  SELECT
    p.* EXCEPT (order_id, client_id), -- Select all from parent_orders_df and re-add order_id, client_id at the end to manage potential field overlaps or column ordering, or select explicitly.
    p.order_id,
    p.client_id,
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
    parent_orders_df AS p
    LEFT JOIN fills_agg_df AS f ON p.order_id = f.order_id AND p.client_id = f.client_id
    LEFT JOIN acks_agg_df AS a ON p.order_id = a.order_id AND p.client_id = a.client_id
    LEFT JOIN terminations_agg_df AS t ON p.order_id = t.order_id AND p.client_id = t.client_id
    LEFT JOIN close_fill_price_df AS c ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id
),
enriched_orders_df AS (
  SELECT
    *,
    LEAST(GREATEST(COALESCE(ack_AckStartTime, utc_time_market_open), utc_time_market_open), fill_FillStartTime) AS EffectiveStartTime,
    LEAST(COALESCE(term_ExecutionEndTime, utc_time_market_close), utc_time_market_close) AS EffectiveEndTime,
    GENERATE_UUID() AS order_pk -- BigQuery's equivalent to Spark's monotonically_increasing_id for unique row identification
  FROM
    enriched_orders_pre_window
),
quotes_df_source AS (
  SELECT * FROM `your-gcp-project.your_dataset.level1_quotes`
),
quotes_df_window_buffers AS (
  SELECT
    *,
    TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,
    TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,
    TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,
    TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,
    TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,
    TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower
  FROM
    enriched_orders_df
),
open_quotes AS (
  SELECT
    q_buff.order_pk,
    q.best_bid AS start_best_bid,
    q.best_ask AS start_best_ask,
    q.quote_timestamp AS start_quote_timestamp
  FROM
    quotes_df_window_buffers AS q_buff
    LEFT JOIN quotes_df_source AS q ON q_buff.ticker = q.ticker
      AND q.quote_timestamp BETWEEN q_buff.Start_lower AND q_buff.EffectiveStartTime
  QUALIFY ROW_NUMBER() OVER(PARTITION BY q_buff.order_pk ORDER BY ABS(TIMESTAMP_DIFF(q.quote_timestamp, q_buff.EffectiveStartTime, MICROSECOND)), q.quote_timestamp) = 1
),
end_quotes AS (
  SELECT
    q_buff.order_pk,
    q.best_bid AS end_best_bid,
    q.best_ask AS end_best_ask
  FROM
    quotes_df_window_buffers AS q_buff
    LEFT JOIN quotes_df_source AS q ON q_buff.ticker = q.ticker
      AND q.quote_timestamp BETWEEN q_buff.End_lower AND q_buff.EffectiveEndTime
  QUALIFY ROW_NUMBER() OVER(PARTITION BY q_buff.order_pk ORDER BY ABS(TIMESTAMP_DIFF(q.quote_timestamp, q_buff.EffectiveEndTime, MICROSECOND)), q.quote_timestamp) = 1
),
end_1m_quotes AS (
  SELECT
    q_buff.order_pk,
    q.best_bid AS end_plus1_best_bid,
    q.best_ask AS end_plus1_best_ask
  FROM
    quotes_df_window_buffers AS q_buff
    LEFT JOIN quotes_df_source AS q ON q_buff.ticker = q.ticker
      AND q.quote_timestamp BETWEEN q_buff.End_plus1_lower AND q_buff.End_plus1
  QUALIFY ROW_NUMBER() OVER(PARTITION BY q_buff.order_pk ORDER BY ABS(TIMESTAMP_DIFF(q.quote_timestamp, q_buff.End_plus1, MICROSECOND)), q.quote_timestamp) = 1
),
trades_df AS (
  SELECT * FROM `your-gcp-project.your_dataset.market_trades`
),
vwap_df AS (
  SELECT
    e.order_id,
    e.client_id,
    SUM(t.trade_size) AS market_interval_volume,
    SAFE_DIVIDE(SUM(t.trade_price * t.trade_size), SUM(t.trade_size)) AS market_interval_vwap
  FROM
    enriched_orders_df AS e
    JOIN trades_df AS t ON e.ticker = t.ticker
      AND t.trade_timestamp BETWEEN e.EffectiveStartTime AND e.EffectiveEndTime
  GROUP BY
    e.order_id,
    e.client_id
),
final_calculation_prep AS (
  SELECT
    e.* EXCEPT(order_pk, Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower), -- exclude helper columns
    v.market_interval_volume,
    v.market_interval_vwap,
    o.start_best_bid,
    o.start_best_ask,
    o.start_quote_timestamp,
    eq.end_best_bid,
    eq.end_best_ask,
    e1.end_plus1_best_bid,
    e1.end_plus1_best_ask
  FROM
    enriched_orders_df AS e
    LEFT JOIN vwap_df AS v ON e.order_id = v.order_id AND e.client_id = v.client_id
    LEFT JOIN open_quotes AS o ON e.order_pk = o.order_pk
    LEFT JOIN end_quotes AS eq ON e.order_pk = eq.order_pk
    LEFT JOIN end_1m_quotes AS e1 ON e.order_pk = e1.order_pk
)
SELECT
  *,
  SAFE_DIVIDE(COALESCE(start_best_bid, 0) + COALESCE(start_best_ask, 0), 2) AS arrival_mid_price, -- Coalesce to 0 for safe calculation if quotes are missing
  CASE
    WHEN side = 'BUY' THEN SAFE_DIVIDE((fill_AverageExecutionPrice - market_interval_vwap), market_interval_vwap) * 10000
    WHEN side = 'SELL' THEN SAFE_DIVIDE((market_interval_vwap - fill_AverageExecutionPrice), market_interval_vwap) * 10000
    ELSE NULL
  END AS slippage_from_vwap_bps,
  CASE
    WHEN side = 'BUY' THEN (SAFE_DIVIDE(COALESCE(start_best_bid, 0) + COALESCE(start_best_ask, 0), 2) - fill_AverageExecutionPrice) * fill_TotalSharesExecuted
    WHEN side = 'SELL' THEN (fill_AverageExecutionPrice - SAFE_DIVIDE(COALESCE(start_best_bid, 0) + COALESCE(start_best_ask, 0), 2)) * fill_TotalSharesExecuted
    ELSE NULL
  END AS implementation_shortfall_pl,
  CASE
    WHEN side = 'BUY' THEN (SAFE_DIVIDE(COALESCE(end_plus1_best_bid, 0) + COALESCE(end_plus1_best_ask, 0), 2) - SAFE_DIVIDE(COALESCE(end_best_bid, 0) + COALESCE(end_best_ask, 0), 2)) * fill_TotalSharesExecuted
    WHEN side = 'SELL' THEN (SAFE_DIVIDE(COALESCE(end_best_bid, 0) + COALESCE(end_best_ask, 0), 2) - SAFE_DIVIDE(COALESCE(end_plus1_best_bid, 0) + COALESCE(end_plus1_best_ask, 0), 2)) * fill_TotalSharesExecuted
    ELSE NULL
  END AS post_trade_1m_momentum,
  CASE
    WHEN side = 'BUY' THEN (requested_shares - fill_TotalSharesExecuted) * (fill_AverageExecutionPrice - closing_price)
    WHEN side = 'SELL' THEN (requested_shares - fill_TotalSharesExecuted) * (closing_price - fill_AverageExecutionPrice)
    ELSE NULL
  END AS opportunity_cost_pl
FROM
  final_calculation_prep;

WITH
  all_order_events AS (
    -- Equivalent to spark.read.parquet("hdfs://trading_events_base/")
    -- Assuming `trading_events_base` is an existing BigQuery table or external table.
    SELECT * FROM trading_events_base
  ),
  parent_orders AS (
    -- Equivalent to spark.read.parquet("hdfs://parent_orders/")
    -- Assuming `parent_orders` is an existing BigQuery table or external table.
    SELECT * FROM parent_orders
  ),
  fills AS (
    -- Equivalent to fills_df = all_order_events_df.filter(...)
    SELECT
      *
    FROM
      all_order_events
    WHERE
      ((protocol_version = 'V1' AND status IN ('FILLED', 'PARTIAL') AND event_type = 'TRADE')
        OR (protocol_version >= 'V2' AND event_type = 'FILL'))
  ),
  ranked_close_fills AS (
    -- Intermediate step for close_fill_price_df
    SELECT
      order_id,
      client_id,
      last_exec_price,
      last_exec_qty,
      ROW_NUMBER() OVER (PARTITION BY order_id, client_id ORDER BY event_timestamp) AS rn
    FROM
      fills
  ),
  close_fill_price AS (
    -- Equivalent to close_fill_price_df
    SELECT
      order_id AS close_order_id,
      client_id AS close_client_id,
      last_exec_price AS closing_price,
      last_exec_qty AS closing_qty
    FROM
      ranked_close_fills
    WHERE
      rn = 1
  ),
  fills_agg AS (
    -- Equivalent to fills_agg_df
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
      fills
    GROUP BY
      order_id, client_id, trade_date
  ),
  acks AS (
    -- Equivalent to acks_df
    SELECT
      *
    FROM
      all_order_events
    WHERE
      status IN ('NEW', 'REPLACED') AND event_type = 'ACK'
  ),
  acks_agg AS (
    -- Equivalent to acks_agg_df
    SELECT
      order_id,
      client_id,
      trade_date,
      LEAST(MIN(event_timestamp), MIN(routing_timestamp)) AS ack_AckStartTime
    FROM
      acks
    GROUP BY
      order_id, client_id, trade_date
  ),
  terminations AS (
    -- Equivalent to terminations_df
    SELECT
      *
    FROM
      all_order_events
    WHERE
      status IN ('CANCELED', 'DONE_FOR_DAY', 'EXPIRED', 'REJECTED')
  ),
  terminations_agg AS (
    -- Equivalent to terminations_agg_df
    SELECT
      order_id,
      client_id,
      trade_date,
      GREATEST(MAX(event_timestamp), MAX(routing_timestamp)) AS term_ExecutionEndTime
    FROM
      terminations
    GROUP BY
      order_id, client_id, trade_date
  ),
  -- Python's local_to_utc_time for '9:30' and '16:00' on the run_date.
  -- Assuming run_date is provided as a STRING 'YYYY-MM-DD'.
  -- Example with a run_date like '2023-10-27':
  -- `utc_time_market_open` = TIMESTAMP '2023-10-27 13:30:00 UTC' (for America/New_York DST offset)
  -- `utc_time_market_close` = TIMESTAMP '2023-10-27 20:00:00 UTC' (for America/New_York DST offset)
  -- For line-by-line conversion, we represent this as a direct TIMESTAMP literal
  -- where the run_date variable is replaced with its actual value for this execution.
  -- BigQuery's `current_date` used as a proxy for `@run_date` for timestamp conversion logic demonstration.
  -- For a real production system, this would be a parameter, e.g., DECLARE run_date STRING DEFAULT '2023-10-27';
  -- and use PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', FORMAT_TIMESTAMP('%Y-%m-%d', DATE(TIMESTAMP(run_date), 'America/New_York')) || ' 09:30:00', 'America/New_York') AT TIME ZONE 'UTC'
  -- or use parameterised timestamps if preferred for performance.
  run_date_vars AS (
    SELECT
      PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CONCAT(@run_date, ' 09:30:00'), 'America/New_York') AS local_market_open_ny,
      PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CONCAT(@run_date, ' 16:00:00'), 'America/New_York') AS local_market_close_ny
  ),
  enriched_orders_base AS (
    -- Equivalent to initial enriched_orders_df joining parent_orders with aggregates
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
      parent_orders AS p
    LEFT JOIN
      fills_agg AS f ON p.order_id = f.order_id AND p.client_id = f.client_id
    LEFT JOIN
      acks_agg AS a ON p.order_id = a.order_id AND p.client_id = a.client_id
    LEFT JOIN
      terminations_agg AS t ON p.order_id = t.order_id AND p.client_id = t.client_id
    LEFT JOIN
      close_fill_price AS c ON p.order_id = c.close_order_id AND p.client_id = c.close_client_id
  ),
  enriched_orders_with_effective_times AS (
    -- Equivalent to enriched_orders_df after adding EffectiveStartTime and EffectiveEndTime
    SELECT
      *,
      LEAST(GREATEST(ack_AckStartTime, (SELECT local_market_open_ny FROM run_date_vars)), fill_FillStartTime) AS EffectiveStartTime,
      LEAST(term_ExecutionEndTime, (SELECT local_market_close_ny FROM run_date_vars)) AS EffectiveEndTime
    FROM
      enriched_orders_base
  ),
  quotes AS (
    -- Equivalent to quotes_df = spark.read.parquet("hdfs://level1_quotes/")
    -- Assuming `level1_quotes` is an existing BigQuery table or external table.
    -- The repartition and sortWithinPartitions hints are not directly translated;
    -- the BigQuery optimizer handles physical data layout.
    SELECT
      *,
      -- Using ROW_NUMBER() as a replacement for F.monotonically_increasing_id()
      -- for a stable unique ID within this dataset for join purposes.
      ROW_NUMBER() OVER () AS quote_pk
    FROM
      level1_quotes
    ORDER BY ticker, quote_timestamp -- Emulating sortWithinPartitions, actual partitioning is for BigQuery optimizer
  ),
  enriched_orders_with_pk AS (
    -- Equivalent to enriched_orders_df after adding order_pk
    SELECT
      *,
      ROW_NUMBER() OVER () AS order_pk -- Using ROW_NUMBER() as a replacement for F.monotonically_increasing_id()
    FROM
      enriched_orders_with_effective_times
    ORDER BY ticker, EffectiveStartTime -- Emulating sortWithinPartitions
  ),
  enriched_orders_with_time_buffers AS (
    -- Equivalent to enriched_orders_df after adding time buffer columns
    SELECT
      *,
      TIMESTAMP_SUB(EffectiveStartTime, INTERVAL 10 MINUTE) AS Start_lower,
      TIMESTAMP_SUB(EffectiveEndTime, INTERVAL 10 MINUTE) AS End_lower,
      TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE) AS End_plus1,
      TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 1 MINUTE), INTERVAL 10 MINUTE) AS End_plus1_lower,
      TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE) AS End_plus5,
      TIMESTAMP_SUB(TIMESTAMP_ADD(EffectiveEndTime, INTERVAL 5 MINUTE), INTERVAL 10 MINUTE) AS End_plus5_lower
    FROM
      enriched_orders_with_pk
  ),
  -- Begin find_nearest_quote conversions for `open_quotes`
  open_quotes_joined AS (
    SELECT
      o.order_pk,
      o.ticker,
      o.EffectiveStartTime,
      q.quote_timestamp,
      q.best_bid,
      q.best_ask,
      ABS(TIMESTAMP_DIFF(o.EffectiveStartTime, q.quote_timestamp, MILLISECOND)) AS time_diff
    FROM
      enriched_orders_with_time_buffers AS o
    INNER JOIN
      quotes AS q ON o.ticker = q.ticker
      AND q.quote_timestamp BETWEEN o.Start_lower AND o.EffectiveStartTime
  ),
  open_quotes_ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY time_diff, quote_timestamp DESC) AS rn -- Adding quote_timestamp DESC to break ties
    FROM
      open_quotes_joined
  ),
  open_quotes AS (
    SELECT
      order_pk,
      quote_timestamp AS start_quote_timestamp,
      best_bid AS start_best_bid,
      best_ask AS start_best_ask
    FROM
      open_quotes_ranked
    WHERE
      rn = 1
  ),
  -- End find_nearest_quote conversions for `open_quotes`

  -- Begin find_nearest_quote conversions for `end_quotes`
  end_quotes_joined AS (
    SELECT
      o.order_pk,
      o.ticker,
      o.EffectiveEndTime,
      q.quote_timestamp,
      q.best_bid,
      q.best_ask,
      ABS(TIMESTAMP_DIFF(o.EffectiveEndTime, q.quote_timestamp, MILLISECOND)) AS time_diff
    FROM
      enriched_orders_with_time_buffers AS o
    INNER JOIN
      quotes AS q ON o.ticker = q.ticker
      AND q.quote_timestamp BETWEEN o.End_lower AND o.EffectiveEndTime
  ),
  end_quotes_ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY time_diff, quote_timestamp DESC) AS rn -- Adding quote_timestamp DESC to break ties
    FROM
      end_quotes_joined
  ),
  end_quotes AS (
    SELECT
      order_pk,
      quote_timestamp AS end_quote_timestamp,
      best_bid AS end_best_bid,
      best_ask AS end_best_ask
    FROM
      end_quotes_ranked
    WHERE
      rn = 1
  ),
  -- End find_nearest_quote conversions for `end_quotes`

  -- Begin find_nearest_quote conversions for `end_1m_quotes`
  end_1m_quotes_joined AS (
    SELECT
      o.order_pk,
      o.ticker,
      o.End_plus1,
      q.quote_timestamp,
      q.best_bid,
      q.best_ask,
      ABS(TIMESTAMP_DIFF(o.End_plus1, q.quote_timestamp, MILLISECOND)) AS time_diff
    FROM
      enriched_orders_with_time_buffers AS o
    INNER JOIN
      quotes AS q ON o.ticker = q.ticker
      AND q.quote_timestamp BETWEEN o.End_plus1_lower AND o.End_plus1
  ),
  end_1m_quotes_ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY order_pk ORDER BY time_diff, quote_timestamp DESC) AS rn -- Adding quote_timestamp DESC to break ties
    FROM
      end_1m_quotes_joined
  ),
  end_1m_quotes AS (
    SELECT
      order_pk,
      quote_timestamp AS end_plus1_quote_timestamp,
      best_bid AS end_plus1_best_bid,
      best_ask AS end_plus1_best_ask
    FROM
      end_1m_quotes_ranked
    WHERE
      rn = 1
  ),
  -- End find_nearest_quote conversions for `end_1m_quotes`

  trades AS (
    -- Equivalent to trades_df = spark.read.parquet("hdfs://market_trades/")
    -- Assuming `market_trades` is an existing BigQuery table or external table.
    SELECT * FROM market_trades
  ),
  vwap AS (
    -- Equivalent to vwap_df
    SELECT
      o.order_id,
      o.client_id,
      SUM(t.trade_size) AS market_interval_volume,
      SAFE_DIVIDE(SUM(t.trade_price * t.trade_size), SUM(t.trade_size)) AS market_interval_vwap
    FROM
      enriched_orders_with_time_buffers AS o
    INNER JOIN
      trades AS t ON o.ticker = t.ticker
      AND t.trade_timestamp >= o.EffectiveStartTime
      AND t.trade_timestamp <= o.EffectiveEndTime
    GROUP BY
      o.order_id, o.client_id
  ),
  final_joined_df AS (
    -- Equivalent to initial final_df after all joins
    SELECT
      e.* EXCEPT (Start_lower, End_lower, End_plus1, End_plus1_lower, End_plus5, End_plus5_lower), -- Drop buffer columns from main DF
      v.market_interval_volume,
      v.market_interval_vwap,
      oq.start_best_bid,
      oq.start_best_ask,
      oq.start_quote_timestamp,
      eq.end_best_bid,
      eq.end_best_ask,
      e1q.end_plus1_best_bid,
      e1q.end_plus1_best_ask
    FROM
      enriched_orders_with_time_buffers AS e
    LEFT JOIN
      vwap AS v ON e.order_id = v.order_id AND e.client_id = v.client_id
    LEFT JOIN
      open_quotes AS oq ON e.order_pk = oq.order_pk
    LEFT JOIN
      end_quotes AS eq ON e.order_pk = eq.order_pk
    LEFT JOIN
      end_1m_quotes AS e1q ON e.order_pk = e1q.order_pk
  )
SELECT
  *,
  -- arrival_mid_price calculation
  (start_best_bid + start_best_ask) / 2 AS arrival_mid_price,
  -- slippage_from_vwap_bps calculation
  CASE
    WHEN side = 'BUY' THEN SAFE_DIVIDE((fill_AverageExecutionPrice - market_interval_vwap), market_interval_vwap) * 10000
    WHEN side = 'SELL' THEN SAFE_DIVIDE((market_interval_vwap - fill_AverageExecutionPrice), market_interval_vwap) * 10000
    ELSE NULL
  END AS slippage_from_vwap_bps,
  -- implementation_shortfall_pl calculation
  CASE
    WHEN side = 'BUY' THEN (arrival_mid_price - fill_AverageExecutionPrice) * fill_TotalSharesExecuted
    WHEN side = 'SELL' THEN (fill_AverageExecutionPrice - arrival_mid_price) * fill_TotalSharesExecuted
    ELSE NULL
  END AS implementation_shortfall_pl,
  -- post_trade_1m_momentum calculation
  CASE
    WHEN side = 'BUY' THEN (SAFE_DIVIDE((end_plus1_best_bid + end_plus1_best_best_ask), 2) - SAFE_DIVIDE((end_best_bid + end_best_ask), 2)) * fill_TotalSharesExecuted
    WHEN side = 'SELL' THEN (SAFE_DIVIDE((end_best_bid + end_best_ask), 2) - SAFE_DIVIDE((end_plus1_best_bid + end_plus1_best_ask), 2)) * fill_TotalSharesExecuted
    ELSE NULL
  END AS post_trade_1m_momentum,
  -- opportunity_cost_pl calculation
  CASE
    WHEN side = 'BUY' THEN (requested_shares - fill_TotalSharesExecuted) * (fill_AverageExecutionPrice - closing_price)
    WHEN side = 'SELL' THEN (requested_shares - fill_TotalSharesExecuted) * (closing_price - fill_AverageExecutionPrice)
    ELSE NULL
  END AS opportunity_cost_pl
FROM
  final_joined_df
;
-- The final write operation `final_df.write.parquet(f"hdfs://trading_analytics/run_date={run_date}", mode="overwrite")`
-- is represented by a DDL statement that would capture the results of the above query.
-- The specific syntax depends on whether `trading_analytics` is a managed BigQuery table or an external table.
-- For a managed table, you would use:
-- CREATE OR REPLACE TABLE `your_project.your_dataset.trading_analytics_@run_date` AS
-- SELECT ... FROM final_joined_df ...;
-- Or if writing to a single table and overwriting a partition:
-- INSERT OVERWRITE `your_project.your_dataset.trading_analytics`
-- PARTITION BY DATE(run_date_column)
-- SELECT ... FROM final_joined_df ...;
-- Given the remediation guidance for TRUNCATE/INSERT for HDFS equivalent, it might be:
-- TRUNCATE TABLE `your_project.your_dataset.trading_analytics`;
-- INSERT INTO `your_project.your_dataset.trading_analytics`
-- SELECT ... FROM final_joined_df ...;
-- Assuming the output should ONLY contain converted lines without additional instructions,
-- the above SELECT statement is the representation of `final_df`.
-- If the final result should be materialized, a DDL wrapper would be needed:
-- CREATE OR REPLACE TABLE `your_project.your_dataset.trading_analytics`
-- AS (
--    ... entire query above ...
-- );
-- Note: Replace `your_project.your_dataset.trading_analytics` with your actual BigQuery table path.
-- For the placeholder variable `@run_date`, ensure it's declared and passed if executing directly in BigQuery:
-- DECLARE run_date STRING DEFAULT 'YYYY-MM-DD'; -- e.g., '2023-10-27'

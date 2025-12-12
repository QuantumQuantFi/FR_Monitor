-- Dataset extract template: event (decision_ts) + raw snapshot (same ts) + future_outcome label.
-- Notes:
-- - Use watch_signal_raw at ts=event.start_ts to avoid leakage (event.features_agg is updated over time).
-- - For Type B (perp-perp), canonicalize spread label in analysis if needed (see plan doc).
--
-- Params:
--   :since  timestamptz
--   :until  timestamptz
--   :horizon_min int
--   :signal_types char(1)[]  e.g. ARRAY['B','C']

SELECT
  e.id AS event_id,
  e.start_ts AS decision_ts,
  e.exchange,
  e.symbol,
  e.signal_type,
  e.leg_a_exchange,
  e.leg_a_symbol,
  e.leg_a_kind,
  e.leg_a_price_first,
  e.leg_a_funding_rate_first,
  e.leg_a_funding_interval_hours,
  e.leg_a_next_funding_time,
  e.leg_b_exchange,
  e.leg_b_symbol,
  e.leg_b_kind,
  e.leg_b_price_first,
  e.leg_b_funding_rate_first,
  e.leg_b_funding_interval_hours,
  e.leg_b_next_funding_time,
  r.*,
  o.horizon_min,
  o.pnl,
  o.spread_change,
  o.funding_change,
  o.max_drawdown,
  o.volatility,
  o.funding_applied,
  o.created_at
FROM watchlist.watch_signal_event e
JOIN watchlist.future_outcome o
  ON o.event_id = e.id AND o.horizon_min = :horizon_min
LEFT JOIN LATERAL (
  SELECT
    r.id AS raw_id,
    r.ts,
    r.spread_rel,
    r.funding_rate,
    r.funding_interval_hours,
    r.next_funding_time,
    r.range_1h,
    r.range_12h,
    r.volatility,
    r.slope_3m,
    r.crossings_1h,
    r.drift_ratio,
    r.best_buy_high_sell_low,
    r.best_sell_high_buy_low,
    r.funding_diff_max,
    r.spot_perp_volume_ratio,
    r.oi_to_volume_ratio,
    r.bid_ask_spread,
    r.depth_imbalance,
    r.volume_spike_zscore,
    r.premium_index_diff,
    r.meta
  FROM watchlist.watch_signal_raw r
  WHERE r.ts = e.start_ts
    AND r.exchange = e.exchange
    AND r.symbol = e.symbol
    AND r.signal_type = e.signal_type
  ORDER BY r.id DESC
  LIMIT 1
) r ON TRUE
WHERE e.start_ts >= :since
  AND e.start_ts < :until
  AND e.signal_type = ANY(:signal_types)
  AND o.pnl IS NOT NULL
ORDER BY e.start_ts ASC;


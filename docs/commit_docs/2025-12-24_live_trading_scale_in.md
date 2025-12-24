# Live Trading (Type B) — Scale-in Rescue & Retry Reliability

Date: 2025-12-24

This document describes the batch of commits that implement “spread-widening rescue scale-in” for live trading (Type B perp-perp) and a follow-up reliability fix for skipped-signal retries under watchlist event merge.

## Commit 1 — Fix skipped-signal retry gating for merged events

Title: `Fix skipped-signal retry gating for merged events`

Problem:
- When watchlist event merge is enabled, `watchlist.watch_signal_event` timestamps may stop advancing while the event remains `status='open'`.
- Live trading skipped retries (`watchlist.live_trade_signal.status='skipped'`) were filtered by an event-time lookback window, so after a few minutes they would silently age out and stop being retried. Operationally this looks like “live trading stopped generating/updating records”.

Change:
- In `trading/live_trading_manager.py`, treat events with `e.status='open'` as always eligible for retry.
- Keep the original time-window guard for non-open events (so historical closed events don’t loop forever).

Operational impact:
- Skipped signals continue to be periodically revalidated, so the system can recover when spreads normalize or transient orderbook issues clear.

## Commit 2 — Live trading: add Type B scale-in rescue & multi-entry accounting

Title: `Live trading: add Type B scale-in rescue & multi-entry accounting`

Goal:
- Avoid mechanical stop-loss on spread-widening losses by allowing controlled scale-in (add-entry) up to a maximum number of entries.
- Preserve the existing opening flow (orderbook sweep + confirm + market orders) for each entry.
- Ensure monitoring/TP/force-close works for a multi-entry position.

Core behavior:
- Applies to Type B (perp-perp) trades for the same `symbol` and the same long/short exchange pair.
- If spread widens relative to the initial entry spread by a configurable multiplier step (default 1.5×), allow another entry, up to `scale_in_max_entries` (default 4).
- Requires a fresh watchlist event within a configurable age window to permit scale-in; also enforces a minimum interval between scale-ins to prevent rapid-fire averaging down.
- Position monitoring uses notional-weighted aggregation across entries (quantities and entry prices).
- Take-profit logic is evaluated against the overall position, but the TP target remains anchored to the initial trade’s `take_profit_pnl` rule.
- When scale-in is enabled, disable mechanical stop-loss on total PnL; keep funding hard-stop and max-hold force-close as the primary safety rails.

Notes:
- This change introduces “multi-entry” semantics at the live-trade-order level and updates internal helpers to aggregate quantity/price/notional accordingly.
- Scale-in related snapshots are persisted into the signal payload for auditability.

## Commit 3 — Config: enable scale-in knobs and extend max hold to 7d

Title: `Config: enable scale-in knobs and extend max hold to 7d`

Change:
- `config.py`: add env-backed configuration for scale-in tuning:
  - `LIVE_TRADING_SCALE_IN_ENABLED`
  - `LIVE_TRADING_SCALE_IN_MAX_ENTRIES`
  - `LIVE_TRADING_SCALE_IN_TRIGGER_MULT`
  - `LIVE_TRADING_SCALE_IN_MIN_INTERVAL_MIN`
  - `LIVE_TRADING_SCALE_IN_SIGNAL_MAX_AGE_MIN`
  - `LIVE_TRADING_SCALE_IN_MAX_TOTAL_NOTIONAL`
- Default `LIVE_TRADING_MAX_HOLD_DAYS` to 7.
- `simple_app.py`: wire new config fields into `LiveTradingConfig(...)` so they are visible and applied at runtime.

## Commit 4 — Add audit script for live trading scale-in state

Title: `Add audit script for live trading scale-in state`

Adds:
- `scripts/audit_live_trading_scale_in.py`: a small ops helper to print open live trades with scale-in state (entries count, next trigger) for quick audits.

Usage:
- `./venv/bin/python scripts/audit_live_trading_scale_in.py --status open`

## Suggested runtime checks

- Confirm service is running: `ss -ltnp | rg :4002`
- Confirm new signals are being updated: `curl -fsS http://127.0.0.1:4002/api/live_trading/overview?limit=5`
- Confirm open positions scale-in state: `./venv/bin/python scripts/audit_live_trading_scale_in.py --status open`


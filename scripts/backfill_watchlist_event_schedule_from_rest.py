#!/usr/bin/env python3
"""
Backfill funding schedule fields in Postgres watch_signal_event using exchange REST funding history.

Goal:
- Repair historical issues in "next funding time" and "funding interval hours" for events.
- Do NOT modify funding_rate values (event/legs funding rates stay as originally observed).

This script adds columns (if missing) and fills:
  - funding_interval_hours, next_funding_time (event-level, derived from perp legs)
  - leg_a_funding_interval_hours, leg_a_next_funding_time
  - leg_b_funding_interval_hours, leg_b_next_funding_time

Usage:
  venv/bin/python scripts/backfill_watchlist_event_schedule_from_rest.py \
      --pg-dsn "postgresql://..." --days 30 --batch 200 --dry-run
"""

from __future__ import annotations

import argparse
import bisect
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from funding_utils import DEFAULT_FUNDING_INTERVALS, derive_funding_interval_hours


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_dt(value: Any) -> Optional[datetime]:
    if value in (None, "", False):
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 1e12:
            seconds /= 1000.0
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        return _as_utc(dt)
    except Exception:
        return None


def _is_valid_interval_hours(interval: Optional[float]) -> bool:
    if interval is None:
        return False
    try:
        v = float(interval)
    except Exception:
        return False
    return 0 < v <= 48


def _is_valid_next_time(ts: datetime, next_ft: Optional[datetime], *, allow_past_seconds: float = 60.0) -> bool:
    if next_ft is None:
        return False
    try:
        return _as_utc(next_ft) > (_as_utc(ts) - timedelta(seconds=float(allow_past_seconds)))
    except Exception:
        return False


def _ceil_next_boundary(ts: datetime, interval_hours: float) -> datetime:
    """
    Align to the next UTC boundary of an interval that divides 24h (1/2/3/4/6/8/12/24).
    Fallback to ts + interval if interval doesn't divide 24 cleanly.
    """
    ts = _as_utc(ts)
    interval = float(interval_hours)
    if interval <= 0:
        return ts
    if 24 % int(interval) != 0 or abs(interval - round(interval)) > 1e-9:
        return ts + timedelta(hours=interval)

    interval_i = int(round(interval))
    base = ts.replace(minute=0, second=0, microsecond=0)
    hour = base.hour
    next_hour = ((hour // interval_i) + 1) * interval_i
    if next_hour >= 24:
        base = base.replace(hour=0) + timedelta(days=1)
    else:
        base = base.replace(hour=next_hour)
    if base <= ts:
        base = base + timedelta(hours=interval_i)
    return base


def _median_interval_hours(funding_times: List[datetime]) -> Optional[float]:
    if len(funding_times) < 3:
        return None
    diffs: List[float] = []
    for a, b in zip(funding_times, funding_times[1:]):
        hours = (b - a).total_seconds() / 3600.0
        if 0.5 <= hours <= 48:
            diffs.append(hours)
    if not diffs:
        return None
    diffs.sort()
    mid = len(diffs) // 2
    median = diffs[mid] if len(diffs) % 2 == 1 else (diffs[mid - 1] + diffs[mid]) / 2.0
    # snap close to common intervals
    for candidate in (1.0, 2.0, 4.0, 8.0, 12.0, 24.0):
        if abs(median - candidate) <= (60.0 / 3600.0):
            return candidate
    return round(median, 6)


def _schedule_at(
    ts: datetime,
    funding_times: List[datetime],
    *,
    default_interval_hours: Optional[float],
) -> Tuple[Optional[float], Optional[datetime]]:
    ts = _as_utc(ts)
    times = sorted({_as_utc(t) for t in (funding_times or [])})
    interval_hint = _median_interval_hours(times) or default_interval_hours

    if not times:
        if interval_hint is None:
            return None, None
        return interval_hint, _ceil_next_boundary(ts, interval_hint)

    idx = bisect.bisect_right(times, ts)
    next_ft = times[idx] if idx < len(times) else None
    prev_ft = times[idx - 1] if idx > 0 else None

    interval = None
    if prev_ft and next_ft:
        interval = (next_ft - prev_ft).total_seconds() / 3600.0
    if not _is_valid_interval_hours(interval):
        interval = interval_hint

    if next_ft is None and interval:
        # advance from last known funding time
        candidate = times[-1]
        step = timedelta(hours=float(interval))
        while candidate <= ts:
            candidate = candidate + step
        next_ft = candidate

    if next_ft is None and interval_hint:
        next_ft = _ceil_next_boundary(ts, float(interval_hint))

    return (float(interval) if interval is not None else None), next_ft


def _normalize_symbol(exchange: str, base_symbol: str) -> str:
    ex = (exchange or "").lower()
    sym = (base_symbol or "").upper().replace("-", "").replace("_", "")
    if ex in ("binance", "bybit", "bitget"):
        return sym if sym.endswith("USDT") else f"{sym}USDT"
    return base_symbol.upper()


@dataclass
class FundingTimesRange:
    start: datetime
    end: datetime

    def expanded(self, margin: timedelta) -> "FundingTimesRange":
        return FundingTimesRange(start=self.start - margin, end=self.end + margin)


class FundingHistoryClient:
    def __init__(self, *, timeout_sec: float = 8.0, max_calls: int = 2000, sleep_sec: float = 0.0):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "FR_Monitor/1.0"})
        self.timeout_sec = timeout_sec
        self.max_calls = max_calls
        self.sleep_sec = sleep_sec
        self.calls = 0
        self.cache: Dict[Tuple[str, str, int, int], List[datetime]] = {}

    def fetch_funding_times(self, exchange: str, symbol: str, start_ts: datetime, end_ts: datetime) -> List[datetime]:
        start_ts = _as_utc(start_ts)
        end_ts = _as_utc(end_ts)
        if end_ts <= start_ts:
            return []
        key = (exchange.lower(), symbol.upper(), int(start_ts.timestamp()), int(end_ts.timestamp()))
        if key in self.cache:
            return self.cache[key]

        ex = (exchange or "").lower()
        out: List[datetime] = []
        try:
            if ex == "binance":
                out = self._fetch_binance(symbol, start_ts, end_ts)
            elif ex == "okx":
                out = self._fetch_okx(symbol, start_ts, end_ts)
            elif ex == "bybit":
                out = self._fetch_bybit(symbol, start_ts, end_ts)
            elif ex == "bitget":
                out = self._fetch_bitget(symbol, start_ts, end_ts)
            else:
                out = []
        except Exception:
            out = []

        # de-dup & sort
        out = sorted({_as_utc(t) for t in out if t})
        self.cache[key] = out
        return out

    def _throttle(self) -> None:
        self.calls += 1
        if self.sleep_sec > 0:
            time.sleep(self.sleep_sec)

    def _fetch_binance(self, symbol: str, start_ts: datetime, end_ts: datetime) -> List[datetime]:
        sym = _normalize_symbol("binance", symbol)
        out: List[datetime] = []
        cursor_start = int(start_ts.timestamp() * 1000)
        # NOTE: Binance sometimes returns 403 when startTime+endTime are combined; prefer startTime-only pagination.
        while self.calls < self.max_calls:
            # Keep limit modest to reduce the chance of WAF/403.
            params = {"symbol": sym, "startTime": cursor_start, "limit": 200}
            self._throttle()
            resp = self.session.get("https://fapi.binance.com/fapi/v1/fundingRate", params=params, timeout=self.timeout_sec)
            resp.raise_for_status()
            data = resp.json() or []
            if not data:
                break
            last_ts_ms = None
            reached_end = False
            for item in data:
                try:
                    ts_ms = int(item.get("fundingTime"))
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    if ts > end_ts:
                        reached_end = True
                        break
                    if start_ts <= ts:
                        out.append(ts)
                    last_ts_ms = ts_ms
                except Exception:
                    continue
            if reached_end or last_ts_ms is None or len(data) < 1000:
                break
            cursor_start = last_ts_ms + 1
        return out

    def _fetch_okx(self, symbol: str, start_ts: datetime, end_ts: datetime) -> List[datetime]:
        inst_id = f"{symbol.upper()}-USDT-SWAP"
        out: List[datetime] = []
        before = int(end_ts.timestamp() * 1000)
        after = int(start_ts.timestamp() * 1000)
        while before > after and self.calls < self.max_calls:
            params: Dict[str, Any] = {"instId": inst_id, "limit": 100, "before": before, "after": after}
            self._throttle()
            resp = self.session.get(
                "https://www.okx.com/api/v5/public/funding-rate-history", params=params, timeout=self.timeout_sec
            )
            resp.raise_for_status()
            data = (resp.json() or {}).get("data") or []
            if not data:
                break
            chunk_times: List[datetime] = []
            for item in data:
                try:
                    ts = datetime.fromtimestamp(int(item.get("fundingTime")) / 1000, tz=timezone.utc)
                    if start_ts <= ts <= end_ts:
                        chunk_times.append(ts)
                except Exception:
                    continue
            out.extend(chunk_times)
            # page backwards by smallest ts we saw
            if not chunk_times:
                break
            min_ts = min(chunk_times)
            before = int((min_ts - timedelta(milliseconds=1)).timestamp() * 1000)
            if len(data) < 100:
                break
        # fallback: latest page if window query returned nothing
        if not out and self.calls < self.max_calls:
            self._throttle()
            resp2 = self.session.get(
                "https://www.okx.com/api/v5/public/funding-rate-history",
                params={"instId": inst_id, "limit": 100},
                timeout=self.timeout_sec,
            )
            resp2.raise_for_status()
            for item in (resp2.json() or {}).get("data") or []:
                try:
                    ts = datetime.fromtimestamp(int(item.get("fundingTime")) / 1000, tz=timezone.utc)
                    if start_ts <= ts <= end_ts:
                        out.append(ts)
                except Exception:
                    continue
        return out

    def _fetch_bybit(self, symbol: str, start_ts: datetime, end_ts: datetime) -> List[datetime]:
        sym = _normalize_symbol("bybit", symbol)
        out: List[datetime] = []
        cursor = None
        for _ in range(8):
            if self.calls >= self.max_calls:
                break
            params: Dict[str, Any] = {
                "category": "linear",
                "symbol": sym,
                "start": int(start_ts.timestamp() * 1000),
                "end": int(end_ts.timestamp() * 1000),
                "limit": 200,
            }
            if cursor:
                params["cursor"] = cursor
            self._throttle()
            resp = self.session.get("https://api.bybit.com/v5/market/funding/history", params=params, timeout=self.timeout_sec)
            resp.raise_for_status()
            result = (resp.json() or {}).get("result") or {}
            list_data = result.get("list") or []
            for item in list_data:
                try:
                    ts = datetime.fromtimestamp(int(item.get("fundingRateTimestamp")) / 1000, tz=timezone.utc)
                    if start_ts <= ts <= end_ts:
                        out.append(ts)
                except Exception:
                    continue
            cursor = result.get("nextPageCursor")
            if not cursor or not list_data:
                break
        return out

    def _fetch_bitget(self, symbol: str, start_ts: datetime, end_ts: datetime) -> List[datetime]:
        inst_id = _normalize_symbol("bitget", symbol)
        out: List[datetime] = []
        cursor_start = int(start_ts.timestamp() * 1000)
        end_ms = int(end_ts.timestamp() * 1000)
        for _ in range(6):
            if self.calls >= self.max_calls or cursor_start >= end_ms:
                break
            params: Dict[str, Any] = {
                "symbol": inst_id,
                "productType": "umcbl",
                "startTime": cursor_start,
                "endTime": end_ms,
                "limit": 1000,
            }
            self._throttle()
            resp = self.session.get("https://api.bitget.com/api/v2/mix/market/history-fund-rate", params=params, timeout=self.timeout_sec)
            resp.raise_for_status()
            data = (resp.json() or {}).get("data") or []
            if not data:
                break
            max_ts_ms = None
            for item in data:
                try:
                    ts_raw = item.get("fundingTime") or item.get("timestamp")
                    ts_ms = int(ts_raw)
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    if start_ts <= ts <= end_ts:
                        out.append(ts)
                    if max_ts_ms is None or ts_ms > max_ts_ms:
                        max_ts_ms = ts_ms
                except Exception:
                    continue
            if max_ts_ms is None or len(data) < 1000:
                break
            cursor_start = max_ts_ms + 1
        return out


def ensure_event_schedule_columns(pg_conn) -> None:
    pg_conn.execute(
        """
        ALTER TABLE watchlist.watch_signal_event
          ADD COLUMN IF NOT EXISTS funding_interval_hours double precision,
          ADD COLUMN IF NOT EXISTS next_funding_time timestamptz,
          ADD COLUMN IF NOT EXISTS leg_a_funding_interval_hours double precision,
          ADD COLUMN IF NOT EXISTS leg_a_next_funding_time timestamptz,
          ADD COLUMN IF NOT EXISTS leg_b_funding_interval_hours double precision,
          ADD COLUMN IF NOT EXISTS leg_b_next_funding_time timestamptz;
        """
    )


def iter_events(pg_conn, since: datetime, until: datetime, limit: Optional[int]) -> Iterable[Dict[str, Any]]:
    sql = """
    SELECT
      id,
      start_ts,
      exchange,
      symbol,
      signal_type,
      funding_interval_hours,
      next_funding_time,
      leg_a_exchange, leg_a_symbol, leg_a_kind,
      leg_a_funding_interval_hours, leg_a_next_funding_time,
      leg_b_exchange, leg_b_symbol, leg_b_kind,
      leg_b_funding_interval_hours, leg_b_next_funding_time
    FROM watchlist.watch_signal_event
    WHERE start_ts >= %s AND start_ts <= %s
      AND (leg_a_kind='perp' OR leg_b_kind='perp')
    ORDER BY start_ts ASC
    """
    params: Tuple[Any, ...] = (since, until)
    if limit:
        sql += " LIMIT %s"
        params = (since, until, int(limit))
    cur = pg_conn.execute(sql, params)
    cols = [d.name for d in cur.description]
    for row in cur.fetchall():
        yield dict(zip(cols, row))


def _group_ranges(events: List[Dict[str, Any]]) -> Dict[Tuple[str, str], FundingTimesRange]:
    ranges: Dict[Tuple[str, str], FundingTimesRange] = {}
    for e in events:
        start_ts = _as_utc(e["start_ts"])
        for prefix in ("leg_a", "leg_b"):
            if (e.get(f"{prefix}_kind") or "").lower() != "perp":
                continue
            ex = str(e.get(f"{prefix}_exchange") or "").lower()
            sym = str(e.get(f"{prefix}_symbol") or e.get("symbol") or "").upper()
            if not ex or not sym:
                continue
            key = (ex, sym)
            if key not in ranges:
                ranges[key] = FundingTimesRange(start=start_ts, end=start_ts)
            else:
                r = ranges[key]
                ranges[key] = FundingTimesRange(start=min(r.start, start_ts), end=max(r.end, start_ts))
    return ranges


def _needs_patch(ts: datetime, current_interval: Any, current_next: Any, *, force: bool) -> bool:
    if force:
        return True
    interval = None
    try:
        interval = float(current_interval) if current_interval is not None else None
    except Exception:
        interval = None
    next_ft = _parse_dt(current_next)
    return (not _is_valid_interval_hours(interval)) or (not _is_valid_next_time(ts, next_ft))


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill watch_signal_event schedule from REST funding history")
    parser.add_argument("--pg-dsn", default=os.environ.get("WATCHLIST_PG_DSN") or "", help="Postgres DSN")
    parser.add_argument("--days", type=int, default=30, help="backfill window (days)")
    parser.add_argument("--until-hours-ago", type=float, default=0.0, help="exclude most recent hours (avoid in-flight)")
    parser.add_argument("--limit", type=int, default=0, help="debug limit events")
    parser.add_argument("--batch", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="overwrite invalid/existing schedule fields")
    parser.add_argument("--margin-hours", type=float, default=48.0, help="history range margin (hours)")
    parser.add_argument("--sleep-sec", type=float, default=0.0, help="sleep between REST calls")
    parser.add_argument("--max-rest-calls", type=int, default=2000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("backfill_event_schedule_rest")

    if not args.pg_dsn:
        raise SystemExit("missing --pg-dsn (or WATCHLIST_PG_DSN env)")

    now = _utcnow()
    until = now - timedelta(hours=float(args.until_hours_ago or 0.0))
    since = until - timedelta(days=max(1, int(args.days)))
    limit = args.limit or None

    client = FundingHistoryClient(max_calls=int(args.max_rest_calls), sleep_sec=float(args.sleep_sec or 0.0))
    margin = timedelta(hours=float(args.margin_hours or 0.0))

    with psycopg.connect(args.pg_dsn, autocommit=True) as pg_conn:
        ensure_event_schedule_columns(pg_conn)

        events = list(iter_events(pg_conn, since, until, limit))
        if not events:
            logger.info("no events in window")
            return

        # Only fetch REST funding history for events that actually need schedule patching.
        events_need_patch: List[Dict[str, Any]] = []
        for e in events:
            start_ts = _as_utc(e["start_ts"])
            need = False
            for prefix in ("leg_a", "leg_b"):
                if (e.get(f"{prefix}_kind") or "").lower() != "perp":
                    continue
                if _needs_patch(
                    start_ts,
                    e.get(f"{prefix}_funding_interval_hours"),
                    e.get(f"{prefix}_next_funding_time"),
                    force=bool(args.force),
                ):
                    need = True
                    break
            if not need and _needs_patch(
                start_ts, e.get("funding_interval_hours"), e.get("next_funding_time"), force=bool(args.force)
            ):
                need = True
            if need:
                events_need_patch.append(e)

        if not events_need_patch:
            logger.info("no events need patching in window")
            return

        ranges = _group_ranges(events_need_patch)
        logger.info(
            "loaded events=%s need_patch=%s unique_pairs=%s window=%s..%s",
            len(events),
            len(events_need_patch),
            len(ranges),
            since.isoformat(),
            until.isoformat(),
        )

        funding_times_map: Dict[Tuple[str, str], List[datetime]] = {}
        for (ex, sym), r in sorted(ranges.items()):
            r2 = r.expanded(margin)
            normalized_sym = sym
            # for fetchers expecting <base>USDT, sym may already be base; keep both paths consistent
            fetch_sym = normalized_sym
            default_iv = derive_funding_interval_hours(ex, None, fallback=True)
            if default_iv is None:
                default_iv = DEFAULT_FUNDING_INTERVALS.get(ex)
            # If REST isn't supported, we will fall back later.
            if ex not in ("binance", "okx", "bybit", "bitget"):
                funding_times_map[(ex, sym)] = []
                continue
            try:
                times = client.fetch_funding_times(ex, fetch_sym, r2.start, r2.end)
                funding_times_map[(ex, sym)] = times
                if not times:
                    logger.warning("no funding history: %s %s range=%s..%s (fallback)", ex, sym, r2.start, r2.end)
                else:
                    logger.info("funding history: %s %s points=%s", ex, sym, len(times))
            except Exception as exc:
                logger.warning("funding history failed: %s %s err=%s (fallback)", ex, sym, exc)
                funding_times_map[(ex, sym)] = []

        scanned = 0
        patched = 0
        skipped = 0
        missing = 0

        for e in events_need_patch:
            scanned += 1
            eid = int(e["id"])
            start_ts = _as_utc(e["start_ts"])
            patch: Dict[str, Any] = {}

            perp_schedules: List[Tuple[Optional[float], Optional[datetime]]] = []
            for prefix in ("leg_a", "leg_b"):
                kind = (e.get(f"{prefix}_kind") or "").lower()
                if kind != "perp":
                    continue
                ex = str(e.get(f"{prefix}_exchange") or "").lower()
                sym = str(e.get(f"{prefix}_symbol") or e.get("symbol") or "").upper()
                if not ex or not sym:
                    continue

                default_iv = derive_funding_interval_hours(ex, None, fallback=True)
                times = funding_times_map.get((ex, sym), [])
                interval, next_ft = _schedule_at(start_ts, times, default_interval_hours=default_iv)
                if interval is None and default_iv is not None:
                    interval = float(default_iv)
                if next_ft is None and interval is not None:
                    next_ft = _ceil_next_boundary(start_ts, float(interval))

                perp_schedules.append((interval, next_ft))

                curr_iv = e.get(f"{prefix}_funding_interval_hours")
                curr_next = e.get(f"{prefix}_next_funding_time")
                if _needs_patch(start_ts, curr_iv, curr_next, force=bool(args.force)):
                    if interval is not None:
                        patch[f"{prefix}_funding_interval_hours"] = float(interval)
                    if next_ft is not None:
                        patch[f"{prefix}_next_funding_time"] = _as_utc(next_ft)

            # event-level schedule: choose earliest next_funding_time among perp legs
            next_candidates = [(iv, nft) for iv, nft in perp_schedules if nft is not None]
            if next_candidates:
                iv_sel, nft_sel = sorted(next_candidates, key=lambda x: x[1])[0]
                if _needs_patch(start_ts, e.get("funding_interval_hours"), e.get("next_funding_time"), force=bool(args.force)):
                    if iv_sel is not None:
                        patch["funding_interval_hours"] = float(iv_sel)
                    patch["next_funding_time"] = _as_utc(nft_sel)
            else:
                missing += 1

            if not patch:
                skipped += 1
                continue

            if args.dry_run:
                patched += 1
                if patched <= 10:
                    logger.info("dry-run patch event_id=%s start_ts=%s patch_keys=%s", eid, start_ts.isoformat(), sorted(patch.keys()))
                continue

            set_cols = sorted(patch.keys())
            assigns = ", ".join([f"{c}=%({c})s" for c in set_cols])
            sql = f"UPDATE watchlist.watch_signal_event SET {assigns} WHERE id=%(id)s"
            params = {**patch, "id": eid}
            pg_conn.execute(sql, params)
            patched += 1

            if patched % int(args.batch) == 0:
                logger.info("progress patched=%s scanned=%s skipped=%s missing=%s rest_calls=%s", patched, scanned, skipped, missing, client.calls)

        logger.info(
            "done scanned=%s patched=%s skipped=%s missing=%s rest_calls=%s dry_run=%s",
            scanned,
            patched,
            skipped,
            missing,
            client.calls,
            bool(args.dry_run),
        )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
from typing import Any, Dict, List, Optional, Tuple

import psycopg
import requests


DEFAULT_DSN = os.environ.get(
    "WATCHLIST_PG_DSN",
    "postgresql://wl_writer:wl_writer_A3f9xB2@127.0.0.1:5432/watchlist",
)


def _parse_iso(value: Any) -> Optional[datetime]:
    if value in (None, "", False):
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _ms_to_iso(value: Any) -> Optional[str]:
    try:
        if value in (None, "", 0):
            return None
        return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", False):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class PgRow:
    symbol: str
    ts: datetime
    interval_h: Optional[float]
    next_ft: Optional[datetime]


@dataclass(frozen=True)
class RestSchedule:
    interval_h: Optional[float]
    next_ft: Optional[datetime]


def _fetch_pg_latest_rows(dsn: str, exchange: str, minutes: int, limit: int) -> List[PgRow]:
    sql = """
        SELECT DISTINCT ON (symbol)
            symbol,
            ts,
            funding_interval_hours,
            next_funding_time
        FROM watchlist.watch_signal_raw
        WHERE exchange = %(exchange)s
          AND ts >= NOW() - (%(minutes)s || ' minutes')::interval
        ORDER BY symbol, ts DESC
        LIMIT %(limit)s
    """
    rows: List[PgRow] = []
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "exchange": exchange,
                    "minutes": int(minutes),
                    "limit": int(limit),
                },
            )
            for symbol, ts, interval_h, next_ft in cur.fetchall():
                ts_dt = ts if isinstance(ts, datetime) else _parse_iso(ts)
                if not ts_dt:
                    continue
                rows.append(
                    PgRow(
                        symbol=str(symbol),
                        ts=ts_dt.astimezone(timezone.utc),
                        interval_h=_safe_float(interval_h),
                        next_ft=_parse_iso(next_ft),
                    )
                )
    return rows


def _fetch_pg_latest_snapshot_rows(dsn: str, snapshot_exchange: str, minutes: int, limit: int) -> List[PgRow]:
    sql = """
        SELECT DISTINCT ON (symbol)
            symbol,
            ts,
            NULLIF((meta->'snapshots'->%(snapshot_exchange)s->>'funding_interval_hours'), '')::double precision AS interval_h,
            (meta->'snapshots'->%(snapshot_exchange)s->>'next_funding_time') AS next_ft
        FROM watchlist.watch_signal_raw
        WHERE exchange = 'multi'
          AND meta ? 'snapshots'
          AND (meta->'snapshots') ? %(snapshot_exchange)s
          AND (
                (meta->'snapshots'->%(snapshot_exchange)s->>'funding_rate') IS NOT NULL
             OR (meta->'snapshots'->%(snapshot_exchange)s->>'futures_price') IS NOT NULL
          )
          AND ts >= NOW() - (%(minutes)s || ' minutes')::interval
        ORDER BY symbol, ts DESC
        LIMIT %(limit)s
    """
    out: List[PgRow] = []
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "snapshot_exchange": snapshot_exchange,
                    "minutes": int(minutes),
                    "limit": int(limit),
                },
            )
            for symbol, ts, interval_h, next_ft in cur.fetchall():
                ts_dt = ts if isinstance(ts, datetime) else _parse_iso(ts)
                if not ts_dt:
                    continue
                out.append(
                    PgRow(
                        symbol=str(symbol),
                        ts=ts_dt.astimezone(timezone.utc),
                        interval_h=_safe_float(interval_h),
                        next_ft=_parse_iso(next_ft),
                    )
                )
    return out


def _req_json(url: str, timeout: int = 10) -> Any:
    headers = {"User-Agent": "FR_Monitor audit/1.0", "Accept": "application/json"}
    resp = requests.get(url, timeout=timeout, headers=headers)
    resp.raise_for_status()
    return resp.json()


def _rest_binance(symbol: str) -> RestSchedule:
    symbol = symbol.upper()
    premium = _req_json(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}USDT")
    next_ft = _parse_iso(_ms_to_iso(premium.get("nextFundingTime")))

    funding_info = _req_json("https://fapi.binance.com/fapi/v1/fundingInfo")
    interval_h: Optional[float] = None
    if isinstance(funding_info, list):
        for entry in funding_info:
            if (entry.get("symbol") or "").upper() == f"{symbol}USDT":
                interval_h = _safe_float(entry.get("fundingIntervalHours"))
                break

    return RestSchedule(interval_h=interval_h, next_ft=next_ft)


def _rest_okx(symbol: str) -> RestSchedule:
    symbol = symbol.upper()
    payload = _req_json(f"https://www.okx.com/api/v5/public/funding-rate?instId={symbol}-USDT-SWAP")
    interval_h: Optional[float] = None
    next_ft: Optional[datetime] = None
    if isinstance(payload, dict) and payload.get("code") == "0":
        data = payload.get("data") or []
        if data and isinstance(data[0], dict):
            entry = data[0]
            next_ft = _parse_iso(_ms_to_iso(entry.get("nextFundingTime")))
            ft = _parse_iso(_ms_to_iso(entry.get("fundingTime")))
            if ft and next_ft:
                interval_h = (next_ft - ft).total_seconds() / 3600.0
    return RestSchedule(interval_h=interval_h, next_ft=next_ft)


def _rest_bybit(symbol: str) -> RestSchedule:
    symbol = symbol.upper()
    payload = _req_json(f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}USDT")
    interval_h: Optional[float] = None
    next_ft: Optional[datetime] = None
    if isinstance(payload, dict) and payload.get("retCode") == 0:
        items = payload.get("result", {}).get("list") or []
        if items and isinstance(items[0], dict):
            entry = items[0]
            next_ft = _parse_iso(_ms_to_iso(entry.get("nextFundingTime")))
            interval_h = _safe_float(entry.get("fundingIntervalHour"))
    return RestSchedule(interval_h=interval_h, next_ft=next_ft)


def _rest_bitget(symbol: str) -> RestSchedule:
    symbol = symbol.upper()
    payload = _req_json(
        "https://api.bitget.com/api/v2/mix/market/current-fund-rate"
        f"?productType=USDT-FUTURES&symbol={symbol}USDT"
    )
    interval_h: Optional[float] = None
    next_ft: Optional[datetime] = None
    if isinstance(payload, dict) and payload.get("code") == "00000":
        data = payload.get("data") or []
        if data and isinstance(data[0], dict):
            entry = data[0]
            next_ft = _parse_iso(_ms_to_iso(entry.get("nextUpdate")))
            interval_h = _safe_float(entry.get("fundingRateInterval"))
    return RestSchedule(interval_h=interval_h, next_ft=next_ft)


def _rule_schedule(exchange: str) -> RestSchedule:
    ex = (exchange or "").lower()
    now = datetime.now(timezone.utc)
    if ex in ("lighter", "hyperliquid"):
        interval_h = 1.0
        next_ft = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return RestSchedule(interval_h=interval_h, next_ft=next_ft)
    if ex == "grvt":
        interval_h = 8.0
        floored = now.replace(minute=0, second=0, microsecond=0)
        base_hour = (floored.hour // 8) * 8
        base = floored.replace(hour=base_hour)
        next_ft = base + timedelta(hours=8)
        if next_ft <= now:
            next_ft = next_ft + timedelta(hours=8)
        return RestSchedule(interval_h=interval_h, next_ft=next_ft)
    return RestSchedule(interval_h=None, next_ft=None)


def _compare(
    pg: PgRow, rest: RestSchedule, *, next_tolerance_seconds: int, interval_tolerance_hours: float
) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    if pg.interval_h is None or pg.interval_h <= 0:
        issues.append("pg.interval_h missing")
    if pg.next_ft is None:
        issues.append("pg.next_ft missing")
    if rest.interval_h is None or rest.interval_h <= 0:
        issues.append("rest.interval_h missing")
    if rest.next_ft is None:
        issues.append("rest.next_ft missing")

    if pg.interval_h is not None and rest.interval_h is not None:
        if abs(pg.interval_h - rest.interval_h) > interval_tolerance_hours:
            issues.append(f"interval mismatch pg={pg.interval_h} rest={rest.interval_h}")

    if pg.next_ft is not None and rest.next_ft is not None:
        delta = abs((pg.next_ft - rest.next_ft).total_seconds())
        if delta > float(next_tolerance_seconds):
            issues.append(f"next_ft mismatch delta_s={int(delta)} pg={pg.next_ft.isoformat()} rest={rest.next_ft.isoformat()}")

    ok = not issues
    return ok, issues


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--minutes", type=int, default=15)
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--next-tolerance-seconds", type=int, default=90)
    parser.add_argument("--interval-tolerance-hours", type=float, default=0.25)
    args = parser.parse_args()

    exchanges = [
        ("binance", _rest_binance),
        ("okx", _rest_okx),
        ("bybit", _rest_bybit),
        ("bitget", _rest_bitget),
    ]

    for exchange, fetcher in exchanges:
        rows = _fetch_pg_latest_rows(args.dsn, exchange, args.minutes, args.limit)
        print(f"\n== {exchange} (pg rows={len(rows)}) ==")
        bad = 0
        for row in rows:
            try:
                rest = fetcher(row.symbol)
            except Exception as exc:
                bad += 1
                print(f"- {row.symbol}: REST error: {exc}")
                continue
            ok, issues = _compare(
                row,
                rest,
                next_tolerance_seconds=args.next_tolerance_seconds,
                interval_tolerance_hours=args.interval_tolerance_hours,
            )
            if not ok:
                bad += 1
                print(f"- {row.symbol}: " + "; ".join(issues))
        print(f"bad={bad} / total={len(rows)}")

    snapshot_exchanges = ["binance", "okx", "bybit", "bitget", "hyperliquid", "lighter", "grvt"]
    for snapshot_exchange in snapshot_exchanges:
        rows = _fetch_pg_latest_snapshot_rows(args.dsn, snapshot_exchange, args.minutes, args.limit)
        print(f"\n== snapshots.{snapshot_exchange} (pg rows={len(rows)}) ==")
        bad = 0
        fetcher = {
            "binance": _rest_binance,
            "okx": _rest_okx,
            "bybit": _rest_bybit,
            "bitget": _rest_bitget,
        }.get(snapshot_exchange)
        for row in rows:
            try:
                rest = fetcher(row.symbol) if fetcher else _rule_schedule(snapshot_exchange)
            except Exception as exc:
                bad += 1
                print(f"- {row.symbol}: REST error: {exc}")
                continue
            ok, issues = _compare(
                row,
                rest,
                next_tolerance_seconds=args.next_tolerance_seconds,
                interval_tolerance_hours=args.interval_tolerance_hours,
            )
            if not ok:
                bad += 1
                print(f"- {row.symbol}: " + "; ".join(issues))
        print(f"bad={bad} / total={len(rows)}")

    print("\nNote: lighter/hyperliquid/grvt schedule is rule/SDK based in this audit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

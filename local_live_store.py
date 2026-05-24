from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_DB_PATH = Path(__file__).resolve().parent / "runtime" / "live_trading.sqlite3"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path else DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS live_trade_signal (
          id INTEGER PRIMARY KEY,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at TEXT NOT NULL DEFAULT (datetime('now')),
          event_id INTEGER,
          symbol TEXT NOT NULL,
          signal_type TEXT NOT NULL,
          horizon_min INTEGER,
          pnl_hat REAL,
          win_prob REAL,
          pnl_hat_ob REAL,
          win_prob_ob REAL,
          pred_source TEXT,
          leg_long_exchange TEXT,
          leg_short_exchange TEXT,
          status TEXT NOT NULL DEFAULT 'new',
          reason TEXT,
          payload_json TEXT,
          opened_at TEXT,
          closed_at TEXT,
          close_reason TEXT,
          realized_pnl_usdt REAL,
          funding_pnl_usdt REAL,
          fee_pnl_usdt REAL,
          fee_complete INTEGER NOT NULL DEFAULT 0,
          net_pnl_usdt REAL,
          archive_report TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_live_trade_signal_created ON live_trade_signal(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_live_trade_signal_status ON live_trade_signal(status, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_live_trade_signal_symbol ON live_trade_signal(symbol, created_at DESC);

        CREATE TABLE IF NOT EXISTS live_trade_order (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          signal_id INTEGER NOT NULL REFERENCES live_trade_signal(id) ON DELETE CASCADE,
          action TEXT NOT NULL,
          leg TEXT NOT NULL,
          exchange TEXT NOT NULL,
          side TEXT,
          market_type TEXT NOT NULL DEFAULT 'perp',
          filled_qty REAL,
          avg_price REAL,
          cum_quote REAL,
          fee_usdt REAL,
          fee_currency TEXT,
          exchange_order_id TEXT,
          client_order_id TEXT,
          status TEXT,
          raw_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_live_trade_order_signal ON live_trade_order(signal_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS live_trade_spread_sample (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT (datetime('now')),
          signal_id INTEGER REFERENCES live_trade_signal(id) ON DELETE CASCADE,
          symbol TEXT,
          spread_metric REAL,
          pnl_spread REAL,
          decision TEXT,
          context_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_live_trade_spread_signal ON live_trade_spread_sample(signal_id, ts DESC);

        CREATE TABLE IF NOT EXISTS live_trade_error (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT (datetime('now')),
          signal_id INTEGER,
          stage TEXT,
          error_type TEXT,
          message TEXT,
          context_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_live_trade_error_ts ON live_trade_error(ts DESC);

        CREATE TABLE IF NOT EXISTS live_trade_balance_snapshot (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT (datetime('now')),
          source TEXT NOT NULL DEFAULT 'manual',
          balances_json TEXT,
          totals_json TEXT,
          context_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_live_trade_balance_ts ON live_trade_balance_snapshot(ts DESC);
        """
    )
    conn.commit()


def initialize(db_path: Optional[str] = None) -> str:
    path = str(Path(db_path) if db_path else DEFAULT_DB_PATH)
    with connect(path) as conn:
        ensure_schema(conn)
        seed_archive(conn)
    return path


def seed_archive(conn: sqlite3.Connection) -> None:
    signal = {
        "id": 970,
        "created_at": "2025-12-31T22:26:40.302761+00:00",
        "updated_at": utc_now_iso(),
        "event_id": 30354,
        "symbol": "RIVER",
        "signal_type": "B",
        "horizon_min": 1440,
        "pred_source": "archive_report",
        "leg_long_exchange": "bitget",
        "leg_short_exchange": "bybit",
        "status": "closed",
        "reason": "archive_only_pg_disabled",
        "opened_at": "2025-12-31T22:26:40.302761+00:00",
        "closed_at": "2025-12-31T22:26:44.522189+00:00",
        "close_reason": "archive_audit",
        "realized_pnl_usdt": -0.05759999999999507,
        "funding_pnl_usdt": 0.0,
        "fee_pnl_usdt": None,
        "fee_complete": 0,
        "net_pnl_usdt": None,
        "archive_report": "reports/live_trading_audit_970.md",
        "payload_json": json.dumps({"source": "reports/live_trading_audit_970.md"}, ensure_ascii=False),
    }
    cols = ", ".join(signal.keys())
    placeholders = ", ".join([":" + k for k in signal.keys()])
    update_cols = ", ".join([f"{k}=excluded.{k}" for k in signal.keys() if k != "id"])
    conn.execute(
        f"INSERT INTO live_trade_signal ({cols}) VALUES ({placeholders}) ON CONFLICT(id) DO UPDATE SET {update_cols};",
        signal,
    )
    orders = [
        ("2025-12-31T22:26:39.194265+00:00", "open", "long", "bitget", "buy", 4.0, 10.168, 40.672, None, None, "1390386937834676226", "wl30354O1767219997-L", "filled"),
        ("2025-12-31T22:26:40.191275+00:00", "open", "short", "bybit", "sell", 4.4, 11.25, 49.5, None, None, "4d2cc962-50b0-4369-b974-20c4fb3b379d", "wl30354O1767219997-S", "Filled"),
        ("2025-12-31T22:26:43.997955+00:00", "close", "long", "bitget", "sell", 4.0, 10.147, 40.588, None, None, "1390386959196266497", "wl970C1767220003-L", "filled"),
        ("2025-12-31T22:26:44.123962+00:00", "close", "short", "bybit", "buy", 4.4, 11.244, 49.4736, None, None, "cc900751-be82-48d8-bcb7-e9d99b81afb3", "wl970C1767220003-S", "Filled"),
    ]
    for row in orders:
        conn.execute(
            """
            INSERT INTO live_trade_order(
              signal_id, created_at, action, leg, exchange, side, market_type,
              filled_qty, avg_price, cum_quote, fee_usdt, fee_currency,
              exchange_order_id, client_order_id, status
            )
            SELECT ?, ?, ?, ?, ?, ?, 'perp', ?, ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
              SELECT 1 FROM live_trade_order WHERE signal_id=? AND client_order_id=? AND action=? AND leg=?
            );
            """,
            (970, *row, 970, row[11], row[1], row[2]),
        )
    conn.commit()


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    d = dict(row)
    if "payload_json" in d:
        raw = d.pop("payload_json")
        try:
            d["payload"] = json.loads(raw) if raw else None
        except Exception:
            d["payload"] = raw
    d["fee_complete"] = bool(d.get("fee_complete"))
    return d


def list_signals(limit: int = 200, db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    initialize(db_path)
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM live_trade_signal ORDER BY created_at DESC LIMIT ?;",
            (int(limit),),
        ).fetchall()
    signals = [_row_to_dict(r) for r in rows]
    attach_order_metrics(signals, db_path)
    return signals


def attach_order_metrics(signals: List[Dict[str, Any]], db_path: Optional[str] = None) -> None:
    if not signals:
        return
    ids = [int(s["id"]) for s in signals if s.get("id") is not None]
    if not ids:
        return
    qmarks = ",".join("?" for _ in ids)
    with connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT * FROM live_trade_order WHERE signal_id IN ({qmarks});",
            ids,
        ).fetchall()
    by_key: Dict[tuple, sqlite3.Row] = {}
    for r in rows:
        by_key[(int(r["signal_id"]), str(r["action"]), str(r["leg"]))] = r
    for s in signals:
        sid = int(s["id"])
        for action in ("open", "close"):
            for leg in ("long", "short"):
                r = by_key.get((sid, action, leg))
                if not r:
                    continue
                prefix = f"{action}_{leg}"
                s[f"{prefix}_avg_price"] = r["avg_price"]
                s[f"{prefix}_filled_qty"] = r["filled_qty"]
                s[f"{prefix}_exchange"] = r["exchange"]
                s[f"{prefix}_status"] = r["status"]


def pnl_points(granularity: str = "day", db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    initialize(db_path)
    fmt_len = 13 if granularity == "hour" else 10
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT substr(COALESCE(closed_at, created_at), 1, ?) AS bucket,
                   count(*) AS closed_count,
                   sum(realized_pnl_usdt) AS realized_pnl_usdt,
                   sum(funding_pnl_usdt) AS funding_pnl_usdt,
                   sum(CASE WHEN fee_complete=1 THEN fee_pnl_usdt ELSE NULL END) AS fee_pnl_usdt,
                   sum(CASE WHEN fee_complete=1 THEN net_pnl_usdt ELSE NULL END) AS net_pnl_usdt
              FROM live_trade_signal
             WHERE status='closed'
             GROUP BY 1
             ORDER BY 1 ASC;
            """,
            (fmt_len,),
        ).fetchall()
    return [dict(r) for r in rows]


def summary(db_path: Optional[str] = None) -> Dict[str, Any]:
    initialize(db_path)
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT count(*) AS strict_live_trade_count,
                   sum(CASE WHEN status='closed' THEN 1 ELSE 0 END) AS closed_count,
                   sum(realized_pnl_usdt) AS realized_pnl_usdt_sum,
                   sum(funding_pnl_usdt) AS funding_pnl_usdt_sum,
                   min(fee_complete) AS all_fee_complete,
                   sum(CASE WHEN fee_complete=1 THEN net_pnl_usdt ELSE NULL END) AS net_pnl_usdt_sum
              FROM live_trade_signal;
            """
        ).fetchone()
    out = dict(row or {})
    out["fee_coverage_complete"] = bool(out.pop("all_fee_complete") or False)
    if not out["fee_coverage_complete"]:
        out["net_pnl_usdt_sum"] = None
    return out


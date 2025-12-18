from __future__ import annotations

import argparse
import csv
import math
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from orderbook_utils import fetch_orderbook_prices


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return "-"
    return dt.isoformat(timespec="seconds")


def _safe_float(val: Any) -> Optional[float]:
    try:
        f = float(val)
        if math.isfinite(f):
            return f
    except Exception:
        return None
    return None


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _spread_abs(a: float, b: float) -> float:
    base = min(a, b)
    return abs(a - b) / base if base else 0.0


def _log_spread(high: float, low: float) -> float:
    if high <= 0 or low <= 0:
        return 0.0
    return float(math.log(high / low))


@dataclass(frozen=True)
class DbLast:
    ts: Optional[datetime]
    futures_price: Optional[float]
    mark_price: Optional[float]
    index_price: Optional[float]


def _db_latest_price(conn: sqlite3.Connection, *, exchange: str, symbol: str) -> DbLast:
    row = conn.execute(
        """
        SELECT timestamp, futures_price, mark_price, index_price
          FROM price_data
         WHERE exchange = ? AND symbol = ?
         ORDER BY timestamp DESC
         LIMIT 1
        """,
        (exchange, symbol),
    ).fetchone()
    if not row:
        return DbLast(None, None, None, None)
    ts_raw, fut, mark, index_p = row
    return DbLast(_parse_ts(ts_raw), _safe_float(fut), _safe_float(mark), _safe_float(index_p))


def _db_latest_1min_close(conn: sqlite3.Connection, *, exchange: str, symbol: str) -> Tuple[Optional[datetime], Optional[float]]:
    row = conn.execute(
        """
        SELECT timestamp, futures_price_close
          FROM price_data_1min
         WHERE exchange = ? AND symbol = ?
         ORDER BY timestamp DESC
         LIMIT 1
        """,
        (exchange, symbol),
    ).fetchone()
    if not row:
        return None, None
    ts_raw, fut_close = row
    return _parse_ts(ts_raw), _safe_float(fut_close)


def _render_md(
    *,
    rows: List[Dict[str, Any]],
    exchanges: Tuple[str, str],
    symbol: str,
    notional: float,
    interval_s: float,
    duration_s: float,
) -> str:
    ex_a, ex_b = exchanges
    header = [
        "# REST 订单簿 vs 本地SQLite 价格点对比（2min）",
        "",
        f"- Symbol: `{symbol}`",
        f"- Exchanges: `{ex_a}` vs `{ex_b}`",
        f"- Orderbook sweep notional: `{notional} USDT`/腿",
        f"- Sampling: 每 `{interval_s:.1f}s` 一次，持续 `{duration_s:.0f}s`",
        "",
        "## 结论摘要（快速看）",
    ]

    def _stale_stats(exchange: str) -> Tuple[int, float, float]:
        stales: List[float] = []
        for r in rows:
            v = r.get(f"db_stale_s__{exchange}")
            if v is None:
                continue
            stales.append(float(v))
        if not stales:
            return 0, float("nan"), float("nan")
        return len(stales), max(stales), sum(stales) / len(stales)

    n_a, max_a, avg_a = _stale_stats(ex_a)
    n_b, max_b, avg_b = _stale_stats(ex_b)

    header += [
        f"- `{ex_a}` DB 最新价格点延迟：max `{max_a:.1f}s` / avg `{avg_a:.1f}s`（样本 `{n_a}`）",
        f"- `{ex_b}` DB 最新价格点延迟：max `{max_b:.1f}s` / avg `{avg_b:.1f}s`（样本 `{n_b}`）",
        "- 说明：DB timestamp 是本机写库时刻（非交易所撮合时间），所以“延迟”包含采集周期 + 写库周期 + 时钟偏差。",
        "",
        "## 样本（前 20 条）",
        "",
        "|ts|ob_buy_A|ob_sell_A|ob_buy_B|ob_sell_B|ob_spread(tradable)|db_fut_A(ts)|db_fut_B(ts)|db_spread(abs)|备注|",
        "|-|-:|-:|-:|-:|-:|-|-|-:|-|",
    ]

    lines = header
    for r in rows[:20]:
        ts = r["ts"]
        ob_buy_a = r.get(f"ob_buy__{ex_a}")
        ob_sell_a = r.get(f"ob_sell__{ex_a}")
        ob_buy_b = r.get(f"ob_buy__{ex_b}")
        ob_sell_b = r.get(f"ob_sell__{ex_b}")
        ob_spread = r.get("ob_tradable_spread")
        db_a = r.get(f"db_fut__{ex_a}")
        db_b = r.get(f"db_fut__{ex_b}")
        db_ts_a = r.get(f"db_ts__{ex_a}")
        db_ts_b = r.get(f"db_ts__{ex_b}")
        db_spread = r.get("db_abs_spread")
        note = r.get("note") or "-"
        lines.append(
            f"|{ts}|{ob_buy_a or '-'}|{ob_sell_a or '-'}|{ob_buy_b or '-'}|{ob_sell_b or '-'}|{ob_spread or '-'}|"
            f"{db_a or '-'} ({db_ts_a})|{db_b or '-'} ({db_ts_b})|{db_spread or '-'}|{note}|"
        )

    lines += [
        "",
        "## 你可以重点关注的异常模式",
        "- **DB 价格长时间不变 / timestamp 不更新**：可能是采集线程卡住、单交易所 WS/REST 掉线、或写库失败。",
        "- **DB futures_price 与订单簿 mid/buy/sell 长期偏离**：可能是价格口径不同（last/mark/index）或数据源异常点（单笔成交打点）。",
        "- **订单簿可成交价差很小，但 DB abs_spread 很大**：很可能是 DB 价格点来自 last/mark 的瞬时异常，而订单簿代表真实可成交性。",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="market_data.db")
    p.add_argument("--symbol", default="ETH")
    p.add_argument("--ex_a", default="bybit")
    p.add_argument("--ex_b", default="okx")
    p.add_argument("--market_type", default="perp", choices=("perp", "spot"))
    p.add_argument("--notional", type=float, default=50.0)
    p.add_argument("--interval", type=float, default=5.0)
    p.add_argument("--duration", type=float, default=120.0)
    p.add_argument("--out_dir", default="reports")
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"db not found: {db_path}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    symbol = str(args.symbol).upper()
    ex_a = str(args.ex_a).lower()
    ex_b = str(args.ex_b).lower()

    started = _utcnow()
    rows: List[Dict[str, Any]] = []

    conn = sqlite3.connect(str(db_path), timeout=10.0)
    try:
        end_t = time.time() + float(args.duration)
        while time.time() < end_t:
            ts = _utcnow()
            ob_a = fetch_orderbook_prices(ex_a, symbol, args.market_type, notional=float(args.notional)) or {}
            ob_b = fetch_orderbook_prices(ex_b, symbol, args.market_type, notional=float(args.notional)) or {}

            db_a = _db_latest_price(conn, exchange=ex_a, symbol=symbol)
            db_b = _db_latest_price(conn, exchange=ex_b, symbol=symbol)
            db1_a_ts, db1_a_close = _db_latest_1min_close(conn, exchange=ex_a, symbol=symbol)
            db1_b_ts, db1_b_close = _db_latest_1min_close(conn, exchange=ex_b, symbol=symbol)

            note = None
            if ob_a.get("error") or ob_b.get("error"):
                note = f"orderbook_error A={ob_a.get('error')} B={ob_b.get('error')}"

            # Tradable spread for two-leg: sell high (bids) vs buy low (asks)
            ob_tradable_spread = None
            ob_entry_log = None
            if not note:
                # direction A short / B long
                a_sell = _safe_float(ob_a.get("sell"))
                a_buy = _safe_float(ob_a.get("buy"))
                b_sell = _safe_float(ob_b.get("sell"))
                b_buy = _safe_float(ob_b.get("buy"))
                if a_sell and b_buy and a_sell > b_buy:
                    ob_tradable_spread = (a_sell - b_buy) / b_buy
                    ob_entry_log = _log_spread(a_sell, b_buy)
                elif b_sell and a_buy and b_sell > a_buy:
                    ob_tradable_spread = (b_sell - a_buy) / a_buy
                    ob_entry_log = _log_spread(b_sell, a_buy)
                else:
                    ob_tradable_spread = 0.0
                    ob_entry_log = 0.0

            db_abs_spread = None
            if db_a.futures_price and db_b.futures_price:
                db_abs_spread = _spread_abs(float(db_a.futures_price), float(db_b.futures_price))

            def _stale_seconds(last_ts: Optional[datetime]) -> Optional[float]:
                if not last_ts:
                    return None
                return max(0.0, (ts - last_ts).total_seconds())

            rows.append(
                {
                    "ts": ts.isoformat(timespec="seconds"),
                    f"ob_buy__{ex_a}": _safe_float(ob_a.get("buy")),
                    f"ob_sell__{ex_a}": _safe_float(ob_a.get("sell")),
                    f"ob_mid__{ex_a}": _safe_float(ob_a.get("mid")),
                    f"ob_buy__{ex_b}": _safe_float(ob_b.get("buy")),
                    f"ob_sell__{ex_b}": _safe_float(ob_b.get("sell")),
                    f"ob_mid__{ex_b}": _safe_float(ob_b.get("mid")),
                    "ob_tradable_spread": _safe_float(ob_tradable_spread),
                    "ob_entry_log": _safe_float(ob_entry_log),
                    f"db_ts__{ex_a}": _fmt_dt(db_a.ts),
                    f"db_fut__{ex_a}": db_a.futures_price,
                    f"db_mark__{ex_a}": db_a.mark_price,
                    f"db_index__{ex_a}": db_a.index_price,
                    f"db_stale_s__{ex_a}": _safe_float(_stale_seconds(db_a.ts)),
                    f"db_ts__{ex_b}": _fmt_dt(db_b.ts),
                    f"db_fut__{ex_b}": db_b.futures_price,
                    f"db_mark__{ex_b}": db_b.mark_price,
                    f"db_index__{ex_b}": db_b.index_price,
                    f"db_stale_s__{ex_b}": _safe_float(_stale_seconds(db_b.ts)),
                    f"db1_ts__{ex_a}": _fmt_dt(db1_a_ts),
                    f"db1_close__{ex_a}": db1_a_close,
                    f"db1_ts__{ex_b}": _fmt_dt(db1_b_ts),
                    f"db1_close__{ex_b}": db1_b_close,
                    "db_abs_spread": _safe_float(db_abs_spread),
                    "note": note,
                }
            )

            time.sleep(max(0.1, float(args.interval)))
    finally:
        conn.close()

    ended = _utcnow()
    stem = f"rest_vs_db_{symbol}_{ex_a}_{ex_b}_{started.strftime('%Y%m%d_%H%M%S')}"
    csv_path = out_dir / f"{stem}.csv"
    md_path = out_dir / f"{stem}.md"

    # CSV
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sorted({k for r in rows for k in r.keys()}))
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    # Markdown
    md = _render_md(
        rows=rows,
        exchanges=(ex_a, ex_b),
        symbol=symbol,
        notional=float(args.notional),
        interval_s=float(args.interval),
        duration_s=float(args.duration),
    )
    md_path.write_text(md, encoding="utf-8")

    print(f"OK: wrote {csv_path}")
    print(f"OK: wrote {md_path}")
    print(f"window: {started.isoformat(timespec='seconds')} -> {ended.isoformat(timespec='seconds')}  samples={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


"""
Hyperliquid WebSocket client for streaming perp mid prices.
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import websocket


class HyperliquidMarketWebSocket:
    """
    Lightweight manager that mirrors websocket.WebSocketApp usage so it can
    be driven by ExchangeDataCollector._connect_with_retry.

    使用方式：官方 WebSocket `wss://api.hyperliquid.xyz/ws` 只需订阅
    `{"method":"subscribe","subscription":{"type":"allMids"}}`，即可得到所有
    永续 mid 价格，本类只做解包并回调核心 collector。
    """

    def __init__(
        self,
        url: str,
        symbols: Optional[List[str]],
        on_ticker: Callable[[Dict[str, Any]], None],
    ):
        self.url = url
        self.symbols = sorted({sym.upper() for sym in (symbols or [])})
        self._on_ticker = on_ticker

        self._ws: Optional[websocket.WebSocketApp] = None
        self._closing = threading.Event()
        self._ping_thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger("HyperliquidMarketWebSocket")

    def run_forever(self):
        """Start the WebSocket loop and block until it terminates."""
        self._closing.clear()
        self._ws = websocket.WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.logger.info("Connecting to Hyperliquid WS: %s", self.url)
        self._ws.run_forever()

    def close(self):
        """Request the underlying socket to close."""
        self._closing.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        if self._ping_thread and self._ping_thread.is_alive():
            self._ping_thread.join(timeout=1.0)

    # ------------------------------------------------------------------ #
    # Internal callbacks
    # ------------------------------------------------------------------ #

    def _on_open(self, ws):
        self.logger.info("Hyperliquid WebSocket connected")
        self._start_ping(ws)
        self._subscribe(ws)

    def _on_message(self, ws, message: str):
        try:
            payload = json.loads(message)
        except Exception:
            self.logger.debug("收到非JSON消息: %s", message)
            return

        channel = payload.get('channel')
        if channel == 'pong':
            return
        if channel == 'allMids' and isinstance(payload.get('data'), dict):
            self._handle_all_mids(payload['data'])
            return
        if payload.get('status') == 'success' and payload.get('type') == 'subscribe':
            self.logger.debug("订阅返回: %s", payload)
            return
        if payload.get('error'):
            self.logger.warning("Hyperliquid WS错误: %s", payload)

    def _on_error(self, ws, error):
        if not self._closing.is_set():
            self.logger.error("Hyperliquid WebSocket错误: %s", error)

    def _on_close(self, ws, status_code, msg):
        self.logger.warning("Hyperliquid WebSocket关闭: %s %s", status_code, msg)
        self._stop_ping()

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _start_ping(self, ws):
        def _run():
            while not self._closing.is_set():
                try:
                    ws.send(json.dumps({"method": "ping"}))
                except Exception:
                    break
                time.sleep(40)

        self._ping_thread = threading.Thread(target=_run, daemon=True)
        self._ping_thread.start()

    def _stop_ping(self):
        self._closing.set()
        if self._ping_thread and self._ping_thread.is_alive():
            self._ping_thread.join(timeout=1.0)
        self._ping_thread = None

    def _subscribe(self, ws):
        if not self.symbols:
            self.logger.info("Hyperliquid订阅全部全局mid价格")
        payload = {"method": "subscribe", "subscription": {"type": "allMids"}}
        ws.send(json.dumps(payload))

    def _handle_all_mids(self, data: Dict[str, Any]):
        timestamp = datetime.now(timezone.utc).isoformat()
        symbol_filter = set(self.symbols)
        for base, raw_price in data.items():
            base_upper = (base or '').upper()
            if not base_upper or '/' in base_upper or base_upper.startswith('@'):
                continue
            if symbol_filter and base_upper not in symbol_filter:
                continue
            try:
                price = float(raw_price)
            except (TypeError, ValueError):
                continue

            payload = {
                'symbol': base_upper,
                'price': price,
                'timestamp': timestamp,
                'market_type': 'futures',
                'instrument': f"{base_upper}-PERP",
            }
            try:
                self._on_ticker(payload)
            except Exception as exc:
                self.logger.error("处理Hyperliquid ticker出错: %s", exc)

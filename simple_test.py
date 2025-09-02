#!/usr/bin/env python3
"""
简化版WebSocket连接测试
"""

import websocket
import json
import time

def test_binance_connection():
    """测试Binance WebSocket连接"""
    print("测试Binance WebSocket连接...")
    
    def on_message(ws, message):
        data = json.loads(message)
        print(f"收到Binance数据: {data}")
        ws.close()
    
    def on_error(ws, error):
        print(f"Binance连接错误: {error}")
    
    def on_close(ws, close_status_code, close_msg):
        print(f"Binance连接关闭: {close_status_code} - {close_msg}")
    
    def on_open(ws):
        print("Binance连接已建立")
    
    # 测试单个币种
    url = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
    ws = websocket.WebSocketApp(url,
                               on_message=on_message,
                               on_error=on_error,
                               on_close=on_close,
                               on_open=on_open)
    
    print(f"连接URL: {url}")
    ws.run_forever()

def test_okx_connection():
    """测试OKX WebSocket连接"""
    print("\n测试OKX WebSocket连接...")
    
    def on_message(ws, message):
        data = json.loads(message)
        print(f"收到OKX数据: {data}")
        ws.close()
    
    def on_error(ws, error):
        print(f"OKX连接错误: {error}")
    
    def on_close(ws, close_status_code, close_msg):
        print(f"OKX连接关闭: {close_status_code} - {close_msg}")
    
    def on_open(ws):
        print("OKX连接已建立")
        # 订阅BTC-USDT数据
        subscribe_msg = {
            "op": "subscribe",
            "args": [
                {"channel": "tickers", "instId": "BTC-USDT"},
                {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
                {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"}
            ]
        }
        ws.send(json.dumps(subscribe_msg))
        print("已发送订阅请求")
    
    url = "wss://ws.okx.com:8443/ws/v5/public"
    ws = websocket.WebSocketApp(url,
                               on_message=on_message,
                               on_error=on_error,
                               on_close=on_close,
                               on_open=on_open)
    
    print(f"连接URL: {url}")
    ws.run_forever()

if __name__ == "__main__":
    print("开始WebSocket连接测试")
    test_binance_connection()
    time.sleep(2)
    test_okx_connection()
#!/usr/bin/env python3
"""
测试多币种WebSocket连接
"""

import websocket
import json
import time
from config import SUPPORTED_SYMBOLS

def test_binance_multi_symbols():
    """测试Binance多币种连接"""
    print("测试Binance多币种连接...")
    
    # 选择前50个币种进行测试
    test_symbols = SUPPORTED_SYMBOLS[:50]
    
    def on_message(ws, message):
        try:
            data = json.loads(message)
            if 'stream' in data:
                symbol = data['stream'].split('@')[0].upper()
                print(f"收到 {symbol} 数据")
        except:
            pass
    
    def on_error(ws, error):
        print(f"连接错误: {error}")
    
    def on_close(ws, close_status_code, close_msg):
        print(f"连接关闭")
    
    def on_open(ws):
        print("Binance多币种连接已建立")
    
    # 创建组合stream URL
    streams = [f"{symbol.lower()}usdt@ticker" for symbol in test_symbols]
    stream_path = '/'.join(streams)
    url = f"wss://stream.binance.com:9443/ws/{stream_path}"
    
    print(f"连接 {len(test_symbols)} 个币种")
    print(f"URL长度: {len(url)} 字符")
    
    ws = websocket.WebSocketApp(url,
                               on_message=on_message,
                               on_error=on_error,
                               on_close=on_close,
                               on_open=on_open)
    
    # 运行10秒测试
    print("运行10秒测试...")
    ws.run_forever()

def test_okx_multi_symbols():
    """测试OKX多币种连接"""
    print("\n测试OKX多币种连接...")
    
    # 选择前20个币种进行测试
    test_symbols = SUPPORTED_SYMBOLS[:20]
    
    received_data = {}
    
    def on_message(ws, message):
        try:
            data = json.loads(message)
            if 'event' in data and data['event'] == 'subscribe':
                print(f"订阅成功: {data.get('arg', {})}")
            elif 'data' in data:
                print(f"收到数据")
                received_data['count'] = received_data.get('count', 0) + 1
        except:
            pass
    
    def on_error(ws, error):
        print(f"连接错误: {error}")
    
    def on_close(ws, close_status_code, close_msg):
        print(f"连接关闭，收到 {received_data.get('count', 0)} 条数据")
    
    def on_open(ws):
        print("OKX连接已建立")
        
        # 创建订阅参数
        args = []
        for symbol in test_symbols:
            args.extend([
                {"channel": "tickers", "instId": f"{symbol}-USDT"},
                {"channel": "tickers", "instId": f"{symbol}-USDT-SWAP"}
            ])
        
        subscribe_msg = {"op": "subscribe", "args": args}
        print(f"发送 {len(args)} 个订阅请求")
        ws.send(json.dumps(subscribe_msg))
    
    url = "wss://ws.okx.com:8443/ws/v5/public"
    ws = websocket.WebSocketApp(url,
                               on_message=on_message,
                               on_error=on_error,
                               on_close=on_close,
                               on_open=on_open)
    
    print(f"连接 {len(test_symbols)} 个币种")
    
    # 运行15秒测试
    print("运行15秒测试...")
    ws.run_forever()

if __name__ == "__main__":
    print(f"总币种数量: {len(SUPPORTED_SYMBOLS)}")
    print(f"测试币种: {SUPPORTED_SYMBOLS[:5]}...")
    
    try:
        test_binance_multi_symbols()
        time.sleep(3)
        test_okx_multi_symbols()
    except KeyboardInterrupt:
        print("\n测试中断")
    except Exception as e:
        print(f"测试错误: {e}")
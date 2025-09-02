#!/usr/bin/env python3
"""
测试各大交易所WebSocket订阅限制
用于验证在单个连接中能订阅的最大币种数量
"""

import asyncio
import websockets
import json
import time
import sys
from datetime import datetime

# 获取所有主要交易对（前100个市值最大的币种）
def get_major_symbols():
    """获取主要加密货币交易对列表"""
    major_symbols = [
        'BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'DOGE', 'SOL', 'TRX', 'DOT', 'MATIC',
        'AVAX', 'LTC', 'SHIB', 'WBTC', 'LEO', 'UNI', 'ATOM', 'ETC', 'LINK', 'XMR',
        'BCH', 'XLM', 'NEAR', 'APT', 'VET', 'ICP', 'FIL', 'HBAR', 'QNT', 'ALGO',
        'MANA', 'FLOW', 'SAND', 'AXS', 'THETA', 'CHZ', 'EGLD', 'AAVE', 'GRT', 'KLAY',
        'ROSE', 'FTM', 'XTZ', 'ENJ', 'LRC', 'CRV', 'BAT', 'ZEC', 'COMP', 'SUSHI',
        'YFI', 'SNX', 'MKR', '1INCH', 'REN', 'OCEAN', 'STORJ', 'AUDIO', 'CTK', 'NKN',
        'HOT', 'IOTA', 'QTUM', 'ONT', 'ZIL', 'ICX', 'NANO', 'OMG', 'SC', 'WAVES',
        'DASH', 'DGB', 'LSK', 'STEEM', 'ARK', 'STRAT', 'PIVX', 'VIA', 'SYS', 'BLOCK',
        'ANT', 'STORJ', 'MTL', 'SALT', 'SUB', 'POWR', 'REQ', 'MOD', 'FUEL', 'APPC',
        'BLZ', 'NCASH', 'KEY', 'NAS', 'MFT', 'DENT', 'ARDR', 'NULS', 'RCN', 'MITH'
    ]
    return major_symbols

class WebSocketLimitTester:
    def __init__(self):
        self.symbols = get_major_symbols()
        self.results = {}
        
    async def test_binance_limits(self):
        """测试Binance WebSocket订阅限制"""
        print(f"\n{'='*60}")
        print("测试 Binance WebSocket 订阅限制")
        print(f"{'='*60}")
        
        # 测试现货订阅限制
        await self.test_binance_spot()
        # 测试期货订阅限制
        await self.test_binance_futures()
        
    async def test_binance_spot(self):
        """测试Binance现货WebSocket限制"""
        print("\n--- 测试Binance现货订阅限制 ---")
        
        # 创建尽可能多的stream
        max_test_symbols = 500  # 测试500个交易对，接近1024限制
        test_symbols = self.symbols[:max_test_symbols]
        
        streams = []
        for symbol in test_symbols:
            streams.append(f"{symbol.lower()}usdt@ticker")
        
        # 分批测试以找到真实限制
        batch_sizes = [50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        
        for batch_size in batch_sizes:
            if batch_size > len(streams):
                continue
                
            test_streams = streams[:batch_size]
            stream_path = '/'.join(test_streams)
            url = f"wss://stream.binance.com:9443/ws/{stream_path}"
            
            print(f"测试 {batch_size} 个现货stream...")
            
            try:
                async with websockets.connect(url) as websocket:
                    # 等待连接建立
                    await asyncio.sleep(2)
                    
                    # 尝试接收数据
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=10)
                        data = json.loads(message)
                        print(f"✅ {batch_size} 个stream连接成功")
                        self.results['binance_spot_max'] = batch_size
                        
                        # 检查数据质量
                        if 'stream' in data or 'c' in data:
                            print(f"   数据接收正常: {data.get('stream', 'single_stream')}")
                        
                    except asyncio.TimeoutError:
                        print(f"⚠️  {batch_size} 个stream连接但无数据返回")
                        break
                    
            except Exception as e:
                print(f"❌ {batch_size} 个stream连接失败: {str(e)}")
                if batch_size > 50:  # 如果不是第一次测试就记录上一个成功的
                    prev_batch = batch_sizes[batch_sizes.index(batch_size) - 1]
                    self.results['binance_spot_max'] = prev_batch
                break
                
            await asyncio.sleep(1)  # 避免请求过快

    async def test_binance_futures(self):
        """测试Binance期货WebSocket限制"""
        print("\n--- 测试Binance期货订阅限制 ---")
        
        max_test_symbols = 500
        test_symbols = self.symbols[:max_test_symbols]
        
        batch_sizes = [50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        
        for batch_size in batch_sizes:
            if batch_size > len(test_symbols):
                continue
                
            streams = [f"{symbol.lower()}usdt@markPrice" for symbol in test_symbols[:batch_size]]
            stream_path = '/'.join(streams)
            url = f"wss://fstream.binance.com/ws/{stream_path}"
            
            print(f"测试 {batch_size} 个期货stream...")
            
            try:
                async with websockets.connect(url) as websocket:
                    await asyncio.sleep(2)
                    
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=10)
                        data = json.loads(message)
                        print(f"✅ {batch_size} 个期货stream连接成功")
                        self.results['binance_futures_max'] = batch_size
                        
                        if 'stream' in data or 'r' in data:
                            print(f"   数据接收正常")
                        
                    except asyncio.TimeoutError:
                        print(f"⚠️  {batch_size} 个stream连接但无数据返回")
                        break
                    
            except Exception as e:
                print(f"❌ {batch_size} 个期货stream连接失败: {str(e)}")
                if batch_size > 50:
                    prev_batch = batch_sizes[batch_sizes.index(batch_size) - 1]
                    self.results['binance_futures_max'] = prev_batch
                break
                
            await asyncio.sleep(1)

    async def test_okx_limits(self):
        """测试OKX WebSocket订阅限制"""
        print(f"\n{'='*60}")
        print("测试 OKX WebSocket 订阅限制")
        print(f"{'='*60}")
        
        # 分批测试订阅数量
        batch_sizes = [10, 50, 100, 200, 300, 400, 500]
        
        for batch_size in batch_sizes:
            if batch_size > len(self.symbols):
                continue
                
            print(f"\n测试 {batch_size} 个OKX交易对...")
            
            try:
                async with websockets.connect('wss://ws.okx.com:8443/ws/v5/public') as websocket:
                    # 创建订阅参数
                    args = []
                    test_symbols = self.symbols[:batch_size]
                    
                    for symbol in test_symbols:
                        args.extend([
                            {"channel": "tickers", "instId": f"{symbol}-USDT"},
                            {"channel": "tickers", "instId": f"{symbol}-USDT-SWAP"},
                            {"channel": "funding-rate", "instId": f"{symbol}-USDT-SWAP"}
                        ])
                    
                    subscribe_msg = {"op": "subscribe", "args": args}
                    
                    print(f"   发送订阅请求: {len(args)} 个channel")
                    await websocket.send(json.dumps(subscribe_msg))
                    
                    # 等待响应
                    success_count = 0
                    for _ in range(10):  # 最多等待10个消息
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=5)
                            data = json.loads(message)
                            
                            if 'event' in data:
                                if data.get('event') == 'subscribe':
                                    if data.get('code') == '0':
                                        success_count += 1
                                    else:
                                        print(f"   订阅失败: {data}")
                                        break
                            elif 'data' in data:
                                print(f"✅ OKX {batch_size} 个交易对连接成功，开始接收数据")
                                self.results['okx_max'] = batch_size
                                break
                                
                        except asyncio.TimeoutError:
                            break
                    
                    if success_count == 0:
                        print(f"❌ OKX {batch_size} 个交易对连接失败")
                        break
                        
            except Exception as e:
                print(f"❌ OKX {batch_size} 个交易对连接异常: {str(e)}")
                break
                
            await asyncio.sleep(2)

    async def test_bybit_limits(self):
        """测试Bybit WebSocket订阅限制"""
        print(f"\n{'='*60}")
        print("测试 Bybit WebSocket 订阅限制")
        print(f"{'='*60}")
        
        await self.test_bybit_spot()
        await self.test_bybit_linear()
        
    async def test_bybit_spot(self):
        """测试Bybit现货限制"""
        print("\n--- 测试Bybit现货订阅限制 ---")
        
        # Bybit现货限制：10个args/请求
        batch_sizes = [5, 10, 20, 50, 100]  # 测试不同批次大小
        
        for batch_size in batch_sizes:
            if batch_size > len(self.symbols):
                continue
                
            print(f"测试 {batch_size} 个Bybit现货交易对...")
            
            try:
                async with websockets.connect('wss://stream.bybit.com/v5/public/spot') as websocket:
                    test_symbols = self.symbols[:batch_size]
                    args = [f"tickers.{symbol}USDT" for symbol in test_symbols]
                    
                    subscribe_msg = {"op": "subscribe", "args": args}
                    
                    print(f"   发送订阅请求: {len(args)} 个args")
                    await websocket.send(json.dumps(subscribe_msg))
                    
                    # 等待响应
                    for _ in range(5):
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=5)
                            data = json.loads(message)
                            
                            if data.get('op') == 'subscribe':
                                if data.get('success'):
                                    print(f"✅ Bybit现货 {batch_size} 个交易对订阅成功")
                                    self.results['bybit_spot_max'] = batch_size
                                else:
                                    print(f"❌ Bybit现货 {batch_size} 个交易对订阅失败: {data}")
                                    return
                            elif 'topic' in data:
                                print(f"✅ Bybit现货开始接收数据")
                                break
                                
                        except asyncio.TimeoutError:
                            print(f"⚠️  Bybit现货 {batch_size} 超时")
                            break
                            
            except Exception as e:
                print(f"❌ Bybit现货 {batch_size} 连接异常: {str(e)}")
                break
                
            await asyncio.sleep(2)

    async def test_bybit_linear(self):
        """测试Bybit永续合约限制"""
        print("\n--- 测试Bybit永续合约订阅限制 ---")
        
        batch_sizes = [10, 50, 100, 200, 500, 1000, 2000]  # 期权可以2000个args
        
        for batch_size in batch_sizes:
            if batch_size > len(self.symbols):
                continue
                
            print(f"测试 {batch_size} 个Bybit永续合约...")
            
            try:
                async with websockets.connect('wss://stream.bybit.com/v5/public/linear') as websocket:
                    test_symbols = self.symbols[:min(batch_size, len(self.symbols))]
                    args = [f"tickers.{symbol}USDT" for symbol in test_symbols]
                    
                    subscribe_msg = {"op": "subscribe", "args": args}
                    
                    print(f"   发送订阅请求: {len(args)} 个args")
                    await websocket.send(json.dumps(subscribe_msg))
                    
                    for _ in range(5):
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=5)
                            data = json.loads(message)
                            
                            if data.get('op') == 'subscribe':
                                if data.get('success'):
                                    print(f"✅ Bybit永续 {batch_size} 个交易对订阅成功")
                                    self.results['bybit_linear_max'] = batch_size
                                else:
                                    print(f"❌ Bybit永续 {batch_size} 个交易对订阅失败: {data}")
                                    return
                            elif 'topic' in data:
                                print(f"✅ Bybit永续开始接收数据")
                                break
                                
                        except asyncio.TimeoutError:
                            print(f"⚠️  Bybit永续 {batch_size} 超时")
                            break
                            
            except Exception as e:
                print(f"❌ Bybit永续 {batch_size} 连接异常: {str(e)}")
                break
                
            await asyncio.sleep(2)

    async def test_bitget_limits(self):
        """测试Bitget WebSocket订阅限制"""
        print(f"\n{'='*60}")
        print("测试 Bitget WebSocket 订阅限制")
        print(f"{'='*60}")
        
        # Bitget限制：1000个channel订阅/连接
        batch_sizes = [10, 50, 100, 200, 500, 1000]
        
        for batch_size in batch_sizes:
            if batch_size > len(self.symbols):
                continue
                
            print(f"\n测试 {batch_size} 个Bitget交易对...")
            
            try:
                async with websockets.connect('wss://ws.bitget.com/v2/ws/public') as websocket:
                    test_symbols = self.symbols[:batch_size]
                    args = []
                    
                    for symbol in test_symbols:
                        args.extend([
                            {"instType": "SPOT", "channel": "ticker", "instId": f"{symbol}USDT"},
                            {"instType": "USDT-FUTURES", "channel": "ticker", "instId": f"{symbol}USDT"}
                        ])
                    
                    subscribe_msg = {"op": "subscribe", "args": args}
                    
                    print(f"   发送订阅请求: {len(args)} 个channel")
                    await websocket.send(json.dumps(subscribe_msg))
                    
                    success_channels = 0
                    for _ in range(20):  # 等待更多响应
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=3)
                            data = json.loads(message)
                            
                            if data.get('event') == 'subscribe':
                                if data.get('code') == '0':
                                    success_channels += 1
                                else:
                                    print(f"   订阅失败: {data}")
                            elif 'data' in data and 'action' in data:
                                print(f"✅ Bitget {batch_size} 个交易对连接成功，开始接收数据")
                                print(f"   成功订阅 {success_channels} 个channel")
                                self.results['bitget_max'] = batch_size
                                break
                                
                        except asyncio.TimeoutError:
                            if success_channels > 0:
                                print(f"✅ Bitget {batch_size} 个交易对部分成功: {success_channels} 个channel")
                                self.results['bitget_max'] = batch_size
                            break
                    
                    if success_channels == 0:
                        print(f"❌ Bitget {batch_size} 个交易对连接失败")
                        break
                        
            except Exception as e:
                print(f"❌ Bitget {batch_size} 个交易对连接异常: {str(e)}")
                break
                
            await asyncio.sleep(3)
    
    def print_summary(self):
        """打印测试结果汇总"""
        print(f"\n{'='*60}")
        print("WebSocket 订阅限制测试结果汇总")
        print(f"{'='*60}")
        
        exchanges = {
            'Binance现货': self.results.get('binance_spot_max', 'N/A'),
            'Binance期货': self.results.get('binance_futures_max', 'N/A'),
            'OKX': self.results.get('okx_max', 'N/A'),
            'Bybit现货': self.results.get('bybit_spot_max', 'N/A'),
            'Bybit永续': self.results.get('bybit_linear_max', 'N/A'),
            'Bitget': self.results.get('bitget_max', 'N/A')
        }
        
        for exchange, max_symbols in exchanges.items():
            print(f"{exchange:15}: {max_symbols:>10} 个交易对")
        
        print(f"\n{'='*60}")
        print("建议优化策略:")
        print(f"{'='*60}")
        
        # 计算总的可监听币种数
        total_max = sum([v for v in self.results.values() if isinstance(v, int)])
        print(f"所有交易所总计可监听: {total_max} 个交易对")
        
        # 资金费率重点关注
        funding_focus = min([
            self.results.get('binance_futures_max', 0),
            self.results.get('bybit_linear_max', 0),
            self.results.get('bitget_max', 0) // 2,  # Bitget每个币种2个channel
            self.results.get('okx_max', 0) // 3      # OKX每个币种3个channel
        ])
        
        print(f"资金费率重点监听建议: {funding_focus} 个币种")
        print("\n推荐币种优先级:")
        print("1. 主流币种 (BTC, ETH, BNB, ADA, SOL, DOGE, MATIC, DOT, AVAX, LINK)")
        print("2. DeFi热门 (UNI, AAVE, COMP, SUSHI, CRV, YFI, 1INCH)")
        print("3. Layer1/Layer2 (NEAR, FTM, ATOM, ALGO, ROSE)")
        print("4. 其他热门山寨币")

async def main():
    """主函数"""
    if len(sys.argv) > 1:
        exchange = sys.argv[1].lower()
        tester = WebSocketLimitTester()
        
        if exchange == 'binance':
            await tester.test_binance_limits()
        elif exchange == 'okx':
            await tester.test_okx_limits()
        elif exchange == 'bybit':
            await tester.test_bybit_limits()
        elif exchange == 'bitget':
            await tester.test_bitget_limits()
        else:
            print("支持的交易所: binance, okx, bybit, bitget")
            return
    else:
        # 测试所有交易所
        tester = WebSocketLimitTester()
        
        print(f"开始测试各大交易所WebSocket订阅限制")
        print(f"测试时间: {datetime.now()}")
        print(f"测试币种数量: {len(tester.symbols)}")
        
        try:
            await tester.test_binance_limits()
            await asyncio.sleep(5)
            
            await tester.test_okx_limits()
            await asyncio.sleep(5)
            
            await tester.test_bybit_limits()
            await asyncio.sleep(5)
            
            await tester.test_bitget_limits()
            
        except KeyboardInterrupt:
            print("\n测试被用户中断")
        
        tester.print_summary()

if __name__ == "__main__":
    asyncio.run(main())
import json
import asyncio
import websocket
import threading
import time
from datetime import datetime, timedelta
from config import (EXCHANGE_WEBSOCKETS, DEFAULT_SYMBOL, SUPPORTED_SYMBOLS, 
                   CURRENT_SUPPORTED_SYMBOLS, WS_UPDATE_INTERVAL, 
                   update_supported_symbols_async)
from market_info import get_exchange_symbols

class ExchangeDataCollector:
    def __init__(self):
        # 使用动态币种列表作为默认值
        self.supported_symbols = CURRENT_SUPPORTED_SYMBOLS.copy()
        
        # 获取各交易所特定的币种列表
        self.exchange_symbols = {}
        self._load_exchange_symbols()
        
        # 多币种数据结构：交易所 -> 币种 -> 现货/期货数据
        self.data = {}
        self.last_update_time = {}  # 记录每个数据的最后更新时间
        
        self._initialize_data_structure()
        
        self.current_symbol = DEFAULT_SYMBOL  # 前端显示的当前币种
        self.ws_connections = {}
        self.running = False
        self.reconnect_attempts = {}  # 记录每个连接的重连次数
        self.max_reconnect_attempts = 10  # 最大重连次数
        self.reconnect_delay = 5  # 重连延迟(秒)
        
        # 数据频率控制
        self.data_throttle_interval = WS_UPDATE_INTERVAL  # WebSocket数据更新间隔
        self.last_broadcast_time = {}  # 记录每个币种的上次广播时间
        
        # 币种列表更新控制
        self.symbols_update_thread = None
        self.symbols_update_interval = 3600  # 1小时更新一次币种列表
    
    def _load_exchange_symbols(self):
        """加载各交易所特定的币种列表"""
        try:
            print("正在获取各交易所实际支持的币种列表...")
            self.exchange_symbols = get_exchange_symbols(force_refresh=False)
            
            # 合并所有交易所的币种作为总的支持列表
            all_symbols = set()
            for exchange, symbols_data in self.exchange_symbols.items():
                all_symbols.update(symbols_data.get('spot', []))
                all_symbols.update(symbols_data.get('futures', []))
            
            # 更新总的支持币种列表
            self.supported_symbols = sorted(list(all_symbols))
            print(f"✅ 从各交易所获取到 {len(self.supported_symbols)} 个唯一币种")
            
            # 显示各交易所支持情况
            for exchange, symbols_data in self.exchange_symbols.items():
                spot_count = len(symbols_data.get('spot', []))
                futures_count = len(symbols_data.get('futures', []))
                print(f"  {exchange.upper()}: 现货 {spot_count} 个, 期货 {futures_count} 个")
                
        except Exception as e:
            print(f"⚠️ 获取交易所币种列表失败，使用默认列表: {e}")
            # 使用默认的静态列表
            for exchange in ['binance', 'okx', 'bybit', 'bitget']:
                self.exchange_symbols[exchange] = {
                    'spot': self.supported_symbols.copy(),
                    'futures': self.supported_symbols.copy()
                }
    
    def _initialize_data_structure(self):
        """初始化数据结构"""
        for exchange in ['okx', 'binance', 'bybit', 'bitget']:
            self.data[exchange] = {}
            self.last_update_time[exchange] = {}
            
            # 获取该交易所支持的所有币种
            exchange_all_symbols = set()
            if exchange in self.exchange_symbols:
                exchange_all_symbols.update(self.exchange_symbols[exchange].get('spot', []))
                exchange_all_symbols.update(self.exchange_symbols[exchange].get('futures', []))
            else:
                exchange_all_symbols = set(self.supported_symbols)
            
            # 为每个币种初始化数据结构
            for symbol in exchange_all_symbols:
                self.data[exchange][symbol] = {
                    'spot': {},
                    'futures': {},
                    'funding_rate': {}
                }
                self.last_update_time[exchange][symbol] = {
                    'spot': datetime.min,
                    'futures': datetime.min
                }

    def set_symbol(self, symbol):
        """切换前端显示的币种（无需重连WebSocket）"""
        if symbol.upper() in self.supported_symbols:
            self.current_symbol = symbol.upper()
            print(f"前端切换显示币种至: {self.current_symbol}")
    
    def should_throttle_data(self, exchange: str, symbol: str, data_type: str) -> bool:
        """检查是否应该节流数据更新"""
        key = f"{exchange}_{symbol}_{data_type}"
        current_time = datetime.now()
        
        if key not in self.last_broadcast_time:
            self.last_broadcast_time[key] = current_time
            return False
        
        time_diff = (current_time - self.last_broadcast_time[key]).total_seconds()
        if time_diff >= self.data_throttle_interval:
            self.last_broadcast_time[key] = current_time
            return False
        
        return True
    
    def update_symbols_list(self):
        """更新支持的币种列表"""
        try:
            new_symbols = update_supported_symbols_async()
            if new_symbols and new_symbols != self.supported_symbols:
                print(f"币种列表已更新: {len(self.supported_symbols)} -> {len(new_symbols)}")
                old_symbols = set(self.supported_symbols)
                new_symbols_set = set(new_symbols)
                
                # 添加新币种
                added_symbols = new_symbols_set - old_symbols
                if added_symbols:
                    print(f"新增币种: {list(added_symbols)}")
                
                # 移除不再支持的币种
                removed_symbols = old_symbols - new_symbols_set
                if removed_symbols:
                    print(f"移除币种: {list(removed_symbols)}")
                
                self.supported_symbols = new_symbols
                self._rebuild_data_structure()
                
                # 如果当前显示的币种被移除，切换到默认币种
                if self.current_symbol not in self.supported_symbols:
                    if DEFAULT_SYMBOL in self.supported_symbols:
                        self.current_symbol = DEFAULT_SYMBOL
                    else:
                        self.current_symbol = self.supported_symbols[0] if self.supported_symbols else 'BTC'
                    print(f"当前币种已切换至: {self.current_symbol}")
                
        except Exception as e:
            print(f"更新币种列表失败: {e}")
    
    def _rebuild_data_structure(self):
        """重建数据结构以适应新的币种列表"""
        new_data = {}
        new_last_update_time = {}
        
        for exchange in ['okx', 'binance', 'bybit', 'bitget']:
            new_data[exchange] = {}
            new_last_update_time[exchange] = {}
            
            for symbol in self.supported_symbols:
                # 如果是已存在的币种，保留数据；如果是新币种，初始化空数据
                if exchange in self.data and symbol in self.data[exchange]:
                    new_data[exchange][symbol] = self.data[exchange][symbol]
                else:
                    new_data[exchange][symbol] = {
                        'spot': {},
                        'futures': {},
                        'funding_rate': {}
                    }
                
                # 初始化时间记录
                if exchange in self.last_update_time and symbol in self.last_update_time[exchange]:
                    new_last_update_time[exchange][symbol] = self.last_update_time[exchange][symbol]
                else:
                    new_last_update_time[exchange][symbol] = {
                        'spot': datetime.min,
                        'futures': datetime.min
                    }
        
        self.data = new_data
        self.last_update_time = new_last_update_time
        print(f"数据结构已重建，当前监控 {len(self.supported_symbols)} 个币种")
    
    def start_symbols_update_thread(self):
        """启动币种列表定期更新线程"""
        def update_loop():
            while self.running:
                try:
                    time.sleep(self.symbols_update_interval)
                    if self.running:  # 再次检查，确保程序仍在运行
                        self.update_symbols_list()
                except Exception as e:
                    print(f"币种更新线程异常: {e}")
        
        if self.symbols_update_thread is None or not self.symbols_update_thread.is_alive():
            self.symbols_update_thread = threading.Thread(target=update_loop, daemon=True)
            self.symbols_update_thread.start()
            print("币种更新线程已启动")

    def start_all_connections(self):
        """启动所有交易所的WebSocket连接"""
        self.running = True
        
        # 启动币种列表定期更新线程
        self.start_symbols_update_thread()
        
        print(f"开始监控 {len(self.supported_symbols)} 个币种")
        
        # 重置重连计数器
        self.reconnect_attempts = {
            'okx': 0,
            'binance_spot': 0, 
            'binance_futures': 0,
            'bybit_spot': 0,
            'bybit_linear': 0,
            'bitget': 0
        }
        
        # OKX连接
        threading.Thread(target=self._connect_with_retry, args=('okx', self._connect_okx), daemon=True).start()
        
        # Binance连接
        threading.Thread(target=self._connect_with_retry, args=('binance_spot', self._connect_binance_spot), daemon=True).start()
        threading.Thread(target=self._connect_with_retry, args=('binance_futures', self._connect_binance_futures), daemon=True).start()
        
        # Bybit连接
        threading.Thread(target=self._connect_with_retry, args=('bybit_spot', self._connect_bybit_spot), daemon=True).start()
        threading.Thread(target=self._connect_with_retry, args=('bybit_linear', self._connect_bybit_linear), daemon=True).start()
        
        # Bitget连接
        threading.Thread(target=self._connect_with_retry, args=('bitget', self._connect_bitget), daemon=True).start()

    def restart_connections(self):
        """重启所有连接（通常用于网络错误恢复）"""
        print("重启所有WebSocket连接...")
        self.stop_all_connections()
        time.sleep(2)
        self.start_all_connections()

    def stop_all_connections(self):
        """停止所有连接"""
        self.running = False
        for ws in self.ws_connections.values():
            if ws:
                ws.close()
        self.ws_connections.clear()
        self.reconnect_attempts.clear()

    def _connect_with_retry(self, connection_name, connect_func):
        """统一的重连机制包装器"""
        while self.running:
            try:
                print(f"{connection_name} 开始连接...")
                connect_func()
                # 如果连接正常结束，重置重连计数
                self.reconnect_attempts[connection_name] = 0
            except Exception as e:
                if not self.running:
                    break
                    
                self.reconnect_attempts[connection_name] += 1
                print(f"{connection_name} 连接失败 (第{self.reconnect_attempts[connection_name]}次): {e}")
                
                if self.reconnect_attempts[connection_name] >= self.max_reconnect_attempts:
                    print(f"{connection_name} 达到最大重连次数 ({self.max_reconnect_attempts})，停止重连")
                    break
                
                # 指数退避延迟，但不超过60秒
                delay = min(self.reconnect_delay * (2 ** (self.reconnect_attempts[connection_name] - 1)), 60)
                print(f"{connection_name} {delay}秒后重试...")
                
                for _ in range(int(delay)):
                    if not self.running:
                        break
                    time.sleep(1)
                    
                if not self.running:
                    break

    def _connect_okx(self):
        """连接OKX WebSocket - 多币种监听"""
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if 'data' in data:
                    # 检查是否有 arg 字段来判断数据类型
                    if 'arg' in data and 'channel' in data['arg']:
                        channel = data['arg']['channel']
                        inst_id = data['arg']['instId']
                        
                        # 解析币种名称
                        for symbol in self.supported_symbols:
                            if inst_id.startswith(f"{symbol}-USDT"):
                                if channel == 'funding-rate' and inst_id.endswith('-SWAP'):
                                    # 资金费率数据
                                    for item in data['data']:
                                        if 'fundingRate' in item:
                                            # 更新现有的合约数据，只修改资金费率部分
                                            if 'futures' not in self.data['okx'][symbol]:
                                                self.data['okx'][symbol]['futures'] = {}
                                            self.data['okx'][symbol]['futures'].update({
                                                'funding_rate': float(item.get('fundingRate', 0)),
                                                'next_funding_time': item.get('nextFundingTime', ''),
                                            })
                                            print(f"OKX {symbol} 资金费率: {item.get('fundingRate', 0)}")
                                break
                    
                    # 处理 tickers 数据（现货和合约价格）
                    for item in data['data']:
                        if 'instId' in item:
                            inst_id = item['instId']
                            # 解析币种名称
                            for symbol in self.supported_symbols:
                                if inst_id.startswith(f"{symbol}-USDT"):
                                    if inst_id.endswith('-SPOT') or inst_id == f"{symbol}-USDT":
                                        # 现货数据 - 添加节流检查
                                        if not self.should_throttle_data('okx', symbol, 'spot'):
                                            self.data['okx'][symbol]['spot'] = {
                                                'price': float(item.get('last', 0)),
                                                'timestamp': datetime.now().isoformat(),
                                                'symbol': inst_id
                                            }
                                            print(f"OKX {symbol} 现货价格: {item.get('last', 0)}")
                                    elif inst_id.endswith('-SWAP'):
                                        # 永续合约价格数据 - 添加节流检查
                                        if not self.should_throttle_data('okx', symbol, 'futures'):
                                            existing_futures = self.data['okx'][symbol].get('futures', {})
                                            self.data['okx'][symbol]['futures'] = {
                                                **existing_futures,  # 保留已有的资金费率信息
                                                'price': float(item.get('last', 0)),
                                                'timestamp': datetime.now().isoformat(),
                                                'symbol': inst_id
                                            }
                                            print(f"OKX {symbol} 合约价格: {item.get('last', 0)}, 资金费率: {existing_futures.get('funding_rate', 0)}")
                                    break
            except Exception as e:
                print(f"OKX解析错误: {e}")

        def on_error(ws, error):
            print(f"OKX WebSocket错误: {error}")

        def on_open(ws):
            print("OKX WebSocket连接已建立 - 基于实际币种订阅")
            
            # 获取OKX实际支持的币种
            okx_symbols = self.exchange_symbols.get('okx', {'spot': [], 'futures': []})
            
            # 批量订阅OKX实际支持的币种
            args = []
            
            # 订阅现货
            for symbol in okx_symbols.get('spot', []):
                spot_symbol = f"{symbol}-USDT"
                args.append({"channel": "tickers", "instId": spot_symbol})
            
            # 订阅期货和资金费率
            for symbol in okx_symbols.get('futures', []):
                futures_symbol = f"{symbol}-USDT-SWAP"
                args.extend([
                    {"channel": "tickers", "instId": futures_symbol},
                    {"channel": "funding-rate", "instId": futures_symbol}
                ])
            
            subscribe_msg = {
                "op": "subscribe",
                "args": args
            }
            
            print(f"OKX 订阅现货: {len(okx_symbols.get('spot', []))} 个币种")
            print(f"OKX 订阅期货: {len(okx_symbols.get('futures', []))} 个币种")
            ws.send(json.dumps(subscribe_msg))

        ws = websocket.WebSocketApp(EXCHANGE_WEBSOCKETS['okx']['public'],
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_open=on_open)
        
        self.ws_connections['okx'] = ws
        ws.run_forever()

    def _connect_binance_spot(self):
        """连接Binance现货WebSocket - 多币种监听"""
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if 'stream' in data and 'data' in data:
                    # 组合流数据格式
                    stream_data = data['data']
                    if 'c' in stream_data:  # 24小时价格变动统计
                        symbol_name = stream_data['s']
                        # 解析币种名称
                        for symbol in self.supported_symbols:
                            if symbol_name == f"{symbol}USDT":
                                self.data['binance'][symbol]['spot'] = {
                                    'price': float(stream_data['c']),
                                    'timestamp': datetime.now().isoformat(),
                                    'symbol': symbol_name
                                }
                                print(f"Binance {symbol} 现货价格: {stream_data['c']}")
                                break
                elif 'c' in data:  # 单一流数据格式
                    symbol_name = data['s']
                    for symbol in self.supported_symbols:
                        if symbol_name == f"{symbol}USDT":
                            self.data['binance'][symbol]['spot'] = {
                                'price': float(data['c']),
                                'timestamp': datetime.now().isoformat(),
                                'symbol': symbol_name
                            }
                            print(f"Binance {symbol} 现货价格: {data['c']}")
                            break
            except Exception as e:
                print(f"Binance现货解析错误: {e}")

        def on_error(ws, error):
            print(f"Binance现货WebSocket错误: {error}")

        def on_open(ws):
            print("Binance现货WebSocket连接已建立 - 基于实际币种订阅")

        # 获取Binance实际支持的现货币种
        binance_symbols = self.exchange_symbols.get('binance', {'spot': [], 'futures': []})
        spot_symbols = binance_symbols.get('spot', [])
        
        # 使用多路径模式同时监听多个币种
        streams = [f"{symbol.lower()}usdt@ticker" for symbol in spot_symbols]
        stream_path = '/'.join(streams)
        url = f"{EXCHANGE_WEBSOCKETS['binance']['spot']}{stream_path}"
        
        print(f"Binance现货订阅币种: {len(spot_symbols)} 个")
        
        ws = websocket.WebSocketApp(url,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_open=on_open)
        
        self.ws_connections['binance_spot'] = ws
        ws.run_forever()

    def _connect_binance_futures(self):
        """连接Binance合约WebSocket - 多币种监听"""
        def on_message(ws, message):
            try:
                data = json.loads(message)
                if 'stream' in data and 'data' in data:
                    # 组合流数据格式
                    stream_data = data['data']
                    if 'r' in stream_data:  # 标记价格和资金费率
                        symbol_name = stream_data['s']
                        for symbol in self.supported_symbols:
                            if symbol_name == f"{symbol}USDT":
                                self.data['binance'][symbol]['futures'] = {
                                    'price': float(stream_data['p']),
                                    'funding_rate': float(stream_data['r']),
                                    'next_funding_time': stream_data.get('T', ''),
                                    'timestamp': datetime.now().isoformat(),
                                    'symbol': symbol_name
                                }
                                print(f"Binance {symbol} 合约价格: {stream_data['p']}, 资金费率: {stream_data['r']}")
                                break
                elif 'r' in data:  # 单一流数据格式
                    symbol_name = data['s']
                    for symbol in self.supported_symbols:
                        if symbol_name == f"{symbol}USDT":
                            self.data['binance'][symbol]['futures'] = {
                                'price': float(data['p']),
                                'funding_rate': float(data['r']),
                                'next_funding_time': data.get('T', ''),
                                'timestamp': datetime.now().isoformat(),
                                'symbol': symbol_name
                            }
                            print(f"Binance {symbol} 合约价格: {data['p']}, 资金费率: {data['r']}")
                            break
            except Exception as e:
                print(f"Binance合约解析错误: {e}")

        def on_error(ws, error):
            print(f"Binance合约WebSocket错误: {error}")

        def on_open(ws):
            print("Binance合约WebSocket连接已建立 - 基于实际币种订阅")

        # 获取Binance实际支持的期货币种
        binance_symbols = self.exchange_symbols.get('binance', {'spot': [], 'futures': []})
        futures_symbols = binance_symbols.get('futures', [])
        
        # 使用多路径模式同时监听多个币种
        streams = [f"{symbol.lower()}usdt@markPrice" for symbol in futures_symbols]
        stream_path = '/'.join(streams)
        url = f"{EXCHANGE_WEBSOCKETS['binance']['futures']}{stream_path}"
        
        print(f"Binance合约订阅币种: {len(futures_symbols)} 个")
        
        ws = websocket.WebSocketApp(url,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_open=on_open)
        
        self.ws_connections['binance_futures'] = ws
        ws.run_forever()

    def _connect_bybit_spot(self):
        """连接Bybit现货WebSocket - 多币种监听"""
        def on_message(ws, message):
            try:
                data = json.loads(message)
                print(f"Bybit现货原始消息: {data}")
                
                # 处理ping消息
                if data.get('op') == 'ping':
                    pong_msg = {"op": "pong"}
                    ws.send(json.dumps(pong_msg))
                    return
                
                # 处理订阅确认消息
                if data.get('op') == 'subscribe':
                    if data.get('success'):
                        print(f"Bybit现货订阅成功: {data.get('ret_msg')}")
                    else:
                        print(f"Bybit现货订阅失败: {data}")
                        # 检测无效币种并移除
                        self._handle_bybit_subscription_error(data, 'spot')
                    return
                
                # 处理ticker数据
                if 'topic' in data and data.get('topic', '').startswith('tickers'):
                    item = data.get('data', {})
                    symbol_name = item.get('symbol', '')
                    print(f"Bybit现货接收到数据: {symbol_name} - {item}")
                    
                    # 解析币种名称
                    for symbol in self.supported_symbols:
                        if symbol_name == f"{symbol}USDT":
                            self.data['bybit'][symbol]['spot'] = {
                                'price': float(item.get('lastPrice', 0)),
                                'volume': float(item.get('volume24h', 0)),
                                'timestamp': datetime.now().isoformat(),
                                'symbol': symbol_name
                            }
                            print(f"Bybit {symbol} 现货价格: {item.get('lastPrice', 0)}")
                            break
            except Exception as e:
                print(f"Bybit现货解析错误: {e}, 消息: {message}")

        def on_error(ws, error):
            print(f"Bybit现货WebSocket错误: {error}")

        def on_close(ws, close_status_code, close_msg):
            print(f"Bybit现货WebSocket连接关闭: {close_status_code} - {close_msg}")

        def on_open(ws):
            print("Bybit现货WebSocket连接已建立 - 基于实际币种分批订阅")
            
            # 获取Bybit实际支持的现货币种
            bybit_symbols = self.exchange_symbols.get('bybit', {'spot': [], 'futures': []})
            spot_symbols = bybit_symbols.get('spot', [])
            
            # 分批订阅，每10个币种一组（Bybit限制）
            batch_size = 10
            for i in range(0, len(spot_symbols), batch_size):
                batch_symbols = spot_symbols[i:i+batch_size]
                args = [f"tickers.{symbol}USDT" for symbol in batch_symbols]
                
                subscribe_msg = {
                    "op": "subscribe",
                    "args": args
                }
                print(f"Bybit现货订阅批次 {i//batch_size + 1}: {len(batch_symbols)}个有效币种")
                ws.send(json.dumps(subscribe_msg))
                
                # 添加延迟避免请求过快
                time.sleep(0.1)

        ws = websocket.WebSocketApp(EXCHANGE_WEBSOCKETS['bybit']['spot'],
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close,
                                    on_open=on_open)
        
        self.ws_connections['bybit_spot'] = ws
        ws.run_forever()

    def _connect_bybit_linear(self):
        """连接Bybit永续合约WebSocket - 多币种监听"""
        def on_message(ws, message):
            try:
                data = json.loads(message)
                print(f"Bybit合约原始消息: {data}")
                
                # 处理ping消息
                if data.get('op') == 'ping':
                    pong_msg = {"op": "pong"}
                    ws.send(json.dumps(pong_msg))
                    return
                
                # 处理订阅确认消息
                if data.get('op') == 'subscribe':
                    if data.get('success'):
                        print(f"Bybit合约订阅成功: {data.get('ret_msg')}")
                    else:
                        print(f"Bybit合约订阅失败: {data}")
                        # 检测无效币种并移除
                        self._handle_bybit_subscription_error(data, 'linear')
                    return
                
                # 处理ticker数据
                if 'topic' in data and data.get('topic', '').startswith('tickers'):
                    item = data.get('data', {})
                    symbol_name = item.get('symbol', '')
                    print(f"Bybit合约接收到数据: {symbol_name} - {item}")
                    
                    # 解析币种名称
                    for symbol in self.supported_symbols:
                        if symbol_name == f"{symbol}USDT":
                            funding_rate = item.get('fundingRate', 0)
                            if funding_rate == '':
                                funding_rate = 0
                            
                            # 只有当消息包含lastPrice时才更新价格数据
                            if 'lastPrice' in item:
                                # 完整更新包括价格
                                # 如果有现有数据，保留现有的资金费率，除非消息中明确包含fundingRate
                                current_funding_rate = 0
                                if symbol in self.data['bybit'] and self.data['bybit'][symbol]['futures']:
                                    current_funding_rate = self.data['bybit'][symbol]['futures'].get('funding_rate', 0)
                                
                                # 只有当消息明确包含fundingRate时才更新，否则保留现有值
                                final_funding_rate = current_funding_rate
                                if 'fundingRate' in item:
                                    final_funding_rate = float(funding_rate) if funding_rate != '' and funding_rate != 0 else current_funding_rate
                                
                                self.data['bybit'][symbol]['futures'] = {
                                    'price': float(item.get('lastPrice', 0)),
                                    'funding_rate': final_funding_rate,
                                    'next_funding_time': item.get('nextFundingTime', ''),
                                    'volume': float(item.get('volume24h', 0)),
                                    'timestamp': datetime.now().isoformat(),
                                    'symbol': symbol_name
                                }
                                print(f"Bybit {symbol} 合约价格: {item.get('lastPrice', 0)}, 资金费率: {final_funding_rate}")
                            else:
                                # 保留现有价格，只更新其他可用数据
                                if symbol in self.data['bybit'] and self.data['bybit'][symbol]['futures']:
                                    current_data = self.data['bybit'][symbol]['futures'].copy()
                                    # 只有消息中明确包含fundingRate时才更新资金费率
                                    if 'fundingRate' in item and funding_rate != '' and funding_rate != 0:
                                        current_data['funding_rate'] = float(funding_rate)
                                    if 'nextFundingTime' in item:
                                        current_data['next_funding_time'] = item.get('nextFundingTime', '')
                                    if 'volume24h' in item:
                                        current_data['volume'] = float(item.get('volume24h', 0))
                                    current_data['timestamp'] = datetime.now().isoformat()
                                    self.data['bybit'][symbol]['futures'] = current_data
                                    print(f"Bybit {symbol} 合约增量更新 (保持价格: {current_data.get('price', 0)}, 资金费率: {current_data.get('funding_rate', 0)})")
                                else:
                                    # 如果没有现有数据，跳过这个更新
                                    print(f"Bybit {symbol} 合约无价格数据，跳过增量更新")
                            break
            except Exception as e:
                print(f"Bybit合约解析错误: {e}, 消息: {message}")

        def on_error(ws, error):
            print(f"Bybit合约WebSocket错误: {error}")

        def on_close(ws, close_status_code, close_msg):
            print(f"Bybit合约WebSocket连接关闭: {close_status_code} - {close_msg}")

        def on_open(ws):
            print("Bybit合约WebSocket连接已建立 - 基于实际币种分批订阅")
            
            # 获取Bybit实际支持的期货币种
            bybit_symbols = self.exchange_symbols.get('bybit', {'spot': [], 'futures': []})
            futures_symbols = bybit_symbols.get('futures', [])
            
            # 分批订阅，每10个币种一组（Bybit限制）
            batch_size = 10
            for i in range(0, len(futures_symbols), batch_size):
                batch_symbols = futures_symbols[i:i+batch_size]
                args = [f"tickers.{symbol}USDT" for symbol in batch_symbols]
                
                subscribe_msg = {
                    "op": "subscribe",
                    "args": args
                }
                print(f"Bybit合约订阅批次 {i//batch_size + 1}: {len(batch_symbols)}个有效币种")
                ws.send(json.dumps(subscribe_msg))
                
                # 添加延迟避免请求过快
                time.sleep(0.1)

        ws = websocket.WebSocketApp(EXCHANGE_WEBSOCKETS['bybit']['linear'],
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close,
                                    on_open=on_open)
        
        self.ws_connections['bybit_linear'] = ws
        ws.run_forever()

    def _handle_bybit_subscription_error(self, error_data, channel_type):
        """处理Bybit订阅错误，移除不支持的币种"""
        try:
            ret_msg = error_data.get('ret_msg', '')
            
            # 检测无效币种错误模式
            if 'Invalid symbol' in ret_msg or 'symbol not exist' in ret_msg.lower():
                # 从错误消息中提取币种名称
                import re
                
                # 匹配模式: [tickers.LEOUSDT] 或 tickers.LEOUSDT
                symbol_match = re.search(r'tickers\.([A-Z]+)USDT', ret_msg)
                if symbol_match:
                    invalid_symbol = symbol_match.group(1)
                    
                    # 从支持的币种列表中移除
                    if invalid_symbol in self.supported_symbols:
                        self.supported_symbols.remove(invalid_symbol)
                        print(f"⚠️  检测到Bybit不支持的币种 {invalid_symbol}，已从监控列表中移除")
                        print(f"⚠️  当前有效币种数量: {len(self.supported_symbols)}个")
                        
                        # 重新订阅剩余的有效币种
                        self._resubscribe_bybit(channel_type)
                        
        except Exception as e:
            print(f"处理Bybit订阅错误时异常: {e}")

    def _resubscribe_bybit(self, channel_type):
        """重新订阅Bybit的有效币种"""
        try:
            if channel_type == 'spot' and 'bybit_spot' in self.ws_connections:
                ws = self.ws_connections['bybit_spot']
                # 分批订阅现货币种
                batch_size = 10
                for i in range(0, len(self.supported_symbols), batch_size):
                    batch_symbols = self.supported_symbols[i:i+batch_size]
                    args = [f"tickers.{symbol}USDT" for symbol in batch_symbols]
                    
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": args
                    }
                    print(f"Bybit现货重新订阅批次 {i//batch_size + 1}: {len(batch_symbols)}个有效币种")
                    ws.send(json.dumps(subscribe_msg))
                    time.sleep(0.1)
                    
            elif channel_type == 'linear' and 'bybit_linear' in self.ws_connections:
                ws = self.ws_connections['bybit_linear']
                # 分批订阅合约币种
                batch_size = 10
                for i in range(0, len(self.supported_symbols), batch_size):
                    batch_symbols = self.supported_symbols[i:i+batch_size]
                    args = [f"tickers.{symbol}USDT" for symbol in batch_symbols]
                    
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": args
                    }
                    print(f"Bybit合约重新订阅批次 {i//batch_size + 1}: {len(batch_symbols)}个有效币种")
                    ws.send(json.dumps(subscribe_msg))
                    time.sleep(0.1)
                    
        except Exception as e:
            print(f"重新订阅Bybit时异常: {e}")

    def _handle_bitget_subscription_error(self, error_data):
        """处理Bitget订阅错误，移除不支持的币种"""
        try:
            ret_msg = error_data.get('msg', '')
            
            # 检测无效币种错误模式
            if 'Invalid symbol' in ret_msg or 'symbol not exist' in ret_msg.lower() or 'instrument not exist' in ret_msg.lower() or 'doesn\'t exist' in ret_msg:
                # 从错误消息中提取币种名称
                import re
                
                # 匹配模式: instId:MATICUSDT 或 instId=BTCUSDT
                symbol_match = re.search(r'instId[=:]([A-Z]+)USDT', ret_msg)
                if symbol_match:
                    invalid_symbol = symbol_match.group(1)
                    
                    # 从支持的币种列表中移除
                    if invalid_symbol in self.supported_symbols:
                        self.supported_symbols.remove(invalid_symbol)
                        print(f"⚠️  检测到Bitget不支持的币种 {invalid_symbol}，已从监控列表中移除")
                        print(f"⚠️  当前有效币种数量: {len(self.supported_symbols)}个")
                        
                        # 重新订阅剩余的有效币种
                        self._resubscribe_bitget()
                        
            # 处理没有错误消息但有arg字段的订阅失败
            elif 'arg' in error_data and 'instId' in error_data['arg']:
                inst_id = error_data['arg']['instId']
                # 提取币种名称（从 ADAUSDT 提取 ADA）
                if inst_id.endswith('USDT'):
                    invalid_symbol = inst_id[:-4]  # 移除 USDT 后缀
                    
                    # 从支持的币种列表中移除
                    if invalid_symbol in self.supported_symbols:
                        self.supported_symbols.remove(invalid_symbol)
                        print(f"⚠️  检测到Bitget不支持的币种 {invalid_symbol}，已从监控列表中移除")
                        print(f"⚠️  当前有效币种数量: {len(self.supported_symbols)}个")
                        
                        # 重新订阅剩余的有效币种
                        self._resubscribe_bitget()
                        
        except Exception as e:
            print(f"处理Bitget订阅错误时异常: {e}")

    def _resubscribe_bitget(self):
        """重新订阅Bitget的有效币种"""
        try:
            if 'bitget' in self.ws_connections:
                ws = self.ws_connections['bitget']
                # 分批订阅，每50个币种一组
                batch_size = 50
                for i in range(0, len(self.supported_symbols), batch_size):
                    batch_symbols = self.supported_symbols[i:i+batch_size]
                    args = []
                    
                    for symbol in batch_symbols:
                        symbol_id = f"{symbol}USDT"
                        
                        # 订阅现货
                        args.append({"instType": "SPOT", "channel": "ticker", "instId": symbol_id})
                        
                        # 订阅所有币种的期货（如果不存在会返回错误但不会影响其他订阅）
                        args.append({"instType": "USDT-FUTURES", "channel": "ticker", "instId": symbol_id})
                    
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": args
                    }
                    print(f"Bitget重新订阅批次 {i//batch_size + 1}: {len(batch_symbols)}个有效币种")
                    ws.send(json.dumps(subscribe_msg))
                    time.sleep(0.1)
                    
        except Exception as e:
            print(f"重新订阅Bitget时异常: {e}")

    def _connect_bitget(self):
        """连接Bitget WebSocket - 多币种监听"""
        def on_message(ws, message):
            try:
                print(f"Bitget原始消息: {message}")
                data = json.loads(message)
                
                # 处理ping消息
                if data.get('action') == 'ping':
                    pong_msg = {"action": "pong", "arg": data.get("arg")}
                    ws.send(json.dumps(pong_msg))
                    return
                
                # 处理订阅确认消息和错误消息
                if data.get('event') == 'subscribe':
                    if data.get('code') == '0' or 'arg' in data:
                        print(f"Bitget 订阅成功: {data}")
                    else:
                        print(f"Bitget 订阅失败: {data}")
                        # 检测无效币种并移除
                        self._handle_bitget_subscription_error(data)
                    return
                
                
                # 处理错误消息
                if data.get('event') == 'error':
                    print(f"Bitget 订阅错误: {data}")
                    # 检测无效币种并移除
                    self._handle_bitget_subscription_error(data)
                    return
                
                # 处理数据推送 - 支持 action: snapshot/update 格式
                if 'data' in data and 'arg' in data and data.get('action') in ['snapshot', 'update']:
                    arg_info = data['arg']
                    inst_type = arg_info.get('instType', '')
                    channel = arg_info.get('channel', '')
                    inst_id = arg_info.get('instId', '')
                    
                    if channel == 'ticker':
                        for item in data['data']:
                            print(f"Bitget接收到数据: {inst_id} ({inst_type}) - lastPr: {item.get('lastPr', 0)}")
                            
                            # 解析币种名称
                            for symbol in self.supported_symbols:
                                # 对于现货：BTCUSDT
                                # 对于期货：BTCUSDT (不是BTCUSDT_UMCBL)
                                if inst_id == f"{symbol}USDT":
                                    if inst_type == 'USDT-FUTURES':  # 期货合约
                                        price = float(item.get('lastPr', 0)) if item.get('lastPr') and str(item.get('lastPr')) != '0' else 0
                                        funding_rate = float(item.get('fundingRate', 0)) if item.get('fundingRate') and str(item.get('fundingRate')) != '0' else 0
                                        
                                        if price > 0:  # 只在有有效价格时更新
                                            self.data['bitget'][symbol]['futures'] = {
                                                'price': price,
                                                'funding_rate': funding_rate,
                                                'next_funding_time': item.get('nextFundingTime', ''),
                                                'mark_price': float(item.get('markPrice', 0)) if item.get('markPrice') else 0,
                                                'index_price': float(item.get('indexPrice', 0)) if item.get('indexPrice') else 0,
                                                'timestamp': datetime.now().isoformat(),
                                                'symbol': inst_id
                                            }
                                            print(f"Bitget {symbol} 合约价格: {price}, 资金费率: {funding_rate}")
                                    elif inst_type == 'SPOT':  # 现货
                                        price = float(item.get('lastPr', 0)) if item.get('lastPr') and str(item.get('lastPr')) != '0' else 0
                                        
                                        # 对于WLFI特殊处理：即使价格为0也要更新（确保上线第一时间获取数据）
                                        # 对于其他币种：只在有有效价格时更新
                                        if price > 0 or symbol == 'WLFI':
                                            self.data['bitget'][symbol]['spot'] = {
                                                'price': price,
                                                'volume': float(item.get('baseVolume', 0)) if item.get('baseVolume') else 0,
                                                'timestamp': datetime.now().isoformat(),
                                                'symbol': inst_id,
                                                'is_active': price > 0  # 标记是否有实际价格
                                            }
                                            if price > 0:
                                                print(f"Bitget {symbol} 现货价格: {price}")
                                            elif symbol == 'WLFI':
                                                print(f"Bitget {symbol} 现货监控中 (等待上线): {price}")
                                    break
            except Exception as e:
                print(f"Bitget解析错误: {e}, 原始数据: {message}")

        def on_error(ws, error):
            print(f"Bitget WebSocket错误: {error}")

        def on_close(ws, close_status_code, close_msg):
            print(f"Bitget WebSocket连接关闭: {close_status_code}, {close_msg}")

        def on_open(ws):
            print("Bitget WebSocket连接已建立 - 基于实际币种分批订阅")
            
            # 获取Bitget实际支持的币种
            bitget_symbols = self.exchange_symbols.get('bitget', {'spot': [], 'futures': []})
            spot_symbols = bitget_symbols.get('spot', [])
            futures_symbols = bitget_symbols.get('futures', [])
            
            # 分批订阅，每50个币种一组
            batch_size = 50
            
            # 先处理现货
            all_args = []
            for symbol in spot_symbols:
                symbol_id = f"{symbol}USDT"
                all_args.append({"instType": "SPOT", "channel": "ticker", "instId": symbol_id})
            
            # 再处理期货
            for symbol in futures_symbols:
                symbol_id = f"{symbol}USDT"
                all_args.append({"instType": "USDT-FUTURES", "channel": "ticker", "instId": symbol_id})
            
            # 分批发送订阅请求
            for i in range(0, len(all_args), batch_size):
                batch_args = all_args[i:i+batch_size]
                
                subscribe_msg = {
                    "op": "subscribe",
                    "args": batch_args
                }
                print(f"Bitget重新订阅批次 {i//batch_size + 1}: {len(batch_args)}个有效币种")
                ws.send(json.dumps(subscribe_msg))
                
                # 添加延迟避免请求过快
                time.sleep(0.1)

        ws = websocket.WebSocketApp(EXCHANGE_WEBSOCKETS['bitget']['public'],
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_open=on_open,
                                    on_close=on_close)
        
        self.ws_connections['bitget'] = ws
        print(f"Bitget 尝试连接: {EXCHANGE_WEBSOCKETS['bitget']['public']}")
        ws.run_forever()

    def get_all_data(self):
        """获取所有交易所的实时数据"""
        return self.data
    
    def get_symbol_data(self, symbol=None):
        """获取指定币种的数据（用于前端显示）"""
        if symbol is None:
            symbol = self.current_symbol
            
        symbol_data = {}
        for exchange in self.data:
            if symbol in self.data[exchange]:
                symbol_data[exchange] = self.data[exchange][symbol]
            else:
                symbol_data[exchange] = {'spot': {}, 'futures': {}, 'funding_rate': {}}
        
        return symbol_data

    def calculate_premium(self, symbol=None):
        """计算溢价指数（永续合约价格 - 现货价格）/ 现货价格
        当现货未上线时，使用期货价格平均值作为参考计算相对溢价"""
        if symbol is None:
            symbol = self.current_symbol
            
        premium_data = {}
        
        # 收集所有交易所的价格数据
        all_futures_prices = []
        exchange_data = {}
        
        for exchange in self.data:
            try:
                if symbol in self.data[exchange]:
                    spot_price = self.data[exchange][symbol]['spot'].get('price', 0)
                    futures_price = self.data[exchange][symbol]['futures'].get('price', 0)
                    
                    exchange_data[exchange] = {
                        'spot_price': spot_price,
                        'futures_price': futures_price
                    }
                    
                    if futures_price > 0:
                        all_futures_prices.append(futures_price)
                        
            except Exception as e:
                print(f"收集{exchange} {symbol}价格数据时出错: {e}")
        
        # 计算溢价
        for exchange, data in exchange_data.items():
            try:
                spot_price = data['spot_price']
                futures_price = data['futures_price']
                
                if futures_price > 0:
                    if spot_price > 0:
                        # 正常溢价计算：(期货价格 - 现货价格) / 现货价格
                        premium = ((futures_price - spot_price) / spot_price) * 100
                    elif len(all_futures_prices) > 1:
                        # 现货未上线时：使用期货价格平均值作为参考
                        avg_futures_price = sum(all_futures_prices) / len(all_futures_prices)
                        premium = ((futures_price - avg_futures_price) / avg_futures_price) * 100
                    else:
                        premium = 0
                    
                    premium_data[exchange] = {
                        'premium_percent': round(premium, 4),
                        'spot_price': spot_price,
                        'futures_price': futures_price
                    }
                    
            except Exception as e:
                print(f"计算{exchange} {symbol}溢价时出错: {e}")
                
        return premium_data
    
    def calculate_all_premiums(self):
        """计算所有币种的溢价指数"""
        all_premiums = {}
        for symbol in self.supported_symbols:
            all_premiums[symbol] = self.calculate_premium(symbol)
        return all_premiums
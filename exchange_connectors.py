import json
import asyncio
import websocket
import threading
import time
from datetime import datetime, timedelta, timezone
from config import (EXCHANGE_WEBSOCKETS, DEFAULT_SYMBOL, SUPPORTED_SYMBOLS, 
                   CURRENT_SUPPORTED_SYMBOLS, WS_UPDATE_INTERVAL, WS_CONNECTION_CONFIG,
                   MEMORY_OPTIMIZATION_CONFIG, update_supported_symbols_async,
                   REST_ENABLED, REST_UPDATE_INTERVAL, REST_MERGE_POLICY)
from rest_collectors import (
    fetch_binance as rest_fetch_binance,
    fetch_okx as rest_fetch_okx,
    fetch_bybit as rest_fetch_bybit,
    fetch_bitget as rest_fetch_bitget,
)
from market_info import get_exchange_symbols

class ExchangeDataCollector:
    def __init__(self):
        # 使用动态币种列表作为默认值
        self.supported_symbols = CURRENT_SUPPORTED_SYMBOLS.copy()
        
        # 针对跨交易所同名但实际不同的标的，使用本地别名避免误归并
        # 在此维护硬编码映射：{<exchange>: {<原符号>: <本地别名>}}
        # 维护特殊别名硬编码表：{<exchange>: {<原符号>: <本地别名>}}
        # 如需新增/调整别名，只需在此字典补充映射，并确保 rest_collectors.py 中的 _bybit_observed_to_coin 做到对应映射。
        # 前端聚合与 API 已通过 normalize_symbol_for_exchange() 复用该表，无需额外修改。
        self.symbol_overrides = {
            'bybit': {
                'APP': 'APPbybit',  # Bybit 的 APPUSDT 独立成别名，避免与其它交易所的 APP 比价
            }
        }

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
        self.connection_start_times = {}  # 记录连接开始时间
        
        # 使用配置文件中的连接参数
        self.max_reconnect_attempts = WS_CONNECTION_CONFIG['max_reconnect_attempts']
        self.base_reconnect_delay = WS_CONNECTION_CONFIG['base_reconnect_delay']  
        self.max_reconnect_delay = WS_CONNECTION_CONFIG['max_reconnect_delay']
        self.exponential_backoff = WS_CONNECTION_CONFIG['exponential_backoff']
        
        # 数据频率控制
        self.data_throttle_interval = WS_UPDATE_INTERVAL  # WebSocket数据更新间隔
        self.last_broadcast_time = {}  # 记录每个币种的上次广播时间
        
        # 币种列表更新控制
        self.symbols_update_thread = None
        self.symbols_update_interval = 3600  # 1小时更新一次币种列表

        # REST补充轮询
        self.rest_enabled = REST_ENABLED
        self.rest_update_interval = REST_UPDATE_INTERVAL
        self.rest_thread = None  # legacy single-thread poller (unused in Scheme A)
        self.rest_threads = {}
        self.rest_last_sync = { 'binance': None, 'okx': None, 'bybit': None, 'bitget': None }
    
    def _load_exchange_symbols(self):
        """加载各交易所特定的币种列表"""
        used_static_fallback = False
        try:
            print("正在获取各交易所实际支持的币种列表...")
            self.exchange_symbols = get_exchange_symbols(force_refresh=False)
        except Exception as e:
            used_static_fallback = True
            print(f"⚠️ 获取交易所币种列表失败，使用默认列表: {e}")
            # 使用默认的静态列表；稍后会再应用本地别名映射
            for exchange in ['binance', 'okx', 'bybit', 'bitget']:
                self.exchange_symbols[exchange] = {
                    'spot': self.supported_symbols.copy(),
                    'futures': self.supported_symbols.copy()
                }

        # 统一应用本地硬编码别名（例如 APP -> APPbybit）
        self._apply_symbol_overrides()

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

        if used_static_fallback:
            print("⚠️ 当前使用静态回退列表，建议检查网络或稍后重试动态拉取。")

    def _apply_symbol_overrides(self):
        """应用本地特殊别名，避免跨交易所的同名币种误合并"""
        for exchange, mapping in self.symbol_overrides.items():  # 如需扩展别名，维护 self.symbol_overrides 即可
            if exchange not in self.exchange_symbols:
                continue

            for market_type in ['spot', 'futures']:
                symbols = self.exchange_symbols[exchange].get(market_type)
                if not symbols:
                    continue

                remapped = []
                for symbol in symbols:
                    remapped_symbol = mapping.get(symbol, symbol)
                    if remapped_symbol not in remapped:
                        remapped.append(remapped_symbol)

                self.exchange_symbols[exchange][market_type] = remapped

    def _resolve_exchange_symbol(self, exchange: str, symbol: str) -> str:
        """将全局币种名转换为指定交易所使用的本地别名"""
        mapping = self.symbol_overrides.get(exchange, {})
        return mapping.get(symbol, symbol)

    def normalize_symbol_for_exchange(self, exchange: str, symbol: str) -> str:
        """对外暴露的别名映射，供API聚合等场景复用"""
        return self._resolve_exchange_symbol(exchange, symbol)

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

    def _get_bybit_symbol_mapping(self, symbol):
        """获取Bybit订阅用的特殊币种映射

        输入为我们统一的币种名（如 NEIROETH 或 NEIRO），输出为Bybit需要订阅的基础符号名。
        """
        special_mappings = {
            # 历史兼容：如果统一币种名被写成 NEIRO，则在Bybit上应订阅 NEIROETH
            'NEIRO': {
                'spot': 'NEIROETH',      # Bybit现货：NEIROETHUSDT
                'futures': 'NEIROETH'    # Bybit永续：NEIROETHUSDT
            },
            # 标准：NEIROETH 作为统一币种名，在Bybit上也订阅 NEIROETH
            'NEIROETH': {
                'spot': 'NEIROETH',
                'futures': 'NEIROETH'
            },
            # Bybit 专用：APPUSDT 与其他交易所不同，映射为本地别名 APPbybit
            'APPbybit': {
                'spot': 'APP',
                'futures': 'APP'
            }
        }
        return special_mappings.get(symbol, {'spot': symbol, 'futures': symbol})

    def _bybit_observed_to_coin(self, observed_base: str) -> str:
        """将Bybit返回的基础符号映射为我们内部的统一币种名。

        目标：确保与 NEIRO 相关的品种归并到 NEIROETH。
        例如：NEIROETH -> NEIROETH；NEIRO -> NEIROETH。
        其他币种保持不变。
        """
        alias_to_coin = {
            'NEIRO': 'NEIROETH',
            'NEIROETH': 'NEIROETH',
            'APP': 'APPbybit',
            'APPbybit': 'APPbybit',
            'APPBYBIT': 'APPbybit',
        }
        return alias_to_coin.get(observed_base, observed_base)

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

        # 启动REST补充轮询（可选）
        if self.rest_enabled:
            self.start_rest_poller()

    def start_rest_poller(self):
        """启动每交易所独立的REST轮询线程（方案A）"""
        def make_loop(exchange: str, fetch_func):
            def loop():
                while self.running:
                    try:
                        symbol_map = fetch_func()
                        self._merge_rest_exchange_snapshot(exchange, symbol_map)
                    except Exception as e:
                        print(f"REST轮询错误[{exchange}]: {e}")
                    time.sleep(max(0.5, float(self.rest_update_interval)))
            return loop

        plan = {
            'binance': rest_fetch_binance,
            'okx': rest_fetch_okx,
            'bybit': rest_fetch_bybit,
            'bitget': rest_fetch_bitget,
        }

        # 启动/复用线程
        for ex, func in plan.items():
            if ex in self.rest_threads and self.rest_threads[ex].is_alive():
                continue
            t = threading.Thread(target=make_loop(ex, func), daemon=True)
            t.start()
            self.rest_threads[ex] = t
        print(f"REST补充轮询(并行)已启动，间隔 {self.rest_update_interval}s")

    def _merge_rest_exchange_snapshot(self, exchange: str, symbol_map):
        """合并单一交易所的REST快照；last_sync 记录为合并时刻，避免显示陈旧时间。"""
        from datetime import datetime, timezone
        synced_at = datetime.now(timezone.utc).isoformat()

        if exchange not in self.data:
            return

        prefer_ws_secs = float(REST_MERGE_POLICY.get('prefer_ws_secs', 6))

        for symbol, parts in symbol_map.items():
            if symbol not in self.supported_symbols:
                continue
            if symbol not in self.data[exchange]:
                self.data[exchange][symbol] = {'spot': {}, 'futures': {}, 'funding_rate': {}}

            for kind in ('spot', 'futures'):
                if kind not in parts:
                    continue
                new_data = parts[kind]
                existing = self.data[exchange][symbol].get(kind, {})

                # 近窗期内（默认6s）优先保留WS写入
                try:
                    old_ts = existing.get('timestamp')
                    is_fresh = False
                    if old_ts:
                        dt = datetime.fromisoformat(old_ts.replace('Z', '+00:00'))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        age = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()
                        is_fresh = age <= prefer_ws_secs
                except Exception:
                    is_fresh = False

                if not is_fresh:
                    merged = {k: v for k, v in existing.items() if k not in ('price', 'timestamp', 'symbol')}
                    merged.update({
                        'price': new_data.get('price', 0),
                        'timestamp': synced_at,
                        'symbol': new_data.get('symbol')
                    })

                    # REST 视为权威数据来源，可覆盖旧的资金费率/结算时间
                    rest_funding = new_data.get('funding_rate')
                    if rest_funding not in (None, ''):
                        try:
                            merged['funding_rate'] = float(rest_funding)
                        except (TypeError, ValueError):
                            pass

                    rest_next_ft = new_data.get('next_funding_time')
                    if rest_next_ft:
                        merged['next_funding_time'] = rest_next_ft

                    self.data[exchange][symbol][kind] = merged

        # 标记该交易所合并时刻为最后同步时间（用于前端展示）
        self.rest_last_sync[exchange] = synced_at

    def _merge_rest_snapshots(self, snapshots):
        """将REST快照数据合并到内存数据结构，尽量不覆盖新鲜的WS数据"""
        prefer_ws_secs = float(REST_MERGE_POLICY.get('prefer_ws_secs', 6))

        for exchange, symbol_map in snapshots.items():
            if exchange not in self.data:
                continue
            synced_at = datetime.now(timezone.utc).isoformat()
            for symbol, parts in symbol_map.items():
                # 仅合并在支持列表里的USDT币种
                if symbol not in self.supported_symbols:
                    continue
                # 确保结构存在
                if symbol not in self.data[exchange]:
                    self.data[exchange][symbol] = {'spot': {}, 'futures': {}, 'funding_rate': {}}

                for kind in ('spot', 'futures'):
                    if kind not in parts:
                        continue
                    new_data = parts[kind]
                    # 不覆盖资金费率；只补充价格/时间戳/标识
                    existing = self.data[exchange][symbol].get(kind, {})

                    # 如果现有数据在偏好窗口内更新，跳过覆盖以保留WS的最新数据
                    try:
                        old_ts = existing.get('timestamp')
                        is_fresh = False
                        if old_ts:
                            dt = datetime.fromisoformat(old_ts.replace('Z', '+00:00'))
                            # 正常化到UTC计算新鲜度
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            age = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()
                            is_fresh = age <= prefer_ws_secs
                    except Exception:
                        is_fresh = False

                    if not is_fresh:
                        merged = {k: v for k, v in existing.items() if k not in ('price', 'timestamp', 'symbol')}
                        merged.update({
                            'price': new_data.get('price', 0),
                            'timestamp': synced_at,
                            'symbol': new_data.get('symbol')
                        })

                        # REST 视为权威数据来源，可覆盖旧的资金费率/结算时间
                        rest_funding = new_data.get('funding_rate')
                        if rest_funding not in (None, ''):
                            try:
                                merged['funding_rate'] = float(rest_funding)
                            except (TypeError, ValueError):
                                pass

                        rest_next_ft = new_data.get('next_funding_time')
                        if rest_next_ft:
                            merged['next_funding_time'] = rest_next_ft

                        self.data[exchange][symbol][kind] = merged

            # 更新该交易所的REST最后同步时间
            self.rest_last_sync[exchange] = synced_at

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
        """优化的重连机制包装器 - 避免无限循环和资源耗尽"""
        self.connection_start_times[connection_name] = datetime.now()
        
        while self.running:
            try:
                print(f"[{connection_name}] 开始连接... (尝试 {self.reconnect_attempts.get(connection_name, 0) + 1}/{self.max_reconnect_attempts})")
                connect_func()
                # 连接成功，重置重连计数
                self.reconnect_attempts[connection_name] = 0
                print(f"[{connection_name}] 连接成功建立")
                
            except Exception as e:
                if not self.running:
                    print(f"[{connection_name}] 程序停止，退出重连")
                    break
                    
                self.reconnect_attempts[connection_name] = self.reconnect_attempts.get(connection_name, 0) + 1
                current_attempt = self.reconnect_attempts[connection_name]
                
                print(f"[{connection_name}] 连接失败 (第{current_attempt}次): {e}")
                
                # 检查是否达到最大重连次数
                if current_attempt >= self.max_reconnect_attempts:
                    print(f"[{connection_name}] 达到最大重连次数 ({self.max_reconnect_attempts})，停止重连")
                    break
                
                # 计算退避延迟
                if self.exponential_backoff:
                    delay = min(
                        self.base_reconnect_delay * (2 ** (current_attempt - 1)), 
                        self.max_reconnect_delay
                    )
                else:
                    delay = self.base_reconnect_delay
                
                print(f"[{connection_name}] 等待 {delay} 秒后重试...")
                
                # 分段睡眠，支持优雅停止
                sleep_step = 5  # 每5秒检查一次是否需要停止
                for _ in range(int(delay // sleep_step)):
                    if not self.running:
                        print(f"[{connection_name}] 等待期间程序停止，退出重连")
                        return
                    time.sleep(sleep_step)
                
                # 处理余数时间
                remaining = delay % sleep_step
                if remaining > 0 and self.running:
                    time.sleep(remaining)
                    
                if not self.running:
                    print(f"[{connection_name}] 等待期间程序停止，退出重连")
                    break
        
        print(f"[{connection_name}] 重连线程已退出")

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
                                                # 确保资金费率单独更新也刷新时间戳，便于前端展示“最新更新时间”
                                                'timestamp': datetime.now(timezone.utc).isoformat(),
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
                                                'timestamp': datetime.now(timezone.utc).isoformat(),
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
                                                'timestamp': datetime.now(timezone.utc).isoformat(),
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
                                    'timestamp': datetime.now(timezone.utc).isoformat(),
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
                                'timestamp': datetime.now(timezone.utc).isoformat(),
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
                                    'timestamp': datetime.now(timezone.utc).isoformat(),
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
                                'timestamp': datetime.now(timezone.utc).isoformat(),
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
                    
                    # 解析并归并：以Bybit返回的基础符号为准（去掉USDT后缀）
                    try:
                        base = symbol_name[:-4] if symbol_name.endswith('USDT') else symbol_name
                        coin = self._bybit_observed_to_coin(base)
                        # 仅当该币种在Bybit现货支持列表中时才记录
                        bybit_spot = self.exchange_symbols.get('bybit', {}).get('spot', [])
                        if coin in bybit_spot:
                            self.data['bybit'].setdefault(coin, {'spot': {}, 'futures': {}, 'funding_rate': {}})
                            self.data['bybit'][coin]['spot'] = {
                                'price': float(item.get('lastPrice', 0) or 0),
                                'volume': float(item.get('volume24h', 0) or 0),
                                'timestamp': datetime.now(timezone.utc).isoformat(),
                                'symbol': symbol_name
                            }
                            print(f"Bybit {coin} 现货价格: {item.get('lastPrice', 0)} (源:{base})")
                    except Exception as _e:
                        print(f"Bybit现货解析基础符号失败: {_e}")
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
                # 使用映射后的币种名称进行订阅（包含必要别名）
                args_set = set()
                for symbol in batch_symbols:
                    mapping = self._get_bybit_symbol_mapping(symbol)
                    mapped_symbol = mapping['spot']
                    args_set.add(f"tickers.{mapped_symbol}USDT")
                    if symbol != mapped_symbol:
                        print(f"Bybit现货订阅映射: {symbol} -> {mapped_symbol}")
                    # 针对NEIRO系列，额外订阅可能的别名（如 NEIROUSDT）
                    if mapped_symbol == 'NEIROETH':
                        args_set.add("tickers.NEIROUSDT")
                
                subscribe_msg = {
                    "op": "subscribe",
                    "args": sorted(list(args_set))
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
                    
                    # 解析并归并：以Bybit返回的基础符号为准（去掉USDT后缀）
                    try:
                        base = symbol_name[:-4] if symbol_name.endswith('USDT') else symbol_name
                        coin = self._bybit_observed_to_coin(base)
                        bybit_futures = self.exchange_symbols.get('bybit', {}).get('futures', [])
                        if coin in bybit_futures:
                            self.data['bybit'].setdefault(coin, {'spot': {}, 'futures': {}, 'funding_rate': {}})
                            funding_rate = item.get('fundingRate', 0)
                            if funding_rate == '':
                                funding_rate = 0
                            if 'lastPrice' in item:
                                # 完整更新包括价格
                                current_funding_rate = self.data['bybit'][coin]['futures'].get('funding_rate', 0) if self.data['bybit'][coin]['futures'] else 0
                                final_funding_rate = current_funding_rate
                                if 'fundingRate' in item:
                                    try:
                                        final_funding_rate = float(funding_rate) if funding_rate not in ('', 0) else current_funding_rate
                                    except Exception:
                                        final_funding_rate = current_funding_rate
                                self.data['bybit'][coin]['futures'] = {
                                    'price': float(item.get('lastPrice', 0) or 0),
                                    'funding_rate': final_funding_rate,
                                    'next_funding_time': item.get('nextFundingTime', ''),
                                    'volume': float(item.get('volume24h', 0) or 0),
                                    'timestamp': datetime.now(timezone.utc).isoformat(),
                                    'symbol': symbol_name
                                }
                                print(f"Bybit {coin} 合约价格: {item.get('lastPrice', 0)}, 资金费率: {self.data['bybit'][coin]['futures'].get('funding_rate', 0)} (源:{base})")
                            else:
                                # 增量更新：只更新有提供的字段
                                if self.data['bybit'][coin]['futures']:
                                    current_data = self.data['bybit'][coin]['futures'].copy()
                                    if 'fundingRate' in item and funding_rate not in ('', 0):
                                        try:
                                            current_data['funding_rate'] = float(funding_rate)
                                        except Exception:
                                            pass
                                    if 'nextFundingTime' in item:
                                        current_data['next_funding_time'] = item.get('nextFundingTime', '')
                                    if 'volume24h' in item:
                                        current_data['volume'] = float(item.get('volume24h', 0) or 0)
                                    current_data['timestamp'] = datetime.now(timezone.utc).isoformat()
                                    self.data['bybit'][coin]['futures'] = current_data
                                    print(f"Bybit {coin} 合约增量更新 (保持价格: {current_data.get('price', 0)}, 资金费率: {current_data.get('funding_rate', 0)})")
                                else:
                                    print(f"Bybit {coin} 合约无价格数据，跳过增量更新")
                    except Exception as _e:
                        print(f"Bybit合约解析基础符号失败: {_e}")
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
                args_set = set()
                for symbol in batch_symbols:
                    mapping = self._get_bybit_symbol_mapping(symbol)
                    mapped_symbol = mapping['futures']
                    args_set.add(f"tickers.{mapped_symbol}USDT")
                    if symbol != mapped_symbol:
                        print(f"Bybit合约订阅映射: {symbol} -> {mapped_symbol}")
                    # 针对NEIRO系列，额外订阅可能的别名（如 NEIROUSDT）
                    if mapped_symbol == 'NEIROETH':
                        args_set.add("tickers.NEIROUSDT")
                
                subscribe_msg = {
                    "op": "subscribe",
                    "args": sorted(list(args_set))
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
                bybit_spot = self.exchange_symbols.get('bybit', {}).get('spot', [])
                for i in range(0, len(bybit_spot), batch_size):
                    batch_symbols = bybit_spot[i:i+batch_size]
                    args_set = set()
                    for symbol in batch_symbols:
                        mapped = self._get_bybit_symbol_mapping(symbol)['spot']
                        args_set.add(f"tickers.{mapped}USDT")
                        if mapped == 'NEIROETH':
                            args_set.add("tickers.NEIROUSDT")
                    
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": sorted(list(args_set))
                    }
                    print(f"Bybit现货重新订阅批次 {i//batch_size + 1}: {len(batch_symbols)}个有效币种")
                    ws.send(json.dumps(subscribe_msg))
                    time.sleep(0.1)
                    
            elif channel_type == 'linear' and 'bybit_linear' in self.ws_connections:
                ws = self.ws_connections['bybit_linear']
                # 分批订阅合约币种
                batch_size = 10
                bybit_futures = self.exchange_symbols.get('bybit', {}).get('futures', [])
                for i in range(0, len(bybit_futures), batch_size):
                    batch_symbols = bybit_futures[i:i+batch_size]
                    args_set = set()
                    for symbol in batch_symbols:
                        mapped = self._get_bybit_symbol_mapping(symbol)['futures']
                        args_set.add(f"tickers.{mapped}USDT")
                        if mapped == 'NEIROETH':
                            args_set.add("tickers.NEIROUSDT")
                    
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": sorted(list(args_set))
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
                                                'timestamp': datetime.now(timezone.utc).isoformat(),
                                                'symbol': inst_id
                                            }
                                            print(f"Bitget {symbol} 合约价格: {price}, 资金费率: {funding_rate}")
                                    elif inst_type == 'SPOT':  # 现货
                                        price = float(item.get('lastPr', 0)) if item.get('lastPr') and str(item.get('lastPr')) != '0' else 0
                                        
                                        # 只在有有效价格时更新，避免把占位数据写入面板
                                        if price > 0:
                                            self.data['bitget'][symbol]['spot'] = {
                                                'price': price,
                                                'volume': float(item.get('baseVolume', 0)) if item.get('baseVolume') else 0,
                                                'timestamp': datetime.now(timezone.utc).isoformat(),
                                                'symbol': inst_id,
                                                'is_active': price > 0  # 标记是否有实际价格
                                            }
                                            print(f"Bitget {symbol} 现货价格: {price}")
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
            resolved_symbol = self._resolve_exchange_symbol(exchange, symbol)
            if resolved_symbol in self.data[exchange]:
                symbol_data[exchange] = self.data[exchange][resolved_symbol]
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
                resolved_symbol = self._resolve_exchange_symbol(exchange, symbol)
                if resolved_symbol in self.data[exchange]:
                    spot_price = self.data[exchange][resolved_symbol]['spot'].get('price', 0)
                    futures_price = self.data[exchange][resolved_symbol]['futures'].get('price', 0)

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

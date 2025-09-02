from flask import Flask, render_template, jsonify, request
import json
import threading
import time
import gc
import psutil
import os
from datetime import datetime
from exchange_connectors import ExchangeDataCollector
from config import SUPPORTED_SYMBOLS, DATA_REFRESH_INTERVAL, CURRENT_SUPPORTED_SYMBOLS, MEMORY_OPTIMIZATION_CONFIG, WS_UPDATE_INTERVAL, WS_CONNECTION_CONFIG
from database import PriceDatabase
from market_info import get_dynamic_symbols, get_market_report

app = Flask(__name__)

# 全局数据收集器
data_collector = ExchangeDataCollector()

# 数据库实例
db = PriceDatabase()

# 优化的内存数据结构 - 减少内存占用
class MemoryDataManager:
    def __init__(self):
        self.max_records = MEMORY_OPTIMIZATION_CONFIG['max_historical_records']
        self.cleanup_interval = MEMORY_OPTIMIZATION_CONFIG['memory_cleanup_interval']
        self.last_cleanup = datetime.now()
        self.data = {}
        self._init_data_structure()
    
    def _init_data_structure(self):
        """初始化数据结构"""
        for symbol in CURRENT_SUPPORTED_SYMBOLS:
            self.data[symbol] = {
                'okx': [],
                'binance': [],
                'bybit': [],
                'bitget': []
            }
    
    def add_record(self, symbol, exchange, record):
        """添加记录并控制内存使用"""
        if symbol not in self.data:
            self.data[symbol] = {
                'okx': [],
                'binance': [],
                'bybit': [],
                'bitget': []
            }
        
        exchange_data = self.data[symbol].get(exchange, [])
        exchange_data.append(record)
        
        # 控制内存使用 - 保持记录数不超过限制
        if len(exchange_data) > self.max_records:
            # 移除最旧的记录
            exchange_data.pop(0)
        
        self.data[symbol][exchange] = exchange_data
    
    def get_data(self, symbol=None):
        """获取数据"""
        if symbol:
            return self.data.get(symbol, {})
        return self.data
    
    def cleanup_memory(self):
        """定期内存清理"""
        current_time = datetime.now()
        if (current_time - self.last_cleanup).seconds >= self.cleanup_interval:
            print("执行内存清理...")
            # 清理空的数据结构
            empty_symbols = []
            for symbol, symbol_data in self.data.items():
                if all(len(exchange_data) == 0 for exchange_data in symbol_data.values()):
                    empty_symbols.append(symbol)
            
            for symbol in empty_symbols:
                del self.data[symbol]
            
            if empty_symbols:
                print(f"清理了 {len(empty_symbols)} 个空的币种数据")
            
            self.last_cleanup = current_time

# 使用优化的内存管理器
memory_manager = MemoryDataManager()

def background_data_collection():
    """优化的后台数据收集 - 减少磁盘写入频率"""
    last_maintenance = datetime.now()
    maintenance_interval = 300   # 5分钟执行一次维护任务（从1分钟改为5分钟）
    
    # 批处理缓冲区
    batch_buffer = []
    batch_size = MEMORY_OPTIMIZATION_CONFIG['batch_size']
    
    while True:
        try:
            # 获取所有数据（包含所有币种）
            all_data = data_collector.get_all_data()
            timestamp = datetime.now().isoformat()
            
            # 为每个币种保存历史数据 - 优化内存和磁盘使用
            for symbol in SUPPORTED_SYMBOLS:
                premium_data = data_collector.calculate_premium(symbol)
                
                for exchange in all_data:
                    if symbol in all_data[exchange]:
                        symbol_data = all_data[exchange][symbol]
                        if symbol_data['spot'] or symbol_data['futures']:
                            # 保存到优化的内存管理器
                            historical_entry = {
                                'timestamp': timestamp,
                                'spot_price': symbol_data['spot'].get('price', 0),
                                'futures_price': symbol_data['futures'].get('price', 0),
                                'funding_rate': symbol_data['futures'].get('funding_rate', 0),
                                'premium': premium_data.get(exchange, {}).get('premium_percent', 0)
                            }
                            
                            # 使用内存管理器添加记录
                            memory_manager.add_record(symbol, exchange, historical_entry)
                            
                            # 批量保存到数据库以减少磁盘IO
                            batch_buffer.append({
                                'symbol': symbol,
                                'exchange': exchange,
                                'symbol_data': symbol_data,
                                'premium_data': premium_data
                            })
                            
                            # 当批处理缓冲区满时，批量写入数据库
                            if len(batch_buffer) >= batch_size:
                                try:
                                    for batch_item in batch_buffer:
                                        db.save_price_data(
                                            batch_item['symbol'],
                                            batch_item['exchange'],
                                            batch_item['symbol_data'],
                                            batch_item['premium_data']
                                        )
                                    batch_buffer.clear()
                                    print(f"批量写入数据库完成: {batch_size} 条记录")
                                except Exception as e:
                                    print(f"批量写入数据库失败: {e}")
                                    batch_buffer.clear()  # 清空缓冲区避免重复尝试
            
            # 定期维护任务（每5分钟执行一次）
            current_time = datetime.now()
            if (current_time - last_maintenance).seconds >= maintenance_interval:
                try:
                    print("开始定期维护任务...")
                    
                    # 写入剩余的批处理数据
                    if batch_buffer:
                        print(f"写入剩余批处理数据: {len(batch_buffer)} 条记录")
                        for batch_item in batch_buffer:
                            db.save_price_data(
                                batch_item['symbol'],
                                batch_item['exchange'],
                                batch_item['symbol_data'],
                                batch_item['premium_data']
                            )
                        batch_buffer.clear()
                    
                    # 内存清理
                    memory_manager.cleanup_memory()
                    
                    # 强制垃圾回收
                    collected = gc.collect()
                    if collected > 0:
                        print(f"垃圾回收: 清理了 {collected} 个对象")
                    
                    # 资源监控
                    try:
                        process = psutil.Process(os.getpid())
                        memory_info = process.memory_info()
                        cpu_percent = process.cpu_percent()
                        print(f"资源使用: 内存 {memory_info.rss / 1024 / 1024:.1f}MB, CPU {cpu_percent:.1f}%")
                    except:
                        pass  # 忽略监控错误
                    
                    # 数据库维护（降低频率）
                    db.aggregate_to_1min()
                    db.cleanup_old_data()
                    
                    last_maintenance = current_time
                    print("定期维护任务完成")
                    
                except Exception as e:
                    print(f"定期维护错误: {e}")
            
            # 仅显示当前币种的更新日志
            current_symbol = data_collector.current_symbol
            active_exchanges = 0
            for exchange in all_data:
                if current_symbol in all_data[exchange] and (all_data[exchange][current_symbol]['spot'] or all_data[exchange][current_symbol]['futures']):
                    active_exchanges += 1
            
            if active_exchanges > 0:
                print(f"数据更新: {current_symbol} - {active_exchanges}个交易所活跃 - {timestamp}")
            
        except Exception as e:
            print(f"数据收集错误: {e}")
        
        time.sleep(DATA_REFRESH_INTERVAL)

@app.route('/')
def index():
    """主页 - 增强版按币种聚合展示所有可用币种"""
    return render_template('enhanced_aggregated.html', 
                         symbols=data_collector.supported_symbols)

@app.route('/exchanges')
def exchanges_view():
    """按交易所展示页面"""
    return render_template('simple_index.html', 
                         symbols=data_collector.supported_symbols,
                         current_symbol=data_collector.current_symbol)

@app.route('/aggregated')
def aggregated_index():
    """聚合页面 - 按币种聚合展示（重定向到主页）"""
    return render_template('aggregated_index.html', 
                         symbols=data_collector.supported_symbols)

@app.route('/charts')
def charts():
    """图表页面"""
    return render_template('chart_index.html',
                         symbols=SUPPORTED_SYMBOLS,
                         current_symbol=data_collector.current_symbol)

@app.route('/api/data')
def get_current_data():
    """获取当前显示币种的实时数据API"""
    current_symbol = data_collector.current_symbol
    symbol_data = data_collector.get_symbol_data(current_symbol)
    premium_data = data_collector.calculate_premium(current_symbol)
    
    return jsonify({
        'realtime_data': symbol_data,
        'premium_data': premium_data,
        'symbol': current_symbol,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/data/all')
def get_all_data():
    """获取所有币种的实时数据API"""
    all_data = data_collector.get_all_data()
    all_premiums = data_collector.calculate_all_premiums()
    
    return jsonify({
        'all_realtime_data': all_data,
        'all_premium_data': all_premiums,
        'supported_symbols': SUPPORTED_SYMBOLS,
        'current_symbol': data_collector.current_symbol,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/history/<symbol>')
def get_historical_data(symbol):
    """获取内存中的历史数据API (用于实时图表)"""
    symbol = symbol.upper()
    data = memory_manager.get_data(symbol)
    if data:
        return jsonify(data)
    return jsonify({})

@app.route('/api/history/<symbol>/database')
def get_database_historical_data(symbol):
    """获取数据库中的历史数据API"""
    symbol = symbol.upper()
    if symbol not in SUPPORTED_SYMBOLS:
        return jsonify({'error': 'Unsupported symbol'}), 400
    
    # 获取查询参数
    hours = request.args.get('hours', 24, type=int)  # 默认24小时
    exchange = request.args.get('exchange', None)    # 特定交易所，默认所有
    interval = request.args.get('interval', '1min')  # 数据间隔，默认1分钟
    
    # 限制查询范围以提高性能
    hours = min(hours, 168)  # 最多7天
    
    try:
        data = db.get_historical_data(symbol, exchange, hours, interval)
        return jsonify({
            'symbol': symbol,
            'exchange': exchange,
            'hours': hours,
            'interval': interval,
            'data': data,
            'count': len(data)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart/<symbol>')
def get_chart_data(symbol):
    """获取图表数据API - 优化格式用于Chart.js"""
    symbol = symbol.upper()
    if symbol not in SUPPORTED_SYMBOLS:
        return jsonify({'error': 'Unsupported symbol'}), 400
    
    # 获取查询参数
    hours = request.args.get('hours', 6, type=int)  # 默认6小时用于图表
    exchange = request.args.get('exchange', None)
    interval = request.args.get('interval', '1min')
    
    # 限制查询范围
    hours = min(hours, 168)  # 最多7天
    
    try:
        raw_data = db.get_historical_data(symbol, exchange, hours, interval)
        
        # 按交易所组织数据
        chart_data = {}
        for row in raw_data:
            exchange_name = row['exchange']
            if exchange_name not in chart_data:
                chart_data[exchange_name] = {
                    'labels': [],
                    'spot_prices': [],
                    'futures_prices': [],
                    'funding_rates': [],
                    'premiums': []
                }
            
            # 使用时间戳作为标签
            timestamp = row['timestamp']
            chart_data[exchange_name]['labels'].append(timestamp)
            
            # 添加价格数据
            if interval == '1min':
                chart_data[exchange_name]['spot_prices'].append(row.get('spot_price_close', 0))
                chart_data[exchange_name]['futures_prices'].append(row.get('futures_price_close', 0))
                chart_data[exchange_name]['funding_rates'].append(row.get('funding_rate_avg', 0))
                chart_data[exchange_name]['premiums'].append(row.get('premium_percent_avg', 0))
            else:
                chart_data[exchange_name]['spot_prices'].append(row.get('spot_price', 0))
                chart_data[exchange_name]['futures_prices'].append(row.get('futures_price', 0))
                chart_data[exchange_name]['funding_rates'].append(row.get('funding_rate', 0))
                chart_data[exchange_name]['premiums'].append(row.get('premium_percent', 0))
        
        return jsonify({
            'symbol': symbol,
            'hours': hours,
            'interval': interval,
            'chart_data': chart_data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/latest/<symbol>')
def get_latest_prices(symbol):
    """获取最新价格数据API"""
    symbol = symbol.upper()
    if symbol not in SUPPORTED_SYMBOLS:
        return jsonify({'error': 'Unsupported symbol'}), 400
    
    try:
        latest_data = db.get_latest_prices(symbol)
        return jsonify({
            'symbol': symbol,
            'latest_data': latest_data,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/switch_symbol', methods=['POST'])
def switch_symbol():
    """切换币种API"""
    new_symbol = request.json.get('symbol', '').upper()
    if new_symbol in data_collector.supported_symbols:
        data_collector.set_symbol(new_symbol)
        return jsonify({'status': 'success', 'symbol': new_symbol})
    return jsonify({'status': 'error', 'message': 'Unsupported symbol'})

@app.route('/api/aggregated_data')
def get_aggregated_data():
    """获取聚合数据API - 按币种聚合所有交易所数据"""
    try:
        all_data = data_collector.get_all_data()
        
        # 重组数据结构：从 交易所->币种 改为 币种->交易所
        aggregated = {}
        for exchange in all_data:
            for symbol in all_data[exchange]:
                if symbol not in aggregated:
                    aggregated[symbol] = {}
                aggregated[symbol][exchange] = all_data[exchange][symbol]
        
        return jsonify({
            'aggregated_data': aggregated,
            'supported_symbols': data_collector.supported_symbols,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/markets')
def get_markets_info():
    """获取市场信息 - 支持的币种及其交易所覆盖情况"""
    try:
        # 获取市场报告（如果缓存有效则使用缓存）
        report = get_market_report(force_refresh=False)
        
        return jsonify({
            'market_report': report,
            'current_symbols': data_collector.supported_symbols,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/coverage')
def get_coverage_stats():
    """获取市场覆盖度统计"""
    try:
        all_data = data_collector.get_all_data()
        
        # 计算覆盖度统计
        stats = {
            'total_symbols': len(data_collector.supported_symbols),
            'exchange_stats': {},
            'symbol_stats': {},
            'quality_metrics': {}
        }
        
        # 按交易所统计
        for exchange in ['okx', 'binance', 'bybit', 'bitget']:
            spot_count = sum(1 for symbol in data_collector.supported_symbols 
                           if all_data.get(exchange, {}).get(symbol, {}).get('spot', {}).get('price', 0) > 0)
            futures_count = sum(1 for symbol in data_collector.supported_symbols 
                              if all_data.get(exchange, {}).get(symbol, {}).get('futures', {}).get('price', 0) > 0)
            
            stats['exchange_stats'][exchange] = {
                'spot_symbols': spot_count,
                'futures_symbols': futures_count,
                'coverage_percent': round((spot_count + futures_count) / (len(data_collector.supported_symbols) * 2) * 100, 2)
            }
        
        return jsonify({
            'coverage_stats': stats,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/database/stats')
def database_stats():
    """获取数据库统计信息API"""
    try:
        import sqlite3
        import os
        
        stats = {}
        
        # 数据库文件大小
        if os.path.exists(db.db_path):
            stats['database_size_mb'] = round(os.path.getsize(db.db_path) / 1024 / 1024, 2)
        else:
            stats['database_size_mb'] = 0
        
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            
            # 原始数据记录数
            cursor.execute('SELECT COUNT(*) FROM price_data')
            stats['raw_records'] = cursor.fetchone()[0]
            
            # 1分钟聚合数据记录数
            cursor.execute('SELECT COUNT(*) FROM price_data_1min')
            stats['aggregated_records'] = cursor.fetchone()[0]
            
            # 最早和最晚的数据时间
            cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM price_data')
            raw_range = cursor.fetchone()
            stats['raw_data_range'] = {
                'earliest': raw_range[0],
                'latest': raw_range[1]
            }
            
            cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM price_data_1min')
            agg_range = cursor.fetchone()
            stats['aggregated_data_range'] = {
                'earliest': agg_range[0],
                'latest': agg_range[1]
            }
            
            # 按交易所和币种统计数据
            cursor.execute('''
                SELECT symbol, exchange, COUNT(*) as count
                FROM price_data
                WHERE timestamp >= datetime('now', '-24 hours')
                GROUP BY symbol, exchange
                ORDER BY symbol, exchange
            ''')
            stats['recent_data_by_symbol_exchange'] = [
                {'symbol': row[0], 'exchange': row[1], 'count': row[2]}
                for row in cursor.fetchall()
            ]
        
        return jsonify({
            'status': 'success',
            'stats': stats,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/database/maintenance', methods=['POST'])
def manual_maintenance():
    """手动触发数据库维护API"""
    try:
        print("手动触发数据库维护...")
        
        # 数据聚合
        db.aggregate_to_1min()
        
        # 数据清理
        db.cleanup_old_data()
        
        print("手动数据库维护完成")
        
        return jsonify({
            'status': 'success', 
            'message': '数据库维护任务已完成',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/system/status')
def get_system_status():
    """获取系统状态监控API"""
    try:
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        # 获取内存管理器状态
        memory_stats = {}
        for symbol, symbol_data in memory_manager.data.items():
            total_records = sum(len(exchange_data) for exchange_data in symbol_data.values())
            if total_records > 0:
                memory_stats[symbol] = total_records
        
        status = {
            'system': {
                'memory_usage_mb': round(memory_info.rss / 1024 / 1024, 1),
                'cpu_percent': process.cpu_percent(),
                'threads_count': process.num_threads(),
                'connections_count': len(data_collector.ws_connections),
            },
            'data': {
                'total_symbols': len(CURRENT_SUPPORTED_SYMBOLS),
                'active_memory_symbols': len(memory_stats),
                'memory_records_per_symbol': memory_stats,
                'max_records_per_exchange': MEMORY_OPTIMIZATION_CONFIG['max_historical_records']
            },
            'connections': {
                'websocket_status': {
                    name: 'connected' if ws else 'disconnected'
                    for name, ws in data_collector.ws_connections.items()
                },
                'reconnect_attempts': data_collector.reconnect_attempts
            },
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(status)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("🚀 启动WLFI资金费率监控系统...")
    
    # 显示优化配置信息
    print(f"📊 监控币种数量: {len(CURRENT_SUPPORTED_SYMBOLS)}")
    print(f"⚡ 数据更新间隔: {DATA_REFRESH_INTERVAL}秒")
    print(f"🔄 WebSocket数据间隔: {WS_UPDATE_INTERVAL}秒") 
    print(f"💾 内存最大记录数: {MEMORY_OPTIMIZATION_CONFIG['max_historical_records']}")
    print(f"🔧 最大重连次数: {WS_CONNECTION_CONFIG['max_reconnect_attempts']}")
    print(f"🕒 重连基础延迟: {WS_CONNECTION_CONFIG['base_reconnect_delay']}秒")
    
    try:
        # 显示系统资源
        process = psutil.Process(os.getpid())
        print(f"🖥️  初始内存使用: {process.memory_info().rss / 1024 / 1024:.1f}MB")
    except:
        pass
    
    print("📡 启动数据收集...")
    # 启动数据收集
    data_collector.start_all_connections()
    
    print("🔄 启动后台数据处理...")
    # 启动后台数据收集线程
    background_thread = threading.Thread(target=background_data_collection, daemon=True)
    background_thread.start()
    
    print("🌐 启动Web服务器...")
    print("📊 系统状态监控: http://localhost:4002/api/system/status")
    
    # 启动Flask应用
    app.run(debug=False, host='0.0.0.0', port=4002, threaded=True)
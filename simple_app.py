from flask import Flask, render_template, jsonify, request
import json
import threading
import time
from datetime import datetime
from exchange_connectors import ExchangeDataCollector
from config import SUPPORTED_SYMBOLS, DATA_REFRESH_INTERVAL
from database import PriceDatabase

app = Flask(__name__)

# 全局数据收集器
data_collector = ExchangeDataCollector()

# 数据库实例
db = PriceDatabase()

# 历史数据存储 (简单内存存储 - 保留用于实时显示)
historical_data = {symbol: {exchange: [] for exchange in ['okx', 'binance', 'bybit', 'bitget']} 
                  for symbol in SUPPORTED_SYMBOLS}

def background_data_collection():
    """后台数据收集 - 多币种模式"""
    last_maintenance = datetime.now()
    maintenance_interval = 60   # 1分钟执行一次维护任务
    
    while True:
        try:
            # 获取所有数据（包含所有币种）
            all_data = data_collector.get_all_data()
            timestamp = datetime.now().isoformat()
            
            # 为每个币种保存历史数据
            for symbol in SUPPORTED_SYMBOLS:
                premium_data = data_collector.calculate_premium(symbol)
                
                for exchange in all_data:
                    if symbol in all_data[exchange]:
                        symbol_data = all_data[exchange][symbol]
                        if symbol_data['spot'] or symbol_data['futures']:
                            # 保存到内存（用于实时显示）
                            historical_entry = {
                                'timestamp': timestamp,
                                'spot_price': symbol_data['spot'].get('price', 0),
                                'futures_price': symbol_data['futures'].get('price', 0),
                                'funding_rate': symbol_data['futures'].get('funding_rate', 0),
                                'premium': premium_data.get(exchange, {}).get('premium_percent', 0)
                            }
                            
                            # 保持历史数据不超过1000条
                            if len(historical_data[symbol][exchange]) >= 1000:
                                historical_data[symbol][exchange].pop(0)
                            historical_data[symbol][exchange].append(historical_entry)
                            
                            # 保存到数据库（用于持久化和图表展示）
                            db.save_price_data(symbol, exchange, symbol_data, premium_data)
            
            # 数据库维护任务（每1分钟执行一次）
            current_time = datetime.now()
            if (current_time - last_maintenance).seconds >= maintenance_interval:
                try:
                    print("开始数据库维护任务...")
                    
                    # 数据聚合：将原始数据聚合为1分钟精度
                    db.aggregate_to_1min()
                    
                    # 数据清理：清理7天前的原始数据，保留30天的1分钟数据
                    db.cleanup_old_data()
                    
                    last_maintenance = current_time
                    print("数据库维护任务完成")
                    
                except Exception as e:
                    print(f"数据库维护错误: {e}")
            
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
    """主页"""
    return render_template('simple_index.html', 
                         symbols=SUPPORTED_SYMBOLS,
                         current_symbol=data_collector.current_symbol)

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
    if symbol in historical_data:
        return jsonify(historical_data[symbol])
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
    if new_symbol in SUPPORTED_SYMBOLS:
        data_collector.set_symbol(new_symbol)
        return jsonify({'status': 'success', 'symbol': new_symbol})
    return jsonify({'status': 'error', 'message': 'Unsupported symbol'})

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

if __name__ == '__main__':
    # 启动数据收集
    data_collector.start_all_connections()
    
    # 启动后台数据收集线程
    background_thread = threading.Thread(target=background_data_collection, daemon=True)
    background_thread.start()
    
    # 启动Flask应用
    app.run(debug=False, host='0.0.0.0', port=5000, threaded=True)
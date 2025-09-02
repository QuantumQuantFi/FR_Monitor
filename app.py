from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import json
import threading
import time
from datetime import datetime
from exchange_connectors import ExchangeDataCollector
from config import SUPPORTED_SYMBOLS, DATA_REFRESH_INTERVAL, CURRENT_SUPPORTED_SYMBOLS
from market_info import get_dynamic_symbols, get_market_report

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
socketio = SocketIO(app, cors_allowed_origins="*")

# 全局数据收集器
data_collector = ExchangeDataCollector()

# 历史数据存储 (简单内存存储) - 使用动态币种列表
historical_data = {symbol: {exchange: [] for exchange in ['okx', 'binance', 'bybit', 'bitget']} 
                  for symbol in CURRENT_SUPPORTED_SYMBOLS}

def background_data_collection():
    """后台数据收集和广播"""
    while True:
        try:
            # 获取当前数据
            current_data = data_collector.get_all_data()
            premium_data = data_collector.calculate_premium()
            
            # 保存历史数据
            timestamp = datetime.now().isoformat()
            current_symbol = data_collector.current_symbol
            
            for exchange in current_data:
                if current_data[exchange]['spot'] or current_data[exchange]['futures']:
                    historical_entry = {
                        'timestamp': timestamp,
                        'spot_price': current_data[exchange]['spot'].get('price', 0),
                        'futures_price': current_data[exchange]['futures'].get('price', 0),
                        'funding_rate': current_data[exchange]['futures'].get('funding_rate', 0),
                        'premium': premium_data.get(exchange, {}).get('premium_percent', 0)
                    }
                    
                    # 保持历史数据不超过1000条
                    if current_symbol in historical_data:
                        if len(historical_data[current_symbol][exchange]) >= 1000:
                            historical_data[current_symbol][exchange].pop(0)
                        historical_data[current_symbol][exchange].append(historical_entry)
            
            # 通过WebSocket广播数据
            socketio.emit('market_update', {
                'realtime_data': current_data,
                'premium_data': premium_data,
                'symbol': current_symbol,
                'timestamp': timestamp
            })
            
        except Exception as e:
            print(f"数据收集错误: {e}")
        
        time.sleep(DATA_REFRESH_INTERVAL)

@app.route('/')
def index():
    """主页 - 传统按交易所展示"""
    return render_template('simple_index.html', 
                         symbols=data_collector.supported_symbols,
                         current_symbol=data_collector.current_symbol)

@app.route('/aggregated')
def aggregated_index():
    """聚合页面 - 按币种聚合展示"""
    return render_template('aggregated_index.html', 
                         symbols=data_collector.supported_symbols)

@app.route('/api/data')
def get_current_data():
    """获取当前实时数据API"""
    current_data = data_collector.get_all_data()
    premium_data = data_collector.calculate_premium()
    
    return jsonify({
        'realtime_data': current_data,
        'premium_data': premium_data,
        'symbol': data_collector.current_symbol,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/history/<symbol>')
def get_historical_data(symbol):
    """获取历史数据API"""
    symbol = symbol.upper()
    if symbol in historical_data:
        return jsonify(historical_data[symbol])
    return jsonify({})

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

@app.route('/api/symbol/<symbol>/aggregated')
def get_symbol_aggregated(symbol):
    """获取单个币种的聚合数据"""
    symbol = symbol.upper()
    if symbol not in data_collector.supported_symbols:
        return jsonify({'error': 'Symbol not supported'}), 404
    
    try:
        symbol_data = data_collector.get_symbol_data(symbol)
        premium_data = data_collector.calculate_premium(symbol)
        
        return jsonify({
            'symbol': symbol,
            'exchanges': symbol_data,
            'premium_data': premium_data,
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

@app.route('/api/markets/refresh', methods=['POST'])
def refresh_markets():
    """强制刷新市场信息"""
    try:
        # 强制刷新市场信息
        report = get_market_report(force_refresh=True)
        new_symbols = get_dynamic_symbols(force_refresh=True)
        
        # 触发数据收集器更新币种列表
        data_collector.update_symbols_list()
        
        return jsonify({
            'status': 'success',
            'new_symbols_count': len(new_symbols),
            'market_report': report,
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
        
        # 按币种统计覆盖度
        high_coverage = medium_coverage = low_coverage = 0
        for symbol in data_collector.supported_symbols:
            exchanges_with_data = 0
            for exchange in ['okx', 'binance', 'bybit', 'bitget']:
                if (all_data.get(exchange, {}).get(symbol, {}).get('spot', {}).get('price', 0) > 0 or
                    all_data.get(exchange, {}).get(symbol, {}).get('futures', {}).get('price', 0) > 0):
                    exchanges_with_data += 1
            
            coverage_percent = exchanges_with_data / 4 * 100
            if coverage_percent >= 75:
                high_coverage += 1
            elif coverage_percent >= 25:
                medium_coverage += 1
            else:
                low_coverage += 1
        
        stats['symbol_stats'] = {
            'high_coverage': high_coverage,
            'medium_coverage': medium_coverage, 
            'low_coverage': low_coverage
        }
        
        # 数据质量指标
        total_possible_connections = len(data_collector.supported_symbols) * 4 * 2  # 币种 * 交易所 * (现货+期货)
        active_connections = sum(
            sum(1 for symbol in data_collector.supported_symbols 
                if (all_data.get(exchange, {}).get(symbol, {}).get('spot', {}).get('price', 0) > 0 or
                    all_data.get(exchange, {}).get(symbol, {}).get('futures', {}).get('price', 0) > 0))
            for exchange in ['okx', 'binance', 'bybit', 'bitget']
        )
        
        stats['quality_metrics'] = {
            'data_completeness': round(active_connections / total_possible_connections * 100, 2) if total_possible_connections > 0 else 0,
            'active_connections': active_connections,
            'total_possible': total_possible_connections
        }
        
        return jsonify({
            'coverage_stats': stats,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    print('客户端已连接')
    emit('connected', {'data': '连接成功'})

@socketio.on('disconnect')
def handle_disconnect():
    print('客户端已断开')

@socketio.on('switch_symbol')
def handle_switch_symbol(data):
    """WebSocket币种切换"""
    new_symbol = data.get('symbol', '').upper()
    if new_symbol in SUPPORTED_SYMBOLS:
        data_collector.set_symbol(new_symbol)
        emit('symbol_switched', {'symbol': new_symbol})

if __name__ == '__main__':
    # 启动数据收集
    data_collector.start_all_connections()
    
    # 启动后台数据收集线程
    background_thread = threading.Thread(target=background_data_collection, daemon=True)
    background_thread.start()
    
    # 启动Flask应用
    socketio.run(app, debug=False, host='0.0.0.0', port=5000, async_mode='threading')
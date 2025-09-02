from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import json
import threading
import time
from datetime import datetime
from exchange_connectors import ExchangeDataCollector
from config import SUPPORTED_SYMBOLS, DATA_REFRESH_INTERVAL

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
socketio = SocketIO(app, cors_allowed_origins="*")

# 全局数据收集器
data_collector = ExchangeDataCollector()

# 历史数据存储 (简单内存存储)
historical_data = {symbol: {exchange: [] for exchange in ['okx', 'binance', 'bybit', 'bitget']} 
                  for symbol in SUPPORTED_SYMBOLS}

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
    """主页"""
    return render_template('index.html', 
                         symbols=SUPPORTED_SYMBOLS,
                         current_symbol=data_collector.current_symbol)

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
    if new_symbol in SUPPORTED_SYMBOLS:
        data_collector.set_symbol(new_symbol)
        return jsonify({'status': 'success', 'symbol': new_symbol})
    return jsonify({'status': 'error', 'message': 'Unsupported symbol'})

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
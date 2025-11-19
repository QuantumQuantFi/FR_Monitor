import sqlite3
import json
from datetime import datetime
import threading
import os

class PriceDatabase:
    def __init__(self, db_path='market_data.db'):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.init_database()
    
    def init_database(self):
        """初始化数据库表结构"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 创建价格数据表 - 每秒精度
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    exchange VARCHAR(20) NOT NULL,
                    spot_price REAL DEFAULT 0.0,
                    futures_price REAL DEFAULT 0.0,
                    funding_rate REAL DEFAULT 0.0,
                    mark_price REAL DEFAULT 0.0,
                    index_price REAL DEFAULT 0.0,
                    premium_percent REAL DEFAULT 0.0,
                    volume_24h REAL DEFAULT 0.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建聚合数据表 - 每分钟精度
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS price_data_1min (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    exchange VARCHAR(20) NOT NULL,
                    spot_price_open REAL DEFAULT 0.0,
                    spot_price_high REAL DEFAULT 0.0,
                    spot_price_low REAL DEFAULT 0.0,
                    spot_price_close REAL DEFAULT 0.0,
                    futures_price_open REAL DEFAULT 0.0,
                    futures_price_high REAL DEFAULT 0.0,
                    futures_price_low REAL DEFAULT 0.0,
                    futures_price_close REAL DEFAULT 0.0,
                    funding_rate_avg REAL DEFAULT 0.0,
                    premium_percent_avg REAL DEFAULT 0.0,
                    volume_24h_avg REAL DEFAULT 0.0,
                    data_points INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp, symbol, exchange)
                )
            ''')
            
            # 创建索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_timestamp ON price_data (timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_symbol ON price_data (symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_exchange ON price_data (exchange)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_1min_timestamp ON price_data_1min (timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_1min_symbol ON price_data_1min (symbol)')
            
            conn.commit()
    
    def save_price_data(self, symbol, exchange, data, premium_data=None):
        """保存实时价格数据到数据库"""
        try:
            with self.lock:
                timestamp = datetime.now()
                
                spot_price = data.get('spot', {}).get('price', 0.0) if data.get('spot') else 0.0
                futures_price = data.get('futures', {}).get('price', 0.0) if data.get('futures') else 0.0
                funding_rate = data.get('futures', {}).get('funding_rate', 0.0) if data.get('futures') else 0.0
                mark_price = data.get('futures', {}).get('mark_price', 0.0) if data.get('futures') else 0.0
                index_price = data.get('futures', {}).get('index_price', 0.0) if data.get('futures') else 0.0
                volume_24h = data.get('spot', {}).get('volume', 0.0) if data.get('spot') else 0.0
                
                # 计算溢价
                premium_percent = 0.0
                if premium_data and exchange in premium_data:
                    premium_percent = premium_data[exchange].get('premium_percent', 0.0)
                
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO price_data 
                        (timestamp, symbol, exchange, spot_price, futures_price, funding_rate, 
                         mark_price, index_price, premium_percent, volume_24h)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        timestamp, symbol, exchange, spot_price, futures_price, 
                        funding_rate, mark_price, index_price, premium_percent, volume_24h
                    ))
                    conn.commit()
                    
        except Exception as e:
            print(f"数据库保存错误: {e}")
    
    def get_historical_data(self, symbol, exchange=None, hours=24, interval='1min'):
        """获取历史数据"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 根据间隔选择表
                interval_key = (interval or '').lower()
                if interval_key.endswith('min') or interval_key.endswith('h') or interval_key.endswith('day'):
                    table = 'price_data_1min'
                    timestamp_col = 'timestamp'
                else:
                    table = 'price_data'
                    timestamp_col = 'timestamp'
                
                # 不同粒度设置不同的行数上限，分钟级允许返回完整30天 (~43k行)
                max_rows = 50000 if table == 'price_data_1min' else 1000
                limit_clause = f"LIMIT {max_rows}" if max_rows else ""

                # 构建查询
                if exchange:
                    query = f'''
                        SELECT * FROM {table} 
                        WHERE symbol = ? AND exchange = ? 
                        AND {timestamp_col} >= datetime('now', '-{hours} hours')
                        ORDER BY {timestamp_col} DESC
                        {limit_clause}
                    '''
                    params = (symbol, exchange)
                else:
                    query = f'''
                        SELECT * FROM {table} 
                        WHERE symbol = ? 
                        AND {timestamp_col} >= datetime('now', '-{hours} hours')
                        ORDER BY {timestamp_col} DESC
                        {limit_clause}
                    '''
                    params = (symbol,)
                
                cursor.execute(query, params)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            print(f"获取历史数据错误: {e}")
            return []
    
    def aggregate_to_1min(self):
        """聚合数据到1分钟精度"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 获取最新的1分钟聚合时间戳
                cursor.execute('''
                    SELECT MAX(timestamp) FROM price_data_1min
                ''')
                last_aggregated = cursor.fetchone()[0]
                
                # 如果没有聚合过的数据，从1小时前开始
                if not last_aggregated:
                    start_time = "datetime('now', '-1 hour')"
                else:
                    # 从最后聚合时间开始重新聚合，确保覆盖最新数据
                    start_time = f"datetime('{last_aggregated}', '-1 minute')"
                
                # 聚合数据
                cursor.execute(f'''
                    INSERT OR REPLACE INTO price_data_1min 
                    (timestamp, symbol, exchange, spot_price_open, spot_price_high, spot_price_low, spot_price_close,
                     futures_price_open, futures_price_high, futures_price_low, futures_price_close,
                     funding_rate_avg, premium_percent_avg, volume_24h_avg, data_points)
                    SELECT 
                        datetime(strftime('%Y-%m-%d %H:%M:00', timestamp)) as timestamp,
                        symbol,
                        exchange,
                        FIRST_VALUE(spot_price) OVER (PARTITION BY symbol, exchange, datetime(strftime('%Y-%m-%d %H:%M:00', timestamp)) ORDER BY timestamp) as spot_price_open,
                        MAX(spot_price) as spot_price_high,
                        MIN(CASE WHEN spot_price > 0 THEN spot_price ELSE NULL END) as spot_price_low,
                        LAST_VALUE(spot_price) OVER (PARTITION BY symbol, exchange, datetime(strftime('%Y-%m-%d %H:%M:00', timestamp)) ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as spot_price_close,
                        FIRST_VALUE(futures_price) OVER (PARTITION BY symbol, exchange, datetime(strftime('%Y-%m-%d %H:%M:00', timestamp)) ORDER BY timestamp) as futures_price_open,
                        MAX(futures_price) as futures_price_high,
                        MIN(CASE WHEN futures_price > 0 THEN futures_price ELSE NULL END) as futures_price_low,
                        LAST_VALUE(futures_price) OVER (PARTITION BY symbol, exchange, datetime(strftime('%Y-%m-%d %H:%M:00', timestamp)) ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as futures_price_close,
                        AVG(CASE WHEN funding_rate != 0 THEN funding_rate ELSE NULL END) as funding_rate_avg,
                        AVG(premium_percent) as premium_percent_avg,
                        AVG(volume_24h) as volume_24h_avg,
                        COUNT(*) as data_points
                    FROM price_data 
                    WHERE timestamp > {start_time}
                    GROUP BY symbol, exchange, datetime(strftime('%Y-%m-%d %H:%M:00', timestamp))
                ''')
                
                conn.commit()
                
        except Exception as e:
            print(f"数据聚合错误: {e}")
    
    def cleanup_old_data(self, days=7):
        """清理旧数据，保留指定天数"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 清理原始数据（保留7天）
                cursor.execute('''
                    DELETE FROM price_data 
                    WHERE timestamp < datetime('now', '-{} days')
                '''.format(days))
                
                # 清理1分钟数据（保留30天）
                cursor.execute('''
                    DELETE FROM price_data_1min 
                    WHERE timestamp < datetime('now', '-30 days')
                ''')
                
                conn.commit()
                
        except Exception as e:
            print(f"数据清理错误: {e}")

    def get_latest_prices(self, symbol):
        """获取最新价格数据"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT exchange, spot_price, futures_price, funding_rate, premium_percent,
                           timestamp
                    FROM price_data 
                    WHERE symbol = ? 
                    AND timestamp >= datetime('now', '-5 minutes')
                    ORDER BY timestamp DESC
                ''', (symbol,))
                
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            print(f"获取最新价格错误: {e}")
            return []

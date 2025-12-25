import sqlite3
import json
from datetime import datetime
import threading
import os

from precision_utils import funding_rate_to_float
from funding_utils import normalize_next_funding_time

class PriceDatabase:
    DEFAULT_TIMEOUT = 30.0  # seconds
    CLEANUP_BATCH_SIZE = 2000
    LOW_TRAFFIC_UTC_HOURS = {1, 2, 3, 4, 5}

    def __init__(self, db_path='market_data.db'):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.connection_timeout = self.DEFAULT_TIMEOUT
        self._wal_configured = False
        self.init_database()

    def _get_connection(self, timeout_seconds: float = None):
        timeout = self.connection_timeout if timeout_seconds is None else float(timeout_seconds)
        conn = sqlite3.connect(self.db_path, timeout=timeout)
        if not self._wal_configured:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
            except sqlite3.DatabaseError:
                pass
            finally:
                # WAL 可能已在历史连接/进程中设置完成；避免在高并发场景下反复尝试 PRAGMA 导致阻塞。
                self._wal_configured = True
        return conn

    def _ensure_column(self, cursor, table: str, column: str, definition: str):
        cursor.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in cursor.fetchall()}
        if column not in existing:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    
    def init_database(self):
        """初始化数据库表结构"""
        with self._get_connection() as conn:
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
                    funding_interval_hours REAL DEFAULT 0.0,
                    next_funding_time TEXT,
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
                    funding_interval_hours REAL DEFAULT 0.0,
                    next_funding_time TEXT,
                    premium_percent_avg REAL DEFAULT 0.0,
                    volume_24h_avg REAL DEFAULT 0.0,
                    data_points INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(timestamp, symbol, exchange)
                )
            ''')

            # 迁移旧表缺失的列
            self._ensure_column(cursor, 'price_data', 'funding_interval_hours', 'REAL DEFAULT 0.0')
            self._ensure_column(cursor, 'price_data', 'next_funding_time', 'TEXT')
            self._ensure_column(cursor, 'price_data_1min', 'funding_interval_hours', 'REAL DEFAULT 0.0')
            self._ensure_column(cursor, 'price_data_1min', 'next_funding_time', 'TEXT')
            
            # 创建索引
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_timestamp ON price_data (timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_symbol ON price_data (symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_exchange ON price_data (exchange)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_1min_timestamp ON price_data_1min (timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_1min_symbol ON price_data_1min (symbol)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_price_data_1min_exchange_symbol_ts ON price_data_1min (exchange, symbol, timestamp)')
            
            conn.commit()
    
    def save_price_data(self, symbol, exchange, data, premium_data=None):
        """保存实时价格数据到数据库"""
        try:
            with self.lock:
                timestamp = datetime.now()

                def _positive_or_none(value):
                    try:
                        v = float(value)
                        return v if v > 0 else None
                    except (TypeError, ValueError):
                        return None

                def _float_or_none(value):
                    try:
                        return float(value)
                    except (TypeError, ValueError):
                        return None

                spot_price = _positive_or_none(data.get('spot', {}).get('price')) if data.get('spot') else None
                futures_payload = data.get('futures') or {}
                futures_price = _positive_or_none(futures_payload.get('price'))
                funding_rate_raw = futures_payload.get('funding_rate')
                funding_rate = funding_rate_to_float(funding_rate_raw) if funding_rate_raw is not None else None
                interval_raw = futures_payload.get('funding_interval_hours')
                try:
                    funding_interval = float(interval_raw) if interval_raw not in (None, '') else None
                except (TypeError, ValueError):
                    funding_interval = None
                next_funding_time = normalize_next_funding_time(futures_payload.get('next_funding_time'))
                mark_price = _positive_or_none(futures_payload.get('mark_price')) if data.get('futures') else None
                index_price = _positive_or_none(futures_payload.get('index_price')) if data.get('futures') else None
                volume_24h = _positive_or_none(data.get('spot', {}).get('volume')) if data.get('spot') else None
                
                # 计算溢价
                premium_percent = None
                if premium_data and exchange in premium_data:
                    premium_percent = _float_or_none(premium_data[exchange].get('premium_percent'))
                
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO price_data 
                        (timestamp, symbol, exchange, spot_price, futures_price, funding_rate,
                         funding_interval_hours, next_funding_time, mark_price, index_price,
                         premium_percent, volume_24h)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        timestamp, symbol, exchange, spot_price, futures_price,
                        funding_rate, funding_interval, next_funding_time,
                        mark_price, index_price, premium_percent, volume_24h
                    ))
                    conn.commit()
                    
        except Exception as e:
            print(f"数据库保存错误: {e}")

    def save_price_data_batch(self, items):
        """
        批量写入 price_data（单事务），用于提升多币种落库吞吐。
        items: list[{'symbol':..., 'exchange':..., 'symbol_data':..., 'premium_data':...}]
        """
        if not items:
            return
        try:
            with self.lock:
                timestamp = datetime.now()

                def _positive_or_none(value):
                    try:
                        v = float(value)
                        return v if v > 0 else None
                    except (TypeError, ValueError):
                        return None

                def _float_or_none(value):
                    try:
                        return float(value)
                    except (TypeError, ValueError):
                        return None

                rows = []
                for it in items:
                    symbol = it.get('symbol')
                    exchange = it.get('exchange')
                    data = it.get('symbol_data') or {}
                    premium_data = it.get('premium_data') or {}
                    if not symbol or not exchange:
                        continue

                    spot_price = _positive_or_none(data.get('spot', {}).get('price')) if data.get('spot') else None
                    futures_payload = data.get('futures') or {}
                    futures_price = _positive_or_none(futures_payload.get('price'))
                    funding_rate_raw = futures_payload.get('funding_rate')
                    funding_rate = funding_rate_to_float(funding_rate_raw) if funding_rate_raw is not None else None
                    interval_raw = futures_payload.get('funding_interval_hours')
                    try:
                        funding_interval = float(interval_raw) if interval_raw not in (None, '') else None
                    except (TypeError, ValueError):
                        funding_interval = None
                    next_funding_time = normalize_next_funding_time(futures_payload.get('next_funding_time'))
                    mark_price = _positive_or_none(futures_payload.get('mark_price')) if data.get('futures') else None
                    index_price = _positive_or_none(futures_payload.get('index_price')) if data.get('futures') else None
                    volume_24h = _positive_or_none(data.get('spot', {}).get('volume')) if data.get('spot') else None

                    premium_percent = None
                    if premium_data and exchange in premium_data:
                        premium_percent = _float_or_none(premium_data[exchange].get('premium_percent'))

                    rows.append(
                        (
                            timestamp,
                            symbol,
                            exchange,
                            spot_price,
                            futures_price,
                            funding_rate,
                            funding_interval,
                            next_funding_time,
                            mark_price,
                            index_price,
                            premium_percent,
                            volume_24h,
                        )
                    )

                if not rows:
                    return

                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.executemany(
                        '''
                        INSERT INTO price_data
                        (timestamp, symbol, exchange, spot_price, futures_price, funding_rate,
                         funding_interval_hours, next_funding_time, mark_price, index_price,
                         premium_percent, volume_24h)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''',
                        rows,
                    )
                    conn.commit()
        except Exception as e:
            print(f"数据库批量写入错误: {e}")
    
    def get_historical_data(self, symbol, exchange=None, hours=24, interval='1min', raise_on_error=False, timeout_seconds: float = None):
        """获取历史数据"""
        try:
            with self._get_connection(timeout_seconds=timeout_seconds) as conn:
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
            if raise_on_error:
                raise
            return []
    
    def aggregate_to_1min(self):
        """聚合数据到1分钟精度"""
        try:
            with self._get_connection() as conn:
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
                     funding_rate_avg, funding_interval_hours, next_funding_time,
                     premium_percent_avg, volume_24h_avg, data_points)
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
                        AVG(CASE WHEN funding_interval_hours > 0 THEN funding_interval_hours ELSE NULL END) as funding_interval_hours,
                        MAX(next_funding_time) as next_funding_time,
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
    
    def _batched_delete(self, conn, table, threshold_expression, batch_size):
        cursor = conn.cursor()
        total_deleted = 0
        while True:
            cursor.execute(
                f'''
                DELETE FROM {table}
                WHERE rowid IN (
                    SELECT rowid FROM {table}
                    WHERE timestamp < {threshold_expression}
                    LIMIT ?
                )
                ''',
                (batch_size,)
            )
            deleted = cursor.rowcount or 0
            if deleted == 0:
                break
            total_deleted += deleted
            conn.commit()
        if total_deleted:
            print(f"{table} 清理完成: 删除 {total_deleted} 条历史数据")
        return total_deleted

    def cleanup_old_data(self, days=7, force=False):
        """清理旧数据，保留指定天数"""
        current_utc_hour = datetime.utcnow().hour
        if not force and current_utc_hour not in self.LOW_TRAFFIC_UTC_HOURS:
            print(f"跳过数据库清理: 当前UTC小时 {current_utc_hour} 不在低峰期 {sorted(self.LOW_TRAFFIC_UTC_HOURS)}")
            return

        try:
            with self._get_connection() as conn:
                raw_threshold = f"datetime('now', '-{days} days')"
                agg_threshold = "datetime('now', '-30 days')"

                self._batched_delete(conn, 'price_data', raw_threshold, self.CLEANUP_BATCH_SIZE)
                self._batched_delete(conn, 'price_data_1min', agg_threshold, self.CLEANUP_BATCH_SIZE)
                
        except Exception as e:
            print(f"数据清理错误: {e}")

    def checkpoint_and_vacuum(self, vacuum_timeout: float = 600.0):
        """执行 WAL checkpoint + VACUUM，减少 WAL 和碎片。"""
        start = time.time()
        try:
            with self._get_connection() as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception as exc:
            print(f"WAL checkpoint 失败: {exc}")
            return

        try:
            # VACUUM 需要独占锁，可能耗时，临时提升超时时间
            with sqlite3.connect(self.db_path, timeout=vacuum_timeout) as conn:
                conn.execute("VACUUM")
        except Exception as exc:
            print(f"VACUUM 失败: {exc}")
            return

        elapsed = time.time() - start
        print(f"WAL checkpoint + VACUUM 完成，耗时 {elapsed:.2f}s")

    def get_latest_prices(self, symbol):
        """获取最新价格数据"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT exchange, spot_price, futures_price, funding_rate, funding_interval_hours,
                           next_funding_time, premium_percent, timestamp
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

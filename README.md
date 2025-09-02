# WLFI Funding Rate Monitor

基于动态币种发现的多交易所实时监控系统，支持现货和永续合约的价格及资金费率监控。

## 🚀 项目特色

- **动态币种发现**：自动获取各交易所实际支持的USDT币对，避免无效订阅
- **精准WebSocket订阅**：基于各交易所API预筛选，只订阅确实存在的币种对
- **按币种聚合展示**：将同一币种在多个交易所的数据聚合显示，方便比较
- **4大交易所支持**：OKX、Binance、Bybit、Bitget
- **实时数据更新**：WebSocket实时推送，支持现货价格和合约资金费率
- **智能频率控制**：500ms数据刷新，100ms WebSocket节流，避免数据过载

## 🏗️ 系统架构

### 核心模块

1. **market_info.py** - 动态市场发现
   - 通过REST API获取各交易所支持的币种列表
   - 自动过滤和合并币种数据
   - 智能缓存机制，避免频繁API调用

2. **exchange_connectors.py** - WebSocket连接管理
   - 基于实际币种列表的精准订阅
   - 自动错误检测和币种移除机制
   - 分批订阅避免连接限制

3. **simple_app.py** - Web应用和API
   - Flask后端提供数据API
   - 聚合页面作为首页展示
   - 支持按交易所和按币种两种视图

4. **config.py** - 配置管理
   - 动态币种列表缓存
   - 静态备用列表
   - 频率和过滤参数配置

## 📊 功能特性

### 动态币种发现
- **Binance**: 获取现货和永续合约的活跃USDT币对
- **OKX**: 支持SPOT和SWAP类型的实时币种
- **Bybit**: 现货和线性合约的在线币种
- **Bitget**: 现货和USDT期货的可用币种

### 数据聚合显示
- 首页默认显示按币种聚合的数据
- 显示每个币种在各交易所的覆盖情况
- 实时价格和资金费率对比
- 支持搜索和筛选功能

### 智能订阅机制
```python
# 示例：各交易所特定币种列表
exchange_symbols = {
    'binance': {
        'spot': ['BTC', 'ETH', 'BNB', ...],      # 只包含Binance实际支持的现货
        'futures': ['BTC', 'ETH', 'ADA', ...]    # 只包含Binance实际支持的期货
    },
    'okx': {
        'spot': ['BTC', 'ETH', 'WLFI', ...],
        'futures': ['BTC', 'ETH', 'SOL', ...]
    }
    # 每个交易所都有专门的订阅列表
}
```

## 🔄 重要优化更新

### 2025年1月版本更新
在本次更新中，我们完全重构了币种发现和WebSocket订阅机制：

#### 问题解决
- **❌ 原问题**：使用静态币种列表导致大量无效订阅和错误
- **✅ 解决方案**：实现基于REST API的动态币种发现

#### 核心改进
1. **预筛选机制**：在建立WebSocket前先验证各交易所支持的币种
2. **交易所特定订阅**：每个交易所只订阅其确实支持的币种对
3. **消除盲目订阅**：不再尝试订阅可能不存在的币种
4. **首页聚合展示**：将聚合页面设为默认首页

#### 技术实现
- 增强`market_info.py`添加`get_exchange_specific_symbols()`功能
- 修改所有WebSocket连接使用交易所特定的币种列表
- 优化订阅逻辑，提高连接效率和稳定性

## 🚀 部署说明

### 环境要求
```bash
Python 3.8+
Flask
websocket-client
requests
aiohttp
```

### 快速启动
```bash
# 安装依赖
pip install flask websocket-client requests aiohttp

# 启动应用
python simple_app.py
```

### 访问地址
- 主页（币种聚合）: `http://localhost:5000/`
- 交易所视图: `http://localhost:5000/exchanges`
- API数据: `http://localhost:5000/api/data`

## 📝 API接口

### 主要端点
- `GET /` - 币种聚合主页
- `GET /exchanges` - 按交易所展示页面  
- `GET /api/data` - 获取实时数据
- `GET /api/aggregated_data` - 获取聚合数据
- `GET /api/markets` - 获取市场信息
- `GET /api/coverage` - 获取覆盖度报告

### 数据格式
```json
{
  "exchange": "binance",
  "symbol": "BTC",
  "spot": {
    "price": 109500.00,
    "timestamp": "2025-01-02T12:00:00"
  },
  "futures": {
    "price": 109480.1,
    "funding_rate": 0.0001,
    "timestamp": "2025-01-02T12:00:00"
  }
}
```

## 🎯 使用场景

1. **资金费率套利**：监控不同交易所的资金费率差异
2. **价格比较**：实时对比各交易所的现货和期货价格
3. **市场覆盖分析**：了解哪些币种在多个交易所都有支持
4. **交易决策支持**：基于实时数据做出交易决策

## ⚠️ 注意事项

- 请确保网络连接稳定，WebSocket连接可能因网络问题中断
- 各交易所有不同的API限制，系统已实现适当的频率控制
- 建议在生产环境中配置适当的错误重试和监控机制
- 币种列表每小时自动更新一次，可手动强制刷新

## 🔗 相关链接

- [OKX API文档](https://www.okx.com/docs-v5/en/)
- [Binance API文档](https://binance-docs.github.io/apidocs/)
- [Bybit API文档](https://bybit-exchange.github.io/docs/)
- [Bitget API文档](https://www.bitget.com/api-doc/common/intro)

## 📄 许可证

MIT License - 详见 LICENSE 文件

---

*本项目专注于为加密货币交易者提供准确、实时的多交易所数据聚合服务。*
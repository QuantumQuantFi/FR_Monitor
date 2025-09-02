# 多交易所实时价格监控系统

一个实时监控 OKX、Binance、Bybit、Bitget 四大交易所现货和永续合约价格的 Web 应用。

## 功能特点

- **实时价格监控**: 同时监控现货和永续合约价格
- **资金费率追踪**: 显示各交易所的实时资金费率
- **溢价指数计算**: 自动计算期货相对现货的溢价百分比
- **多币种支持**: 支持 LINK、WLFI、BTC、ETH 等币种切换
- **实时图表**: 价格走势和资金费率对比图表
- **响应式界面**: 适配桌面和移动设备

## 技术架构

- **后端**: Flask + SocketIO
- **前端**: HTML5 + JavaScript + Chart.js
- **数据连接**: WebSocket 实时连接各交易所
- **UI设计**: 深色主题，类似专业交易界面

## 安装和运行

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 运行应用

```bash
python app.py
```

### 3. 访问界面

打开浏览器访问 `http://localhost:5000`

## 文件结构

```
wlfi/
├── app.py                    # Flask 主应用
├── exchange_connectors.py    # 交易所 WebSocket 连接器
├── config.py                # 配置文件
├── requirements.txt         # Python 依赖
├── templates/
│   └── index.html          # 前端页面
└── README.md               # 说明文档
```

## API 接口

### GET /api/data
获取当前实时数据

### GET /api/history/<symbol>
获取指定币种的历史数据

### POST /api/switch_symbol
切换监听币种

## WebSocket 事件

- `market_update`: 市场数据更新
- `switch_symbol`: 币种切换
- `connect/disconnect`: 连接状态

## 数据结构

每个交易所的数据包含：
- 现货价格
- 期货价格  
- 资金费率
- 下次结算时间
- 溢价指数

## 注意事项

1. **网络要求**: 需要稳定的互联网连接访问交易所 WebSocket
2. **数据延迟**: 实际延迟取决于网络环境和交易所服务器响应
3. **内存使用**: 历史数据保存在内存中，重启会清空
4. **API 限制**: 遵守各交易所的 WebSocket 连接限制

## 后续扩展

- 添加更多币种支持
- 数据持久化存储
- 价格警报功能
- 套利机会提示
- 移动端 App

## 支持的交易所

- **OKX**: 现货 + 永续合约
- **Binance**: 现货 + USDT 永续
- **Bybit**: 现货 + USDT 永续  
- **Bitget**: 现货 + USDT 永续

使用前请确保网络能正常访问这些交易所的 WebSocket 服务。
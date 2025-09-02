# WLFI 资金费率监控系统

🚀 实时多交易所价格和资金费率监控系统，支持976+币种跨平台套利机会发现。

## 📊 系统特性

### 🔥 核心功能
- **实时价格监控**: 支持Binance、OKX、Bybit、Bitget四大交易所
- **资金费率追踪**: 实时资金费率数据，发现套利机会
- **价差分析**: 自动计算跨交易所价差，智能排序筛选
- **多币种支持**: 动态发现并监控976+个热门币种
- **WebSocket连接**: 低延迟实时数据更新

### 📈 数据展示
- **聚合视图**: 按币种聚合显示所有交易所数据
- **价差排序**: 一键切换价差排序模式，快速发现套利机会
- **历史数据**: 支持历史价格和资金费率查询
- **系统状态**: 实时监控系统运行状态和连接健康度

## 🛠️ 技术架构

- **后端**: Python Flask + WebSocket
- **数据库**: SQLite (本地存储)
- **连接**: 多交易所WebSocket实时连接
- **前端**: HTML5 + JavaScript (响应式设计)

### 核心模块

1. **simple_app.py** - 主应用程序 (推荐使用)
   - Flask Web服务 (端口4002)
   - 完整API接口
   - 内存优化
   - 系统状态监控

2. **app.py** - 旧版应用 ❌ 可删除
   - 包含SocketIO (端口5000)
   - 功能较少
   - 已被simple_app.py替代

3. **exchange_connectors.py** - WebSocket连接管理
   - 多交易所WebSocket连接
   - 自动重连机制
   - 数据标准化处理

4. **market_info.py** - 动态市场发现
   - REST API获取币种列表
   - 智能缓存机制
   - 976+币种自动发现

5. **database.py** - 数据存储
   - SQLite数据库操作
   - 历史数据管理
   - 数据维护功能

## 📋 系统要求

- Python 3.8+
- Ubuntu/Linux (推荐)
- 稳定的网络连接

## 🚀 快速开始

### 1. 环境准备

```bash
# 克隆项目
git clone <your-repo-url>
cd FR_Monitor

# 安装系统依赖
sudo apt update
sudo apt install python3-venv python3-pip
```

### 2. 安装依赖

```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate

# 安装Python依赖
pip install -r requirements.txt

# 安装额外依赖
pip install aiohttp psutil
```

### 3. 启动系统

```bash
# 激活虚拟环境并启动
source venv/bin/activate && python simple_app.py
```

### 4. 访问系统

打开浏览器访问: `http://your-server-ip:4002`

## 📂 项目结构

```
FR_Monitor/
├── simple_app.py          # 主应用程序
├── exchange_connectors.py # 交易所连接器
├── config.py             # 系统配置
├── market_info.py        # 市场信息获取
├── database.py           # 数据库操作
├── requirements.txt      # Python依赖
├── templates/           # HTML模板
│   ├── simple_index.html    # 主页
│   ├── aggregated_index.html # 聚合页面
│   └── enhanced_aggregated.html # 增强聚合页面
└── venv/               # 虚拟环境 (自动生成)
```

## 🌐 页面访问

### 主要页面
```bash
GET  /                        # 主页 (简洁视图)
GET  /aggregated             # 聚合页面 (价差分析) - 推荐
GET  /exchanges              # 交易所视图
GET  /charts                 # 图表页面
```

## 📝 API接口

### 核心数据接口
```bash
GET  /api/data                    # 获取实时数据
GET  /api/aggregated_data         # 获取聚合数据
GET  /api/history/<symbol>        # 获取历史数据
POST /api/switch_symbol          # 切换监控币种
```

### 市场信息接口
```bash
GET  /api/markets               # 获取市场信息
GET  /api/coverage             # 获取覆盖度统计
GET  /api/system/status        # 系统状态监控
```

## 📊 使用指南

### 1. 主页面功能
- **实时价格**: 查看所有交易所的现货和期货价格
- **资金费率**: 监控各交易所的资金费率变化
- **币种切换**: 快速切换要监控的币种

### 2. 聚合页面 (推荐)
- **价差分析**: 自动计算并显示跨交易所价差
- **排序功能**: 点击"📊 价差排序"按钮切换排序模式
- **套利机会**: 快速识别价差超过0.1%的套利机会

### 3. 系统监控
访问 `/api/system/status` 查看:
- 系统资源使用情况
- 连接状态
- 数据更新频率
- 错误统计

## ⚙️ 配置说明

主要配置在 `config.py` 中:

```python
# 数据更新间隔
DATA_REFRESH_INTERVAL = 2.0  # 秒

# WebSocket配置
WS_CONNECTION_CONFIG = {
    'max_reconnect_attempts': 50,
    'base_delay': 5,
    'max_delay': 120,
    'ping_interval': 30,
    'ping_timeout': 10,
    'connection_timeout': 20
}

# 内存优化
MEMORY_OPTIMIZATION_CONFIG = {
    'max_historical_records': 500,
    'memory_cleanup_interval': 300
}
```

## 🔧 维护操作

### 重启服务
```bash
# 停止当前服务 (Ctrl+C)
# 重新启动
source venv/bin/activate && python simple_app.py
```

### 清理数据库
```bash
# 通过API清理数据库
curl -X POST http://localhost:4002/api/database/maintenance
```

### 检查系统状态
```bash
# 检查服务状态
curl http://localhost:4002/api/system/status

# 检查数据库状态  
curl http://localhost:4002/api/database/stats
```

## 🐛 常见问题

### Q: 服务无法启动？
A: 检查依赖是否完整安装，确保虚拟环境激活

### Q: 连接交易所失败？
A: 检查网络连接，某些地区可能需要代理

### Q: 数据不更新？
A: 查看控制台日志，检查WebSocket连接状态

### Q: 内存占用过高？
A: 调整 `MEMORY_OPTIMIZATION_CONFIG` 中的 `max_historical_records`

## 📝 更新日志

### v2.0 (2025-09-02)
- ✅ 价差筛选和排序功能
- ✅ WebSocket连接稳定性优化  
- ✅ 前端数字显示精度优化
- ✅ 服务器启动问题修复
- ✅ 删除旧版app.py，统一使用simple_app.py

### v1.0 (初始版本)
- ✅ 多交易所实时数据收集
- ✅ 基础Web界面
- ✅ 资金费率监控

## 🤝 贡献

欢迎提交Issue和Pull Request来改进这个项目！

## 📄 许可证

[MIT License](LICENSE)

---

**💡 提示**: 推荐使用聚合页面 `/aggregated` 进行价差分析和套利机会发现。

**🔗 在线访问**: http://your-server-ip:4002

**📧 联系**: 如有问题，请提交GitHub Issue
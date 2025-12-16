# 跨交易所期现套利监控系统

🚀 实时多交易所价格和资金费率监控系统，支持976+币种跨平台套利机会发现，聚焦跨交易所期货/现货价差捕捉与套利执行协助。

## 📊 系统特性

### 🔥 核心功能
- **实时价格监控**: 支持 Binance、OKX、Bybit、Bitget、GRVT、Lighter、Hyperliquid 七大交易所
- **资金费率追踪**: 实时资金费率数据，发现套利机会
- **价差分析**: 自动计算跨交易所价差，智能排序筛选
- **套利信号**: 内置 `ArbitrageMonitor`，基于0.6%阈值与3次连续采样输出可执行套利提示
- **多币种支持**: 动态发现并监控976+个热门币种
- **REST快照兜底**: 当WebSocket断流时自动启用REST补全并可运行脚本验证覆盖度
- **WebSocket连接**: 低延迟实时数据更新

### 📈 数据展示
- **聚合视图**: 按币种聚合显示所有交易所数据
- **价差排序**: 一键切换价差排序模式，快速发现套利机会
- **套利信号面板**: `/aggregated` 页面实时展示最近5条跨交易所信号
- **历史数据**: 支持历史价格和资金费率查询，图表接口内置缓存与锁冲突自动重试，弱网场景也能快速返回
- **精度保真**: 后端 JSON 与前端模板统一沿用交易所原始精度（含尾随 0），不会再强制截断为 3~4 位；图表/卡片/Tooltip 也会呈现真实小数
- **系统状态**: 实时监控系统运行状态和连接健康度

## 🛠️ 技术架构

- **后端**: Python Flask + WebSocket
- **数据库**: SQLite (本地存储，启用了 WAL + 30s timeout，并在 UTC 1~5 点按批次清理历史数据以避免锁表)
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
   - 多交易所WebSocket连接（含 GRVT/Lighter/Hyperliquid）
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

6. **arbitrage/monitor.py** - 跨交易所套利信号监控
   - 0.6%价差阈值 + 3次连续采样防抖
   - 30秒冷却期与最近信号缓存
   - 暴露 `/api/arbitrage/signals` 供前端面板/自动化调用

7. **bitget_symbol_filter.py** - Bitget符号健康度过滤器
   - 缓存30分钟的现货/合约元数据，过滤 `halt`/异常状态
   - 仅输出可交易的 USDT 交易对，避免脏数据污染价差计算
   - 失败时自动回退到旧缓存，保障系统连续性

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
source venv/bin/activate && scripts/run_simple_app.sh
```

#### 后台守护运行（不依赖 code-server/终端会话）

```bash
mkdir -p runtime/simple_app
nohup bash -c 'source venv/bin/activate && scripts/run_simple_app.sh' \
  >/dev/null 2>&1 &
echo $! > runtime/simple_app/simple_app.pid
```
- `nohup` 可保证即便 code-server/SSH 会话被关闭，进程仍持续运行
- 所有应用日志自动写入 `logs/simple_app/simple_app.log`（含轮转文件）
- 停止时运行 `source venv/bin/activate && scripts/stop_simple_app.sh`（或手动 `kill "$(cat runtime/simple_app/simple_app.pid)"`），再清理 `.pid` 文件

### 4. GRVT 配置 & 环境变量

若需开启 GRVT 市场数据，需在 `config_private.py` 或环境变量中写入以下字段：

| 变量名 | 说明 |
| --- | --- |
| `GRVT_API_KEY` | API Key，用于 `edge.grvt.io` 登录 |
| `GRVT_SECRET_KEY` | 私钥（与 SDK `private_key` 兼容） |
| `GRVT_TRADING_ACCOUNT_ID` | 交易账户 ID，用于私有订阅 |
| `GRVT_ENVIRONMENT` | `prod` / `testnet` / `staging` 等，默认 `prod` |
| `GRVT_WS_PUBLIC_URL` | 可选：覆盖默认 `wss://market-data.<env>.grvt.io/ws` |
| `GRVT_REST_BASE_URL` | 可选：覆盖 REST 域名 |

> ✅ 建议先将运行机器的公网 IPv4/IPv6 加入 GRVT 白名单；`config.py` 默认启用了 IPv4 优先策略，确保登录请求命中已授权地址。

### 5. Lighter 配置 & 环境变量

Lighter 默认提供公开的行情广播，如需自定义网关或降低刷新频次，可通过以下配置覆盖：

| 变量名 | 说明 |
| --- | --- |
| `LIGHTER_WS_PUBLIC_URL` | 可选：覆盖默认 `wss://mainnet.zklighter.elliot.ai/stream` |
| `LIGHTER_REST_BASE_URL` | 可选：覆盖 `https://mainnet.zklighter.elliot.ai/api/v1` |
| `LIGHTER_MARKET_REFRESH_SECONDS` | 市场列表缓存时长（秒），默认 900 |

> ⚠️ 若需在 `dex/` 子模块内做市/交易，请参考官方 `lighter` SDK 文档配置 API Key、Account Index 等参数。

### 6. Hyperliquid 配置 & 环境变量

Hyperliquid 行情/REST 接口可直接访问，如需切换至测试网或调整 funding 缓存，可覆盖以下变量（写入 `config_private.py` 或环境变量）：

| 变量名 | 说明 |
| --- | --- |
| `HYPERLIQUID_API_BASE_URL` | 默认为 `https://api.hyperliquid.xyz` |
| `HYPERLIQUID_WS_PUBLIC_URL` | 默认为 `wss://api.hyperliquid.xyz/ws` |
| `HYPERLIQUID_FUNDING_REFRESH_SECONDS` | `metaAndAssetCtxs` 的缓存刷新间隔（秒），默认 60 |

若需在本项目中开启 Hyperliquid 实盘交易，还需提供以下私密字段（仅写入本机 `config_private.py` 或环境变量，切勿提交到仓库）：

| 变量名 | 说明 |
| --- | --- |
| `HYPERLIQUID_PRIVATE_KEY` | Hyperliquid 交易私钥（hex 格式），用于下单签名 |
| `HYPERLIQUID_ADDRESS` | 可选：钱包地址（不填将由私钥推导） |

> ✅ WebSocket 订阅使用官方 `allMids` 广播，无需密钥；交易权限使用官方 SDK 完成签名与下单。

### 7. 日志与运行目录

- Flask 服务日志默认写入 `logs/simple_app/`，可通过 `SIMPLE_APP_LOG_DIR=/custom/path scripts/run_simple_app.sh` 自定义位置
- 运行期文件（PID 等）建议放在 `runtime/simple_app/`，可通过 `SIMPLE_APP_RUNTIME_DIR` 覆盖
- `scripts/run_simple_app.sh` 会自动创建上述目录并设置所需环境变量
- `simple_app.py` 额外把日志输出到 `stdout`，方便在容器或 `nohup` 环境中实时查看

### 8. 停止与重启

```bash
# 停止（会尝试通过 PID 文件与 pgrep 终止 simple_app.py）
source venv/bin/activate && scripts/stop_simple_app.sh

# 重启 = 停止 + 启动（内部复用 run_simple_app.sh）
source venv/bin/activate && scripts/restart_simple_app.sh
```

### 9. 访问系统

打开浏览器访问: `http://your-server-ip:4002`

### 10. 如何接入新的交易所（Checklist）

> 以下步骤适用于 CEX/DEX 统一行情接入，确保 REST/WS 与前端展示一致。

1. **整理官方文档**：确认 WebSocket/REST 端点、鉴权方式、订阅 payload、节流/白名单要求。
2. **新增配置**（`config.py`）：为新交易所暴露 REST/WS URL、刷新周期、API Key/私钥等变量，便于在服务器和开发机切换环境。
3. **实现 REST 补齐**（`rest_collectors.py`）：按交易所格式编写 `fetch_xxx()`，并在 `fetch_all_exchanges()` 里注入；必要时提供缓存或元数据拉取函数（类似 `get_lighter_market_info`、`get_hyperliquid_supported_bases`）。
4. **实现 WebSocket 客户端**（`dex/exchanges/`）：封装对应的订阅/心跳/重连逻辑，只需输出标准化 payload，便于 collector 统一处理。
5. **注册 Collector**（`exchange_connectors.py`）：在 `_load_exchange_symbols`、`_initialize_data_structure`、`start_all_connections`、`start_rest_poller` 以及具体 `_connect_xxx` 中接入新的 REST/WS 客户端。
6. **前端与 API**：`simple_app.py` 中更新 `EXCHANGE_DISPLAY_ORDER`，Templates/REST API 会自动带上新交易所；若有特定 UI 需求可在模板内追加卡片描述。
7. **文档与测试**：在 `README.md` 记录新增交易所的环境变量与验证命令；扩展 `test_rest_apis.py` / `test_websocket_limits.py` 以提供最小自检脚本，方便他人快速确认访问是否成功。

完成上述修改后，通过 `python verify_config.py` 与若干 REST/WS 测试脚本确认整体链路，再提交 PR。

## 📂 项目结构

```
FR_Monitor/
├── simple_app.py          # 主应用程序
├── exchange_connectors.py # 交易所连接器
├── config.py             # 系统配置
├── market_info.py        # 市场信息获取
├── database.py           # 数据库操作
├── arbitrage/            # ArbitrageMonitor 与套利信号封装
├── bitget_symbol_filter.py # Bitget符号过滤器
├── requirements.txt      # Python依赖
├── dex/                  # `perp-dex-tools` 多交易所做市/刷量机器人
├── templates/           # HTML模板
│   ├── simple_index.html      # 交易所视图
│   ├── enhanced_aggregated.html # 聚合页/套利看板
│   └── chart_index.html       # 图表&下单页面
├── test_rest_apis.py     # REST快照覆盖度测试脚本
└── venv/               # 虚拟环境 (自动生成)
```

### 🔍 内置测试脚本

| 命令 | 作用 |
| --- | --- |
| `python verify_config.py` | 校验币种配置与容量设置 |
| `python test_rest_apis.py` | 同时测试 Binance/OKX/Bybit/Bitget/GRVT/Lighter/Hyperliquid 的 REST 快照覆盖度 |
| `python test_websocket_limits.py grvt` | 触发 GRVT WS 登录 + 全量订阅并打印行情样本 |
| `python test_websocket_limits.py lighter` | 检查 Lighter `market_stats/all` 通道推送与市场覆盖 |
| `python test_websocket_limits.py hyperliquid` | 订阅 Hyperliquid `allMids` 广播并统计可推送的标的数量 |
| `python test_market_integration.py` | 打印动态支持币种、REST/WS 覆盖率 |

## 🔁 `dex/`（perp-dex-tools）联动指引

`dex/` 目录内置 [your-quantguy/perp-dex-tools](https://github.com/your-quantguy/perp-dex-tools) 源码，可用于 EdgeX、Backpack、Paradex、Aster、Lighter、GRVT、Extended、ApeX 等多家新兴永续交易所的刷量、对冲与 Boost 交易。结合本项目的实时价差/资金费率监控，可实现「发现套利 → 立即下单」的闭环。

### 1. 初始化依赖

```bash
cd dex
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt

# Paradex 独立依赖
python3 -m venv para_env
source para_env/bin/activate
pip install -r para_requirements.txt

# ApeX / EdgeX 扩展
source env/bin/activate
pip install -r apex_requirements.txt
```

### 2. 配置密钥

```bash
cd dex
cp env_example.txt .env
```

根据 `env_example.txt` 填写交易所 API Key、私钥、代理、Telegram/Lark Token，可与 `FR_Monitor` 根目录同名变量复用，便于统一管理密钥。

### 3. 启动多交易所机器人

`runbot.py` 接受所有策略参数，适合直接写入 `systemd` 或 `screen`：

```bash
cd dex
source env/bin/activate
python runbot.py \
  --exchange backpack \
  --ticker ETH \
  --direction buy \
  --quantity 50 \
  --take-profit 0.02 \
  --max-orders 40 \
  --wait-time 600 \
  --grid-step 0.5 \
  --env-file .env
```

- `--grid-step` 控制平仓价间距，避免过度密集
- `--stop-price` / `--pause-price` 可配合本仓库监控到的极端价差做风控
- `--boost` 仅在 Backpack/Aster 交易所生效，用于冲积分/交易量

### 4. 与资金费率监控联动技巧

- 在 `/aggregated` 页面根据价差、`/api/arbitrage/signals` 中的方向，决定 `--ticker` 与 `--direction`
- `dex/helpers/logger.py` 默认输出 `logs/trading_bot_<exchange>_<ticker>.log`，可纳入 `list_logs.tsv` 统一巡检
- 需要自动化联动时，可在 `scripts/` 添加任务，拉取 `FR_Monitor` 识别的套利机会后调用 `runbot.py`

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
GET  /api/arbitrage/signals       # 获取最近套利信号（最多50条）
GET  /api/history/<symbol>        # 获取历史数据
POST /api/switch_symbol          # 切换监控币种
```

### 市场信息接口
```bash
GET  /api/markets               # 获取市场信息
GET  /api/coverage             # 获取覆盖度统计
GET  /api/system/status        # 系统状态监控
```

## 🧪 测试与诊断

### REST API 快照自检
```bash
python test_rest_apis.py
```
- 并发调用 Binance/OKX/Bybit/Bitget 的全量 ticker REST 接口
- 输出现货/期货 API 是否可用、可获取的币种数量与示例数据
- 适合在 WebSocket 异常或扩充覆盖面时快速验证 REST 兜底能力

### Bitget 符号过滤器复用示例
```python
from bitget_symbol_filter import bitget_filter

valid = bitget_filter.get_valid_symbols()
print(valid["spot"], valid["futures"])
```
- 内置 30 分钟缓存，过滤 `halt`、`symbolStatus != normal` 的交易对
- 失败时保留旧缓存，避免监控因API抖动而大规模下线

### 日志与资源采样
- `list_logs.tsv`：列出主要日志文件及大小，便于排查磁盘压力
- `monitor_samples.tsv`：采样 `simple_app.py` 进程 CPU/内存，用于回归分析
- `binance_contract_cache.json` / `dynamic_symbols_cache.json`：缓存最新合约清单，可删除后重建以强制刷新

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

# WebSocket 订阅优化策略

## 各交易所订阅限制总结

### 1. Binance
- **最大订阅数**: 1024个stream/连接
- **推荐配置**: 800-900个币种
- **策略**: 使用单连接管理多个stream，每个币种需要1-2个stream

### 2. OKX
- **订阅请求限制**: 480次/小时/连接
- **推荐配置**: 150-200个币种
- **策略**: 每个币种需要3个channel (tickers现货, tickers合约, funding-rate)

### 3. Bybit
- **现货限制**: 10个args/请求
- **永续合约限制**: 2000个args/连接
- **推荐配置**: 500-800个币种
- **策略**: 永续合约支持更多订阅

### 4. Bitget
- **最大订阅数**: 1000个channel/连接
- **推荐配置**: 400-500个币种
- **策略**: 每个币种需要2个channel (现货+期货)

## 优化后的币种配置

当前配置支持 **150 个币种**，按类别分组：

### 主流币种 (30个)
BTC, ETH, BNB, XRP, ADA, DOGE, SOL, TRX, DOT, MATIC, AVAX, LTC, SHIB, WBTC, LEO, UNI, ATOM, ETC, LINK, XMR, BCH, XLM, NEAR, APT, VET, ICP, FIL, HBAR, QNT, ALGO

### DeFi热门 (25个)
AAVE, COMP, SUSHI, CRV, YFI, 1INCH, MKR, SNX, REN, BAL, UMA, ZRX, KNC, BAND, NMR, LRC, OMG, REP, MLN, PNT, RSR, RLC, ANT, NEXO, CVC

### Layer1/Layer2 (25个)
FTM, ROSE, EGLD, KLAY, FLOW, ONE, CELO, HNT, IOTA, QTUM, ZIL, ICX, NANO, WAVES, DASH, ZEC, XTZ, ONT, VTHO, RVN, SC, DGB, LSK, STEEM, ARK

### GameFi/NFT (20个)
MANA, SAND, AXS, ENJ, CHZ, GALA, ILV, YGG, MAGIC, APE, IMX, RNDR, HIGH, TVK, SLP, ALICE, DG, GHST, WAXP, SFP

### 基础设施 (20个)
GRT, LPT, BAT, STORJ, OCEAN, ANKR, FET, AGIX, NKN, HOT, DENT, KEY, DATA, BLZ, REQ, POWR, SUB, MTL, SALT

### 新兴热门 (30个)
AR, RUNE, KSM, DYDX, PERP, ENS, LDO, FXS, CVX, BICO, JASMY, C98, GTC, BTRST, RAD, API3, CTSI, AUCTION, BADGER, BOND, DPX, FIDA, GOG, HFT, IQ, JUP, LCX, LOKA, METIS, MNGO

### 策略重点 (10个)
ARB, OP, GMX, PYTH, TIA, SUI, ONDO, PENDLE, JTO, NTRN

## 连接优化建议

1. **Binance**: 使用组合stream URL，单连接可处理800+币种
2. **OKX**: 分批订阅，控制订阅频率在480次/小时以内
3. **Bybit**: 优先使用永续合约连接，支持更多订阅
4. **Bitget**: 合理分配channel订阅，避免超过1000限制

## 监控建议

- 定期检查连接状态和订阅成功率
- 实现自动重连机制
- 监控各交易所的API限制变化
- 根据实际需求动态调整订阅币种列表

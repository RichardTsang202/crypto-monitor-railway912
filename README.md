# 加密货币形态识别监控系统 - Railway部署版

这是一个专门用于识别加密货币市场中双顶、双底、EMA趋势等形态的实时监控系统，采用极值点缓存策略，支持多时间粒度监控，并通过Telegram发送实时通知和K线图表。

## 功能特性

### 核心功能
- **多种形态识别**：双顶、双底、EMA趋势信号
- **多时间粒度监控**：1小时、4小时、1日
- **极值点缓存策略**：高效的数据处理和形态识别
- **实时监控**：基于时间触发的智能监控
- **Telegram通知**：形态识别后自动发送通知和K线图表

### 技术特性
- **多API源支持**：Binance、OKX API自动切换
- **并发处理**：多线程并发分析提高效率
- **容错机制**：API失败自动重试和切换
- **质量评分**：基于ATR的形态质量评估
- **技术指标**：EMA、MACD、RSI等辅助分析

## 监控策略

### 时间粒度监控
- **1小时**：每小时整点监控，间隔3秒分析各交易对
- **4小时**：每4小时的30分监控，间隔5秒分析各交易对
- **1日**：每日8:15监控，间隔10秒分析各交易对

### 监控交易对
前10个主流加密货币：
- BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT
- DOGEUSDT, TONUSDT, ADAUSDT, SHIBUSDT, AVAXUSDT

## 形态识别逻辑

### 双顶形态
- **A点**：左数第13-34根K线范围内的最高点
- **B点**：最新收盘K线的最高点
- **C点**：A与B之间的最低点
- **验证条件**：AB距离 ≤ 0.8×ATR，C距离 ≥ 2.3×ATR

### 双底形态
- **A点**：左数第13-34根K线范围内的最低点
- **B点**：最新收盘K线的最低点
- **C点**：A与B之间的最高点
- **验证条件**：AB距离 ≤ 0.8×ATR，C距离 ≥ 2.3×ATR

### 头肩顶形态
- **A点**：左数第13-34根K线范围内的最高点（头部）
- **B点**：最新收盘K线的最高点（右肩）
- **C点**：A与B之间的最低点
- **D点**：左数第35-55根K线范围内的最高点（左肩）
- **验证条件**：BA距离 ≤ 1.0×ATR，BD距离 ≤ 0.8×ATR，AC距离 ≥ 2.0×ATR

### 头肩底形态
- **A点**：左数第13-34根K线范围内的最低点（头部）
- **B点**：最新收盘K线的最低点（右肩）
- **C点**：A与B之间的最高点
- **D点**：左数第35-55根K线范围内的最低点（左肩）
- **验证条件**：BA距离 ≤ 1.0×ATR，BD距离 ≤ 0.8×ATR，AC距离 ≥ 2.0×ATR

## Railway部署指南

### 1. 准备工作
确保你有以下账户：
- GitHub账户
- Railway账户（可用GitHub登录）

### 2. 创建GitHub仓库
```bash
# 在railway文件夹中初始化Git仓库
git init
git add .
git commit -m "Initial commit: Crypto pattern monitor for Railway"

# 添加远程仓库（替换为你的仓库地址）
git remote add origin https://github.com/yourusername/crypto-monitor-railway912.git
git branch -M main
git push -u origin main
```

### 3. Railway部署步骤

1. **登录Railway**
   - 访问 [railway.app](https://railway.app)
   - 使用GitHub账户登录

2. **创建新项目**
   - 点击 "New Project"
   - 选择 "Deploy from GitHub repo"
   - 选择你的 `crypto-monitor-railway912` 仓库

3. **配置环境变量**（可选）
   - 在Railway项目设置中添加环境变量
   - `WEBHOOK_URL`: 你的webhook接收地址
   - `PORT`: Railway会自动设置，无需手动配置

4. **部署配置**
   - Railway会自动检测到 `railway.json` 配置文件
   - 自动使用 `requirements.txt` 安装依赖
   - 使用 `Procfile` 中的启动命令

5. **监控部署**
   - 在Railway控制台查看部署日志
   - 部署成功后会获得一个公网URL

### 4. 验证部署

访问你的Railway应用URL：
- `https://your-app.railway.app/` - 健康检查
- `https://your-app.railway.app/status` - 系统状态

## API端点

### GET /
健康检查端点
```json
{
  "status": "running",
  "timestamp": 1703123456,
  "monitored_symbols": 10,
  "timeframes": ["1h", "4h", "1d"]
}
```

### GET /status
系统状态端点
```json
{
  "monitoring_active": true,
  "cache_stats": {
    "BTCUSDT": {"1h": 25, "4h": 18, "1d": 12}
  },
  "api_source": "binance"
}
```

## Webhook数据格式

当检测到形态时，系统会发送以下格式的数据到配置的webhook地址：

```json
{
  "pattern_type": "double_top",
  "symbol": "BTCUSDT",
  "timeframe": "1h",
  "trigger_time": 1703123456,
  "points": {
    "a_point": {"timestamp": 1703120000, "price": 42500.0, "index": 25},
    "b_point": {"timestamp": 1703123400, "price": 42480.0, "index": 199},
    "c_point": {"timestamp": 1703121800, "price": 42200.0, "index": 150}
  },
  "atr_value": 285.5,
  "quality_score": 85.2,
  "indicators": {
    "ema21": 42350.0,
    "ema55": 42100.0,
    "ema144": 41800.0,
    "macd_line": 125.5,
    "macd_signal": 113.0,
    "rsi": 65.8,
    "kline_pattern": "normal_bearish",
    "trend_direction": "uptrend"
  }
}
```

## 性能优化

### 资源使用
- **内存使用**：约50-100MB
- **CPU使用**：低负载，主要在监控时间点
- **网络请求**：每次监控约30-50个API请求

### 监控频率
- **1小时粒度**：每小时1次，每次约30秒
- **4小时粒度**：每4小时1次，每次约50秒
- **1日粒度**：每日1次，每次约100秒

## 故障排除

## 部署到Railway

### 1. 准备Telegram Bot

1. 在Telegram中找到 @BotFather
2. 发送 `/newbot` 创建新机器人
3. 获取Bot Token
4. 创建频道并将Bot添加为管理员
5. 获取频道ID（可以通过 @userinfobot 获取）

### 2. 部署到Railway

1. Fork这个仓库到你的GitHub账户
2. 在 [Railway](https://railway.app) 创建新项目
3. 连接你的GitHub仓库
4. 设置环境变量：
   - `TELEGRAM_BOT_TOKEN`: 你的Telegram Bot Token
   - `TELEGRAM_CHANNEL_ID`: 你的Telegram频道ID

### 3. 环境变量配置

在Railway项目设置中添加环境变量：

```
TELEGRAM_BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_CHANNEL_ID=-1001234567890
```

### 常见问题

1. **API请求失败**
   - 系统会自动切换API源
   - 检查网络连接和API限制

2. **Telegram发送失败**
   - 检查Bot Token是否正确
   - 确认Bot已添加到频道并有发送权限
   - 频道ID必须是负数格式

3. **内存不足**
   - Railway免费计划有内存限制
   - 考虑升级到付费计划

### 日志查看
在Railway控制台的"Deployments"页面可以查看实时日志。

## 技术栈

- **Python 3.11**
- **Flask** - Web框架
- **Gunicorn** - WSGI服务器
- **NumPy** - 数据处理
- **Matplotlib/mplfinance** - 图表生成
- **python-telegram-bot** - Telegram集成
- **Requests** - HTTP客户端
- **Threading** - 并发处理
- **Pillow** - 图像处理

## 许可证

MIT License

## 支持

如有问题或建议，请创建GitHub Issue。
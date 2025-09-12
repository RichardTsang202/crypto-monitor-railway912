#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密货币形态识别监控系统 - Railway部署版本
专注于识别双顶、双底、头肩顶、头肩底四种形态
采用极值点缓存策略，支持多时间粒度监控
"""

import os
import sys
import time
import json
import logging
import requests
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from flask import Flask, jsonify
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('realtime_monitor.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ExtremePoint:
    """极值点数据结构"""
    timestamp: int
    price: float
    point_type: str  # 'high' or 'low'
    index: int  # 在K线序列中的索引

@dataclass
class PatternResult:
    """形态识别结果"""
    pattern_type: str
    symbol: str
    timeframe: str
    trigger_time: int
    a_point: Dict[str, Any]
    b_point: Dict[str, Any]
    c_point: Dict[str, Any]
    d_point: Optional[Dict[str, Any]] = None
    atr_value: float = 0.0
    quality_score: float = 0.0
    indicators: Dict[str, Any] = None

class CryptoPatternMonitor:
    """加密货币形态识别监控系统"""
    
    def __init__(self):
        self.app = Flask(__name__)
        self.setup_routes()
        
        # 配置参数
        self.webhook_url = "https://n8n-ayzvkyda.ap-northeast-1.clawcloudrun.com/webhook/double_t_b"
        self.timeframes = ['1h', '4h', '1d']
        self.pattern_types = ['double_top', 'double_bottom', 'head_shoulders_top', 'head_shoulders_bottom']
        
        # 极值点缓存
        self.extreme_points_cache: Dict[str, Dict[str, List[ExtremePoint]]] = {}
        
        # API配置
        self.api_sources = [
            {
                'name': 'binance',
                'base_url': 'https://api.binance.com/api/v3',
                'klines_endpoint': '/klines',
                'timeout': 15,
                'rate_limit': 1200
            },
            {
                'name': 'okx',
                'base_url': 'https://www.okx.com/api/v5',
                'klines_endpoint': '/market/candles',
                'timeout': 10,
                'rate_limit': 600
            }
        ]
        self.current_api_index = 0
        
        # 监控配置
        self.symbols_to_monitor = self._get_symbols_to_monitor()
        self.last_signal_time = {}
        self.signal_interval = 300  # 5分钟
        
        # 线程控制
        self.monitoring_active = True
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        logger.info(f"系统初始化完成，监控 {len(self.symbols_to_monitor)} 个交易对")
        logger.info(f"支持时间粒度: {', '.join(self.timeframes)}")
        logger.info(f"支持形态类型: {', '.join(self.pattern_types)}")
    
    def setup_routes(self):
        """设置Flask路由"""
        @self.app.route('/')
        def index():
            return '''
            <!DOCTYPE html>
            <html lang="zh-CN">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>加密货币形态监控系统</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
                    .container { max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                    h1 { color: #333; text-align: center; margin-bottom: 30px; }
                    .status-card { background: #e8f5e8; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #4caf50; }
                    .info-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }
                    .info-item { background: #f8f9fa; padding: 15px; border-radius: 6px; text-align: center; }
                    .info-item h3 { margin: 0 0 10px 0; color: #666; font-size: 14px; }
                    .info-item .value { font-size: 24px; font-weight: bold; color: #333; }
                    .api-section { margin: 20px 0; }
                    .btn { display: inline-block; padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; margin: 5px; }
                    .btn:hover { background: #0056b3; }
                    .footer { text-align: center; margin-top: 30px; color: #666; font-size: 12px; }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>🚀 加密货币形态监控系统</h1>
                    
                    <div class="status-card">
                        <h2>✅ 系统状态：运行中</h2>
                        <p>实时监控加密货币技术形态，包括双顶、双底、头肩顶、头肩底等经典形态</p>
                    </div>
                    
                    <div class="info-grid">
                        <div class="info-item">
                            <h3>监控币种</h3>
                            <div class="value">''' + str(len(self.symbols_to_monitor)) + '''</div>
                        </div>
                        <div class="info-item">
                            <h3>时间周期</h3>
                            <div class="value">''' + str(len(self.timeframes)) + '''</div>
                        </div>
                        <div class="info-item">
                            <h3>运行时间</h3>
                            <div class="value" id="uptime">计算中...</div>
                        </div>
                    </div>
                    
                    <div class="api-section">
                        <h3>📊 API接口</h3>
                        <a href="/api/health" class="btn">健康检查</a>
                        <a href="/api/status" class="btn">详细状态</a>
                    </div>
                    
                    <div class="footer">
                        <p>Crypto Pattern Monitor v1.0 | 部署在 Railway 平台</p>
                        <p>监控币种: BTC, ETH, BNB, SOL, XRP, DOGE, TON, ADA, SHIB, AVAX</p>
                    </div>
                </div>
                
                <script>
                    // 更新运行时间
                    function updateUptime() {
                        const startTime = ''' + str(int(time.time())) + '''; // 服务器启动时间
                        const now = Math.floor(Date.now() / 1000);
                        const uptime = now - startTime;
                        const hours = Math.floor(uptime / 3600);
                        const minutes = Math.floor((uptime % 3600) / 60);
                        document.getElementById('uptime').textContent = hours + 'h ' + minutes + 'm';
                    }
                    
                    updateUptime();
                    setInterval(updateUptime, 60000); // 每分钟更新一次
                </script>
            </body>
            </html>
            '''
        
        @self.app.route('/api/health')
        def health_check():
            return jsonify({
                'status': 'running',
                'timestamp': int(time.time()),
                'monitored_symbols': len(self.symbols_to_monitor),
                'timeframes': self.timeframes
            })
        
        @self.app.route('/api/status')
        def get_status():
            cache_stats = {}
            for symbol in self.symbols_to_monitor[:5]:  # 只显示前5个
                cache_stats[symbol] = {}
                for tf in self.timeframes:
                    key = f"{symbol}_{tf}"
                    if key in self.extreme_points_cache:
                        cache_stats[symbol][tf] = len(self.extreme_points_cache[key])
                    else:
                        cache_stats[symbol][tf] = 0
            
            return jsonify({
                'monitoring_active': self.monitoring_active,
                'cache_stats': cache_stats,
                'api_source': self.api_sources[self.current_api_index]['name']
            })
    
    def _get_symbols_to_monitor(self) -> List[str]:
        """获取要监控的交易对列表"""
        # 前10个主流代币
        symbols = [
            'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT',
            'DOGEUSDT', 'TONUSDT', 'ADAUSDT', 'SHIBUSDT', 'AVAXUSDT'
        ]
        return symbols
    
    def _make_api_request(self, symbol: str, timeframe: str, limit: int = 200) -> Optional[List[List]]:
        """发起API请求获取K线数据"""
        for attempt in range(len(self.api_sources)):
            api_config = self.api_sources[self.current_api_index]
            
            try:
                if api_config['name'] == 'binance':
                    url = f"{api_config['base_url']}{api_config['klines_endpoint']}"
                    params = {
                        'symbol': symbol,
                        'interval': timeframe,
                        'limit': limit
                    }
                elif api_config['name'] == 'okx':
                    url = f"{api_config['base_url']}{api_config['klines_endpoint']}"
                    params = {
                        'instId': symbol,
                        'bar': timeframe.upper(),
                        'limit': limit
                    }
                
                response = requests.get(url, params=params, timeout=api_config['timeout'])
                response.raise_for_status()
                
                data = response.json()
                
                # 处理不同API的数据格式
                if api_config['name'] == 'binance':
                    return data
                elif api_config['name'] == 'okx':
                    if 'data' in data:
                        # OKX返回字符串格式，需要转换
                        klines = []
                        for item in data['data']:
                            kline = [
                                int(item[0]),  # timestamp
                                float(item[1]),  # open
                                float(item[2]),  # high
                                float(item[3]),  # low
                                float(item[4]),  # close
                                float(item[5])   # volume
                            ]
                            klines.append(kline)
                        return klines
                
                return None
                
            except Exception as e:
                logger.warning(f"API请求失败 {api_config['name']}: {e}")
                self.current_api_index = (self.current_api_index + 1) % len(self.api_sources)
                time.sleep(1)
        
        logger.error(f"所有API源都失败，无法获取 {symbol} {timeframe} 数据")
        return None
    
    def _detect_extreme_points(self, klines: List[List], window_size: int = 5) -> List[ExtremePoint]:
        """检测极值点"""
        if len(klines) < window_size * 2 + 1:
            return []
        
        extreme_points = []
        
        for i in range(window_size, len(klines) - window_size):
            current_high = klines[i][2]  # high price
            current_low = klines[i][3]   # low price
            timestamp = klines[i][0]
            
            # 检查是否为高点
            is_high_point = True
            for j in range(i - window_size, i + window_size + 1):
                if j != i and klines[j][2] >= current_high:
                    is_high_point = False
                    break
            
            if is_high_point:
                extreme_points.append(ExtremePoint(
                    timestamp=timestamp,
                    price=current_high,
                    point_type='high',
                    index=i
                ))
            
            # 检查是否为低点
            is_low_point = True
            for j in range(i - window_size, i + window_size + 1):
                if j != i and klines[j][3] <= current_low:
                    is_low_point = False
                    break
            
            if is_low_point:
                extreme_points.append(ExtremePoint(
                    timestamp=timestamp,
                    price=current_low,
                    point_type='low',
                    index=i
                ))
        
        return extreme_points
    
    def _calculate_atr(self, klines: List[List], period: int = 14) -> float:
        """计算ATR指标"""
        if len(klines) < period + 1:
            return 0.0
        
        true_ranges = []
        for i in range(1, len(klines)):
            high = klines[i][2]
            low = klines[i][3]
            prev_close = klines[i-1][4]
            
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            true_ranges.append(tr)
        
        if len(true_ranges) >= period:
            return sum(true_ranges[-period:]) / period
        return 0.0
    
    def _find_extreme_in_range(self, extreme_points: List[ExtremePoint], 
                              start_idx: int, end_idx: int, point_type: str) -> Optional[ExtremePoint]:
        """在指定范围内查找极值点"""
        candidates = [ep for ep in extreme_points 
                     if ep.point_type == point_type and start_idx <= ep.index <= end_idx]
        
        if not candidates:
            return None
        
        if point_type == 'high':
            return max(candidates, key=lambda x: x.price)
        else:
            return min(candidates, key=lambda x: x.price)
    
    def _detect_double_top(self, symbol: str, timeframe: str, klines: List[List]) -> Optional[PatternResult]:
        """检测双顶形态"""
        if len(klines) < 55:
            return None
        
        # 获取极值点
        extreme_points = self._detect_extreme_points(klines)
        high_points = [ep for ep in extreme_points if ep.point_type == 'high']
        
        if len(high_points) < 2:
            return None
        
        # B点：最新收盘K线的最高点
        latest_kline_idx = len(klines) - 1
        b_point = None
        for hp in reversed(high_points):
            if hp.index == latest_kline_idx:
                b_point = hp
                break
        
        if not b_point:
            return None
        
        # A点：左数第13-34根K线范围内的最高点
        a_start_idx = len(klines) - 34
        a_end_idx = len(klines) - 13
        a_point = self._find_extreme_in_range(high_points, a_start_idx, a_end_idx, 'high')
        
        if not a_point:
            return None
        
        # 计算ATR
        atr = self._calculate_atr(klines)
        if atr == 0:
            return None
        
        # 验证AB距离
        ab_distance = abs(a_point.price - b_point.price)
        if ab_distance > 0.8 * atr:
            return None
        
        # 找到A与B之间的最低点C
        low_points = [ep for ep in extreme_points if ep.point_type == 'low']
        c_candidates = [lp for lp in low_points 
                       if a_point.index < lp.index < b_point.index]
        
        if not c_candidates:
            return None
        
        c_point = min(c_candidates, key=lambda x: x.price)
        
        # 验证C与最高点的距离
        max_ab_price = max(a_point.price, b_point.price)
        c_distance = max_ab_price - c_point.price
        if c_distance < 2.3 * atr:
            return None
        
        # 计算质量评分
        quality_score = min(100, (c_distance / (2.3 * atr)) * 100)
        
        return PatternResult(
            pattern_type='double_top',
            symbol=symbol,
            timeframe=timeframe,
            trigger_time=int(time.time()),
            a_point={'timestamp': a_point.timestamp, 'price': a_point.price, 'index': a_point.index},
            b_point={'timestamp': b_point.timestamp, 'price': b_point.price, 'index': b_point.index},
            c_point={'timestamp': c_point.timestamp, 'price': c_point.price, 'index': c_point.index},
            atr_value=atr,
            quality_score=quality_score
        )
    
    def _detect_double_bottom(self, symbol: str, timeframe: str, klines: List[List]) -> Optional[PatternResult]:
        """检测双底形态"""
        if len(klines) < 55:
            return None
        
        # 获取极值点
        extreme_points = self._detect_extreme_points(klines)
        low_points = [ep for ep in extreme_points if ep.point_type == 'low']
        
        if len(low_points) < 2:
            return None
        
        # B点：最新收盘K线的最低点
        latest_kline_idx = len(klines) - 1
        b_point = None
        for lp in reversed(low_points):
            if lp.index == latest_kline_idx:
                b_point = lp
                break
        
        if not b_point:
            return None
        
        # A点：左数第13-34根K线范围内的最低点
        a_start_idx = len(klines) - 34
        a_end_idx = len(klines) - 13
        a_point = self._find_extreme_in_range(low_points, a_start_idx, a_end_idx, 'low')
        
        if not a_point:
            return None
        
        # 计算ATR
        atr = self._calculate_atr(klines)
        if atr == 0:
            return None
        
        # 验证AB距离
        ab_distance = abs(a_point.price - b_point.price)
        if ab_distance > 0.8 * atr:
            return None
        
        # 找到A与B之间的最高点C
        high_points = [ep for ep in extreme_points if ep.point_type == 'high']
        c_candidates = [hp for hp in high_points 
                       if a_point.index < hp.index < b_point.index]
        
        if not c_candidates:
            return None
        
        c_point = max(c_candidates, key=lambda x: x.price)
        
        # 验证C与最低点的距离
        min_ab_price = min(a_point.price, b_point.price)
        c_distance = c_point.price - min_ab_price
        if c_distance < 2.3 * atr:
            return None
        
        # 计算质量评分
        quality_score = min(100, (c_distance / (2.3 * atr)) * 100)
        
        return PatternResult(
            pattern_type='double_bottom',
            symbol=symbol,
            timeframe=timeframe,
            trigger_time=int(time.time()),
            a_point={'timestamp': a_point.timestamp, 'price': a_point.price, 'index': a_point.index},
            b_point={'timestamp': b_point.timestamp, 'price': b_point.price, 'index': b_point.index},
            c_point={'timestamp': c_point.timestamp, 'price': c_point.price, 'index': c_point.index},
            atr_value=atr,
            quality_score=quality_score
        )
    
    def _detect_head_shoulders_top(self, symbol: str, timeframe: str, klines: List[List]) -> Optional[PatternResult]:
        """检测头肩顶形态"""
        if len(klines) < 55:
            return None
        
        # 获取极值点
        extreme_points = self._detect_extreme_points(klines)
        high_points = [ep for ep in extreme_points if ep.point_type == 'high']
        
        if len(high_points) < 3:
            return None
        
        # B点：最新收盘K线的最高点（右肩）
        latest_kline_idx = len(klines) - 1
        b_point = None
        for hp in reversed(high_points):
            if hp.index == latest_kline_idx:
                b_point = hp
                break
        
        if not b_point:
            return None
        
        # A点：左数第13-34根K线范围内的最高点（头部）
        a_start_idx = len(klines) - 34
        a_end_idx = len(klines) - 13
        a_point = self._find_extreme_in_range(high_points, a_start_idx, a_end_idx, 'high')
        
        if not a_point:
            return None
        
        # D点：左数第35-55根K线范围内的最高点（左肩）
        d_start_idx = len(klines) - 55
        d_end_idx = len(klines) - 35
        d_point = self._find_extreme_in_range(high_points, d_start_idx, d_end_idx, 'high')
        
        if not d_point:
            return None
        
        # 计算ATR
        atr = self._calculate_atr(klines)
        if atr == 0:
            return None
        
        # 验证BA距离
        ba_distance = abs(b_point.price - a_point.price)
        if ba_distance > 1.0 * atr:
            return None
        
        # 验证BD距离
        bd_distance = abs(b_point.price - d_point.price)
        if bd_distance > 0.8 * atr:
            return None
        
        # 验证A为最高点
        if not (a_point.price >= b_point.price and a_point.price >= d_point.price):
            return None
        
        # 找到A与B之间的最低点C
        low_points = [ep for ep in extreme_points if ep.point_type == 'low']
        c_candidates = [lp for lp in low_points 
                       if a_point.index < lp.index < b_point.index]
        
        if not c_candidates:
            return None
        
        c_point = min(c_candidates, key=lambda x: x.price)
        
        # 验证AC距离
        ac_distance = abs(a_point.price - c_point.price)
        if ac_distance < 2.0 * atr:
            return None
        
        # 计算质量评分
        quality_score = min(100, (ac_distance / (2.0 * atr)) * 100)
        
        return PatternResult(
            pattern_type='head_shoulders_top',
            symbol=symbol,
            timeframe=timeframe,
            trigger_time=int(time.time()),
            a_point={'timestamp': a_point.timestamp, 'price': a_point.price, 'index': a_point.index},
            b_point={'timestamp': b_point.timestamp, 'price': b_point.price, 'index': b_point.index},
            c_point={'timestamp': c_point.timestamp, 'price': c_point.price, 'index': c_point.index},
            d_point={'timestamp': d_point.timestamp, 'price': d_point.price, 'index': d_point.index},
            atr_value=atr,
            quality_score=quality_score
        )
    
    def _detect_head_shoulders_bottom(self, symbol: str, timeframe: str, klines: List[List]) -> Optional[PatternResult]:
        """检测头肩底形态"""
        if len(klines) < 55:
            return None
        
        # 获取极值点
        extreme_points = self._detect_extreme_points(klines)
        low_points = [ep for ep in extreme_points if ep.point_type == 'low']
        
        if len(low_points) < 3:
            return None
        
        # B点：最新收盘K线的最低点（右肩）
        latest_kline_idx = len(klines) - 1
        b_point = None
        for lp in reversed(low_points):
            if lp.index == latest_kline_idx:
                b_point = lp
                break
        
        if not b_point:
            return None
        
        # A点：左数第13-34根K线范围内的最低点（头部）
        a_start_idx = len(klines) - 34
        a_end_idx = len(klines) - 13
        a_point = self._find_extreme_in_range(low_points, a_start_idx, a_end_idx, 'low')
        
        if not a_point:
            return None
        
        # D点：左数第35-55根K线范围内的最低点（左肩）
        d_start_idx = len(klines) - 55
        d_end_idx = len(klines) - 35
        d_point = self._find_extreme_in_range(low_points, d_start_idx, d_end_idx, 'low')
        
        if not d_point:
            return None
        
        # 计算ATR
        atr = self._calculate_atr(klines)
        if atr == 0:
            return None
        
        # 验证BA距离
        ba_distance = abs(b_point.price - a_point.price)
        if ba_distance > 1.0 * atr:
            return None
        
        # 验证BD距离
        bd_distance = abs(b_point.price - d_point.price)
        if bd_distance > 0.8 * atr:
            return None
        
        # 验证A为最低点
        if not (a_point.price <= b_point.price and a_point.price <= d_point.price):
            return None
        
        # 找到A与B之间的最高点C
        high_points = [ep for ep in extreme_points if ep.point_type == 'high']
        c_candidates = [hp for hp in high_points 
                       if a_point.index < hp.index < b_point.index]
        
        if not c_candidates:
            return None
        
        c_point = max(c_candidates, key=lambda x: x.price)
        
        # 验证AC距离
        ac_distance = abs(a_point.price - c_point.price)
        if ac_distance < 2.0 * atr:
            return None
        
        # 计算质量评分
        quality_score = min(100, (ac_distance / (2.0 * atr)) * 100)
        
        return PatternResult(
            pattern_type='head_shoulders_bottom',
            symbol=symbol,
            timeframe=timeframe,
            trigger_time=int(time.time()),
            a_point={'timestamp': a_point.timestamp, 'price': a_point.price, 'index': a_point.index},
            b_point={'timestamp': b_point.timestamp, 'price': b_point.price, 'index': b_point.index},
            c_point={'timestamp': c_point.timestamp, 'price': c_point.price, 'index': c_point.index},
            d_point={'timestamp': d_point.timestamp, 'price': d_point.price, 'index': d_point.index},
            atr_value=atr,
            quality_score=quality_score
        )
    
    def _analyze_pattern(self, symbol: str, timeframe: str) -> Optional[PatternResult]:
        """分析单个交易对的形态"""
        try:
            # 获取K线数据
            klines = self._make_api_request(symbol, timeframe, 200)
            if not klines:
                return None
            
            # 检测各种形态
            patterns = [
                self._detect_double_top(symbol, timeframe, klines),
                self._detect_double_bottom(symbol, timeframe, klines),
                self._detect_head_shoulders_top(symbol, timeframe, klines),
                self._detect_head_shoulders_bottom(symbol, timeframe, klines)
            ]
            
            # 返回第一个检测到的形态
            for pattern in patterns:
                if pattern:
                    logger.info(f"检测到形态: {pattern.pattern_type} - {symbol} {timeframe}")
                    return pattern
            
            return None
            
        except Exception as e:
            logger.error(f"分析形态时出错 {symbol} {timeframe}: {e}")
            return None
    
    def _calculate_additional_indicators(self, pattern: PatternResult) -> Dict[str, Any]:
        """计算额外的技术指标"""
        try:
            # 获取144根K线用于计算EMA
            klines = self._make_api_request(pattern.symbol, pattern.timeframe, 144)
            if not klines or len(klines) < 144:
                return {}
            
            # 计算EMA
            closes = [float(k[4]) for k in klines]
            ema21 = self._calculate_ema(closes, 21)
            ema55 = self._calculate_ema(closes, 55)
            ema144 = self._calculate_ema(closes, 144)
            
            # 计算MACD和RSI用于背离分析
            macd_line, macd_signal = self._calculate_macd(closes)
            rsi = self._calculate_rsi(closes)
            
            # 分析K线形态
            latest_kline = klines[-1]
            kline_pattern = self._analyze_kline_pattern(latest_kline)
            
            return {
                'ema21': ema21,
                'ema55': ema55,
                'ema144': ema144,
                'macd_line': macd_line,
                'macd_signal': macd_signal,
                'rsi': rsi,
                'kline_pattern': kline_pattern,
                'trend_direction': self._determine_trend(ema21, ema55, ema144)
            }
            
        except Exception as e:
            logger.error(f"计算指标时出错: {e}")
            return {}
    
    def _calculate_ema(self, prices: List[float], period: int) -> float:
        """计算EMA"""
        if len(prices) < period:
            return 0.0
        
        multiplier = 2 / (period + 1)
        ema = prices[0]
        
        for price in prices[1:]:
            ema = (price * multiplier) + (ema * (1 - multiplier))
        
        return ema
    
    def _calculate_macd(self, prices: List[float], fast=12, slow=26, signal=9) -> Tuple[float, float]:
        """计算MACD"""
        if len(prices) < slow:
            return 0.0, 0.0
        
        ema_fast = self._calculate_ema(prices, fast)
        ema_slow = self._calculate_ema(prices, slow)
        macd_line = ema_fast - ema_slow
        
        # 简化的信号线计算
        macd_signal = macd_line * 0.9  # 简化处理
        
        return macd_line, macd_signal
    
    def _calculate_rsi(self, prices: List[float], period: int = 14) -> float:
        """计算RSI"""
        if len(prices) < period + 1:
            return 50.0
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        if len(gains) < period:
            return 50.0
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _analyze_kline_pattern(self, kline: List) -> str:
        """分析K线形态"""
        open_price = float(kline[1])
        high_price = float(kline[2])
        low_price = float(kline[3])
        close_price = float(kline[4])
        
        body_size = abs(close_price - open_price)
        upper_shadow = high_price - max(open_price, close_price)
        lower_shadow = min(open_price, close_price) - low_price
        
        total_range = high_price - low_price
        
        if total_range == 0:
            return "doji"
        
        body_ratio = body_size / total_range
        upper_ratio = upper_shadow / total_range
        lower_ratio = lower_shadow / total_range
        
        if body_ratio < 0.1:
            return "doji"
        elif upper_ratio > 0.6:
            return "shooting_star" if close_price < open_price else "inverted_hammer"
        elif lower_ratio > 0.6:
            return "hammer" if close_price > open_price else "hanging_man"
        elif body_ratio > 0.7:
            return "strong_bullish" if close_price > open_price else "strong_bearish"
        else:
            return "normal_bullish" if close_price > open_price else "normal_bearish"
    
    def _determine_trend(self, ema21: float, ema55: float, ema144: float) -> str:
        """判断趋势方向"""
        if ema21 > ema55 > ema144:
            return "strong_uptrend"
        elif ema21 > ema55:
            return "uptrend"
        elif ema21 < ema55 < ema144:
            return "strong_downtrend"
        elif ema21 < ema55:
            return "downtrend"
        else:
            return "sideways"
    
    def _send_webhook(self, pattern: PatternResult) -> bool:
        """发送webhook通知"""
        try:
            # 检查信号间隔
            signal_key = f"{pattern.symbol}_{pattern.timeframe}_{pattern.pattern_type}"
            current_time = time.time()
            
            if signal_key in self.last_signal_time:
                if current_time - self.last_signal_time[signal_key] < self.signal_interval:
                    logger.info(f"信号间隔未到，跳过发送: {signal_key}")
                    return False
            
            # 计算额外指标
            additional_indicators = self._calculate_additional_indicators(pattern)
            pattern.indicators = additional_indicators
            
            # 构建webhook数据
            webhook_data = {
                'pattern_type': pattern.pattern_type,
                'symbol': pattern.symbol,
                'timeframe': pattern.timeframe,
                'trigger_time': pattern.trigger_time,
                'points': {
                    'a_point': pattern.a_point,
                    'b_point': pattern.b_point,
                    'c_point': pattern.c_point
                },
                'atr_value': pattern.atr_value,
                'quality_score': pattern.quality_score,
                'indicators': pattern.indicators
            }
            
            if pattern.d_point:
                webhook_data['points']['d_point'] = pattern.d_point
            
            # 发送请求
            response = requests.post(
                self.webhook_url,
                json=webhook_data,
                timeout=15
            )
            response.raise_for_status()
            
            # 更新最后发送时间
            self.last_signal_time[signal_key] = current_time
            
            logger.info(f"Webhook发送成功: {pattern.pattern_type} - {pattern.symbol} {pattern.timeframe}")
            return True
            
        except Exception as e:
            logger.error(f"Webhook发送失败: {e}")
            return False
    
    def _monitor_timeframe(self, timeframe: str):
        """监控指定时间粒度"""
        while self.monitoring_active:
            try:
                current_time = datetime.now()
                should_monitor = False
                
                # 判断是否应该监控
                if timeframe == '1h':
                    should_monitor = current_time.minute == 0
                elif timeframe == '4h':
                    should_monitor = (current_time.hour % 4 == 0 and 
                                    current_time.minute == 30)
                elif timeframe == '1d':
                    should_monitor = (current_time.hour == 8 and 
                                    current_time.minute == 15)
                
                if should_monitor:
                    logger.info(f"开始监控 {timeframe} 时间粒度")
                    
                    # 并发分析所有交易对
                    futures = []
                    for symbol in self.symbols_to_monitor:
                        future = self.executor.submit(self._analyze_pattern, symbol, timeframe)
                        futures.append(future)
                        
                        # 添加间隔避免API限制
                        if timeframe == '1h':
                            time.sleep(3)
                        elif timeframe == '4h':
                            time.sleep(5)
                        elif timeframe == '1d':
                            time.sleep(10)
                    
                    # 处理结果
                    for future in as_completed(futures):
                        try:
                            pattern = future.result()
                            if pattern:
                                self._send_webhook(pattern)
                        except Exception as e:
                            logger.error(f"处理分析结果时出错: {e}")
                
                # 等待下一分钟
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"监控 {timeframe} 时出错: {e}")
                time.sleep(60)
    
    def start_monitoring(self):
        """启动监控"""
        logger.info("启动形态识别监控系统")
        
        # 为每个时间粒度启动监控线程
        for timeframe in self.timeframes:
            thread = threading.Thread(
                target=self._monitor_timeframe,
                args=(timeframe,),
                daemon=True
            )
            thread.start()
            logger.info(f"启动 {timeframe} 监控线程")
        
        # 启动Flask应用
        port = int(os.environ.get('PORT', 5000))
        self.app.run(host='0.0.0.0', port=port, debug=False)
    
    def stop_monitoring(self):
        """停止监控"""
        logger.info("停止监控系统")
        self.monitoring_active = False
        self.executor.shutdown(wait=True)

# 创建全局Flask应用实例供gunicorn使用
monitor = None
app = None

def create_app():
    """创建Flask应用实例"""
    global monitor, app
    if monitor is None:
        monitor = CryptoPatternMonitor()
        app = monitor.app
    return app

# 初始化应用
app = create_app()

def main():
    """主函数"""
    try:
        global monitor
        if monitor is None:
            monitor = CryptoPatternMonitor()
        monitor.start_monitoring()
    except KeyboardInterrupt:
        logger.info("接收到停止信号")
    except Exception as e:
        logger.error(f"系统运行出错: {e}")
    finally:
        logger.info("系统已停止")

if __name__ == '__main__':
    main()
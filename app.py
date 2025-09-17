#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密货币形态识别监控系统 - 修复版

本系统专注于识别四种经典的价格形态：双顶、双底、头肩顶、头肩底。
采用多数据源架构，具备自动故障转移能力，通过严格的技术指标验证确保信号质量。

主要特性：
- 智能缓存管理，仅缓存极值点和必要数据
- 并发处理提升性能
- 14周期ATR动态计算
- 精确的ABCD点获取逻辑
- 多时间粒度监控（1H、4H、1D）
- 内存使用优化和自动清理
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
from flask import Flask, jsonify, render_template_string
import schedule
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
from dataclasses import dataclass
import gc
import psutil
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
import mplfinance as mpf
from PIL import Image
import io
import base64
from telegram import Bot
from telegram.error import TelegramError
import asyncio

# 配置日志 - 优化Railway环境支持
def setup_logging():
    """设置日志配置，优化Railway环境支持"""
    # 检测是否在Railway环境
    is_railway = os.environ.get('RAILWAY_ENVIRONMENT') is not None or os.environ.get('PORT') is not None
    
    handlers = []
    
    # 文件日志处理器
    try:
        file_handler = logging.FileHandler('realtime_monitor.log', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        handlers.append(file_handler)
    except Exception as e:
        print(f"无法创建文件日志处理器: {e}")
    
    # 控制台日志处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    handlers.append(console_handler)
    
    # Railway环境特殊配置
    if is_railway:
        # 确保日志立即刷新到stdout
        console_handler.setLevel(logging.INFO)
        # 添加额外的stderr处理器以确保错误日志可见
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        stderr_handler.setLevel(logging.WARNING)
        handlers.append(stderr_handler)
    
    # 配置根日志记录器
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers,
        force=True  # 强制重新配置
    )
    
    # 设置特定库的日志级别
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

# 初始化日志
logger = setup_logging()

# Railway环境检测日志
if os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('PORT'):
    logger.info("检测到Railway部署环境，已优化日志配置")
else:
    logger.info("本地开发环境，使用标准日志配置")

@dataclass
class ExtremePoint:
    """极值点数据结构"""
    timestamp: int
    price: float
    point_type: str  # 'high' or 'low'

@dataclass
class PatternCache:
    """形态缓存数据结构"""
    a_point: Optional[ExtremePoint] = None
    d_point: Optional[ExtremePoint] = None  # 头肩形态的左肩点
    last_update: Optional[datetime] = None
    atr_value: Optional[float] = None
    atr_timestamp: Optional[int] = None

class DataCache:
    """数据缓存管理类"""
    
    def __init__(self, max_size: int = 1000, ttl_seconds: int = 300):
        self.kline_cache = {}
        self.extreme_cache = {}
        self.atr_cache = {}
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._access_times = {}
        
    def get_klines(self, symbol: str, timeframe: str, limit: int = 55) -> Optional[List[List]]:
        """获取K线数据（带缓存）"""
        cache_key = f"{symbol}_{timeframe}_{limit}"
        current_time = time.time()
        
        # 检查缓存是否存在且未过期
        if cache_key in self.kline_cache:
            cached_data, timestamp = self.kline_cache[cache_key]
            if current_time - timestamp < self.ttl_seconds:
                self._access_times[cache_key] = current_time
                return cached_data
        
        return None
    
    def set_klines(self, symbol: str, timeframe: str, data: List[List], limit: int = 55):
        """缓存K线数据"""
        cache_key = f"{symbol}_{timeframe}_{limit}"
        current_time = time.time()
        
        # 检查缓存大小，必要时清理
        if len(self.kline_cache) >= self.max_size:
            self._cleanup_cache()
        
        self.kline_cache[cache_key] = (data, current_time)
        self._access_times[cache_key] = current_time
    
    def get_atr(self, symbol: str, timeframe: str) -> Optional[float]:
        """获取ATR值（带缓存）"""
        cache_key = f"{symbol}_{timeframe}_atr"
        current_time = time.time()
        
        if cache_key in self.atr_cache:
            atr_value, timestamp = self.atr_cache[cache_key]
            if current_time - timestamp < self.ttl_seconds:
                return atr_value
        
        return None
    
    def set_atr(self, symbol: str, timeframe: str, atr_value: float):
        """缓存ATR值"""
        cache_key = f"{symbol}_{timeframe}_atr"
        current_time = time.time()
        self.atr_cache[cache_key] = (atr_value, current_time)
    
    def _cleanup_cache(self):
        """清理过期和最少使用的缓存"""
        current_time = time.time()
        
        # 清理过期缓存
        expired_keys = []
        for key, (data, timestamp) in self.kline_cache.items():
            if current_time - timestamp > self.ttl_seconds:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.kline_cache[key]
            if key in self._access_times:
                del self._access_times[key]
        
        # 如果仍然超过大小限制，删除最少使用的缓存
        if len(self.kline_cache) >= self.max_size:
            # 按访问时间排序，删除最老的
            sorted_keys = sorted(self._access_times.items(), key=lambda x: x[1])
            keys_to_remove = [k for k, _ in sorted_keys[:self.max_size // 4]]  # 删除25%
            
            for key in keys_to_remove:
                if key in self.kline_cache:
                    del self.kline_cache[key]
                if key in self._access_times:
                    del self._access_times[key]
        
        # 强制垃圾回收
        gc.collect()
    
    def clear_all(self):
        """清空所有缓存"""
        self.kline_cache.clear()
        self.extreme_cache.clear()
        self.atr_cache.clear()
        self._access_times.clear()
        gc.collect()

class CryptoPatternMonitor:
    """加密货币形态识别监控系统"""
    
    def __init__(self):
        """初始化监控系统"""
        self.running = False
        self.pattern_cache = {}  # 统一的形态缓存
        self.last_analysis_time = {}  # 最后分析时间（防重复分析）
        self.last_webhook_time = 0  # 全局webhook发送时间控制
        self.api_sources = self._init_api_sources()
        self.current_api_index = 0
        self.monitored_pairs = self._get_monitored_pairs()
        self.timeframes = ['1h']  # 只监控1小时时间粒度
        self.webhook_url = "https://n8n-ayzvkyda.ap-northeast-1.clawcloudrun.com/webhook-test/double_t_b"
        
        # Telegram配置
        self.telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
        self.telegram_channel_id = os.environ.get('TELEGRAM_CHANNEL_ID', '')
        self.telegram_bot = Bot(token=self.telegram_token) if self.telegram_token else None
        self.last_telegram_time = 0  # Telegram发送时间控制
        self.telegram_send_interval = 3  # 3秒间隔
        
        # 数据缓存系统
        self.data_cache = DataCache(max_size=500, ttl_seconds=240)  # 4分钟缓存
        
        # 性能配置
        self.max_concurrent_analysis = 15  # 增加并发数
        self.analysis_timeout = 30
        self.memory_limit = 300 * 1024 * 1024  # 300MB
        
        # 系统健康状态跟踪
        self.system_health = {
            'status': 'healthy',
            'start_time': datetime.now(),
            'last_heartbeat': datetime.now(),
            'error_count': 0,
            'consecutive_errors': 0,
            'last_error_time': None,
            'recovery_attempts': 0,
            'max_recovery_attempts': 3  # 减少最大恢复尝试次数
        }
        
        # 监控线程状态
        self.monitor_threads = {}
        self.thread_health = {}
        
        # 异常恢复配置
        self.recovery_config = {
            'max_consecutive_errors': 15,  # 增加容错性
            'recovery_delay': [60, 120, 300],  # 减少恢复延迟层级
            'health_check_interval': 120,  # 增加健康检查间隔
            'auto_restart_threshold': 25  # 提高自动重启阈值
        }
        
        logger.info("加密货币形态识别监控系统初始化完成")
    
    def _update_system_health(self, status: str = 'healthy', error: Exception = None):
        """更新系统健康状态"""
        try:
            self.system_health['last_heartbeat'] = datetime.now()
            
            if status == 'error' and error:
                self.system_health['error_count'] += 1
                self.system_health['consecutive_errors'] += 1
                self.system_health['last_error_time'] = datetime.now()
                
                if self.system_health['consecutive_errors'] < 8:
                    self.system_health['status'] = 'degraded'
                else:
                    self.system_health['status'] = 'critical'
                    
                logger.error(f"系统健康状态更新: {status}, 连续错误: {self.system_health['consecutive_errors']}, 错误: {str(error)}")
            elif status == 'healthy':
                self.system_health['consecutive_errors'] = 0
                self.system_health['status'] = 'healthy'
            
            # 检查是否需要自动恢复
            if self.system_health['consecutive_errors'] >= self.recovery_config['max_consecutive_errors']:
                self._attempt_system_recovery()
                
        except Exception as e:
            logger.error(f"更新系统健康状态失败: {str(e)}")
    
    def _attempt_system_recovery(self):
        """尝试系统自动恢复"""
        try:
            if self.system_health['recovery_attempts'] >= self.system_health['max_recovery_attempts']:
                logger.critical("系统恢复尝试次数已达上限，需要人工干预")
                return False
            
            self.system_health['recovery_attempts'] += 1
            recovery_delay = self.recovery_config['recovery_delay']
            delay_index = min(self.system_health['recovery_attempts'] - 1, len(recovery_delay) - 1)
            delay_time = recovery_delay[delay_index]
            
            logger.warning(f"开始系统恢复尝试 {self.system_health['recovery_attempts']}/{self.system_health['max_recovery_attempts']}, 延迟 {delay_time} 秒")
            
            # 清理缓存和状态
            self._cleanup_system_state()
            
            # 等待恢复延迟
            time.sleep(delay_time)
            
            # 重新初始化API源
            self.api_sources = self._init_api_sources()
            self.current_api_index = 0
            
            logger.info(f"系统恢复尝试 {self.system_health['recovery_attempts']} 完成")
            return True
            
        except Exception as e:
            logger.error(f"系统恢复失败: {str(e)}")
            return False
    
    def _cleanup_system_state(self):
        """清理系统状态"""
        try:
            # 清理所有缓存
            self.pattern_cache.clear()
            self.data_cache.clear_all()
            
            # 重置错误计数
            self.system_health['consecutive_errors'] = max(0, self.system_health['consecutive_errors'] - 5)
            self.system_health['status'] = 'recovering'
            
            # 强制垃圾回收
            gc.collect()
            
            logger.info("系统状态清理完成")
        except Exception as e:
            logger.error(f"系统状态清理失败: {str(e)}")
    
    def _check_thread_health(self):
        """检查监控线程健康状态"""
        try:
            current_time = datetime.now()
            for timeframe in self.timeframes:
                thread = self.monitor_threads.get(timeframe)
                health = self.thread_health.get(timeframe, {})
                
                if not thread or not thread.is_alive():
                    logger.warning(f"监控线程 {timeframe} 已停止，尝试重启")
                    self._restart_single_monitor(timeframe)
                elif health.get('last_activity'):
                    inactive_time = (current_time - health['last_activity']).total_seconds()
                    # 增加无响应阈值，避免误判
                    if inactive_time > 900:  # 15分钟无活动
                        logger.warning(f"监控线程 {timeframe} 无响应 {inactive_time:.0f} 秒，尝试重启")
                        self._restart_single_monitor(timeframe)
        except Exception as e:
            logger.error(f"线程健康检查失败: {str(e)}")
    
    def _restart_single_monitor(self, timeframe: str):
        """重启单个监控线程"""
        try:
            # 停止旧线程
            old_thread = self.monitor_threads.get(timeframe)
            if old_thread and old_thread.is_alive():
                old_thread.join(timeout=10)
            
            # 启动新线程
            new_thread = threading.Thread(target=self._monitor_timeframe, args=(timeframe,), daemon=True)
            new_thread.start()
            self.monitor_threads[timeframe] = new_thread
            self.thread_health[timeframe] = {
                'status': 'running',
                'last_activity': datetime.now(),
                'error_count': 0
            }
            logger.info(f"监控线程 {timeframe} 重启成功")
        except Exception as e:
            logger.error(f"重启监控线程 {timeframe} 失败: {str(e)}")
    
    def _init_api_sources(self) -> List[Dict[str, Any]]:
        """初始化API数据源配置"""
        return [
            {
                'name': 'Binance',
                'base_url': 'https://api.binance.com/api/v3',
                'klines_endpoint': '/klines',
                'timeout': 15,
                'rate_limit': 1200,
                'requires_key': True
            },
            {
                'name': 'OKX',
                'base_url': 'https://www.okx.com/api/v5',
                'klines_endpoint': '/market/candles',
                'timeout': 10,
                'rate_limit': 600,
                'requires_key': True
            }
        ]
    
    def _get_monitored_pairs(self) -> List[str]:
        """获取监控的交易对列表"""
        # 黑名单
        blacklist = ['USDCUSDT', 'TUSDUSDT', 'BUSDUSDT', 'FDUSDT']
        
        pairs = [
    'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT',
    'DOGEUSDT', 'ADAUSDT', 'TRXUSDT', 'AVAXUSDT', 'TONUSDT',
    'LINKUSDT', 'DOTUSDT', 'POLUSDT', 'ICPUSDT', 'NEARUSDT',
    'UNIUSDT', 'LTCUSDT', 'APTUSDT', 'FILUSDT', 'ETCUSDT',
    'ATOMUSDT', 'HBARUSDT', 'BCHUSDT', 'INJUSDT', 'SUIUSDT',
    'ARBUSDT', 'OPUSDT', 'FTMUSDT', 'IMXUSDT', 'STRKUSDT',
    'MANAUSDT', 'VETUSDT', 'ALGOUSDT', 'GRTUSDT', 'SANDUSDT',
    'AXSUSDT', 'FLOWUSDT', 'THETAUSDT', 'CHZUSDT', 'APEUSDT',
    'MKRUSDT', 'AAVEUSDT', 'SNXUSDT', 'QNTUSDT',
    'GALAUSDT', 'ROSEUSDT', 'KLAYUSDT', 'ENJUSDT', 'RUNEUSDT',
    'WIFUSDT', 'BONKUSDT', 'FLOKIUSDT', 'NOTUSDT',
    'PEOPLEUSDT', 'JUPUSDT', 'WLDUSDT', 'ORDIUSDT', 'SEIUSDT',
    'TIAUSDT', 'RENDERUSDT', 'FETUSDT', 'ARKMUSDT',
    'PENGUUSDT', 'PNUTUSDT', 'ACTUSDT', 'NEIROUSDT',
    'RAYUSDT', 'BOMEUSDT', 'MEMEUSDT', 'MOVEUSDT',
    'EIGENUSDT', 'DYDXUSDT', 'TURBOUSDT','PYTHUSDT', 'JASMYUSDT', 'COMPUSDT', 'CRVUSDT', 'LRCUSDT',
    'SUSHIUSDT', 'SUSDT', 'YGGUSDT', 'CAKEUSDT', 'OGUSDT',
    'STORJUSDT', 'KNCUSDT', 'LENDUSDT', 'YFIUSDT', 'FORMUSDT',
    'ZRXUSDT', 'XLMUSDT', 'XMRUSDT', 'XTZUSDT','BAKEUSDT',
     'DOLOUSDT', 'SOMIUSDT', 'TRUMPUSDT', 'ONDOUSDT',
    'NMRUSDT', 'BBUSDT',  'ZECUSDT'
]
        
        # 过滤黑名单
        filtered_pairs = [pair for pair in pairs if pair not in blacklist]
        logger.info(f"监控交易对数量: {len(filtered_pairs)}")
        return filtered_pairs
    
    def _get_klines_data(self, symbol: str, interval: str, limit: int = 100) -> Optional[List[List]]:
        """获取K线数据，支持缓存和API轮换"""
        # 先检查缓存
        cached_data = self.data_cache.get_klines(symbol, interval, limit)
        if cached_data:
            return cached_data
        
        max_retries = len(self.api_sources) * 2
        retry_delays = [1, 2, 3, 5, 8]
        
        for attempt in range(max_retries):
            api_source = self.api_sources[self.current_api_index]
            delay = retry_delays[min(attempt, len(retry_delays) - 1)]
            
            try:
                if api_source['name'] == 'Binance':
                    url = f"{api_source['base_url']}{api_source['klines_endpoint']}"
                    params = {
                        'symbol': symbol,
                        'interval': interval,
                        'limit': limit
                    }
                    
                    response = requests.get(
                        url, 
                        params=params, 
                        timeout=api_source.get('timeout', 15),
                        headers={'User-Agent': 'CryptoPatternMonitor/2.0'}
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if not data or not isinstance(data, list) or len(data) == 0:
                            raise ValueError("Binance API返回空数据或格式错误")
                        
                        if len(data[0]) < 6:
                            raise ValueError("Binance K线数据格式不完整")
                        
                        validated_data = []
                        for kline in data:
                            try:
                                validated_kline = [float(x) for x in kline[:6]]
                                if validated_kline[2] < validated_kline[3]:
                                    logger.warning(f"跳过异常K线数据: high({validated_kline[2]}) < low({validated_kline[3]})")
                                    continue
                                validated_data.append(validated_kline)
                            except (ValueError, IndexError):
                                continue
                        
                        if len(validated_data) >= limit * 0.8:
                            # 缓存数据
                            self.data_cache.set_klines(symbol, interval, validated_data, limit)
                            return validated_data
                        else:
                            raise ValueError(f"有效数据不足: {len(validated_data)}/{limit}")
                    
                    elif response.status_code == 429:
                        logger.warning(f"{api_source['name']} API限流，延长等待时间")
                        time.sleep(delay * 2)
                        continue
                    else:
                        logger.warning(f"{api_source['name']} API请求失败: {response.status_code}")
                
                elif api_source['name'] == 'OKX':
                    url = f"{api_source['base_url']}{api_source['klines_endpoint']}"
                    okx_symbol = symbol.replace('USDT', '-USDT') if 'USDT' in symbol else symbol
                    
                    # OKX API时间间隔映射
                    interval_map = {
                        '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
                        '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '8h': '8H', '12h': '12H',
                        '1d': '1D', '3d': '3D', '1w': '1W', '1M': '1M', '3M': '3M'
                    }
                    okx_interval = interval_map.get(interval, interval)
                    
                    params = {
                        'instId': okx_symbol,
                        'bar': okx_interval,
                        'limit': str(limit)
                    }
                    
                    response = requests.get(
                        url, 
                        params=params, 
                        timeout=api_source.get('timeout', 15),
                        headers={'User-Agent': 'CryptoPatternMonitor/2.0'}
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if data.get('code') == '0' and data.get('data'):
                            okx_data = data['data']
                            
                            if not okx_data or len(okx_data) == 0:
                                raise ValueError("OKX API返回空数据")
                            
                            validated_data = []
                            for kline in okx_data:
                                try:
                                    if len(kline) < 6:
                                        continue
                                    validated_kline = [float(x) for x in kline[:6]]
                                    if validated_kline[2] < validated_kline[3]:
                                        continue
                                    validated_data.append(validated_kline)
                                except (ValueError, IndexError):
                                    continue
                            
                            if len(validated_data) >= limit * 0.8:
                                # 缓存数据
                                self.data_cache.set_klines(symbol, interval, validated_data, limit)
                                return validated_data
                            else:
                                raise ValueError(f"有效数据不足: {len(validated_data)}/{limit}")
                        else:
                            raise ValueError(f"OKX API错误: {data.get('msg', '未知错误')}")
                
            except requests.exceptions.Timeout:
                logger.warning(f"{api_source['name']} API请求超时 (尝试 {attempt + 1}/{max_retries})")
            except requests.exceptions.ConnectionError:
                logger.warning(f"{api_source['name']} API连接错误 (尝试 {attempt + 1}/{max_retries})")
            except Exception as e:
                logger.warning(f"{api_source['name']} API请求异常 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
            
            # 切换到下一个API源
            if (attempt + 1) % 2 == 0:
                old_index = self.current_api_index
                self.current_api_index = (self.current_api_index + 1) % len(self.api_sources)
                logger.info(f"切换API源: {self.api_sources[old_index]['name']} -> {self.api_sources[self.current_api_index]['name']}")
            
            if attempt < max_retries - 1:
                time.sleep(delay)
        
        logger.error(f"所有API源都失败，无法获取 {symbol} 的K线数据")
        return None
    
    def _detect_extreme_points(self, klines: List[List]) -> List[ExtremePoint]:
        """检测极值点"""
        if len(klines) < 3:
            return []
        
        extreme_points = []
        
        for i in range(1, len(klines) - 1):
            current = klines[i]
            prev = klines[i - 1]
            next_kline = klines[i + 1]
            
            timestamp = int(current[0])
            high = float(current[2])  # 确保为浮点数
            low = float(current[3])   # 确保为浮点数
            
            # 检测高点
            if high > float(prev[2]) and high > float(next_kline[2]):
                extreme_points.append(ExtremePoint(
                    timestamp=timestamp,
                    price=high,
                    point_type='high'
                ))
            
            # 检测低点
            if low < float(prev[3]) and low < float(next_kline[3]):
                extreme_points.append(ExtremePoint(
                    timestamp=timestamp,
                    price=low,
                    point_type='low'
                ))
        
        return extreme_points
    
    def _initialize_all_pattern_cache(self):
        """启动时初始化所有代币的A点缓存"""
        try:
            symbols = self._get_monitored_pairs()
            logger.info(f"开始为 {len(symbols)} 个代币初始化A点缓存")
            
            for symbol in symbols:
                for timeframe in self.timeframes:
                    try:
                        self._initialize_pattern_cache(symbol, timeframe)
                        logger.debug(f"初始化 {symbol}_{timeframe} A点缓存完成")
                    except Exception as e:
                        logger.error(f"初始化 {symbol}_{timeframe} A点缓存失败: {str(e)}")
                        continue
            
            logger.info("所有代币A点缓存初始化完成")
            
        except Exception as e:
            logger.error(f"初始化所有代币A点缓存失败: {str(e)}")
    
    def _initialize_pattern_cache(self, symbol: str, timeframe: str):
        """初始化形态缓存区"""
        cache_key = f"{symbol}_{timeframe}"
        
        if cache_key in self.pattern_cache:
            return
        
        # 获取足够的历史K线数据
        klines = self._get_klines_data(symbol, timeframe, 55)
        if not klines or len(klines) < 55:
            logger.error(f"无法初始化 {cache_key} 的形态缓存")
            return
        
        # 初始化缓存
        self.pattern_cache[cache_key] = {
            'double_top': PatternCache(),
            'double_bottom': PatternCache(),
            'head_shoulders_top': PatternCache(),
            'head_shoulders_bottom': PatternCache()
        }
        
        # 更新缓存数据
        self._update_pattern_cache(symbol, timeframe, klines)
        
        logger.info(f"初始化 {cache_key} 形态缓存完成")
    
    def _update_pattern_cache(self, symbol: str, timeframe: str, klines: List[List] = None):
        """更新形态缓存区"""
        cache_key = f"{symbol}_{timeframe}"
        
        if klines is None:
            klines = self._get_klines_data(symbol, timeframe, 55)
        
        if not klines or len(klines) < 55:
            return
        
        if cache_key not in self.pattern_cache:
            self.pattern_cache[cache_key] = {
                'double_top': PatternCache(),
                'double_bottom': PatternCache(),
                'head_shoulders_top': PatternCache(),
                'head_shoulders_bottom': PatternCache()
            }
        
        current_time = datetime.now()
        
        # A点区间：第13-34根K线
        a_range_klines = klines[-34:-13]
        # D点区间：第35-55根K线（用于头肩形态）
        d_range_klines = klines[-55:-35]
        
        if len(a_range_klines) < 20 or len(d_range_klines) < 18:
            return
        
        # 更新双顶缓存
        cache = self.pattern_cache[cache_key]['double_top']
        if not cache.a_point or self._is_point_expired(cache.a_point, klines):
            # 找A点区间最高点
            max_high = float('-inf')
            max_high_timestamp = None
            for kline in a_range_klines:
                if float(kline[2]) > max_high:
                    max_high = float(kline[2])  # 修复：确保价格为浮点数
                    max_high_timestamp = int(kline[0])
            
            cache.a_point = ExtremePoint(max_high_timestamp, max_high, 'high')
            cache.last_update = current_time
        
        # 更新双底缓存
        cache = self.pattern_cache[cache_key]['double_bottom']
        if not cache.a_point or self._is_point_expired(cache.a_point, klines):
            # 找A点区间最低点
            min_low = float('inf')
            min_low_timestamp = None
            for kline in a_range_klines:
                if float(kline[3]) < min_low:
                    min_low = float(kline[3])  # 修复：确保价格为浮点数
                    min_low_timestamp = int(kline[0])
            
            cache.a_point = ExtremePoint(min_low_timestamp, min_low, 'low')
            cache.last_update = current_time
        
        # 更新头肩顶缓存
        cache = self.pattern_cache[cache_key]['head_shoulders_top']
        if not cache.a_point or self._is_point_expired(cache.a_point, klines):
            # 找A点区间最高点
            max_high = float('-inf')
            max_high_timestamp = None
            for kline in a_range_klines:
                if float(kline[2]) > max_high:
                    max_high = float(kline[2])  # 修复：确保价格为浮点数
                    max_high_timestamp = int(kline[0])
            
            cache.a_point = ExtremePoint(max_high_timestamp, max_high, 'high')
            cache.last_update = current_time
        
        if not cache.d_point or self._is_point_expired(cache.d_point, klines):
            # 找D点区间最高点（左肩）
            max_high_d = float('-inf')
            max_high_d_timestamp = None
            for kline in d_range_klines:
                if float(kline[2]) > max_high_d:
                    max_high_d = float(kline[2])  # 修复：确保价格为浮点数
                    max_high_d_timestamp = int(kline[0])
            
            cache.d_point = ExtremePoint(max_high_d_timestamp, max_high_d, 'high')
        
        # 更新头肩底缓存
        cache = self.pattern_cache[cache_key]['head_shoulders_bottom']
        if not cache.a_point or self._is_point_expired(cache.a_point, klines):
            # 找A点区间最低点
            min_low = float('inf')
            min_low_timestamp = None
            for kline in a_range_klines:
                if float(kline[3]) < min_low:
                    min_low = float(kline[3])  # 修复：确保价格为浮点数
                    min_low_timestamp = int(kline[0])
            
            cache.a_point = ExtremePoint(min_low_timestamp, min_low, 'low')
            cache.last_update = current_time
        
        if not cache.d_point or self._is_point_expired(cache.d_point, klines):
            # 找D点区间最低点（左肩）
            min_low_d = float('inf')
            min_low_d_timestamp = None
            for kline in d_range_klines:
                if float(kline[3]) < min_low_d:
                    min_low_d = float(kline[3])  # 修复：确保价格为浮点数
                    min_low_d_timestamp = int(kline[0])
            
            cache.d_point = ExtremePoint(min_low_d_timestamp, min_low_d, 'low')
    
    def _check_and_update_a_points(self, symbol: str, timeframe: str):
        """检查A点有效性，如果超出34根K线则重新获取"""
        try:
            # 获取最新K线数据
            klines = self._get_klines_data(symbol, timeframe, 55)
            if not klines or len(klines) < 34:
                logger.warning(f"{symbol} K线数据不足，无法检查A点有效性")
                return
            
            cache_key = f"{symbol}_{timeframe}"
            
            # 检查双顶A点
            if cache_key in self.pattern_cache:
                cache = self.pattern_cache[cache_key]
                
                # 检查双顶A点是否过期
                if cache.a_point and cache.a_point.point_type == 'high':
                    if self._is_point_expired(cache.a_point, klines):
                        logger.info(f"{symbol} 双顶A点已过期，重新获取")
                        # 重新获取A点
                        extreme_points = self._detect_extreme_points(klines)
                        high_points = [p for p in extreme_points if p.point_type == 'high']
                        if high_points:
                            # 选择最近的高点作为新的A点
                            new_a_point = high_points[-1]
                            cache.a_point = new_a_point
                            cache.last_update = datetime.now()
                            logger.info(f"{symbol} 双顶A点已更新: {new_a_point.price} @ {new_a_point.timestamp}")
                
                # 检查双底A点是否过期
                if cache.a_point and cache.a_point.point_type == 'low':
                    if self._is_point_expired(cache.a_point, klines):
                        logger.info(f"{symbol} 双底A点已过期，重新获取")
                        # 重新获取A点
                        extreme_points = self._detect_extreme_points(klines)
                        low_points = [p for p in extreme_points if p.point_type == 'low']
                        if low_points:
                            # 选择最近的低点作为新的A点
                            new_a_point = low_points[-1]
                            cache.a_point = new_a_point
                            cache.last_update = datetime.now()
                            logger.info(f"{symbol} 双底A点已更新: {new_a_point.price} @ {new_a_point.timestamp}")
            
        except Exception as e:
            logger.error(f"检查A点有效性失败 {symbol}: {str(e)}")

    def _is_point_expired(self, point: ExtremePoint, klines: List[List]) -> bool:
        """检查缓存点是否过期（超出有效范围）"""
        if not point:
            return True
        
        # 检查点是否还在K线数据的有效范围内
        for i, kline in enumerate(klines):
            # 修复类型转换：确保时间戳比较时类型一致
            if int(kline[0]) == point.timestamp:
                position_from_right = len(klines) - 1 - i
                # A点应该在第13-34根范围内
                if 13 <= position_from_right <= 34:
                    return False
                # D点应该在第35-55根范围内  
                elif 35 <= position_from_right <= 55:
                    return False
        
        return True
    
    def _calculate_atr(self, klines: List[List], period: int = 14) -> float:
        """计算14周期ATR（使用指数移动平均）"""
        if len(klines) < period + 1:
            return 0.0
        
        true_ranges = []
        
        for i in range(1, len(klines)):
            current = klines[i]
            previous = klines[i - 1]
            
            high = float(current[2])      # 确保为浮点数
            low = float(current[3])       # 确保为浮点数
            prev_close = float(previous[4])  # 确保为浮点数
            
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            
            true_range = max(tr1, tr2, tr3)
            true_ranges.append(true_range)
        
        # 计算指数移动平均ATR
        if len(true_ranges) >= period:
            multiplier = 2.0 / (period + 1)
            atr = true_ranges[0]  # 初始值
            
            for tr in true_ranges[1:period]:
                atr = (tr * multiplier) + (atr * (1 - multiplier))
            
            return atr
        
        return 0.0
    
    def _get_cached_atr(self, symbol: str, timeframe: str, klines: List[List]) -> float:
        """获取缓存的ATR值"""
        # 先检查缓存
        cached_atr = self.data_cache.get_atr(symbol, timeframe)
        if cached_atr:
            return cached_atr
        
        # 计算新的ATR
        atr_klines = klines[:-1]  # 使用前一根收盘K线
        atr = self._calculate_atr(atr_klines)
        
        # 缓存结果
        if atr > 0:
            self.data_cache.set_atr(symbol, timeframe, atr)
        
        return atr
    
    def _find_point_between(self, klines: List[List], point_a: ExtremePoint, point_b: ExtremePoint, find_type: str) -> Optional[ExtremePoint]:
        """在两点之间查找极值点"""
        start_time = min(point_a.timestamp, point_b.timestamp)
        end_time = max(point_a.timestamp, point_b.timestamp)
        
        extreme_price = float('inf') if find_type == 'low' else float('-inf')
        extreme_timestamp = None
        
        for kline in klines:
            kline_time = int(kline[0])
            if start_time < kline_time < end_time:
                price = float(kline[3]) if find_type == 'low' else float(kline[2])  # low or high，确保为浮点数
                
                if find_type == 'low' and price < extreme_price:
                    extreme_price = price
                    extreme_timestamp = kline_time
                elif find_type == 'high' and price > extreme_price:
                    extreme_price = price
                    extreme_timestamp = kline_time
        
        if extreme_timestamp:
            return ExtremePoint(extreme_timestamp, extreme_price, find_type)
        
        return None
    
    def _detect_double_top(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """检测双顶形态"""
        cache_key = f"{symbol}_{timeframe}"
        
        if cache_key not in self.pattern_cache:
            return None
        
        cache = self.pattern_cache[cache_key]['double_top']
        if not cache.a_point:
            logger.info(f"正在获取{symbol}的A_top点")
            return None
        
        # A_top点：缓存的第一个顶部
        a_top = cache.a_point
        a_time = datetime.fromtimestamp(a_top.timestamp/1000).strftime('%Y年%m月%d日%H:%M')
        logger.info(f"已获取A_top点：最高价为{a_top.price:.2f}，时间戳为{a_time}")
        logger.info(f"A_top点已存入缓存中，等待和B_top点进行比较")
        
        # 获取最新K线数据
        klines = self._get_klines_data(symbol, timeframe, 55)
        if not klines or len(klines) < 35:
            return None
        
        # B_top点：最新收盘K线的最高价
        latest_kline = klines[-1]
        b_top = ExtremePoint(
            timestamp=int(latest_kline[0]),
            price=float(latest_kline[2]),  # 最高价
            point_type='high'
        )
        
        b_time = datetime.fromtimestamp(b_top.timestamp/1000).strftime('%Y年%m月%d日%H点')
        logger.info(f"现在获取{symbol}最新收盘K线B_top点")
        logger.info(f"B_top点时间戳为{b_time}，最高价为{b_top.price:.2f}")
        
        # 获取ATR
        logger.info(f"正在计算B_top点对应的ATR")
        atr = self._get_cached_atr(symbol, timeframe, klines)
        if atr <= 0:
            return None
        
        logger.info(f"ATR为{atr:.2f}")
        
        # 验证A_top与B_top差值 ≤ 0.8ATR
        ab_diff = abs(a_top.price - b_top.price)
        atr_threshold = 0.8 * atr
        logger.info(f"A_top与B_top的绝对距离为{ab_diff:.2f}，{'小于等于' if ab_diff <= atr_threshold else '大于'}0.8*ATR({atr_threshold:.2f})，B_top点{'有效' if ab_diff <= atr_threshold else '无效'}")
        
        if ab_diff > atr_threshold:
            return None
        
        # 查找C_bottom点（A_top与B_top间最低点）
        logger.info(f"现在获取A_top与B_top之间的C_bottom点")
        c_bottom = self._find_point_between(klines, a_top, b_top, 'low')
        if not c_bottom:
            logger.info(f"未找到C_bottom点，双顶检测失败")
            return None
        
        c_time = datetime.fromtimestamp(c_bottom.timestamp/1000).strftime('%Y年%m月%d日%H点')
        logger.info(f"C_bottom点位{c_bottom.price:.2f}，时间戳为{c_time}")
        
        # 验证max(A_top,B_top) - C_bottom ≥ 2.3ATR
        max_ab = max(a_top.price, b_top.price)
        c_diff = abs(max_ab - c_bottom.price)
        atr_c_threshold = 2.3 * atr
        logger.info(f"现在计算C_bottom点与max[A_top,B_top]的绝对距离，绝对距离为{c_diff:.2f}")
        logger.info(f"{'大于等于' if c_diff >= atr_c_threshold else '小于'}2.3*ATR({atr_c_threshold:.2f})，{'符合' if c_diff >= atr_c_threshold else '不符合'}双顶形态")
        
        if c_diff < atr_c_threshold:
            return None
        
        logger.info(f"✓ 检测到双顶形态: {symbol} {timeframe} [A_top:{a_top.price:.2f}, B_top:{b_top.price:.2f}, C_bottom:{c_bottom.price:.2f}]")
        
        return {
            'pattern_type': 'double_top',
            'symbol': symbol,
            'timeframe': timeframe,
            'point_a': {'timestamp': datetime.fromtimestamp(a_top.timestamp/1000).isoformat(), 'price': a_top.price, 'type': a_top.point_type},
            'point_b': {'timestamp': datetime.fromtimestamp(b_top.timestamp/1000).isoformat(), 'price': b_top.price, 'type': b_top.point_type},
            'point_c': {'timestamp': datetime.fromtimestamp(c_bottom.timestamp/1000).isoformat(), 'price': c_bottom.price, 'type': c_bottom.point_type},
            'atr': atr,
            'quality_score': min(100, (c_diff / (2.3 * atr)) * 100)
        }
    
    def _detect_double_bottom(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """检测双底形态"""
        cache_key = f"{symbol}_{timeframe}"
        
        if cache_key not in self.pattern_cache:
            return None
        
        cache = self.pattern_cache[cache_key]['double_bottom']
        if not cache.a_point:
            return None
        
        # 获取最新K线数据
        klines = self._get_klines_data(symbol, timeframe, 55)
        if not klines or len(klines) < 35:
            return None
        
        # A_bottom点：缓存的第一个底部
        a_bottom = cache.a_point
        a_time = datetime.fromtimestamp(a_bottom.timestamp/1000)
        logger.info(f"正在获取{symbol}的A_bottom点")
        logger.info(f"已获取A_bottom点：最低价为{a_bottom.price}，时间戳为{a_time.strftime('%Y年%m月%d日%H:%M')}")
        logger.info(f"A_bottom点已存入缓存中，等待和B_bottom点进行比较")
        
        # B_bottom点：最新收盘K线的最低价
        latest_kline = klines[-1]
        b_bottom = ExtremePoint(
            timestamp=int(latest_kline[0]),
            price=float(latest_kline[3]),  # 最低价
            point_type='low'
        )
        
        b_time = datetime.fromtimestamp(b_bottom.timestamp/1000)
        logger.info(f"现在获取{symbol}最新收盘K线B_bottom点")
        logger.info(f"B_bottom点时间戳为{b_time.strftime('%Y年%m月%d日%H点')}，最低价为{b_bottom.price}")
        
        # 获取ATR
        atr = self._get_cached_atr(symbol, timeframe, klines)
        if atr <= 0:
            return None
        
        logger.info(f"正在计算B_bottom点对应的ATR，ATR为{atr}")
        
        # 验证A_bottom与B_bottom差值 ≤ 0.8ATR
        ab_diff = abs(a_bottom.price - b_bottom.price)
        atr_threshold = 0.8 * atr
        logger.info(f"A_bottom与B_bottom的绝对距离为{ab_diff}，{'小于等于' if ab_diff <= atr_threshold else '大于'}0.8*ATR({atr_threshold:.2f})，B_bottom点{'有效' if ab_diff <= atr_threshold else '无效'}")
        
        if ab_diff > atr_threshold:
            return None
        
        # 查找C_top点（A_bottom与B_bottom间最高点）
        logger.info(f"现在获取A_bottom与B_bottom之间的C_top点")
        c_top = self._find_point_between(klines, a_bottom, b_bottom, 'high')
        if not c_top:
            logger.info(f"未找到C_top点，双底检测失败")
            return None
        
        c_time = datetime.fromtimestamp(c_top.timestamp/1000)
        logger.info(f"C_top点位{c_top.price}，时间戳为{c_time.strftime('%Y年%m月%d日%H点')}")
        
        # 验证C_top - min(A_bottom,B_bottom) ≥ 2.3ATR
        min_ab = min(a_bottom.price, b_bottom.price)
        c_diff = abs(c_top.price - min_ab)
        atr_c_threshold = 2.3 * atr
        logger.info(f"现在计算C_top点与min[A_bottom,B_bottom]的绝对距离，绝对距离为{c_diff}")
        logger.info(f"{'大于等于' if c_diff >= atr_c_threshold else '小于'}2.3*ATR({atr_c_threshold:.2f})，{'符合' if c_diff >= atr_c_threshold else '不符合'}双底形态")
        
        if c_diff < atr_c_threshold:
            return None
        
        logger.info(f"✓ 检测到双底形态: {symbol} {timeframe} [A_bottom:{a_bottom.price}, B_bottom:{b_bottom.price}, C_top:{c_top.price}]")
        
        return {
            'pattern_type': 'double_bottom',
            'symbol': symbol,
            'timeframe': timeframe,
            'point_a': {'timestamp': datetime.fromtimestamp(a_bottom.timestamp/1000).isoformat(), 'price': a_bottom.price, 'type': a_bottom.point_type},
            'point_b': {'timestamp': datetime.fromtimestamp(b_bottom.timestamp/1000).isoformat(), 'price': b_bottom.price, 'type': b_bottom.point_type},
            'point_c': {'timestamp': datetime.fromtimestamp(c_top.timestamp/1000).isoformat(), 'price': c_top.price, 'type': c_top.point_type},
            'atr': atr,
            'quality_score': min(100, (c_diff / (2.3 * atr)) * 100)
        }
    

    

    
    def _calculate_additional_indicators(self, symbol: str, timeframe: str, pattern_result: Dict) -> Dict:
        """计算额外的技术指标（仅在形态确认后）"""
        # 获取144根K线用于计算EMA
        klines = self._get_klines_data(symbol, timeframe, 144)
        if not klines or len(klines) < 144:
            return {}
        
        # 提取收盘价
        closes = [float(kline[4]) for kline in klines]  # 修复：确保收盘价为浮点数
        
        # 计算EMA
        ema21 = self._calculate_ema(closes, 21)
        ema55 = self._calculate_ema(closes, 55)
        ema144 = self._calculate_ema(closes, 144)
        
        # 判断EMA趋势
        ema_trend = self._determine_ema_trend(ema21, ema55, ema144)
        
        # 分析K线形态
        kline_pattern = self._analyze_kline_pattern(klines)
        
        return {
            'ema21': ema21,
            'ema55': ema55,
            'ema144': ema144,
            'ema_trend': ema_trend,
            'kline_pattern': kline_pattern
        }
    
    def _calculate_ema(self, prices: List[float], period: int) -> float:
        """计算指数移动平均线"""
        if len(prices) < period:
            return 0.0
        
        multiplier = 2 / (period + 1)
        ema = prices[0]
        
        for price in prices[1:]:
            ema = (price * multiplier) + (ema * (1 - multiplier))
        
        return ema
    

    
    def _determine_ema_trend(self, ema21: float, ema55: float, ema144: float) -> str:
        """判断EMA趋势"""
        if not (ema21 and ema55 and ema144):
            return "unknown"
            
        if ema21 > ema55 > ema144:
            return "bullish"  # 多头排列
        elif ema21 < ema55 < ema144:
            return "bearish"  # 空头排列
        else:
            return "neutral"  # 中性/混乱排列
    
    def _calculate_ema_convergence(self, ema21: float, ema55: float, ema144: float, atr: float) -> float:
        """计算EMA聚合度"""
        try:
            if atr <= 0:
                return float('inf')
            
            # 计算EMA之间的最大跨度
            ema_values = [ema21, ema55, ema144]
            span = max(ema_values) - min(ema_values)
            
            # 计算聚合度比率（跨度/ATR）
            convergence_ratio = span / atr
            
            return convergence_ratio
            
        except Exception as e:
            logger.error(f"计算EMA聚合度时出错: {e}")
            return float('inf')
    
    def _calculate_ema_series(self, prices: List[float], period: int) -> List[Optional[float]]:
        """计算EMA序列"""
        try:
            ema_values = []
            multiplier = 2.0 / (period + 1)
            
            for i in range(len(prices)):
                if i < period - 1:
                    ema_values.append(None)
                elif i == period - 1:
                    # 第一个EMA值使用SMA
                    sma = sum(prices[:period]) / period
                    ema_values.append(sma)
                else:
                    # 后续EMA值
                    prev_ema = ema_values[i-1]
                    current_ema = (prices[i] * multiplier) + (prev_ema * (1 - multiplier))
                    ema_values.append(current_ema)
            
            return ema_values
            
        except Exception as e:
            logger.error(f"计算EMA序列时出错: {e}")
            return [None] * len(prices)
    
    def _identify_ema_alignment(self, ema21_values: List[Optional[float]], 
                              ema55_values: List[Optional[float]], 
                              ema144_values: List[Optional[float]], 
                              current_idx: int) -> Optional[Dict]:
        """识别EMA排列状态"""
        try:
            if current_idx < 1:
                return None
            
            # 当前和前一根K线的EMA值
            current_ema21 = ema21_values[current_idx]
            current_ema55 = ema55_values[current_idx]
            current_ema144 = ema144_values[current_idx]
            
            prev_ema21 = ema21_values[current_idx - 1]
            prev_ema55 = ema55_values[current_idx - 1]
            prev_ema144 = ema144_values[current_idx - 1]
            
            if None in [current_ema21, current_ema55, current_ema144, prev_ema21, prev_ema55, prev_ema144]:
                return None
            
            # 判断当前排列
            current_alignment = None
            if current_ema21 > current_ema55 > current_ema144:
                current_alignment = 'bullish'
            elif current_ema21 < current_ema55 < current_ema144:
                current_alignment = 'bearish'
            else:
                current_alignment = 'mixed'
            
            # 判断前一根排列
            prev_alignment = None
            if prev_ema21 > prev_ema55 > prev_ema144:
                prev_alignment = 'bullish'
            elif prev_ema21 < prev_ema55 < prev_ema144:
                prev_alignment = 'bearish'
            else:
                prev_alignment = 'mixed'
            
            # 计算排列强度（EMA之间的相对距离）
            strength = 0.0
            if current_alignment in ['bullish', 'bearish']:
                diff1 = abs(current_ema21 - current_ema55)
                diff2 = abs(current_ema55 - current_ema144)
                avg_price = (current_ema21 + current_ema55 + current_ema144) / 3
                if avg_price > 0:
                    strength = (diff1 + diff2) / (2 * avg_price) * 100
            
            # 判断是否为新的排列
            is_new_alignment = (current_alignment in ['bullish', 'bearish'] and 
                              current_alignment != prev_alignment)
            
            return {
                'alignment': current_alignment,
                'prev_alignment': prev_alignment,
                'is_new_alignment': is_new_alignment,
                'strength': strength
            }
            
        except Exception as e:
            logger.error(f"识别EMA排列时出错: {e}")
            return None
    
    def _detect_ema_trend_signal(self, symbol: str, timeframe: str, convergence_threshold: float = 0.5) -> Optional[Dict]:
        """检测EMA趋势信号，集成更完整的EMA排列检测逻辑"""
        try:
            logger.info(f"开始分析{symbol}的EMA趋势信号")
            
            # 获取足够的K线数据用于计算
            klines = self._get_klines_data(symbol, timeframe, 200)
            if not klines or len(klines) < 200:
                logger.info(f"K线数据不足，需要200根K线，实际获取{len(klines) if klines else 0}根")
                return None
            
            logger.info(f"已获取{len(klines)}根K线数据，开始计算EMA指标")
            
            # 提取收盘价
            closes = [float(kline[4]) for kline in klines]
            
            # 计算EMA指标
            ema21_values = self._calculate_ema_series(closes, 21)
            ema55_values = self._calculate_ema_series(closes, 55)
            ema144_values = self._calculate_ema_series(closes, 144)
            
            logger.info(f"已完成EMA21、EMA55、EMA144的计算")
            
            # 计算ATR
            atr_values = []
            for i in range(len(klines)):
                if i >= 13:
                    segment_klines = klines[max(0, i-13):i+1]
                    atr = self._calculate_atr(segment_klines, 14)
                    atr_values.append(atr)
                else:
                    atr_values.append(None)
            
            logger.info(f"已完成ATR计算")
            
            # 检查数据完整性
            current_idx = len(klines) - 1
            if (current_idx < 144 or 
                ema21_values[current_idx] is None or ema55_values[current_idx] is None or 
                ema144_values[current_idx] is None or atr_values[current_idx] is None):
                logger.info(f"数据完整性检查失败，无法进行EMA排列分析")
                return None
            
            current_time = datetime.fromtimestamp(klines[current_idx][0]/1000)
            logger.info(f"当前K线时间戳为{current_time.strftime('%Y年%m月%d日%H点')}，价格为{closes[current_idx]}")
            logger.info(f"当前EMA21={ema21_values[current_idx]:.2f}, EMA55={ema55_values[current_idx]:.2f}, EMA144={ema144_values[current_idx]:.2f}")
            
            # 识别EMA排列状态
            alignment_result = self._identify_ema_alignment(
                ema21_values, ema55_values, ema144_values, current_idx
            )
            
            if not alignment_result:
                logger.info(f"未识别到有效的EMA排列状态")
                return None
            
            logger.info(f"当前EMA排列状态：{alignment_result['alignment']}，前一根K线排列状态：{alignment_result['prev_alignment']}")
            logger.info(f"是否为新排列：{alignment_result['is_new_alignment']}")
            
            # 仅在识别到新的上升趋势（多头排列）或新的下降趋势（空头排列）时才计算聚合度
            if alignment_result['alignment'] in ['bullish', 'bearish'] and alignment_result['is_new_alignment']:
                logger.info(f"检测到新的{'多头' if alignment_result['alignment'] == 'bullish' else '空头'}排列，开始计算聚合度")
                
                # 计算聚合度
                convergence = self._calculate_ema_convergence(
                    ema21_values[current_idx],
                    ema55_values[current_idx], 
                    ema144_values[current_idx],
                    atr_values[current_idx]
                )
                
                logger.info(f"计算得出聚合度为{convergence:.3f}，阈值为{convergence_threshold}")
                
                # 检查聚合度条件
                if convergence >= convergence_threshold:
                    logger.info(f"聚合度{convergence:.3f}大于等于阈值{convergence_threshold}，不符合趋势信号条件")
                    return None
                
                logger.info(f"聚合度{convergence:.3f}小于阈值{convergence_threshold}，符合趋势信号条件")
                
                # 多头信号：多头排列
                if alignment_result['alignment'] == 'bullish':
                    logger.info(f"✓ 检测到多头EMA趋势信号: {symbol} {timeframe}")
                    return {
                        'type': 'ema_trend',
                        'signal': 'bullish',
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'timestamp': int(klines[current_idx][0]),
                        'price': closes[current_idx],
                        'ema21': ema21_values[current_idx],
                        'ema55': ema55_values[current_idx],
                        'ema144': ema144_values[current_idx],
                        'convergence': convergence,
                        'atr': atr_values[current_idx],
                        'alignment_strength': alignment_result['strength']
                    }
                
                # 空头信号：空头排列
                elif alignment_result['alignment'] == 'bearish':
                    logger.info(f"✓ 检测到空头EMA趋势信号: {symbol} {timeframe}")
                    return {
                        'type': 'ema_trend',
                        'signal': 'bearish',
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'timestamp': int(klines[current_idx][0]),
                        'price': closes[current_idx],
                        'ema21': ema21_values[current_idx],
                        'ema55': ema55_values[current_idx],
                        'ema144': ema144_values[current_idx],
                        'convergence': convergence,
                        'atr': atr_values[current_idx],
                        'alignment_strength': alignment_result['strength']
                    }
            else:
                logger.info(f"当前排列状态为{alignment_result['alignment']}，不是新的多头或空头排列，无需发送信号")
            
            return None
            
        except Exception as e:
            logger.error(f"检测EMA趋势信号时出错: {e}")
            return None
    
    def _analyze_kline_pattern(self, klines: List[List]) -> Dict:
        """分析K线形态"""
        if len(klines) < 3:
            return {'pattern': 'none', 'strength': 0}
        
        # 获取最近3根K线
        current = klines[-1]  # [timestamp, open, high, low, close, volume]
        prev1 = klines[-2]
        prev2 = klines[-3]
        
        patterns = []
        
        # 检测锤子线
        hammer = self._detect_hammer(current)
        if hammer['detected']:
            patterns.append(hammer)
        
        # 检测吞没形态
        engulfing = self._detect_engulfing(prev1, current)
        if engulfing['detected']:
            patterns.append(engulfing)
        
        # 检测十字星
        doji = self._detect_doji(current)
        if doji['detected']:
            patterns.append(doji)
        
        # 检测三根K线形态
        three_candle = self._detect_three_candle_patterns(prev2, prev1, current)
        if three_candle['detected']:
            patterns.append(three_candle)
        
        if not patterns:
            return {'pattern': 'none', 'strength': 0}
        
        # 返回最强的形态
        strongest = max(patterns, key=lambda x: x['strength'])
        return strongest
    
    def _detect_hammer(self, candle: List) -> Dict:
        """检测锤子线形态"""
        open_price, high, low, close = float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4])
        
        body = abs(close - open_price)
        upper_shadow = high - max(close, open_price)
        lower_shadow = min(close, open_price) - low
        total_range = high - low
        
        if total_range == 0:
            return {'detected': False, 'pattern': 'none', 'strength': 0}
        
        # 锤子线条件：下影线长度至少是实体的2倍，上影线很短
        if (lower_shadow >= body * 2 and 
            upper_shadow <= body * 0.1 and 
            body > 0):
            
            pattern_type = 'hammer_bullish' if close > open_price else 'hammer_bearish'
            strength = min(90, (lower_shadow / total_range) * 100)
            
            return {
                'detected': True,
                'pattern': pattern_type,
                'strength': strength,
                'description': '锤子线形态'
            }
        
        return {'detected': False, 'pattern': 'none', 'strength': 0}
    
    def _detect_engulfing(self, prev_candle: List, current_candle: List) -> Dict:
        """检测吞没形态"""
        prev_open, prev_close = float(prev_candle[1]), float(prev_candle[4])
        curr_open, curr_close = float(current_candle[1]), float(current_candle[4])
        
        prev_body = abs(prev_close - prev_open)
        curr_body = abs(curr_close - curr_open)
        
        if prev_body == 0 or curr_body == 0:
            return {'detected': False, 'pattern': 'none', 'strength': 0}
        
        # 看涨吞没：前一根阴线，当前阳线完全吞没前一根
        if (prev_close < prev_open and  # 前一根是阴线
            curr_close > curr_open and   # 当前是阳线
            curr_open < prev_close and   # 当前开盘价低于前一根收盘价
            curr_close > prev_open):     # 当前收盘价高于前一根开盘价
            
            strength = min(95, (curr_body / prev_body) * 50 + 45)
            return {
                'detected': True,
                'pattern': 'bullish_engulfing',
                'strength': strength,
                'description': '看涨吞没形态'
            }
        
        # 看跌吞没：前一根阳线，当前阴线完全吞没前一根
        if (prev_close > prev_open and  # 前一根是阳线
            curr_close < curr_open and   # 当前是阴线
            curr_open > prev_close and   # 当前开盘价高于前一根收盘价
            curr_close < prev_open):     # 当前收盘价低于前一根开盘价
            
            strength = min(95, (curr_body / prev_body) * 50 + 45)
            return {
                'detected': True,
                'pattern': 'bearish_engulfing',
                'strength': strength,
                'description': '看跌吞没形态'
            }
        
        return {'detected': False, 'pattern': 'none', 'strength': 0}
    
    def _detect_doji(self, candle: List) -> Dict:
        """检测十字星形态"""
        open_price, high, low, close = float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4])
        
        body = abs(close - open_price)
        total_range = high - low
        
        if total_range == 0:
            return {'detected': False, 'pattern': 'none', 'strength': 0}
        
        # 十字星条件：实体很小（小于总范围的5%）
        if body <= total_range * 0.05:
            strength = max(60, 100 - (body / total_range) * 100)
            return {
                'detected': True,
                'pattern': 'doji',
                'strength': strength,
                'description': '十字星形态'
            }
        
        return {'detected': False, 'pattern': 'none', 'strength': 0}
    
    def _detect_three_candle_patterns(self, candle1: List, candle2: List, candle3: List) -> Dict:
        """检测三根K线形态"""
        # 获取收盘价
        close1, close2, close3 = candle1[4], candle2[4], candle3[4]
        open1, open2, open3 = candle1[1], candle2[1], candle3[1]
        
        # 检测三只乌鸦（三根连续阴线，每根都比前一根低）
        if (close1 < open1 and close2 < open2 and close3 < open3 and  # 三根都是阴线
            close2 < close1 and close3 < close2):  # 逐步走低
            return {
                'detected': True,
                'pattern': 'three_black_crows',
                'strength': 85,
                'description': '三只乌鸦形态'
            }
        
        # 检测三个白兵（三根连续阳线，每根都比前一根高）
        if (close1 > open1 and close2 > open2 and close3 > open3 and  # 三根都是阳线
            close2 > close1 and close3 > close2):  # 逐步走高
            return {
                'detected': True,
                'pattern': 'three_white_soldiers',
                'strength': 85,
                'description': '三个白兵形态'
            }
        
        return {'detected': False, 'pattern': 'none', 'strength': 0}
    
    def _find_kline_index_by_timestamp(self, klines: List[List], timestamp: int) -> int:
        """根据时间戳找到K线在数组中的索引"""
        for i, kline in enumerate(klines):
            # 修复类型转换：确保时间戳比较时类型一致
            if int(kline[0]) == timestamp:
                return i
        return -1
    
    def _analyze_pattern(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """分析单个交易对的形态"""
        try:
            logger.debug(f"开始分析形态: {symbol} {timeframe}")
            
            # 初始化或更新形态缓存
            cache_key = f"{symbol}_{timeframe}"
            if cache_key not in self.pattern_cache:
                logger.debug(f"初始化形态缓存: {symbol} {timeframe}")
                self._initialize_pattern_cache(symbol, timeframe)
            else:
                logger.debug(f"更新形态缓存: {symbol} {timeframe}")
                self._update_pattern_cache(symbol, timeframe)
            
            detected_patterns = []
            
            # 检测双顶/双底形态
            pattern_types = ['双顶', '双底']
            patterns = [
                self._detect_double_top(symbol, timeframe),
                self._detect_double_bottom(symbol, timeframe)
            ]
            
            # 收集所有检测到的双顶/双底形态
            for i, pattern in enumerate(patterns):
                if pattern:
                    pattern_name = pattern_types[i]
                    quality_score = pattern.get('quality_score', 0)
                    logger.info(f"🎯 检测到形态: {symbol} {timeframe} - {pattern_name} (质量分数: {quality_score:.2f})")
                    
                    # 计算额外指标
                    logger.debug(f"计算技术指标: {symbol} {timeframe}")
                    additional_indicators = self._calculate_additional_indicators(symbol, timeframe, pattern)
                    pattern.update(additional_indicators)
                    
                    # 记录关键点信息
                    if 'point_a' in pattern and 'point_b' in pattern and 'point_c' in pattern:
                        logger.debug(f"关键点 - A: {pattern['point_a']['price']:.4f}, B: {pattern['point_b']['price']:.4f}, C: {pattern['point_c']['price']:.4f}")
                    
                    detected_patterns.append(pattern)
            
            # 检测EMA趋势信号
            ema_signal = self._detect_ema_trend_signal(symbol, timeframe)
            if ema_signal:
                logger.info(f"🎯 检测到EMA趋势信号: {symbol} {timeframe} - {ema_signal['signal']} (聚合度: {ema_signal['convergence']:.2f})")
                detected_patterns.append(ema_signal)
            
            # 返回优先级最高的形态（双顶/双底优先于EMA信号）
            if detected_patterns:
                # 双顶/双底形态优先级更高
                for pattern in detected_patterns:
                    if pattern.get('pattern_type') in ['double_top', 'double_bottom']:
                        return pattern
                # 如果没有双顶/双底，返回EMA信号
                return detected_patterns[0]
            
            logger.debug(f"未检测到形态: {symbol} {timeframe}")
            return None
            
        except Exception as e:
            logger.error(f"❌ 分析 {symbol} {timeframe} 形态失败: {str(e)}")
            raise
    
    def _should_send_signal(self, symbol: str, timeframe: str, pattern_type: str) -> bool:
        """检查是否应该发送信号（防止重复）"""
        current_time = time.time()
        
        # 检查全局webhook发送间隔（10秒）
        if current_time - self.last_webhook_time < 10:
            return False
        
        return True
    
    def _create_chart(self, symbol: str, timeframe: str, pattern_data: Dict) -> Optional[str]:
        """创建K线图表，包含最新收盘K线及左边55根K线、3条EMA、成交量、ABC三个点"""
        try:
            # 获取K线数据（56根K线：最新1根+左边55根）
            klines = self._get_klines_data(symbol, timeframe, limit=56)
            if not klines or len(klines) < 56:
                logger.error(f"K线数据不足，无法绘制图表 - {symbol} {timeframe}")
                return None
            
            # 转换为DataFrame格式
            import pandas as pd
            df_data = []
            for kline in klines:
                df_data.append({
                    'timestamp': pd.to_datetime(int(kline[0]), unit='ms'),
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                })
            
            df = pd.DataFrame(df_data)
            df.set_index('timestamp', inplace=True)
            
            # 计算EMA
            df['ema21'] = df['close'].ewm(span=21).mean()
            df['ema55'] = df['close'].ewm(span=55).mean()
            df['ema144'] = df['close'].ewm(span=144).mean()
            
            # 创建图表
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), 
                                         gridspec_kw={'height_ratios': [3, 1]})
            
            # 绘制K线图
            from matplotlib.patches import Rectangle
            from matplotlib.lines import Line2D
            
            for i, (idx, row) in enumerate(df.iterrows()):
                # K线颜色
                color = 'red' if row['close'] >= row['open'] else 'green'
                
                # 绘制K线实体
                body_height = abs(row['close'] - row['open'])
                body_bottom = min(row['close'], row['open'])
                rect = Rectangle((i - 0.3, body_bottom), 0.6, body_height, 
                               facecolor=color, alpha=0.8)
                ax1.add_patch(rect)
                
                # 绘制上下影线
                ax1.plot([i, i], [row['low'], row['high']], color=color, linewidth=1)
            
            # 绘制EMA线
            ax1.plot(range(len(df)), df['ema21'], color='blue', linewidth=2, label='EMA21', alpha=0.8)
            ax1.plot(range(len(df)), df['ema55'], color='orange', linewidth=2, label='EMA55', alpha=0.8)
            ax1.plot(range(len(df)), df['ema144'], color='purple', linewidth=2, label='EMA144', alpha=0.8)
            
            # 标记ABC点
            if pattern_data.get('pattern_type') in ['双顶', '双底']:
                # 获取ABC点信息
                a_point = pattern_data.get('a_point')
                b_point = pattern_data.get('b_point')
                c_point = pattern_data.get('c_point')
                
                if a_point and b_point and c_point:
                    # 找到对应的K线索引
                    a_idx = self._find_kline_index_by_timestamp(klines, a_point['timestamp'])
                    b_idx = self._find_kline_index_by_timestamp(klines, b_point['timestamp'])
                    c_idx = self._find_kline_index_by_timestamp(klines, c_point['timestamp'])
                    
                    if a_idx >= 0:
                        ax1.scatter(a_idx, a_point['price'], color='red', s=100, marker='o', 
                                  label='A点', zorder=5)
                        ax1.annotate('A', (a_idx, a_point['price']), xytext=(5, 5), 
                                   textcoords='offset points', fontsize=12, fontweight='bold')
                    
                    if b_idx >= 0:
                        ax1.scatter(b_idx, b_point['price'], color='blue', s=100, marker='o', 
                                  label='B点', zorder=5)
                        ax1.annotate('B', (b_idx, b_point['price']), xytext=(5, 5), 
                                   textcoords='offset points', fontsize=12, fontweight='bold')
                    
                    if c_idx >= 0:
                        ax1.scatter(c_idx, c_point['price'], color='green', s=100, marker='o', 
                                  label='C点', zorder=5)
                        ax1.annotate('C', (c_idx, c_point['price']), xytext=(5, 5), 
                                   textcoords='offset points', fontsize=12, fontweight='bold')
            
            # 设置主图标题和标签
            ax1.set_title(f'{symbol} {timeframe} - {pattern_data.get("pattern_type", "信号")}', 
                         fontsize=16, fontweight='bold')
            ax1.set_ylabel('价格', fontsize=12)
            ax1.legend(loc='upper left')
            ax1.grid(True, alpha=0.3)
            
            # 绘制成交量
            colors = ['red' if df.iloc[i]['close'] >= df.iloc[i]['open'] else 'green' 
                     for i in range(len(df))]
            ax2.bar(range(len(df)), df['volume'], color=colors, alpha=0.7)
            ax2.set_ylabel('成交量', fontsize=12)
            ax2.set_xlabel('时间', fontsize=12)
            ax2.grid(True, alpha=0.3)
            
            # 设置X轴标签（显示时间）
            x_ticks = range(0, len(df), max(1, len(df)//10))  # 显示10个时间点
            x_labels = [df.index[i].strftime('%m-%d %H:%M') for i in x_ticks]
            ax1.set_xticks(x_ticks)
            ax1.set_xticklabels(x_labels, rotation=45)
            ax2.set_xticks(x_ticks)
            ax2.set_xticklabels(x_labels, rotation=45)
            
            plt.tight_layout()
            
            # 保存图表到内存
            img_buffer = io.BytesIO()
            plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
            img_buffer.seek(0)
            
            # 转换为base64
            img_base64 = base64.b64encode(img_buffer.getvalue()).decode()
            
            plt.close(fig)  # 释放内存
            
            logger.info(f"✅ 图表创建成功 - {symbol} {timeframe}")
            return img_base64
            
        except Exception as e:
            logger.error(f"❌ 图表创建失败 - {symbol} {timeframe}: {str(e)}")
            return None
    
    def _send_telegram_message(self, pattern_data: Dict, chart_base64: Optional[str] = None) -> bool:
        """发送消息和图片到Telegram"""
        try:
            # 检查Bot实例是否存在
            if not self.telegram_bot:
                logger.warning("Telegram Bot未配置，跳过发送")
                return False
                
            # 检查发送间隔
            current_time = time.time()
            if current_time - self.last_telegram_time < self.telegram_send_interval:
                logger.info(f"Telegram发送间隔未到，跳过发送")
                return False
            
            symbol = pattern_data.get('symbol', 'UNKNOWN')
            pattern_type = pattern_data.get('pattern_type', 'UNKNOWN')
            timeframe = pattern_data.get('timeframe', 'UNKNOWN')
            price = pattern_data.get('current_price', 0)
            
            # 构建消息文本
            message_lines = [
                f"🚨 {pattern_type} 信号检测",
                f"📊 交易对: {symbol}",
                f"⏰ 时间框架: {timeframe}",
                f"💰 当前价格: {price}",
                f"🕐 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            ]
            
            # 添加特定信号信息
            if pattern_type in ['双顶', '双底']:
                if 'a_point' in pattern_data:
                    message_lines.append(f"🔴 A点价格: {pattern_data['a_point']['price']}")
                if 'b_point' in pattern_data:
                    message_lines.append(f"🔵 B点价格: {pattern_data['b_point']['price']}")
                if 'c_point' in pattern_data:
                    message_lines.append(f"🟢 C点价格: {pattern_data['c_point']['price']}")
                if 'quality_score' in pattern_data:
                    message_lines.append(f"⭐ 质量分数: {pattern_data['quality_score']:.2f}")
            elif 'EMA' in pattern_type or pattern_type in ['多头信号', '空头信号']:
                if 'ema21' in pattern_data:
                    message_lines.append(f"📈 EMA21: {pattern_data['ema21']:.4f}")
                if 'ema55' in pattern_data:
                    message_lines.append(f"📈 EMA55: {pattern_data['ema55']:.4f}")
                if 'ema144' in pattern_data:
                    message_lines.append(f"📈 EMA144: {pattern_data['ema144']:.4f}")
                if 'convergence' in pattern_data:
                    message_lines.append(f"🔄 聚合度: {pattern_data['convergence']:.2f}")
            
            message_text = '\n'.join(message_lines)
            
            # 发送文本消息
            self.telegram_bot.send_message(
                chat_id=self.telegram_channel_id,
                text=message_text,
                parse_mode='HTML'
            )
            
            # 如果有图表，发送图片
            if chart_base64:
                img_data = base64.b64decode(chart_base64)
                img_buffer = io.BytesIO(img_data)
                img_buffer.name = f'{symbol}_{timeframe}_{pattern_type}.png'
                
                self.telegram_bot.send_photo(
                    chat_id=self.telegram_channel_id,
                    photo=img_buffer,
                    caption=f"{symbol} {pattern_type} 图表"
                )
            
            # 更新发送时间
            self.last_telegram_time = current_time
            
            logger.info(f"✅ Telegram消息发送成功 - {symbol} {pattern_type}")
            return True
            
        except TelegramError as e:
            logger.error(f"❌ Telegram发送失败 - {symbol} {pattern_type}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"❌ Telegram发送异常 - {symbol} {pattern_type}: {str(e)}")
            return False
    
    def _send_webhook(self, pattern_data: Dict) -> bool:
        """发送信号到Telegram（替代原有webhook功能）"""
        try:
            symbol = pattern_data.get('symbol', 'UNKNOWN')
            pattern_type = pattern_data.get('pattern_type', 'UNKNOWN')
            timeframe = pattern_data.get('timeframe', 'UNKNOWN')
            
            logger.info(f"准备发送Telegram消息 - {symbol} {pattern_type} ({timeframe})")
            
            # 创建图表
            chart_base64 = self._create_chart(symbol, timeframe, pattern_data)
            if not chart_base64:
                logger.warning(f"图表创建失败，仅发送文本消息 - {symbol} {pattern_type}")
            
            # 发送到Telegram
            success = self._send_telegram_message(pattern_data, chart_base64)
            
            if success:
                logger.info(f"✅ Telegram消息发送成功 - {symbol} {pattern_type}")
            else:
                logger.error(f"❌ Telegram消息发送失败 - {symbol} {pattern_type}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 发送Telegram消息异常: {str(e)}")
            return False
    
    def _get_signal_price(self, pattern_data: Dict) -> float:
        """根据信号类型获取当前价格"""
        pattern_type = pattern_data.get('pattern_type', '')
        
        # 双顶/双底形态：优先使用C点价格，其次B点
        if pattern_type in ['双顶', '双底']:
            current_price = pattern_data.get('point_c', {}).get('price', 0)
            if not current_price:
                current_price = pattern_data.get('point_b', {}).get('price', 0)
            return current_price
        
        # EMA趋势信号：使用当前价格
        elif 'EMA' in pattern_type or pattern_type in ['多头信号', '空头信号']:
            return pattern_data.get('current_price', 0)
        
        # 默认处理
        return pattern_data.get('price', 0)
    
    def _add_double_pattern_data(self, webhook_data: Dict, pattern_data: Dict):
        """添加双顶/双底形态特定数据"""
        # 添加形态关键点数据
        webhook_data['pattern_points'] = {
            'A': pattern_data.get('point_a', {}),
            'B': pattern_data.get('point_b', {}),
            'C': pattern_data.get('point_c', {})
        }
        
        # 添加B点收盘价
        if 'point_b' in pattern_data:
            point_b = pattern_data['point_b']
            webhook_data['b_point_close_price'] = point_b.get('price', 0)
        else:
            webhook_data['b_point_close_price'] = None
            
        # 添加形态识别信息
        webhook_data['pattern_identification'] = {
            'pattern_id': f"{pattern_data['symbol']}_{pattern_data['pattern_type']}_{pattern_data['timeframe']}_{int(datetime.now().timestamp())}",
            'detection_time': datetime.now().isoformat()
        }
    
    def _add_ema_signal_data(self, webhook_data: Dict, pattern_data: Dict):
        """添加EMA趋势信号特定数据"""
        # 添加聚合度数值
        webhook_data['convergence'] = pattern_data.get('convergence', 0)
        
        # 添加EMA值
        webhook_data['ema_values'] = {
            'ema21': pattern_data.get('ema21', 0),
            'ema55': pattern_data.get('ema55', 0),
            'ema144': pattern_data.get('ema144', 0)
        }
        
        # 添加信号识别信息
        webhook_data['signal_identification'] = {
            'convergence_ratio': pattern_data.get('convergence', 0),
            'alignment_state': pattern_data.get('alignment_state', 'unknown')
        }
    
    def _add_kline_pattern_data(self, webhook_data: Dict, pattern_data: Dict):
        """添加K线形态信息"""
        kline_pattern = pattern_data.get('kline_pattern', {})
        webhook_data['kline_pattern'] = {
            'pattern': kline_pattern.get('pattern', 'none'),
            'strength': kline_pattern.get('strength', 0),
            'description': kline_pattern.get('description', '无形态')
        }
    
    def _analyze_all_pairs_concurrent(self, timeframe: str):
        """并发分析所有交易对"""
        start_time = datetime.now()
        logger.info(f"========== 开始并发分析 {timeframe} 时间粒度 ========== [{start_time.strftime('%Y-%m-%d %H:%M:%S')}]")
        
        total_pairs = len(self.monitored_pairs)
        success_count = 0
        failed_pairs = []
        patterns_found = []
        webhook_failures = []
        
        # 使用线程池并发处理
        with ThreadPoolExecutor(max_workers=self.max_concurrent_analysis) as executor:
            # 提交所有任务
            future_to_symbol = {
                executor.submit(self._analyze_pattern, symbol, timeframe): symbol 
                for symbol in self.monitored_pairs
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_symbol, timeout=self.analysis_timeout * 2):
                symbol = future_to_symbol[future]
                
                try:
                    pattern_result = future.result(timeout=self.analysis_timeout)
                    
                    if pattern_result:
                        pattern_type = pattern_result['pattern_type']
                        patterns_found.append(f"{symbol}_{pattern_type}")
                        logger.info(f"✓ {symbol} 检测到形态: {pattern_type}")
                        
                        # 检查是否需要发送信号
                        if self._should_send_signal(symbol, timeframe, pattern_type):
                            try:
                                webhook_success = self._send_webhook(pattern_result)
                                if webhook_success:
                                    # 更新全局webhook时间
                                    current_time = time.time()
                                    self.last_webhook_time = current_time
                                    logger.info(f"Webhook发送成功，下次发送需等待10秒: {symbol} {pattern_type}")
                                else:
                                    webhook_failures.append(f"{symbol}_{pattern_type}")
                            except Exception as e:
                                logger.error(f"Webhook发送异常: {symbol} - {str(e)}")
                                webhook_failures.append(f"{symbol}_{pattern_type}")
                    
                    success_count += 1
                    
                except Exception as e:
                    logger.error(f"分析 {symbol} 失败: {str(e)}")
                    failed_pairs.append(symbol)
        
        # 分析完成统计
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        failed_count = len(failed_pairs)
        success_rate = (success_count / total_pairs) * 100 if total_pairs > 0 else 0
        
        logger.info(f"========== {timeframe} 并发分析完成 ==========")
        logger.info(f"总耗时: {total_duration:.2f}秒 (并发处理)")
        logger.info(f"成功率: {success_count}/{total_pairs} ({success_rate:.1f}%)")
        logger.info(f"形态发现: {len(patterns_found)}个")
        
        if failed_pairs:
            logger.warning(f"失败交易对: {', '.join(failed_pairs)}")
        
        if webhook_failures:
            logger.warning(f"Webhook失败: {', '.join(webhook_failures)}")
        
        return {
            'total': total_pairs,
            'success': success_count,
            'failed': failed_count,
            'failed_pairs': failed_pairs,
            'patterns_found': patterns_found,
            'webhook_failures': webhook_failures,
            'success_rate': success_rate,
            'duration': total_duration
        }
    
    def _analyze_all_pairs_sequential(self, timeframe: str):
        """顺序分析所有交易对，每个代币API调用间隔3秒"""
        start_time = datetime.now()
        logger.info(f"========== 开始顺序分析 {timeframe} 时间粒度 (3秒间隔) ========== [{start_time.strftime('%Y-%m-%d %H:%M:%S')}]")
        
        total_pairs = len(self.monitored_pairs)
        success_count = 0
        failed_pairs = []
        patterns_found = []
        webhook_failures = []
        
        # 顺序处理每个交易对
        for i, symbol in enumerate(self.monitored_pairs):
            try:
                logger.info(f"正在分析 {symbol} ({i+1}/{total_pairs})")
                
                # 在分析前检查A点有效性并更新缓存
                self._check_and_update_a_points(symbol, timeframe)
                
                # 分析交易对
                pattern_result = self._analyze_pattern(symbol, timeframe)
                
                if pattern_result:
                    pattern_type = pattern_result['pattern_type']
                    patterns_found.append(f"{symbol}_{pattern_type}")
                    logger.info(f"✓ {symbol} 检测到形态: {pattern_type}")
                    
                    # 检查是否需要发送信号
                    if self._should_send_signal(symbol, timeframe, pattern_type):
                        try:
                            webhook_success = self._send_webhook(pattern_result)
                            if webhook_success:
                                # 更新全局webhook时间
                                current_time = time.time()
                                self.last_webhook_time = current_time
                                logger.info(f"Webhook发送成功，下次发送需等待10秒: {symbol} {pattern_type}")
                            else:
                                webhook_failures.append(f"{symbol}_{pattern_type}")
                        except Exception as e:
                            logger.error(f"Webhook发送异常: {symbol} - {str(e)}")
                            webhook_failures.append(f"{symbol}_{pattern_type}")
                
                success_count += 1
                
                # 在处理下一个代币前等待3秒（除了最后一个）
                if i < total_pairs - 1:
                    logger.info(f"等待3秒后处理下一个代币...")
                    time.sleep(3)
                
            except Exception as e:
                logger.error(f"分析 {symbol} 失败: {str(e)}")
                failed_pairs.append(symbol)
                
                # 即使失败也要等待3秒（除了最后一个）
                if i < total_pairs - 1:
                    logger.info(f"等待3秒后处理下一个代币...")
                    time.sleep(3)
        
        # 分析完成统计
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        failed_count = len(failed_pairs)
        success_rate = (success_count / total_pairs) * 100 if total_pairs > 0 else 0
        
        logger.info(f"========== {timeframe} 顺序分析完成 ==========")
        logger.info(f"总耗时: {total_duration:.2f}秒 (顺序处理，3秒间隔)")
        logger.info(f"成功率: {success_count}/{total_pairs} ({success_rate:.1f}%)")
        logger.info(f"形态发现: {len(patterns_found)}个")
        
        if failed_pairs:
            logger.warning(f"失败交易对: {', '.join(failed_pairs)}")
        
        if webhook_failures:
            logger.warning(f"Webhook失败: {', '.join(webhook_failures)}")
        
        return {
            'total': total_pairs,
            'success': success_count,
            'failed': failed_count,
            'failed_pairs': failed_pairs,
            'patterns_found': patterns_found,
            'webhook_failures': webhook_failures,
            'success_rate': success_rate,
            'duration': total_duration
        }
    
    def _monitor_timeframe(self, timeframe: str):
        """监控指定时间粒度"""
        logger.info(f"开始监控 {timeframe} 时间粒度")
        
        consecutive_errors = 0
        max_consecutive_errors = 8  # 增加容错性
        error_backoff_delays = [30, 60, 120, 300]
        cycle_count = 0  # 添加周期计数
        
        # 更新线程健康状态
        if timeframe in self.thread_health:
            self.thread_health[timeframe]['last_activity'] = datetime.now()
        
        logger.info(f"[{timeframe}] 监控线程已启动，开始运行循环")
        
        while self.running:
            try:
                cycle_count += 1
                # 更新线程活动时间
                if timeframe in self.thread_health:
                    self.thread_health[timeframe]['last_activity'] = datetime.now()
                
                current_time = datetime.now()
                should_analyze = False
                
                # 每10个周期记录一次运行状态
                if cycle_count % 10 == 0:
                    logger.info(f"[{timeframe}] 运行状态检查 - 周期: {cycle_count}, 连续错误: {consecutive_errors}, 时间: {current_time.strftime('%H:%M:%S')}")
                
                # 根据时间粒度确定是否应该分析（每小时01秒触发）
                if timeframe == '1h':
                    if current_time.second == 1:
                        should_analyze = True
                        logger.info(f"[{timeframe}] 触发分析 - {current_time.strftime('%H:%M:%S')}")
                
                # 防重复分析检查
                if should_analyze:
                    if timeframe == '1h':
                        analysis_key = f"{timeframe}_{current_time.strftime('%Y%m%d_%H')}"
                    
                    if analysis_key not in self.last_analysis_time:
                        self.last_analysis_time[analysis_key] = current_time
                        logger.info(f"[{timeframe}] 开始执行分析")
                        
                        try:
                            result = self._analyze_all_pairs_sequential(timeframe)
                            logger.info(f"[{timeframe}] 分析成功 - 耗时: {result.get('duration', 0):.2f}秒")
                            
                            consecutive_errors = 0
                            self._update_system_health('healthy')
                            
                            # 更新线程健康状态
                            if timeframe in self.thread_health:
                                self.thread_health[timeframe]['status'] = 'running'
                                self.thread_health[timeframe]['error_count'] = 0
                                
                        except Exception as e:
                            logger.error(f"[{timeframe}] 分析失败: {str(e)}")
                            consecutive_errors += 1
                            self._update_system_health('error', e)
                            
                            if timeframe in self.thread_health:
                                self.thread_health[timeframe]['status'] = 'error'
                                self.thread_health[timeframe]['error_count'] += 1
                
                # 错误恢复逻辑
                if consecutive_errors >= max_consecutive_errors:
                    delay_index = min(consecutive_errors - max_consecutive_errors, len(error_backoff_delays) - 1)
                    recovery_delay = error_backoff_delays[delay_index]
                    logger.warning(f"{timeframe} 连续错误 {consecutive_errors} 次，休眠 {recovery_delay} 秒")
                    time.sleep(recovery_delay)
                
                # 动态调整检查间隔
                check_interval = 30 if consecutive_errors == 0 else 60
                
                # 每100个周期清理一次缓存
                if cycle_count % 100 == 0:
                    logger.info(f"[{timeframe}] 执行定期缓存清理 - 周期: {cycle_count}")
                    try:
                        self.data_cache._cleanup_cache()
                        logger.info(f"[{timeframe}] 缓存清理完成")
                    except Exception as e:
                        logger.error(f"[{timeframe}] 缓存清理失败: {str(e)}")
                
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                logger.info(f"{timeframe} 监控收到停止信号")
                break
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"{timeframe} 监控异常: {str(e)}")
                self._update_system_health('error', e)
                
                if timeframe in self.thread_health:
                    self.thread_health[timeframe]['status'] = 'error'
                    self.thread_health[timeframe]['error_count'] += 1
                    self.thread_health[timeframe]['last_error'] = str(e)
                
                time.sleep(60)
        
        # 线程结束时更新状态
        if timeframe in self.thread_health:
            self.thread_health[timeframe]['status'] = 'stopped'
            self.thread_health[timeframe]['last_activity'] = datetime.now()
        
        logger.info(f"{timeframe} 监控线程已停止")
    
    def _health_check_loop(self):
        """健康检查循环"""
        logger.info("启动健康检查循环")
        
        while self.running:
            try:
                self._check_thread_health()
                
                # 内存使用检查
                process = psutil.Process()
                memory_usage = process.memory_info().rss
                
                if memory_usage > self.memory_limit:
                    logger.warning(f"内存使用超限: {memory_usage / 1024 / 1024:.1f}MB")
                    # 清理缓存
                    self.data_cache._cleanup_cache()
                    
                    # 如果内存仍然过高，清空所有缓存
                    if process.memory_info().rss > self.memory_limit:
                        logger.warning("强制清理所有缓存")
                        self.data_cache.clear_all()
                        gc.collect()
                
                time.sleep(self.recovery_config['health_check_interval'])
                
            except Exception as e:
                logger.error(f"健康检查异常: {str(e)}")
                time.sleep(60)
        
        logger.info("健康检查循环结束")
    
    def start_monitoring(self):
        """启动监控系统"""
        logger.info("启动加密货币形态识别监控系统")
        self.running = True
        self._update_system_health('starting')
        
        try:
            # 为每个时间粒度启动监控线程
            for timeframe in self.timeframes:
                thread = threading.Thread(
                    target=self._monitor_timeframe,
                    args=(timeframe,),
                    daemon=True,
                    name=f"Monitor-{timeframe}"
                )
                thread.start()
                
                self.monitor_threads[timeframe] = thread
                self.thread_health[timeframe] = {
                    'status': 'starting',
                    'last_activity': datetime.now(),
                    'error_count': 0,
                    'last_error': None
                }
                
                logger.info(f"启动 {timeframe} 监控线程")
            
            # 启动健康检查线程
            health_thread = threading.Thread(
                target=self._health_check_loop,
                daemon=True,
                name="HealthCheck"
            )
            health_thread.start()
            logger.info("启动健康检查线程")
            
            self._update_system_health('running')
            
            # 检测运行环境
            is_railway = os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('PORT')
            if not is_railway:
                # 仅在本地环境启动Flask应用
                logger.info("本地环境检测到，启动Flask应用")
                app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
            else:
                logger.info("Railway环境检测到，监控系统已在后台启动，Flask由gunicorn管理")
            
        except Exception as e:
            logger.error(f"启动监控系统失败: {str(e)}")
            self._update_system_health('error', e)
            raise
    
    def stop_monitoring(self):
        """停止监控"""
        logger.info("停止监控系统")
        self.running = False

# 全局监控实例
monitor = CryptoPatternMonitor()

# Flask应用
app = Flask(__name__)

# 在应用启动时自动启动监控系统
def start_monitoring_on_startup():
    """在应用启动时启动监控系统"""
    if not monitor.running:
        # 无论什么环境，都先初始化A点缓存
        logger.info("开始初始化所有代币的A点缓存...")
        try:
            monitor._initialize_all_pattern_cache()
            logger.info("所有代币A点缓存初始化完成")
        except Exception as e:
            logger.error(f"A点缓存初始化失败: {str(e)}")
        
        is_railway = os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('PORT')
        if is_railway:
            logger.info("Railway环境检测到，自动启动监控系统")
            # 在后台线程中启动监控（不启动Flask应用）
            import threading
            def start_monitor_only():
                try:
                    # 双重检查，防止重复启动
                    if monitor.running:
                        logger.warning("监控系统已在运行，跳过重复启动")
                        return
                        
                    monitor.running = True
                    monitor._update_system_health('starting')
                    
                    # 启动各时间粒度的监控线程
                    for timeframe in monitor.timeframes:
                        # 检查线程是否已存在
                        if timeframe in monitor.monitor_threads and monitor.monitor_threads[timeframe].is_alive():
                            logger.warning(f"{timeframe} 监控线程已存在，跳过启动")
                            continue
                            
                        thread = threading.Thread(
                            target=monitor._monitor_timeframe,
                            args=(timeframe,),
                            daemon=True,
                            name=f"Monitor-{timeframe}"
                        )
                        thread.start()
                        
                        monitor.monitor_threads[timeframe] = thread
                        monitor.thread_health[timeframe] = {
                            'status': 'starting',
                            'last_activity': datetime.now(),
                            'error_count': 0,
                            'last_error': None
                        }
                        
                        logger.info(f"启动 {timeframe} 监控线程")
                    
                    # 启动健康检查线程
                    health_thread = threading.Thread(
                        target=monitor._health_check_loop,
                        daemon=True,
                        name="HealthCheck"
                    )
                    health_thread.start()
                    logger.info("启动健康检查线程")
                    
                    monitor._update_system_health('running')
                    logger.info("监控系统启动完成（Railway环境）")
                    
                except Exception as e:
                    logger.error(f"启动监控系统失败: {str(e)}")
                    monitor._update_system_health('error', e)
            
            monitor_thread = threading.Thread(target=start_monitor_only, daemon=True)
            monitor_thread.start()
        else:
            logger.info("本地环境，A点缓存已初始化，可手动启动监控")

# 使用应用上下文启动监控
with app.app_context():
    start_monitoring_on_startup()

@app.route('/')
def index():
    """主页"""
    logger.info("访问主页 - 返回系统状态信息")
    
    try:
        memory_usage = psutil.Process().memory_info().rss / 1024 / 1024
        status_data = {
            'status': 'running' if monitor.running else 'stopped',
            'monitored_pairs': len(monitor.monitored_pairs),
            'timeframes': monitor.timeframes,
            'cache_stats': {
                'pattern_cache_size': len(monitor.pattern_cache),
                'kline_cache_size': len(monitor.data_cache.kline_cache),
                'atr_cache_size': len(monitor.data_cache.atr_cache)
            },
            'system_health': monitor.system_health['status'],
            'memory_usage_mb': memory_usage
        }
        
        logger.info(f"系统状态: {status_data['status']}, 内存使用: {memory_usage:.2f}MB, 监控对数: {len(monitor.monitored_pairs)}")
        return jsonify(status_data)
        
    except Exception as e:
        logger.error(f"获取系统状态失败: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/status')
def status():
    """系统状态详情"""
    logger.info("访问系统状态详情页面")
    current_time = datetime.now()
    
    # 计算线程状态统计
    thread_stats = {'total': len(monitor.timeframes), 'healthy': 0, 'warning': 0, 'error': 0}
    thread_details = {}
    
    for timeframe, health in monitor.thread_health.items():
        if health['last_activity']:
            inactive_duration = (current_time - health['last_activity']).total_seconds()
            if inactive_duration > 900:
                status_level = 'error'
                thread_stats['error'] += 1
            elif health['error_count'] > 5:
                status_level = 'warning'
                thread_stats['warning'] += 1
            else:
                status_level = 'healthy'
                thread_stats['healthy'] += 1
        else:
            status_level = 'unknown'
        
        thread_details[timeframe] = {
            'status': status_level,
            'last_activity': health['last_activity'].isoformat() if health['last_activity'] else None,
            'error_count': health['error_count'],
            'last_error': str(health['last_error']) if health['last_error'] else None,
            'inactive_seconds': inactive_duration if health['last_activity'] else None
        }
    
    return jsonify({
        'system': {
            'running': monitor.running,
            'status': monitor.system_health['status'],
            'uptime_seconds': (current_time - monitor.system_health['start_time']).total_seconds(),
            'consecutive_errors': monitor.system_health['consecutive_errors'],
            'recovery_attempts': monitor.system_health['recovery_attempts'],
            'memory_usage_mb': psutil.Process().memory_info().rss / 1024 / 1024
        },
        'monitoring': {
            'monitored_pairs_count': len(monitor.monitored_pairs),
            'timeframes': monitor.timeframes,
            'thread_stats': thread_stats,
            'thread_details': thread_details
        },
        'cache': {
            'pattern_cache_size': len(monitor.pattern_cache),
            'kline_cache_size': len(monitor.data_cache.kline_cache),
            'atr_cache_size': len(monitor.data_cache.atr_cache),
            'cache_hit_ratio': 'N/A'  # 可以实现缓存命中率统计
        },
        'performance': {
            'concurrent_analysis': monitor.max_concurrent_analysis,
            'analysis_timeout': monitor.analysis_timeout
        },
        'timestamp': current_time.isoformat()
    })

@app.route('/health')
def health():
    """健康检查端点"""
    logger.info("访问健康检查端点")
    current_time = datetime.now()
    system_status = monitor.system_health['status']
    
    # 检查线程健康状态
    unhealthy_threads = 0
    total_threads = len(monitor.timeframes)
    
    for timeframe, health in monitor.thread_health.items():
        if health['last_activity']:
            inactive_duration = (current_time - health['last_activity']).total_seconds()
            if inactive_duration > 900 or health['error_count'] > 10:
                unhealthy_threads += 1
    
    # 确定HTTP状态码
    if system_status == 'critical' or unhealthy_threads > total_threads / 2:
        http_status = 503
        overall_status = 'unhealthy'
    elif system_status == 'degraded' or unhealthy_threads > 0:
        http_status = 200
        overall_status = 'degraded'
    else:
        http_status = 200
        overall_status = 'healthy'
    
    response_data = {
        'status': overall_status,
        'system_status': system_status,
        'running': monitor.running,
        'threads': {
            'total': total_threads,
            'healthy': total_threads - unhealthy_threads,
            'unhealthy': unhealthy_threads
        },
        'uptime_seconds': (current_time - monitor.system_health['start_time']).total_seconds(),
        'consecutive_errors': monitor.system_health['consecutive_errors'],
        'memory_usage_mb': psutil.Process().memory_info().rss / 1024 / 1024,
        'environment': 'railway' if (os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('PORT')) else 'local',
        'timestamp': current_time.isoformat()
    }
    
    logger.info(f"健康检查完成 - 状态: {overall_status}, 系统状态: {system_status}, 健康线程: {total_threads - unhealthy_threads}/{total_threads}")
    
    return jsonify(response_data), http_status

@app.route('/cache/clear')
def clear_cache():
    """清理缓存端点"""
    logger.info("访问缓存清理接口")
    try:
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # 记录清理前的缓存状态
        before_kline = len(monitor.data_cache.kline_cache)
        before_atr = len(monitor.data_cache.atr_cache)
        before_pattern = len(monitor.pattern_cache)
        
        logger.info(f"清理前缓存状态 - K线缓存: {before_kline}, ATR缓存: {before_atr}, 模式缓存: {before_pattern}")
        logger.info(f"清理前内存使用: {initial_memory:.2f} MB")
        
        monitor.data_cache.clear_all()
        monitor.pattern_cache.clear()
        gc.collect()
        
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024
        memory_freed = initial_memory - final_memory
        
        logger.info(f"缓存清理完成 - 释放内存: {memory_freed:.2f} MB, 当前内存: {final_memory:.2f} MB")
        
        return jsonify({
            'success': True,
            'message': 'Cache cleared successfully',
            'memory_freed_mb': memory_freed,
            'current_memory_mb': final_memory,
            'cleared_items': {
                'kline_cache': before_kline,
                'atr_cache': before_atr,
                'pattern_cache': before_pattern
            }
        })
    except Exception as e:
        logger.error(f"清理缓存失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def create_app():
    """创建Flask应用"""
    return app

def main():
    """主函数"""
    try:
        logger.info("初始化加密货币形态识别监控系统")
        # 检查是否为Railway环境，如果是则不重复启动监控（已在start_monitoring_on_startup中启动）
        is_railway = os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('PORT')
        if not is_railway:
            logger.info("本地环境，启动监控系统")
            monitor.start_monitoring()
        else:
            logger.info("Railway环境，监控系统已在应用启动时自动启动")
            # 保持主线程运行
            import time
            while True:
                time.sleep(60)
    except KeyboardInterrupt:
        logger.info("接收到停止信号")
        monitor.stop_monitoring()
    except Exception as e:
        logger.error(f"系统运行异常: {str(e)}")
        monitor.stop_monitoring()

if __name__ == '__main__':
    main()

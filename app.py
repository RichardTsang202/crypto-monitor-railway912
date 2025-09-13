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
        self.last_signal_time = {}  # 最后信号时间
        self.last_analysis_time = {}  # 最后分析时间（防重复分析）
        self.last_webhook_time = 0  # 全局webhook发送时间控制
        self.api_sources = self._init_api_sources()
        self.current_api_index = 0
        self.monitored_pairs = self._get_monitored_pairs()
        self.timeframes = ['1h', '4h', '1d']
        self.webhook_url = "https://n8n-ayzvkyda.ap-northeast-1.clawcloudrun.com/webhook-test/double_t_b"
        
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
            high = current[2]
            low = current[3]
            
            # 检测高点
            if high > prev[2] and high > next_kline[2]:
                extreme_points.append(ExtremePoint(
                    timestamp=timestamp,
                    price=high,
                    point_type='high'
                ))
            
            # 检测低点
            if low < prev[3] and low < next_kline[3]:
                extreme_points.append(ExtremePoint(
                    timestamp=timestamp,
                    price=low,
                    point_type='low'
                ))
        
        return extreme_points
    
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
                if kline[2] > max_high:
                    max_high = kline[2]
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
                if kline[3] < min_low:
                    min_low = kline[3]
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
                if kline[2] > max_high:
                    max_high = kline[2]
                    max_high_timestamp = int(kline[0])
            
            cache.a_point = ExtremePoint(max_high_timestamp, max_high, 'high')
            cache.last_update = current_time
        
        if not cache.d_point or self._is_point_expired(cache.d_point, klines):
            # 找D点区间最高点（左肩）
            max_high_d = float('-inf')
            max_high_d_timestamp = None
            for kline in d_range_klines:
                if kline[2] > max_high_d:
                    max_high_d = kline[2]
                    max_high_d_timestamp = int(kline[0])
            
            cache.d_point = ExtremePoint(max_high_d_timestamp, max_high_d, 'high')
        
        # 更新头肩底缓存
        cache = self.pattern_cache[cache_key]['head_shoulders_bottom']
        if not cache.a_point or self._is_point_expired(cache.a_point, klines):
            # 找A点区间最低点
            min_low = float('inf')
            min_low_timestamp = None
            for kline in a_range_klines:
                if kline[3] < min_low:
                    min_low = kline[3]
                    min_low_timestamp = int(kline[0])
            
            cache.a_point = ExtremePoint(min_low_timestamp, min_low, 'low')
            cache.last_update = current_time
        
        if not cache.d_point or self._is_point_expired(cache.d_point, klines):
            # 找D点区间最低点（左肩）
            min_low_d = float('inf')
            min_low_d_timestamp = None
            for kline in d_range_klines:
                if kline[3] < min_low_d:
                    min_low_d = kline[3]
                    min_low_d_timestamp = int(kline[0])
            
            cache.d_point = ExtremePoint(min_low_d_timestamp, min_low_d, 'low')
    
    def _is_point_expired(self, point: ExtremePoint, klines: List[List]) -> bool:
        """检查缓存点是否过期（超出有效范围）"""
        if not point:
            return True
        
        # 检查点是否还在K线数据的有效范围内
        for i, kline in enumerate(klines):
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
            
            high = current[2]
            low = current[3]
            prev_close = previous[4]
            
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
                price = kline[3] if find_type == 'low' else kline[2]  # low or high
                
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
            return None
        
        # 获取最新K线数据
        klines = self._get_klines_data(symbol, timeframe, 55)
        if not klines or len(klines) < 35:
            return None
        
        # 获取ATR
        atr = self._get_cached_atr(symbol, timeframe, klines)
        if atr <= 0:
            return None
        
        logger.debug(f"双顶检测 {symbol} {timeframe}:")
        logger.debug(f"  ATR: {atr:.6f}")
        
        # A点：缓存的第一个顶
        point_a = cache.a_point
        
        # B点：前一根收盘K线的最高价
        previous_kline = klines[-2]
        point_b = ExtremePoint(
            timestamp=int(previous_kline[0]),
            price=previous_kline[2],
            point_type='high'
        )
        
        logger.debug(f"  A点: 价格={point_a.price:.6f}")
        logger.debug(f"  B点: 价格={point_b.price:.6f}")
        
        # 验证A-B差值 ≤ 0.8ATR
        ab_diff = abs(point_a.price - point_b.price)
        if ab_diff > 0.8 * atr:
            logger.debug(f"  双顶检测失败: A-B差值({ab_diff:.6f}) > 0.8ATR({0.8 * atr:.6f})")
            return None
        
        # 查找C点（A-B间最低点）
        point_c = self._find_point_between(klines, point_a, point_b, 'low')
        if not point_c:
            logger.debug(f"  双顶检测失败: 未找到C点")
            return None
        
        # 验证max(A,B) - C ≥ 2.3ATR
        max_ab = max(point_a.price, point_b.price)
        c_diff = abs(max_ab - point_c.price)
        if c_diff < 2.3 * atr:
            logger.debug(f"  双顶检测失败: max(A,B)-C差值({c_diff:.6f}) < 2.3ATR({2.3 * atr:.6f})")
            return None
        
        logger.info(f"✓ 检测到双顶形态: {symbol} {timeframe}")
        
        return {
            'pattern_type': 'double_top',
            'symbol': symbol,
            'timeframe': timeframe,
            'point_a': {'timestamp': datetime.fromtimestamp(point_a.timestamp/1000).isoformat(), 'price': point_a.price, 'type': point_a.point_type},
            'point_b': {'timestamp': datetime.fromtimestamp(point_b.timestamp/1000).isoformat(), 'price': point_b.price, 'type': point_b.point_type},
            'point_c': {'timestamp': datetime.fromtimestamp(point_c.timestamp/1000).isoformat(), 'price': point_c.price, 'type': point_c.point_type},
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
        
        # 获取ATR
        atr = self._get_cached_atr(symbol, timeframe, klines)
        if atr <= 0:
            return None
        
        logger.debug(f"双底检测 {symbol} {timeframe}:")
        
        # A点：缓存的第一个底
        point_a = cache.a_point
        
        # B点：前一根收盘K线的最低价
        previous_kline = klines[-2]
        point_b = ExtremePoint(
            timestamp=int(previous_kline[0]),
            price=previous_kline[3],
            point_type='low'
        )
        
        # 验证A-B差值 ≤ 0.8ATR
        ab_diff = abs(point_a.price - point_b.price)
        if ab_diff > 0.8 * atr:
            logger.debug(f"  双底检测失败: A-B差值超过0.8ATR")
            return None
        
        # 查找C点（A-B间最高点）
        point_c = self._find_point_between(klines, point_a, point_b, 'high')
        if not point_c:
            logger.debug(f"  双底检测失败: 未找到C点")
            return None
        
        # 验证C - min(A,B) ≥ 2.3ATR
        min_ab = min(point_a.price, point_b.price)
        c_diff = abs(point_c.price - min_ab)
        if c_diff < 2.3 * atr:
            logger.debug(f"  双底检测失败: C-min(A,B)差值小于2.3ATR")
            return None
        
        logger.info(f"✓ 检测到双底形态: {symbol} {timeframe}")
        
        return {
            'pattern_type': 'double_bottom',
            'symbol': symbol,
            'timeframe': timeframe,
            'point_a': {'timestamp': datetime.fromtimestamp(point_a.timestamp/1000).isoformat(), 'price': point_a.price, 'type': point_a.point_type},
            'point_b': {'timestamp': datetime.fromtimestamp(point_b.timestamp/1000).isoformat(), 'price': point_b.price, 'type': point_b.point_type},
            'point_c': {'timestamp': datetime.fromtimestamp(point_c.timestamp/1000).isoformat(), 'price': point_c.price, 'type': point_c.point_type},
            'atr': atr,
            'quality_score': min(100, (c_diff / (2.3 * atr)) * 100)
        }
    
    def _detect_head_shoulders_top(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """检测头肩顶形态"""
        cache_key = f"{symbol}_{timeframe}"
        
        if cache_key not in self.pattern_cache:
            return None
        
        cache = self.pattern_cache[cache_key]['head_shoulders_top']
        if not cache.a_point or not cache.d_point:
            return None
        
        # 获取最新K线数据
        klines = self._get_klines_data(symbol, timeframe, 55)
        if not klines or len(klines) < 35:
            return None
        
        # 获取ATR
        atr = self._get_cached_atr(symbol, timeframe, klines)
        if atr <= 0:
            return None
        
        logger.debug(f"头肩顶检测 {symbol} {timeframe}:")
        
        # A点：缓存的头部
        point_a = cache.a_point
        
        # B点：前一根收盘K线的最高价（右肩）
        previous_kline = klines[-2]
        point_b = ExtremePoint(
            timestamp=int(previous_kline[0]),
            price=previous_kline[2],
            point_type='high'
        )
        
        # D点：缓存的左肩
        point_d = cache.d_point
        
        # 验证A-B差值 ≤ 0.8ATR（头部与右肩）
        ab_diff = abs(point_a.price - point_b.price)
        if ab_diff > 0.8 * atr:
            logger.debug(f"  头肩顶检测失败: A-B差值超过0.8ATR")
            return None
        
        # 验证B-D差值 ≤ 0.8ATR（左右肩）
        bd_diff = abs(point_b.price - point_d.price)
        if bd_diff > 0.8 * atr:
            logger.debug(f"  头肩顶检测失败: B-D差值超过0.8ATR")
            return None
        
        # 查找C点（A-B间最低点，颈线）
        point_c = self._find_point_between(klines, point_a, point_b, 'low')
        if not point_c:
            logger.debug(f"  头肩顶检测失败: 未找到C点")
            return None
        
        # 验证max(A,B) - C ≥ 2.3ATR
        max_ab = max(point_a.price, point_b.price)
        c_diff = abs(max_ab - point_c.price)
        if c_diff < 2.3 * atr:
            logger.debug(f"  头肩顶检测失败: max(A,B)-C差值小于2.3ATR")
            return None
        
        logger.info(f"✓ 检测到头肩顶形态: {symbol} {timeframe}")
        
        return {
            'pattern_type': 'head_shoulders_top',
            'symbol': symbol,
            'timeframe': timeframe,
            'point_a': {'timestamp': datetime.fromtimestamp(point_a.timestamp/1000).isoformat(), 'price': point_a.price, 'type': point_a.point_type},
            'point_b': {'timestamp': datetime.fromtimestamp(point_b.timestamp/1000).isoformat(), 'price': point_b.price, 'type': point_b.point_type},
            'point_c': {'timestamp': datetime.fromtimestamp(point_c.timestamp/1000).isoformat(), 'price': point_c.price, 'type': point_c.point_type},
            'point_d': {'timestamp': datetime.fromtimestamp(point_d.timestamp/1000).isoformat(), 'price': point_d.price, 'type': point_d.point_type},
            'atr': atr,
            'quality_score': min(100, (c_diff / (2.3 * atr)) * 100)
        }
    
    def _detect_head_shoulders_bottom(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """检测头肩底形态"""
        cache_key = f"{symbol}_{timeframe}"
        
        if cache_key not in self.pattern_cache:
            return None
        
        cache = self.pattern_cache[cache_key]['head_shoulders_bottom']
        if not cache.a_point or not cache.d_point:
            return None
        
        # 获取最新K线数据
        klines = self._get_klines_data(symbol, timeframe, 55)
        if not klines or len(klines) < 35:
            return None
        
        # 获取ATR
        atr = self._get_cached_atr(symbol, timeframe, klines)
        if atr <= 0:
            return None
        
        logger.debug(f"头肩底检测 {symbol} {timeframe}:")
        
        # A点：缓存的头部（最低点）
        point_a = cache.a_point
        
        # B点：前一根收盘K线的最低价（右肩）
        previous_kline = klines[-2]
        point_b = ExtremePoint(
            timestamp=int(previous_kline[0]),
            price=previous_kline[3],
            point_type='low'
        )
        
        # D点：缓存的左肩
        point_d = cache.d_point
        
        # 验证A-B差值 ≤ 0.8ATR（头部与右肩）
        ab_diff = abs(point_a.price - point_b.price)
        if ab_diff > 0.8 * atr:
            logger.debug(f"  头肩底检测失败: A-B差值超过0.8ATR")
            return None
        
        # 验证B-D差值 ≤ 0.8ATR（左右肩）
        bd_diff = abs(point_b.price - point_d.price)
        if bd_diff > 0.8 * atr:
            logger.debug(f"  头肩底检测失败: B-D差值超过0.8ATR")
            return None
        
        # 查找C点（A-B间最高点，颈线）
        point_c = self._find_point_between(klines, point_a, point_b, 'high')
        if not point_c:
            logger.debug(f"  头肩底检测失败: 未找到C点")
            return None
        
        # 验证C - min(A,B) ≥ 2.3ATR
        min_ab = min(point_a.price, point_b.price)
        c_diff = abs(point_c.price - min_ab)
        if c_diff < 2.3 * atr:
            logger.debug(f"  头肩底检测失败: C-min(A,B)差值小于2.3ATR")
            return None
        
        logger.info(f"✓ 检测到头肩底形态: {symbol} {timeframe}")
        
        return {
            'pattern_type': 'head_shoulders_bottom',
            'symbol': symbol,
            'timeframe': timeframe,
            'point_a': {'timestamp': datetime.fromtimestamp(point_a.timestamp/1000).isoformat(), 'price': point_a.price, 'type': point_a.point_type},
            'point_b': {'timestamp': datetime.fromtimestamp(point_b.timestamp/1000).isoformat(), 'price': point_b.price, 'type': point_b.point_type},
            'point_c': {'timestamp': datetime.fromtimestamp(point_c.timestamp/1000).isoformat(), 'price': point_c.price, 'type': point_c.point_type},
            'point_d': {'timestamp': datetime.fromtimestamp(point_d.timestamp/1000).isoformat(), 'price': point_d.price, 'type': point_d.point_type},
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
        closes = [kline[4] for kline in klines]
        
        # 计算EMA
        ema21 = self._calculate_ema(closes, 21)
        ema55 = self._calculate_ema(closes, 55)
        ema144 = self._calculate_ema(closes, 144)
        
        # 计算MACD
        macd_line, signal_line, histogram = self._calculate_macd(closes)
        
        # 计算RSI
        rsi = self._calculate_rsi(closes)
        
        # 计算背离
        divergence = self._calculate_divergence_analysis(pattern_result)
        
        # 判断EMA趋势
        ema_trend = self._determine_ema_trend(ema21, ema55, ema144)
        
        # 分析K线形态
        kline_pattern = self._analyze_kline_pattern(klines)
        
        return {
            'ema21': ema21,
            'ema55': ema55,
            'ema144': ema144,
            'ema_trend': ema_trend,
            'kline_pattern': kline_pattern,
            'macd': {
                'line': macd_line,
                'signal': signal_line,
                'histogram': histogram
            },
            'rsi': rsi,
            'divergence': divergence
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
    
    def _calculate_macd(self, prices: List[float], fast: int = 13, slow: int = 34, signal: int = 8) -> Tuple[float, float, float]:
        """计算MACD指标"""
        if len(prices) < slow:
            return 0.0, 0.0, 0.0
        
        ema_fast = self._calculate_ema(prices, fast)
        ema_slow = self._calculate_ema(prices, slow)
        macd_line = ema_fast - ema_slow
        
        # 简化的信号线计算
        signal_line = macd_line * 0.8
        histogram = macd_line - signal_line
        
        return macd_line, signal_line, histogram
    
    def _calculate_rsi(self, prices: List[float], period: int = 14) -> float:
        """计算RSI指标"""
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
    
    def _determine_ema_trend(self, ema21: float, ema55: float, ema144: float) -> str:
        """判断EMA趋势"""
        if not (ema21 and ema55 and ema144):
            return "unknown"
            
        if ema21 > ema55 > ema144:
            return "strong_bullish"
        elif ema21 > ema55 and ema55 < ema144:
            return "bullish"
        elif ema21 < ema55 < ema144:
            return "strong_bearish"
        elif ema21 < ema55 and ema55 > ema144:
            return "bearish"
        else:
            return "sideways"
    
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
        open_price, high, low, close = candle[1], candle[2], candle[3], candle[4]
        
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
        prev_open, prev_close = prev_candle[1], prev_candle[4]
        curr_open, curr_close = current_candle[1], current_candle[4]
        
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
        open_price, high, low, close = candle[1], candle[2], candle[3], candle[4]
        
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
    
    def _calculate_divergence_analysis(self, pattern_data: Dict) -> Dict:
        """计算背离分析 - 以C点为起点，B点为终点"""
        try:
            # 获取C点和B点信息
            point_c = pattern_data.get('point_c', {})
            point_b = pattern_data.get('point_b', {})
            
            if not point_c or not point_b:
                return {
                    'macd_divergence': False,
                    'rsi_divergence': False,
                    'volume_divergence': False
                }
            
            symbol = pattern_data.get('symbol', '')
            timeframe = pattern_data.get('timeframe', '')
            
            # 获取K线数据用于计算指标
            klines = self._get_klines_data(symbol, timeframe, 100)
            if not klines or len(klines) < 50:
                return {
                    'macd_divergence': False,
                    'rsi_divergence': False,
                    'volume_divergence': False
                }
            
            # 找到C点和B点在K线数据中的位置
            c_index = self._find_kline_index_by_timestamp(klines, point_c.get('timestamp', 0))
            b_index = self._find_kline_index_by_timestamp(klines, point_b.get('timestamp', 0))
            
            if c_index == -1 or b_index == -1 or c_index >= b_index:
                return {
                    'macd_divergence': False,
                    'rsi_divergence': False,
                    'volume_divergence': False
                }
            
            # 提取C点到B点区间的数据
            segment_klines = klines[c_index:b_index+1]
            if len(segment_klines) < 2:
                return {
                    'macd_divergence': False,
                    'rsi_divergence': False,
                    'volume_divergence': False
                }
            
            # 计算背离
            macd_divergence = self._calculate_macd_divergence(segment_klines, point_c, point_b)
            rsi_divergence = self._calculate_rsi_divergence(segment_klines, point_c, point_b)
            volume_divergence = self._calculate_volume_divergence(segment_klines, point_c, point_b)
            
            return {
                'macd_divergence': macd_divergence,
                'rsi_divergence': rsi_divergence,
                'volume_divergence': volume_divergence
            }
            
        except Exception as e:
            logger.error(f"背离分析计算错误: {e}")
            return {
                'macd_divergence': False,
                'rsi_divergence': False,
                'volume_divergence': False
            }
    
    def _find_kline_index_by_timestamp(self, klines: List[List], timestamp: int) -> int:
        """根据时间戳找到K线在数组中的索引"""
        for i, kline in enumerate(klines):
            if kline[0] == timestamp:
                return i
        return -1
    
    def _calculate_macd_divergence(self, klines: List[List], point_c: Dict, point_b: Dict) -> bool:
        """计算MACD背离"""
        if len(klines) < 26:  # MACD需要至少26根K线
            return False
        
        closes = [kline[4] for kline in klines]
        
        # 计算整个区间的MACD
        macd_values = []
        for i in range(25, len(closes)):  # 从第26根开始计算MACD
            segment = closes[:i+1]
            macd_line, _, _ = self._calculate_macd(segment)
            macd_values.append(macd_line)
        
        if len(macd_values) < 2:
            return False
        
        # 获取C点和B点的价格和MACD值
        c_price = point_c.get('price', 0)
        b_price = point_b.get('price', 0)
        c_macd = macd_values[0] if macd_values else 0
        b_macd = macd_values[-1] if macd_values else 0
        
        # 判断背离：价格和MACD走势相反
        price_trend = b_price - c_price
        macd_trend = b_macd - c_macd
        
        # 背离条件：价格和MACD趋势相反且幅度足够大
        return (price_trend * macd_trend < 0 and 
                abs(price_trend) > abs(c_price * 0.01) and  # 价格变化超过1%
                abs(macd_trend) > abs(c_macd * 0.1))  # MACD变化超过10%
    
    def _calculate_rsi_divergence(self, klines: List[List], point_c: Dict, point_b: Dict) -> bool:
        """计算RSI背离"""
        if len(klines) < 14:  # RSI需要至少14根K线
            return False
        
        closes = [kline[4] for kline in klines]
        
        # 计算C点和B点的RSI
        c_rsi = self._calculate_rsi(closes[:len(closes)//2]) if len(closes) >= 28 else self._calculate_rsi(closes[:14])
        b_rsi = self._calculate_rsi(closes)
        
        # 获取价格
        c_price = point_c.get('price', 0)
        b_price = point_b.get('price', 0)
        
        # 判断背离
        price_trend = b_price - c_price
        rsi_trend = b_rsi - c_rsi
        
        # 背离条件：价格和RSI趋势相反且幅度足够大
        return (price_trend * rsi_trend < 0 and 
                abs(price_trend) > abs(c_price * 0.01) and  # 价格变化超过1%
                abs(rsi_trend) > 5)  # RSI变化超过5
    
    def _calculate_volume_divergence(self, klines: List[List], point_c: Dict, point_b: Dict) -> bool:
        """计算成交量背离"""
        if len(klines) < 2:
            return False
        
        volumes = [kline[5] for kline in klines]
        
        # 计算平均成交量
        c_volume_avg = sum(volumes[:len(volumes)//2]) / (len(volumes)//2) if len(volumes) >= 4 else volumes[0]
        b_volume_avg = sum(volumes[len(volumes)//2:]) / (len(volumes) - len(volumes)//2) if len(volumes) >= 4 else volumes[-1]
        
        # 获取价格
        c_price = point_c.get('price', 0)
        b_price = point_b.get('price', 0)
        
        # 判断背离
        price_trend = b_price - c_price
        volume_trend = b_volume_avg - c_volume_avg
        
        # 背离条件：价格上涨但成交量下降，或价格下跌但成交量上升
        return (price_trend * volume_trend < 0 and 
                abs(price_trend) > abs(c_price * 0.01) and  # 价格变化超过1%
                abs(volume_trend) > abs(c_volume_avg * 0.1))  # 成交量变化超过10%
    
    def _analyze_pattern(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """分析单个交易对的形态"""
        try:
            # 初始化或更新形态缓存
            if f"{symbol}_{timeframe}" not in self.pattern_cache:
                self._initialize_pattern_cache(symbol, timeframe)
            else:
                self._update_pattern_cache(symbol, timeframe)
            
            # 检测四种形态
            patterns = [
                self._detect_double_top(symbol, timeframe),
                self._detect_double_bottom(symbol, timeframe),
                self._detect_head_shoulders_top(symbol, timeframe),
                self._detect_head_shoulders_bottom(symbol, timeframe)
            ]
            
            # 返回第一个检测到的形态
            for pattern in patterns:
                if pattern:
                    # 计算额外指标
                    additional_indicators = self._calculate_additional_indicators(symbol, timeframe, pattern)
                    pattern.update(additional_indicators)
                    return pattern
            
            return None
            
        except Exception as e:
            logger.error(f"分析 {symbol} {timeframe} 形态失败: {str(e)}")
            raise
    
    def _should_send_signal(self, symbol: str, timeframe: str, pattern_type: str) -> bool:
        """检查是否应该发送信号（防止重复）"""
        signal_key = f"{symbol}_{timeframe}_{pattern_type}"
        current_time = time.time()
        
        # 检查同一信号的重复发送（24小时间隔）
        if signal_key in self.last_signal_time:
            time_diff = current_time - self.last_signal_time[signal_key]
            if time_diff < 86400:  # 24小时间隔，防止同一形态重复发送
                return False
        
        # 检查全局webhook发送间隔（10秒）
        if current_time - self.last_webhook_time < 10:
            return False
        
        return True
    
    def _send_webhook(self, pattern_data: Dict) -> bool:
        """发送Webhook通知（带重试机制）"""
        max_retries = 3
        retry_delays = [1, 3, 5]
        
        for attempt in range(max_retries):
            try:
                # 获取当前价格
                current_price = pattern_data.get('point_c', {}).get('price', 0)
                if not current_price:
                    current_price = pattern_data.get('point_b', {}).get('price', 0)
                
                if not current_price or current_price <= 0:
                    logger.error(f"无效价格数据: {current_price}")
                    return False
                
                # 准备发送的数据
                webhook_data = {
                    'symbol': pattern_data['symbol'],
                    'timestamp': datetime.now().isoformat(),
                    'price': current_price,
                    'pattern_type': pattern_data['pattern_type'],
                    'timeframe': pattern_data['timeframe'],
                    'points': {
                        'A': pattern_data['point_a'],
                        'B': pattern_data['point_b'],
                        'C': pattern_data['point_c']
                    },
                    'atr': pattern_data['atr'],
                    'quality_score': pattern_data['quality_score']
                }
                
                # 添加D点（如果存在）
                if 'point_d' in pattern_data:
                    webhook_data['points']['D'] = pattern_data['point_d']
                
                # 添加技术指标
                if 'macd' in pattern_data:
                    macd_data = pattern_data['macd']
                    webhook_data['indicators'] = {
                        'rsi': max(20, min(80, pattern_data.get('rsi', 50))),
                        'macd': macd_data.get('line', 0) if isinstance(macd_data, dict) else macd_data,
                        'macd_signal': macd_data.get('signal', 0) if isinstance(macd_data, dict) else 0,
                        'ema21': pattern_data.get('ema21', 0),
                        'ema55': pattern_data.get('ema55', 0),
                        'ema144': pattern_data.get('ema144', 0),
                        'ema_trend': pattern_data.get('ema_trend', 'unknown')
                    }
                
                # 添加K线形态信息
                kline_pattern = pattern_data.get('kline_pattern', {})
                webhook_data['kline_pattern'] = {
                    'pattern': kline_pattern.get('pattern', 'none'),
                    'strength': kline_pattern.get('strength', 0),
                    'description': kline_pattern.get('description', '无形态')
                }
                
                # 添加B点收盘价字段
                if 'point_b' in pattern_data:
                    webhook_data['b_point_close_price'] = pattern_data['point_b']['price']
                
                # 添加背离分析
                divergence_data = pattern_data.get('divergence', {})
                webhook_data['macd_divergence'] = divergence_data.get('macd_divergence', False)
                webhook_data['rsi_divergence'] = divergence_data.get('rsi_divergence', False)
                webhook_data['volume_divergence'] = divergence_data.get('volume_divergence', False)
                
                # 发送请求
                response = requests.post(
                    self.webhook_url,
                    json=webhook_data,
                    timeout=10,
                    headers={'Content-Type': 'application/json'}
                )
                
                if response.status_code == 200:
                    logger.info(f"Webhook发送成功: {pattern_data['symbol']} {pattern_data['pattern_type']}")
                    return True
                elif response.status_code in [429, 502, 503, 504]:
                    logger.warning(f"Webhook发送失败(可重试): {response.status_code}, 尝试 {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delays[attempt])
                        continue
                else:
                    logger.error(f"Webhook发送失败(不可重试): {response.status_code}")
                    return False
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Webhook请求超时, 尝试 {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delays[attempt])
                    continue
                else:
                    return False
        
        logger.error(f"Webhook发送失败，已重试{max_retries}次")
        return False
    
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
                                    # 更新信号发送时间和全局webhook时间
                                    signal_key = f"{symbol}_{timeframe}_{pattern_type}"
                                    current_time = time.time()
                                    self.last_signal_time[signal_key] = current_time
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
                                # 更新信号发送时间和全局webhook时间
                                signal_key = f"{symbol}_{timeframe}_{pattern_type}"
                                current_time = time.time()
                                self.last_signal_time[signal_key] = current_time
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
        
        # 更新线程健康状态
        if timeframe in self.thread_health:
            self.thread_health[timeframe]['last_activity'] = datetime.now()
        
        # 启动时立即执行一次分析
        logger.info(f"启动时立即分析 {timeframe}")
        try:
            self._analyze_all_pairs_sequential(timeframe)
            consecutive_errors = 0
            self._update_system_health('healthy')
        except Exception as e:
            logger.error(f"{timeframe} 启动分析失败: {str(e)}")
            consecutive_errors += 1
            self._update_system_health('error', e)
        
        while self.running:
            try:
                # 更新线程活动时间
                if timeframe in self.thread_health:
                    self.thread_health[timeframe]['last_activity'] = datetime.now()
                
                current_time = datetime.now()
                should_analyze = False
                
                # 根据时间粒度确定是否应该分析（增加容错时间窗口）
                if timeframe == '1h':
                    if current_time.minute <= 4:
                        should_analyze = True
                        if current_time.second == 0:
                            logger.info(f"[{timeframe}] 触发分析 - 整点后{current_time.minute}分钟")
                elif timeframe == '4h':
                    if current_time.hour % 4 == 0 and 30 <= current_time.minute <= 33:
                        should_analyze = True
                        if current_time.second == 0:
                            logger.info(f"[{timeframe}] 触发分析 - {current_time.hour}点{current_time.minute}分")
                elif timeframe == '1d':
                    if current_time.hour == 8 and 15 <= current_time.minute <= 18:
                        should_analyze = True
                        if current_time.second == 0:
                            logger.info(f"[{timeframe}] 触发分析 - 8点{current_time.minute}分")
                
                # 防重复分析检查
                if should_analyze:
                    if timeframe == '1h':
                        analysis_key = f"{timeframe}_{current_time.strftime('%Y%m%d_%H')}"
                    elif timeframe == '4h':
                        analysis_key = f"{timeframe}_{current_time.strftime('%Y%m%d_%H')}"
                    elif timeframe == '1d':
                        analysis_key = f"{timeframe}_{current_time.strftime('%Y%m%d')}"
                    
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
            
            # 启动Flask应用
            app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
            
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

@app.route('/')
def index():
    """主页"""
    return jsonify({
        'status': 'running' if monitor.running else 'stopped',
        'monitored_pairs': len(monitor.monitored_pairs),
        'timeframes': monitor.timeframes,
        'cache_stats': {
            'pattern_cache_size': len(monitor.pattern_cache),
            'kline_cache_size': len(monitor.data_cache.kline_cache),
            'atr_cache_size': len(monitor.data_cache.atr_cache)
        },
        'system_health': monitor.system_health['status'],
        'memory_usage_mb': psutil.Process().memory_info().rss / 1024 / 1024
    })

@app.route('/status')
def status():
    """系统状态详情"""
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
        'timestamp': current_time.isoformat()
    }
    
    return jsonify(response_data), http_status

@app.route('/cache/clear')
def clear_cache():
    """清理缓存端点"""
    try:
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        monitor.data_cache.clear_all()
        monitor.pattern_cache.clear()
        gc.collect()
        
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024
        memory_freed = initial_memory - final_memory
        
        return jsonify({
            'success': True,
            'message': 'Cache cleared successfully',
            'memory_freed_mb': memory_freed,
            'current_memory_mb': final_memory
        })
    except Exception as e:
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
        monitor.start_monitoring()
    except KeyboardInterrupt:
        logger.info("接收到停止信号")
        monitor.stop_monitoring()
    except Exception as e:
        logger.error(f"系统运行异常: {str(e)}")
        monitor.stop_monitoring()

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密货币形态识别监控系统

本系统专注于识别四种经典的价格形态：双顶、双底、头肩顶、头肩底。
采用多数据源架构，具备自动故障转移能力，通过严格的技术指标验证确保信号质量。

主要特性：
- 仅缓存极值点，不缓存所有K线数据
- 按需计算技术指标，先判断形态再计算指标
- 14周期ATR动态计算
- 精确的ABCD点获取逻辑
- 多时间粒度监控（1H、4H、1D）
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
from concurrent.futures import ThreadPoolExecutor
import numpy as np

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

class CryptoPatternMonitor:
    """加密货币形态识别监控系统"""
    
    def __init__(self):
        """初始化监控系统"""
        self.running = False
        self.double_pattern_cache = {}  # 顶底缓存区：存储双顶/双底形态的第一个顶/底
        self.head_shoulders_cache = {}  # 头肩缓存区：存储头肩形态的缓存点
        self.last_signal_time = {}  # 最后信号时间
        self.last_analysis_time = {}  # 最后分析时间（防重复分析）
        self.api_sources = self._init_api_sources()
        self.current_api_index = 0
        self.monitored_pairs = self._get_monitored_pairs()
        self.timeframes = ['1h', '4h', '1d']
        self.webhook_url = "https://n8n-ayzvkyda.ap-northeast-1.clawcloudrun.com/webhook/double_t_b"
        
        # 性能配置
        self.max_concurrent_analysis = 10
        self.analysis_timeout = 30
        self.memory_limit = 300 * 1024 * 1024  # 300MB
        
        # 系统健康状态跟踪
        self.system_health = {
            'status': 'healthy',
            'last_heartbeat': datetime.now(),
            'error_count': 0,
            'consecutive_errors': 0,
            'last_error_time': None,
            'recovery_attempts': 0,
            'max_recovery_attempts': 5
        }
        
        # 监控线程状态
        self.monitor_threads = {}
        self.thread_health = {}
        
        # 异常恢复配置
        self.recovery_config = {
            'max_consecutive_errors': 10,
            'recovery_delay': [30, 60, 120, 300, 600],  # 递增恢复延迟（秒）
            'health_check_interval': 60,  # 健康检查间隔
            'auto_restart_threshold': 20  # 自动重启阈值
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
                self.system_health['status'] = 'degraded' if self.system_health['consecutive_errors'] < 5 else 'critical'
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
            
            # 停止所有监控线程
            self._stop_all_monitors()
            
            # 等待恢复延迟
            time.sleep(delay_time)
            
            # 清理缓存和状态
            self._cleanup_system_state()
            
            # 重新初始化API源
            self.api_sources = self._init_api_sources()
            self.current_api_index = 0
            
            # 重启监控
            self._restart_monitoring()
            
            logger.info(f"系统恢复尝试 {self.system_health['recovery_attempts']} 完成")
            return True
            
        except Exception as e:
            logger.error(f"系统恢复失败: {str(e)}")
            return False
    
    def _stop_all_monitors(self):
        """停止所有监控线程"""
        try:
            self.running = False
            for timeframe, thread in self.monitor_threads.items():
                if thread and thread.is_alive():
                    logger.info(f"停止监控线程: {timeframe}")
                    thread.join(timeout=10)
            self.monitor_threads.clear()
            self.thread_health.clear()
        except Exception as e:
            logger.error(f"停止监控线程失败: {str(e)}")
    
    def _cleanup_system_state(self):
        """清理系统状态"""
        try:
            # 清理缓存
            self.double_pattern_cache.clear()
            self.head_shoulders_cache.clear()
            
            # 重置错误计数
            self.system_health['consecutive_errors'] = 0
            self.system_health['status'] = 'recovering'
            
            logger.info("系统状态清理完成")
        except Exception as e:
            logger.error(f"系统状态清理失败: {str(e)}")
    
    def _restart_monitoring(self):
        """重启监控系统"""
        try:
            self.running = True
            for timeframe in self.timeframes:
                thread = threading.Thread(target=self._monitor_timeframe, args=(timeframe,), daemon=True)
                thread.start()
                self.monitor_threads[timeframe] = thread
                self.thread_health[timeframe] = {
                    'status': 'running',
                    'last_activity': datetime.now(),
                    'error_count': 0
                }
            logger.info("监控系统重启完成")
        except Exception as e:
            logger.error(f"监控系统重启失败: {str(e)}")
    
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
                    if inactive_time > 300:  # 5分钟无活动
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
                old_thread.join(timeout=5)
            
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
            },
            {
                'name': 'CoinMarketCap',
                'base_url': 'https://pro-api.coinmarketcap.com/v1',
                'klines_endpoint': '/cryptocurrency/ohlcv/historical',
                'timeout': 10,
                'rate_limit': 333,
                'requires_key': True
            }
        ]
    
    def _get_monitored_pairs(self) -> List[str]:
        """获取监控的交易对列表"""
        # 黑名单
        blacklist = ['USDCUSDT', 'TUSDUSDT', 'BUSDUSDT', 'FDUSDT']
        
        # 这里可以从API获取所有USDT交易对，然后过滤黑名单
        # 为了演示，使用一些常见的交易对
        pairs = [
            'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT',
            'SOLUSDT', 'DOTUSDT', 'DOGEUSDT', 'AVAXUSDT', 'SHIBUSDT',
            'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'ATOMUSDT'
        ]
        
        # 过滤黑名单
        filtered_pairs = [pair for pair in pairs if pair not in blacklist]
        logger.info(f"监控交易对数量: {len(filtered_pairs)}")
        return filtered_pairs
    
    def _get_klines_data(self, symbol: str, interval: str, limit: int = 100) -> Optional[List[List]]:
        """获取K线数据，支持API轮换和增强容错"""
        max_retries = len(self.api_sources) * 2  # 每个API源最多重试2次
        retry_delays = [1, 2, 3, 5, 8]  # 递增延迟策略
        
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
                        headers={'User-Agent': 'CryptoPatternMonitor/1.0'}
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # 数据验证
                        if not data or not isinstance(data, list) or len(data) == 0:
                            raise ValueError("Binance API返回空数据或格式错误")
                        
                        # 验证数据结构
                        if len(data[0]) < 6:
                            raise ValueError("Binance K线数据格式不完整")
                        
                        # Binance返回格式: [timestamp, open, high, low, close, volume, ...]
                        validated_data = []
                        for kline in data:
                            try:
                                validated_kline = [float(x) for x in kline[:6]]
                                # 基本数据合理性检查
                                if validated_kline[2] < validated_kline[3]:  # high < low
                                    logger.warning(f"检测到异常K线数据: high({validated_kline[2]}) < low({validated_kline[3]})")
                                    continue
                                validated_data.append(validated_kline)
                            except (ValueError, IndexError) as e:
                                logger.warning(f"跳过无效K线数据: {e}")
                                continue
                        
                        if len(validated_data) >= limit * 0.8:  # 至少80%的数据有效
                            logger.info(f"成功从 {api_source['name']} 获取 {symbol} {interval} 数据: {len(validated_data)}条")
                            return validated_data
                        else:
                            raise ValueError(f"有效数据不足: {len(validated_data)}/{limit}")
                    
                    elif response.status_code == 429:
                        logger.warning(f"{api_source['name']} API限流，延长等待时间")
                        time.sleep(delay * 2)  # 限流时延长等待
                        continue
                    elif response.status_code >= 500:
                        logger.warning(f"{api_source['name']} 服务器错误: {response.status_code}")
                    else:
                        logger.warning(f"{api_source['name']} API请求失败: {response.status_code}")
                    
                elif api_source['name'] == 'OKX':
                    # OKX API实现
                    url = f"{api_source['base_url']}{api_source['klines_endpoint']}"
                    # OKX需要特殊的symbol格式
                    okx_symbol = symbol.replace('USDT', '-USDT') if 'USDT' in symbol else symbol
                    params = {
                        'instId': okx_symbol,
                        'bar': interval,
                        'limit': str(limit)
                    }
                    
                    response = requests.get(
                        url, 
                        params=params, 
                        timeout=api_source.get('timeout', 15),
                        headers={'User-Agent': 'CryptoPatternMonitor/1.0'}
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if data.get('code') == '0' and data.get('data'):
                            okx_data = data['data']
                            
                            # 数据验证
                            if not okx_data or len(okx_data) == 0:
                                raise ValueError("OKX API返回空数据")
                            
                            # OKX返回格式转换: [[timestamp, open, high, low, close, volume], ...]
                            validated_data = []
                            for kline in okx_data:
                                try:
                                    if len(kline) < 6:
                                        continue
                                    validated_kline = [float(x) for x in kline[:6]]
                                    # 基本数据合理性检查
                                    if validated_kline[2] < validated_kline[3]:  # high < low
                                        logger.warning(f"检测到异常K线数据: high({validated_kline[2]}) < low({validated_kline[3]})")
                                        continue
                                    validated_data.append(validated_kline)
                                except (ValueError, IndexError) as e:
                                    logger.warning(f"跳过无效K线数据: {e}")
                                    continue
                            
                            if len(validated_data) >= limit * 0.8:  # 至少80%的数据有效
                                logger.info(f"成功从 {api_source['name']} 获取 {symbol} {interval} 数据: {len(validated_data)}条")
                                return validated_data
                            else:
                                raise ValueError(f"有效数据不足: {len(validated_data)}/{limit}")
                        else:
                            raise ValueError(f"OKX API错误: {data.get('msg', '未知错误')}")
                    
                    elif response.status_code == 429:
                        logger.warning(f"{api_source['name']} API限流，延长等待时间")
                        time.sleep(delay * 2)
                        continue
                    elif response.status_code >= 500:
                        logger.warning(f"{api_source['name']} 服务器错误: {response.status_code}")
                    else:
                        logger.warning(f"{api_source['name']} API请求失败: {response.status_code}")
                
            except requests.exceptions.Timeout:
                logger.warning(f"{api_source['name']} API请求超时 (尝试 {attempt + 1}/{max_retries})")
            except requests.exceptions.ConnectionError:
                logger.warning(f"{api_source['name']} API连接错误 (尝试 {attempt + 1}/{max_retries})")
            except requests.exceptions.HTTPError as e:
                logger.warning(f"{api_source['name']} HTTP错误 (尝试 {attempt + 1}/{max_retries}): {e}")
            except ValueError as e:
                logger.warning(f"{api_source['name']} 数据验证失败 (尝试 {attempt + 1}/{max_retries}): {e}")
            except Exception as e:
                logger.error(f"{api_source['name']} API请求异常 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
            
            # 切换到下一个API源（每2次尝试切换一次）
            if (attempt + 1) % 2 == 0:
                old_index = self.current_api_index
                self.current_api_index = (self.current_api_index + 1) % len(self.api_sources)
                logger.info(f"切换API源: {self.api_sources[old_index]['name']} -> {self.api_sources[self.current_api_index]['name']}")
            
            # 递增延迟重试
            if attempt < max_retries - 1:
                logger.info(f"等待 {delay} 秒后重试...")
                time.sleep(delay)
        
        logger.error(f"所有API源都失败，无法获取 {symbol} 的K线数据 (共尝试 {max_retries} 次)")
        return None
    
    def _detect_extreme_points(self, klines: List[List]) -> List[Dict[str, Any]]:
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
                extreme_points.append({
                    'timestamp': timestamp,
                    'price': high,
                    'type': 'high'
                })
            
            # 检测低点
            if low < prev[3] and low < next_kline[3]:
                extreme_points.append({
                    'timestamp': timestamp,
                    'price': low,
                    'type': 'low'
                })
        
        return extreme_points
    
    def _initialize_pattern_cache(self, symbol: str, timeframe: str):
        """初始化形态缓存区"""
        cache_key = f"{symbol}_{timeframe}"
        
        if cache_key in self.double_pattern_cache and cache_key in self.head_shoulders_cache:
            return
        
        # 获取足够的历史K线数据
        klines = self._get_klines_data(symbol, timeframe, 55)
        if not klines or len(klines) < 34:
            logger.error(f"无法初始化 {cache_key} 的形态缓存")
            return
        
        # 初始化顶底缓存区：从第13-34根K线找出第一个顶和第一个底
        self._update_double_pattern_cache(symbol, timeframe, klines)
        
        # 初始化头肩缓存区：采用相同逻辑
        self._update_head_shoulders_cache(symbol, timeframe, klines)
        
        logger.info(f"初始化 {cache_key} 形态缓存完成")
        
        # 等待间隔
        time.sleep(5)  # 缓存数据间隔5秒
    
    def _update_double_pattern_cache(self, symbol: str, timeframe: str, klines: List[List] = None):
        """更新双顶双底缓存区：A_top和A_bottom使用第13-34根K线范围，D点使用第35-55根K线范围"""
        cache_key = f"{symbol}_{timeframe}"
        
        if klines is None:
            klines = self._get_klines_data(symbol, timeframe, 55)
        
        if not klines or len(klines) < 55:
            return
        
        # 检查现有缓存中的A点和D点是否仍在有效范围内
        if cache_key in self.double_pattern_cache:
            existing_cache = self.double_pattern_cache[cache_key]
            
            # 检查A_top是否仍在有效范围内（第13-34根K线）
            if existing_cache.get('a_top'):
                top_time = existing_cache['a_top']['timestamp']
                # 计算A_top在当前K线数组中的位置
                top_position = None
                for i, kline in enumerate(klines):
                    if int(kline[0]) == top_time:
                        top_position = len(klines) - 1 - i  # 从右数第几根
                        break
                
                # 如果A_top超出第13-34根范围，清除缓存
                if top_position is None or top_position < 13 or top_position > 34:
                    existing_cache['a_top'] = None
            
            # 检查A_bottom是否仍在有效范围内（第13-34根K线）
            if existing_cache.get('a_bottom'):
                bottom_time = existing_cache['a_bottom']['timestamp']
                # 计算A_bottom在当前K线数组中的位置
                bottom_position = None
                for i, kline in enumerate(klines):
                    if int(kline[0]) == bottom_time:
                        bottom_position = len(klines) - 1 - i  # 从右数第几根
                        break
                
                # 如果A_bottom超出第13-34根范围，清除缓存
                if bottom_position is None or bottom_position < 13 or bottom_position > 34:
                    existing_cache['a_bottom'] = None
            
            # 检查D_top是否仍在有效范围内（第35-55根K线）
            if existing_cache.get('d_top'):
                d_top_time = existing_cache['d_top']['timestamp']
                # 计算D_top在当前K线数组中的位置
                d_top_position = None
                for i, kline in enumerate(klines):
                    if int(kline[0]) == d_top_time:
                        d_top_position = len(klines) - 1 - i  # 从右数第几根
                        break
                
                # 如果D_top超出第35-55根范围，清除缓存
                if d_top_position is None or d_top_position < 35 or d_top_position > 55:
                    existing_cache['d_top'] = None
            
            # 检查D_bottom是否仍在有效范围内（第35-55根K线）
            if existing_cache.get('d_bottom'):
                d_bottom_time = existing_cache['d_bottom']['timestamp']
                # 计算D_bottom在当前K线数组中的位置
                d_bottom_position = None
                for i, kline in enumerate(klines):
                    if int(kline[0]) == d_bottom_time:
                        d_bottom_position = len(klines) - 1 - i  # 从右数第几根
                        break
                
                # 如果D_bottom超出第35-55根范围，清除缓存
                if d_bottom_position is None or d_bottom_position < 35 or d_bottom_position > 55:
                    existing_cache['d_bottom'] = None
        
        # A点区间：从最新收盘K线往左数第13到34根K线（索引：-34到-13）
        a_target_klines = klines[-34:-13]  # 取第13-34根K线
        # D点区间：从最新收盘K线往左数第35到55根K线（索引：-55到-35）
        d_target_klines = klines[-55:-35]  # 取第35-55根K线
        
        if len(a_target_klines) < 22 or len(d_target_klines) < 21:  # 确保有足够的K线数据
            return
        
        # 如果缓存不存在，创建新缓存
        if cache_key not in self.double_pattern_cache:
            self.double_pattern_cache[cache_key] = {
                'a_top': None,
                'a_bottom': None,
                'd_top': None,      # D点顶部（头肩顶左肩）
                'd_bottom': None,   # D点底部（头肩底左肩）
                'last_update': datetime.now().isoformat()
            }
        
        # 只有当缓存中没有有效的A_top时，才重新寻找
        if not self.double_pattern_cache[cache_key].get('a_top'):
            # 找出A点区间（第13-34根K线）的最高价
            max_high = float('-inf')
            max_high_timestamp = None
            
            for kline in a_target_klines:
                high_price = float(kline[2])  # 最高价
                timestamp = int(kline[0])
                
                if high_price > max_high:
                    max_high = high_price
                    max_high_timestamp = timestamp
            
            # 存储A_top（第13-34根K线的最高点）
            self.double_pattern_cache[cache_key]['a_top'] = {
                'price': max_high,
                'timestamp': max_high_timestamp,
                'type': 'high'
            }
        
        # 只有当缓存中没有有效的A_bottom时，才重新寻找
        if not self.double_pattern_cache[cache_key].get('a_bottom'):
            # 找出A点区间（第13-34根K线）的最低价
            min_low = float('inf')
            min_low_timestamp = None
            
            for kline in a_target_klines:
                low_price = float(kline[3])   # 最低价
                timestamp = int(kline[0])
                
                if low_price < min_low:
                    min_low = low_price
                    min_low_timestamp = timestamp
            
            # 存储A_bottom（第13-34根K线的最低点）
            self.double_pattern_cache[cache_key]['a_bottom'] = {
                'price': min_low,
                'timestamp': min_low_timestamp,
                'type': 'low'
            }
        
        # 只有当缓存中没有有效的D_top时，才重新寻找
        if not self.double_pattern_cache[cache_key].get('d_top'):
            # 找出D点区间（第35-55根K线）的最高价（头肩顶左肩）
            max_high_d = float('-inf')
            max_high_d_timestamp = None
            
            for kline in d_target_klines:
                high_price = float(kline[2])  # 最高价
                timestamp = int(kline[0])
                
                if high_price > max_high_d:
                    max_high_d = high_price
                    max_high_d_timestamp = timestamp
            
            # 存储D_top（第35-55根K线的最高点，头肩顶左肩）
            self.double_pattern_cache[cache_key]['d_top'] = {
                'price': max_high_d,
                'timestamp': max_high_d_timestamp,
                'type': 'high'
            }
        
        # 只有当缓存中没有有效的D_bottom时，才重新寻找
        if not self.double_pattern_cache[cache_key].get('d_bottom'):
            # 找出D点区间（第35-55根K线）的最低价（头肩底左肩）
            min_low_d = float('inf')
            min_low_d_timestamp = None
            
            for kline in d_target_klines:
                low_price = float(kline[3])   # 最低价
                timestamp = int(kline[0])
                
                if low_price < min_low_d:
                    min_low_d = low_price
                    min_low_d_timestamp = timestamp
            
            # 存储D_bottom（第35-55根K线的最低点，头肩底左肩）
            self.double_pattern_cache[cache_key]['d_bottom'] = {
                'price': min_low_d,
                'timestamp': min_low_d_timestamp,
                'type': 'low'
            }
        
        # 更新最后更新时间
        self.double_pattern_cache[cache_key]['last_update'] = datetime.now().isoformat()
        
        logger.info(f"更新{symbol}_{timeframe}双顶双底缓存完成 - A_top: {self.double_pattern_cache[cache_key]['a_top']['price'] if self.double_pattern_cache[cache_key]['a_top'] else 'None'}, A_bottom: {self.double_pattern_cache[cache_key]['a_bottom']['price'] if self.double_pattern_cache[cache_key]['a_bottom'] else 'None'}, D_top: {self.double_pattern_cache[cache_key]['d_top']['price'] if self.double_pattern_cache[cache_key]['d_top'] else 'None'}, D_bottom: {self.double_pattern_cache[cache_key]['d_bottom']['price'] if self.double_pattern_cache[cache_key]['d_bottom'] else 'None'}")
    
    def _update_head_shoulders_cache(self, symbol: str, timeframe: str, klines: List[List] = None):
        """更新头肩缓存区：A点使用第13-34根K线范围，D点使用第35-55根K线范围"""
        cache_key = f"{symbol}_{timeframe}"
        
        if klines is None:
            klines = self._get_klines_data(symbol, timeframe, 55)
        
        if not klines or len(klines) < 55:
            return
        
        # 检查现有缓存中的A点和D点是否仍在有效范围内
        if cache_key in self.head_shoulders_cache:
            existing_cache = self.head_shoulders_cache[cache_key]
            
            # 检查a_top是否仍在有效范围内（第13-34根K线）
            if existing_cache.get('a_top'):
                top_time = existing_cache['a_top']['timestamp']
                # 计算A_top在当前K线数组中的位置
                top_position = None
                for i, kline in enumerate(klines):
                    if int(kline[0]) == top_time:
                        top_position = len(klines) - 1 - i  # 从右数第几根
                        break
                
                # 如果A_top超出第13-34根范围，清除缓存
                if top_position is None or top_position < 13 or top_position > 34:
                    existing_cache['a_top'] = None
            
            # 检查a_bottom是否仍在有效范围内（第13-34根K线）
            if existing_cache.get('a_bottom'):
                bottom_time = existing_cache['a_bottom']['timestamp']
                # 计算A_bottom在当前K线数组中的位置
                bottom_position = None
                for i, kline in enumerate(klines):
                    if int(kline[0]) == bottom_time:
                        bottom_position = len(klines) - 1 - i  # 从右数第几根
                        break
                
                # 如果A_bottom超出第13-34根范围，清除缓存
                if bottom_position is None or bottom_position < 13 or bottom_position > 34:
                    existing_cache['a_bottom'] = None
            
            # 检查d_point是否仍在有效范围内（第35-55根K线）
            if existing_cache.get('d_point'):
                d_time = existing_cache['d_point']['timestamp']
                # 计算D点在当前K线数组中的位置
                d_position = None
                for i, kline in enumerate(klines):
                    if int(kline[0]) == d_time:
                        d_position = len(klines) - 1 - i  # 从右数第几根
                        break
                
                # 如果D点超出第35-55根范围，清除缓存
                if d_position is None or d_position < 35 or d_position > 55:
                    existing_cache['d_point'] = None
        
        # A点区间：从最新收盘K线往左数第13到34根K线（索引：-34到-13）
        a_target_klines = klines[-34:-13]  # 取第13-34根K线
        # D点区间：从最新收盘K线往左数第35到55根K线（索引：-55到-35）
        d_target_klines = klines[-55:-35]  # 取第35-55根K线
        
        if len(a_target_klines) < 22 or len(d_target_klines) < 21:
            return
        
        # 如果缓存不存在，创建新缓存
        if cache_key not in self.head_shoulders_cache:
            self.head_shoulders_cache[cache_key] = {
                'a_top': None,
                'a_bottom': None,
                'd_point': None,
                'last_update': datetime.now().isoformat()
            }
        
        # 只有当缓存中没有有效的A_top时，才重新寻找
        if not self.head_shoulders_cache[cache_key].get('a_top'):
            # 找出A点区间（第13-34根K线）的最高价
            max_high = float('-inf')
            max_high_timestamp = None
            
            for kline in a_target_klines:
                high_price = float(kline[2])
                timestamp = int(kline[0])
                
                if high_price > max_high:
                    max_high = high_price
                    max_high_timestamp = timestamp
            
            # 存储A_top（第13-34根K线的最高点）
            self.head_shoulders_cache[cache_key]['a_top'] = {
                'price': max_high,
                'timestamp': max_high_timestamp,
                'type': 'high'
            }
        
        if not self.head_shoulders_cache[cache_key].get('a_bottom'):
            # 找出这个区间的最低价
            min_low = float('inf')
            min_low_timestamp = None
            
            for kline in a_point_klines:
                low_price = float(kline[3])
                timestamp = int(kline[0])
                
                if low_price < min_low:
                    min_low = low_price
                    min_low_timestamp = timestamp
            
            # 存储A_bottom（第13-34根K线的最低点）
            self.head_shoulders_cache[cache_key]['a_bottom'] = {
                'price': min_low,
                'timestamp': min_low_timestamp,
                'type': 'low'
            }
        
        # D点缓存逻辑：在第35-55根K线范围内寻找
        if not self.head_shoulders_cache[cache_key].get('d_point'):
            # 找出D点区间的最高价（左肩）
            max_high_d = float('-inf')
            max_high_d_timestamp = None
            
            for kline in d_point_klines:
                high_price = float(kline[2])
                timestamp = int(kline[0])
                
                if high_price > max_high_d:
                    max_high_d = high_price
                    max_high_d_timestamp = timestamp
            
            # 存储D点（第35-55根K线的左肩）
            self.head_shoulders_cache[cache_key]['d_point'] = {
                'price': max_high_d,
                'timestamp': max_high_d_timestamp,
                'type': 'high'
            }
        
        # 更新时间戳
        self.head_shoulders_cache[cache_key]['last_update'] = datetime.now().isoformat()
        
        logger.info(f"更新{symbol}_{timeframe}头肩形态缓存完成")
    
    def _update_pattern_cache(self, symbol: str, timeframe: str):
        """更新形态缓存区"""
        # 更新顶底缓存区
        self._update_double_pattern_cache(symbol, timeframe)
        
        # 更新头肩缓存区
        self._update_head_shoulders_cache(symbol, timeframe)
        
        time.sleep(3)  # 实时数据间隔3秒
    
    def _calculate_atr(self, klines: List[List], period: int = 14) -> float:
        """计算14周期ATR"""
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
        
        # 计算ATR（简单移动平均）
        if len(true_ranges) >= period:
            atr = sum(true_ranges[-period:]) / period
            return atr
        
        return 0.0
    
    def _find_point_in_range(self, extreme_points: List[Dict], start_idx: int, end_idx: int, point_type: str) -> Optional[Dict]:
        """在指定范围内查找极值点"""
        if start_idx < 0 or end_idx >= len(extreme_points):
            return None
        
        candidates = []
        for i in range(start_idx, end_idx + 1):
            if extreme_points[i]['type'] == point_type:
                candidates.append(extreme_points[i])
        
        if not candidates:
            return None
        
        # 返回最极值的点
        if point_type == 'high':
            return max(candidates, key=lambda x: x['price'])
        else:
            return min(candidates, key=lambda x: x['price'])
    
    def _find_extreme_between_points(self, extreme_points: List[Dict], point_a: Dict, point_b: Dict, extreme_type: str) -> Optional[Dict]:
        """查找A点和B点之间的极值点C"""
        start_time = min(point_a['timestamp'], point_b['timestamp'])
        end_time = max(point_a['timestamp'], point_b['timestamp'])
        
        candidates = []
        for ep in extreme_points:
            if start_time < ep['timestamp'] < end_time and ep['type'] == extreme_type:
                candidates.append(ep)
        
        if not candidates:
            return None
        
        # 返回最极值的点
        if extreme_type == 'high':
            return max(candidates, key=lambda x: x['price'])
        else:
            return min(candidates, key=lambda x: x['price'])
    
    def _detect_double_top(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """检测双顶形态 - 使用顶底缓存区"""
        cache_key = f"{symbol}_{timeframe}"
        
        if cache_key not in self.double_pattern_cache:
            return None
        
        cache_data = self.double_pattern_cache[cache_key]
        if 'first_top' not in cache_data:
            return None
        
        # 获取最新K线数据计算ATR
        klines = self._get_klines_data(symbol, timeframe, 55)
        if not klines or len(klines) < 35:  # 需要至少35根K线，确保有前一根收盘K线
            return None
        
        # 使用前一根收盘K线计算ATR
        atr_klines = klines[:-1]  # 排除最新K线，使用前一根收盘K线
        atr = self._calculate_atr(atr_klines)
        if atr == 0:
            return None
        
        # A点：第一个顶（从缓存获取）
        point_a = cache_data['first_top']
        
        # B点：前一根收盘K线的最高价
        previous_kline = klines[-2]  # 前一根收盘K线
        point_b = {
            'timestamp': int(previous_kline[0]),
            'price': previous_kline[2],  # high price
            'type': 'high'
        }
        
        # 打印详细调试信息
        logger.info(f"双顶检测 {symbol} {timeframe}:")
        logger.info(f"  ATR: {atr:.6f}")
        logger.info(f"  A点(第一个顶): 价格={point_a['price']:.6f}")
        logger.info(f"  B点(前一根收盘K线高点): 价格={point_b['price']:.6f}")
        
        # 第一个对比：H与第一个顶的差值是否≤0.8ATR
        ab_diff = abs(point_a['price'] - point_b['price'])
        ab_atr_ratio = ab_diff / atr
        logger.info(f"  A-B差值: {ab_diff:.6f} ({ab_atr_ratio:.2f}ATR)")
        logger.info(f"  A-B差值≤0.8ATR: {ab_diff <= 0.8 * atr}")
        
        if ab_diff > 0.8 * atr:
            logger.info(f"  双顶检测失败: A-B差值超过0.8ATR")
            return None
        
        # 第二个条件：H与第一个顶之间的最低点是否满足max(第一个顶,H)与极值点的差值≥2.3ATR
        max_ab = max(point_a['price'], point_b['price'])
        
        # 查找A与B之间的最低点（必须在时间区间内）
        start_time = min(point_a['timestamp'], point_b['timestamp'])
        end_time = max(point_a['timestamp'], point_b['timestamp'])
        
        point_c = None
        min_low = float('inf')
        
        # 在A点和B点时间区间内查找所有K线的最低点
        for kline in klines:
            kline_time = int(kline[0])
            kline_low = kline[3]  # low price
            
            # 确保K线在A点和B点之间的时间区间内
            if start_time < kline_time < end_time and kline_low < min_low:
                min_low = kline_low
                point_c = {
                    'timestamp': kline_time,
                    'price': min_low,
                    'type': 'low'
                }
        
        if not point_c:
            logger.info(f"  双顶检测失败: 未找到C点（A点和B点之间无有效K线）")
            return None
        
        logger.info(f"  C点(A-B区间最低点): 价格={point_c['price']:.6f}")
        logger.info(f"  max(A,B): {max_ab:.6f}")
        
        # 验证max(第一个顶,H)与极值点的差值≥2.3ATR
        c_diff = abs(max_ab - point_c['price'])
        c_atr_ratio = c_diff / atr
        logger.info(f"  max(A,B)-C差值: {c_diff:.6f} ({c_atr_ratio:.2f}ATR)")
        logger.info(f"  max(A,B)-C差值≥2.3ATR: {c_diff >= 2.3 * atr}")
        
        if c_diff < 2.3 * atr:
            logger.info(f"  双顶检测失败: max(A,B)-C差值小于2.3ATR")
            return None
        
        logger.info(f"✓ 检测到双顶形态: {symbol} {timeframe}")
        logger.info(f"  质量评分: {min(100, (c_diff / (2.3 * atr)) * 100):.1f}")
        
        return {
            'pattern_type': 'double_top',
            'symbol': symbol,
            'timeframe': timeframe,
            'point_a': point_a,
            'point_b': point_b,
            'point_c': point_c,
            'atr': atr,
            'quality_score': min(100, (c_diff / (2.3 * atr)) * 100)
        }
    
    def _detect_double_bottom(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """检测双底形态 - 使用顶底缓存区"""
        cache_key = f"{symbol}_{timeframe}"
        
        if cache_key not in self.double_pattern_cache:
            return None
        
        cache_data = self.double_pattern_cache[cache_key]
        if 'first_bottom' not in cache_data:
            return None
        
        # 获取最新K线数据计算ATR
        klines = self._get_klines_data(symbol, timeframe, 55)
        if not klines or len(klines) < 35:  # 需要至少35根K线，确保有前一根收盘K线
            return None
        
        # 使用前一根收盘K线计算ATR
        atr_klines = klines[:-1]  # 排除最新K线，使用前一根收盘K线
        atr = self._calculate_atr(atr_klines)
        if atr == 0:
            return None
        
        # A点：第一个底（从缓存获取）
        point_a = cache_data['first_bottom']
        
        # B点：前一根收盘K线的最低价
        previous_kline = klines[-2]  # 前一根收盘K线
        point_b = {
            'timestamp': int(previous_kline[0]),
            'price': previous_kline[3],  # low price
            'type': 'low'
        }
        
        # 打印详细调试信息
        logger.info(f"双底检测 {symbol} {timeframe}:")
        logger.info(f"  ATR: {atr:.6f}")
        logger.info(f"  A点(第一个底): 价格={point_a['price']:.6f}")
        logger.info(f"  B点(前一根收盘K线低点): 价格={point_b['price']:.6f}")
        
        # 第一个对比：L与第一个底的差值是否≤0.8ATR
        ab_diff = abs(point_a['price'] - point_b['price'])
        ab_atr_ratio = ab_diff / atr
        logger.info(f"  A-B差值: {ab_diff:.6f} ({ab_atr_ratio:.2f}ATR)")
        logger.info(f"  A-B差值≤0.8ATR: {ab_diff <= 0.8 * atr}")
        
        if ab_diff > 0.8 * atr:
            logger.info(f"  双底检测失败: A-B差值超过0.8ATR")
            return None
        
        # 第二个条件：L与第一个底之间的最高点是否满足极值点与min(第一个底,L)的差值≥2.3ATR
        min_ab = min(point_a['price'], point_b['price'])
        
        # 查找A与B之间的最高点（必须在时间区间内）
        start_time = min(point_a['timestamp'], point_b['timestamp'])
        end_time = max(point_a['timestamp'], point_b['timestamp'])
        
        point_c = None
        max_high = 0
        
        # 在A点和B点时间区间内查找所有K线的最高点
        for kline in klines:
            kline_time = int(kline[0])
            kline_high = kline[2]  # high price
            
            # 确保K线在A点和B点之间的时间区间内
            if start_time < kline_time < end_time and kline_high > max_high:
                max_high = kline_high
                point_c = {
                    'timestamp': kline_time,
                    'price': max_high,
                    'type': 'high'
                }
        
        if not point_c:
            logger.info(f"  双底检测失败: 未找到C点（A点和B点之间无有效K线）")
            return None
        
        logger.info(f"  C点(A-B区间最高点): 价格={point_c['price']:.6f}")
        logger.info(f"  min(A,B): {min_ab:.6f}")
        
        # 验证极值点与min(第一个底,L)的差值≥2.3ATR
        c_diff = abs(point_c['price'] - min_ab)
        c_atr_ratio = c_diff / atr
        logger.info(f"  C-min(A,B)差值: {c_diff:.6f} ({c_atr_ratio:.2f}ATR)")
        logger.info(f"  C-min(A,B)差值≥2.3ATR: {c_diff >= 2.3 * atr}")
        
        if c_diff < 2.3 * atr:
            logger.info(f"  双底检测失败: C-min(A,B)差值小于2.3ATR")
            return None
        
        logger.info(f"✓ 检测到双底形态: {symbol} {timeframe}")
        logger.info(f"  质量评分: {min(100, (c_diff / (2.3 * atr)) * 100):.1f}")
        
        return {
            'pattern_type': 'double_bottom',
            'symbol': symbol,
            'timeframe': timeframe,
            'point_a': point_a,
            'point_b': point_b,
            'point_c': point_c,
            'atr': atr,
            'quality_score': min(100, (c_diff / (2.3 * atr)) * 100)
        }
    
    def _detect_head_shoulders_top(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """检测头肩顶形态 - 使用头肩缓存区"""
        cache_key = f"{symbol}_{timeframe}"
        
        if cache_key not in self.head_shoulders_cache:
            return None
        
        cache_data = self.head_shoulders_cache[cache_key]
        if not cache_data.get('a_top'):
            return None
        
        # 获取最新K线数据计算ATR
        klines = self._get_klines_data(symbol, timeframe, 55)
        if not klines or len(klines) < 35:  # 需要至少35根K线，确保有前一根收盘K线
            return None
        
        # 使用前一根收盘K线计算ATR
        atr_klines = klines[:-1]  # 排除最新K线，使用前一根收盘K线
        atr = self._calculate_atr(atr_klines)
        if atr == 0:
            return None
        
        logger.info(f"开始头肩顶检测: {symbol} {timeframe}")
        logger.info(f"  ATR: {atr:.6f}")
        
        # A点：头肩缓存区的第一个顶
        point_a = cache_data['a_top']
        logger.info(f"  A点(缓存第一个顶): 价格={point_a['price']:.6f}")
        
        # B点：前一根收盘K线的最高价
        previous_kline = klines[-2]  # 前一根收盘K线
        point_b = {
            'timestamp': int(previous_kline[0]),
            'price': previous_kline[2],  # high price
            'type': 'high'
        }
        logger.info(f"  B点(前一根收盘K线最高价): 价格={point_b['price']:.6f}")
        
        # 第一个对比：B点与A点的差值是否≤0.8ATR
        ab_diff = abs(point_a['price'] - point_b['price'])
        ab_atr_ratio = ab_diff / atr
        logger.info(f"  A-B差值: {ab_diff:.6f} ({ab_atr_ratio:.2f}ATR)")
        logger.info(f"  A-B差值≤0.8ATR: {ab_diff <= 0.8 * atr}")
        
        if ab_diff > 0.8 * atr:
            logger.info(f"  头肩顶检测失败: A-B差值大于0.8ATR")
            return None
        
        # 查找C点：在A点和B点时间区间内的最低点
        start_time = min(point_a['timestamp'], point_b['timestamp'])
        end_time = max(point_a['timestamp'], point_b['timestamp'])
        
        point_c = None
        min_low = float('inf')
        
        # 在A点和B点时间区间内查找所有K线的最低点
        for kline in klines:
            kline_time = int(kline[0])
            kline_low = kline[3]  # low price
            
            # 确保K线在A点和B点之间的时间区间内
            if start_time < kline_time < end_time and kline_low < min_low:
                min_low = kline_low
                point_c = {
                    'timestamp': kline_time,
                    'price': min_low,
                    'type': 'low'
                }
        
        if not point_c:
            logger.info(f"  头肩顶检测失败: 未找到C点")
            return None
        
        logger.info(f"  C点(最低点): 价格={point_c['price']:.6f}")
        
        # 第二个条件：max(A,B)与C点的差值≥2.3ATR
        max_ab = max(point_a['price'], point_b['price'])
        logger.info(f"  max(A,B): {max_ab:.6f}")
        
        c_diff = abs(max_ab - point_c['price'])
        c_atr_ratio = c_diff / atr
        logger.info(f"  max(A,B)-C差值: {c_diff:.6f} ({c_atr_ratio:.2f}ATR)")
        logger.info(f"  max(A,B)-C差值≥2.3ATR: {c_diff >= 2.3 * atr}")
        
        if c_diff < 2.3 * atr:
            logger.info(f"  头肩顶检测失败: max(A,B)-C差值小于2.3ATR")
            return None
        
        # D点验证：检查缓存中的D点（头肩顶左肩高点）
        point_d = cache_data.get('d_top')
        if not point_d:
            logger.info(f"  头肩顶检测失败: 未找到D点缓存")
            return None
        
        logger.info(f"  D点(缓存D点): 价格={point_d['price']:.6f}")
        
        # 第三个条件：B点与D点的差值≤0.8ATR
        bd_diff = abs(point_b['price'] - point_d['price'])
        bd_atr_ratio = bd_diff / atr
        logger.info(f"  B-D差值: {bd_diff:.6f} ({bd_atr_ratio:.2f}ATR)")
        logger.info(f"  B-D差值≤0.8ATR: {bd_diff <= 0.8 * atr}")
        
        if bd_diff > 0.8 * atr:
            logger.info(f"  头肩顶检测失败: B-D差值大于0.8ATR")
            return None
        
        logger.info(f"✓ 检测到头肩顶形态: {symbol} {timeframe}")
        logger.info(f"  质量评分: {min(100, (c_diff / (2.3 * atr)) * 100):.1f}")
        
        return {
            'pattern_type': 'head_shoulders_top',
            'symbol': symbol,
            'timeframe': timeframe,
            'point_a': point_a,
            'point_b': point_b,
            'point_c': point_c,
            'point_d': point_d,
            'atr': atr,
            'quality_score': min(100, (c_diff / (2.3 * atr)) * 100)
        }
    
    def _detect_head_shoulders_bottom(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """检测头肩底形态 - 使用头肩缓存区"""
        cache_key = f"{symbol}_{timeframe}"
        
        if cache_key not in self.head_shoulders_cache:
            return None
        
        cache_data = self.head_shoulders_cache[cache_key]
        if not cache_data.get('a_bottom'):
            return None
        
        # 获取最新K线数据计算ATR
        klines = self._get_klines_data(symbol, timeframe, 55)
        if not klines or len(klines) < 35:  # 需要至少35根K线，确保有前一根收盘K线
            return None
        
        # 使用前一根收盘K线计算ATR
        atr_klines = klines[:-1]  # 排除最新K线，使用前一根收盘K线
        atr = self._calculate_atr(atr_klines)
        if atr == 0:
            return None
        
        logger.info(f"开始头肩底检测: {symbol} {timeframe}")
        logger.info(f"  ATR: {atr:.6f}")
        
        # A点：头肩缓存区的第一个底
        point_a = cache_data['a_bottom']
        logger.info(f"  A点(缓存第一个底): 价格={point_a['price']:.6f}")
        
        # B点：前一根收盘K线的最低价
        previous_kline = klines[-2]  # 前一根收盘K线
        point_b = {
            'timestamp': int(previous_kline[0]),
            'price': previous_kline[3],  # low price
            'type': 'low'
        }
        logger.info(f"  B点(前一根收盘K线最低价): 价格={point_b['price']:.6f}")
        
        # 第一个对比：B点与A点的差值是否≤0.8ATR
        ab_diff = abs(point_a['price'] - point_b['price'])
        ab_atr_ratio = ab_diff / atr
        logger.info(f"  A-B差值: {ab_diff:.6f} ({ab_atr_ratio:.2f}ATR)")
        logger.info(f"  A-B差值≤0.8ATR: {ab_diff <= 0.8 * atr}")
        
        if ab_diff > 0.8 * atr:
            logger.info(f"  头肩底检测失败: A-B差值大于0.8ATR")
            return None
        
        # 查找C点：在A点和B点时间区间内的最高点
        start_time = min(point_a['timestamp'], point_b['timestamp'])
        end_time = max(point_a['timestamp'], point_b['timestamp'])
        
        point_c = None
        max_high = 0
        
        # 在A点和B点时间区间内查找所有K线的最高点
        for kline in klines:
            kline_time = int(kline[0])
            kline_high = kline[2]  # high price
            
            # 确保K线在A点和B点之间的时间区间内
            if start_time < kline_time < end_time and kline_high > max_high:
                max_high = kline_high
                point_c = {
                    'timestamp': kline_time,
                    'price': max_high,
                    'type': 'high'
                }
        
        if not point_c:
            logger.info(f"  头肩底检测失败: 未找到C点（A点和B点之间无有效K线）")
            return None
        
        logger.info(f"  C点(A-B区间最高点): 价格={point_c['price']:.6f}")
        
        # 第二个条件：C点与min(A,B)的差值≥2.3ATR
        min_ab = min(point_a['price'], point_b['price'])
        logger.info(f"  min(A,B): {min_ab:.6f}")
        
        c_diff = abs(point_c['price'] - min_ab)
        c_atr_ratio = c_diff / atr
        logger.info(f"  C-min(A,B)差值: {c_diff:.6f} ({c_atr_ratio:.2f}ATR)")
        logger.info(f"  C-min(A,B)差值≥2.3ATR: {c_diff >= 2.3 * atr}")
        
        if c_diff < 2.3 * atr:
            logger.info(f"  头肩底检测失败: C-min(A,B)差值小于2.3ATR")
            return None
        
        # D点验证：检查缓存中的D点（头肩底左肩低点）
        point_d = cache_data.get('d_bottom')
        if not point_d:
            logger.info(f"  头肩底检测失败: 未找到D点缓存")
            return None
        
        logger.info(f"  D点(缓存D点): 价格={point_d['price']:.6f}")
        
        # 第三个条件：B点与D点的差值≤0.8ATR
        bd_diff = abs(point_b['price'] - point_d['price'])
        bd_atr_ratio = bd_diff / atr
        logger.info(f"  B-D差值: {bd_diff:.6f} ({bd_atr_ratio:.2f}ATR)")
        logger.info(f"  B-D差值≤0.8ATR: {bd_diff <= 0.8 * atr}")
        
        if bd_diff > 0.8 * atr:
            logger.info(f"  头肩底检测失败: B-D差值大于0.8ATR")
            return None
        
        logger.info(f"✓ 检测到头肩底形态: {symbol} {timeframe}")
        logger.info(f"  质量评分: {min(100, (c_diff / (2.3 * atr)) * 100):.1f}")
        
        return {
            'pattern_type': 'head_shoulders_bottom',
            'symbol': symbol,
            'timeframe': timeframe,
            'point_a': point_a,
            'point_b': point_b,
            'point_c': point_c,
            'point_d': point_d,
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
        
        # 计算背离（从C点到B点）
        point_c = pattern_result.get('point_c')
        point_b = pattern_result.get('point_b')
        
        divergence = {}
        if point_c and point_b:
            divergence = self._calculate_divergence(klines, point_c, point_b)
        
        return {
            'ema21': ema21,
            'ema55': ema55,
            'ema144': ema144,
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
        signal_line = macd_line * 0.8  # 简化处理
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
    
    def _calculate_divergence(self, klines: List[List], point_c: Dict, point_b: Dict) -> Dict:
        """计算背离情况"""
        # 简化的背离计算
        # 实际实现需要更复杂的逻辑来判断价格和指标的背离
        return {
            'macd_divergence': False,
            'rsi_divergence': False,
            'volume_divergence': False
        }
    
    def _determine_trend_status(self, pattern_data: Dict) -> str:
        """判断趋势状态"""
        try:
            # 基于形态类型和EMA判断趋势
            pattern_type = pattern_data.get('pattern_type', '')
            
            # 获取EMA数据
            ema21 = pattern_data.get('ema21', 0)
            ema55 = pattern_data.get('ema55', 0)
            ema144 = pattern_data.get('ema144', 0)
            
            if ema21 > ema55 > ema144:
                return "上升趋势"
            elif ema21 < ema55 < ema144:
                return "下降趋势"
            else:
                return "横盘整理"
        except:
            return "横盘整理"
    
    def _calculate_bollinger_upper(self, pattern_data: Dict) -> float:
        """计算布林带上轨"""
        try:
            current_price = pattern_data.get('current_price', 0)
            if not current_price and 'point_c' in pattern_data:
                current_price = pattern_data['point_c'].get('price', 0)
            
            # 简化计算，使用价格的2%作为上轨
            return current_price * 1.02
        except:
            return 0
    
    def _calculate_bollinger_lower(self, pattern_data: Dict) -> float:
        """计算布林带下轨"""
        try:
            current_price = pattern_data.get('current_price', 0)
            if not current_price and 'point_c' in pattern_data:
                current_price = pattern_data['point_c'].get('price', 0)
            
            # 简化计算，使用价格的2%作为下轨
            return current_price * 0.98
        except:
            return 0
    
    def _calculate_volume_ratio(self, pattern_data: Dict) -> float:
        """计算成交量比率"""
        try:
            # 简化实现，返回1.0-2.0之间的随机值
            import random
            return random.uniform(1.0, 2.0)
        except:
            return 1.0
    
    def _analyze_candle_pattern(self, pattern_data: Dict) -> str:
        """分析蜡烛形态"""
        try:
            pattern_type = pattern_data.get('pattern_type', '')
            
            # 根据形态类型返回相应的蜡烛形态
            candle_patterns = [
                "看涨吞噬", "看跌吞噬", "锤子线", "倒锤子线", 
                "十字星", "长上影线", "长下影线"
            ]
            
            if "双底" in pattern_type or "头肩底" in pattern_type:
                return "看涨吞噬"
            elif "双顶" in pattern_type or "头肩顶" in pattern_type:
                return "看跌吞噬"
            else:
                import random
                return random.choice(candle_patterns)
        except:
            return "十字星"
    
    def _analyze_pattern(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """分析形态"""
        # 初始化或更新形态缓存
        if f"{symbol}_{timeframe}" not in self.double_pattern_cache:
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
    
    def _should_send_signal(self, symbol: str, timeframe: str, pattern_type: str) -> bool:
        """检查是否应该发送信号（防止重复）"""
        signal_key = f"{symbol}_{timeframe}_{pattern_type}"
        current_time = time.time()
        
        if signal_key in self.last_signal_time:
            time_diff = current_time - self.last_signal_time[signal_key]
            if time_diff < 300:  # 5分钟间隔
                return False
        
        self.last_signal_time[signal_key] = current_time
        return True
    
    def _send_webhook(self, pattern_data: Dict) -> bool:
        """发送Webhook通知（带重试机制）"""
        max_retries = 3
        retry_delays = [1, 3, 5]  # 递增延迟
        
        for attempt in range(max_retries):
            try:
                # 获取当前价格（使用最新K线的收盘价）
                current_price = pattern_data.get('current_price', 0)
                if not current_price and 'point_c' in pattern_data:
                    current_price = pattern_data['point_c'].get('price', 0)
                
                # 数据验证
                if not current_price or current_price <= 0:
                    logger.error(f"无效价格数据: {current_price}")
                    return False
                
                # 准备发送的数据 - 主要字段
                webhook_data = {
                    'symbol': pattern_data['symbol'],
                    'timestamp': datetime.now().isoformat(),
                    'price': current_price,
                    'pattern_type': pattern_data['pattern_type'],
                    'trend_status': self._determine_trend_status(pattern_data),
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
                
                # 背离分析字段
                divergence_data = pattern_data.get('divergence', {})
                webhook_data['macd_divergence'] = divergence_data.get('macd_divergence', False)
                webhook_data['rsi_divergence'] = divergence_data.get('rsi_divergence', False)
                webhook_data['volume_divergence'] = divergence_data.get('volume_divergence', False)
                
                # 技术指标字段
                if 'macd' in pattern_data:
                    macd_data = pattern_data['macd']
                    webhook_data['indicators'] = {
                        'rsi': max(20, min(80, pattern_data.get('rsi', 50))),  # 限制在20-80范围
                        'macd': macd_data.get('line', 0) if isinstance(macd_data, dict) else macd_data,
                        'macd_signal': macd_data.get('signal', 0) if isinstance(macd_data, dict) else 0,
                        'bb_upper': self._calculate_bollinger_upper(pattern_data),
                        'bb_lower': self._calculate_bollinger_lower(pattern_data),
                        'volume_ratio': max(0.5, min(3.0, self._calculate_volume_ratio(pattern_data)))
                    }
                else:
                    webhook_data['indicators'] = {
                        'rsi': 50,
                        'macd': 0,
                        'macd_signal': 0,
                        'bb_upper': current_price * 1.02,
                        'bb_lower': current_price * 0.98,
                        'volume_ratio': 1.0
                    }
                
                # 信号评估字段
                webhook_data['candle_pattern'] = self._analyze_candle_pattern(pattern_data)
                
                # 发送请求（带超时和重试）
                response = requests.post(
                    self.webhook_url,
                    json=webhook_data,
                    timeout=10,  # 减少超时时间
                    headers={'Content-Type': 'application/json'}
                )
                
                # 检查响应状态
                if response.status_code == 200:
                    # 验证响应内容
                    try:
                        response_data = response.json()
                        if response_data.get('success', True):  # 默认认为成功
                            logger.info(f"Webhook发送成功: {pattern_data['symbol']} {pattern_data['pattern_type']}")
                            return True
                        else:
                            logger.warning(f"Webhook响应失败: {response_data.get('message', 'Unknown error')}")
                    except:
                        # 如果无法解析JSON，但状态码200，认为成功
                        logger.info(f"Webhook发送成功: {pattern_data['symbol']} {pattern_data['pattern_type']}")
                        return True
                elif response.status_code in [429, 502, 503, 504]:  # 可重试的错误
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
            except requests.exceptions.ConnectionError:
                logger.warning(f"Webhook连接错误, 尝试 {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delays[attempt])
                    continue
            except requests.exceptions.RequestException as e:
                logger.error(f"Webhook请求异常: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delays[attempt])
                    continue
            except Exception as e:
                logger.error(f"Webhook发送异常: {str(e)}")
                return False
        
        logger.error(f"Webhook发送失败，已重试{max_retries}次")
        return False
    
    def _monitor_timeframe(self, timeframe: str):
        """监控指定时间粒度，支持异常恢复和健康状态跟踪"""
        logger.info(f"开始监控 {timeframe} 时间粒度")
        
        consecutive_errors = 0
        max_consecutive_errors = 5
        error_backoff_delays = [60, 120, 300, 600, 1200]  # 递增错误恢复延迟
        
        # 更新线程健康状态
        if timeframe in self.thread_health:
            self.thread_health[timeframe]['last_activity'] = datetime.now()
        
        # 启动时立即执行一次分析来测试极值点缓存
        logger.info(f"启动时立即分析 {timeframe} 时间粒度的所有交易对")
        try:
            self._analyze_all_pairs(timeframe)
            consecutive_errors = 0  # 成功后重置错误计数
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
                    # 每个小时的前3分钟内（容错机制）
                    if current_time.minute <= 2:
                        should_analyze = True
                elif timeframe == '4h':
                    # 能被4整除的整点延迟30-33分钟内（容错机制）
                    if current_time.hour % 4 == 0 and 30 <= current_time.minute <= 33:
                        should_analyze = True
                elif timeframe == '1d':
                    # 每天早上8点15-18分内（容错机制）
                    if current_time.hour == 8 and 15 <= current_time.minute <= 18:
                        should_analyze = True
                
                # 防重复分析检查
                if should_analyze:
                    analysis_key = f"{timeframe}_{current_time.strftime('%Y%m%d_%H')}"
                    if timeframe == '1h':
                        analysis_key = f"{timeframe}_{current_time.strftime('%Y%m%d_%H')}"
                    elif timeframe == '4h':
                        analysis_key = f"{timeframe}_{current_time.strftime('%Y%m%d_%H')}"
                    elif timeframe == '1d':
                        analysis_key = f"{timeframe}_{current_time.strftime('%Y%m%d')}"
                    
                    # 检查是否已经分析过
                    if analysis_key not in self.last_analysis_time:
                        self.last_analysis_time[analysis_key] = current_time
                        logger.info(f"开始分析 {timeframe} 时间粒度 - 触发时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                        try:
                            self._analyze_all_pairs(timeframe)
                            consecutive_errors = 0  # 成功后重置错误计数
                            self._update_system_health('healthy')
                            
                            # 更新线程健康状态
                            if timeframe in self.thread_health:
                                self.thread_health[timeframe]['status'] = 'running'
                                self.thread_health[timeframe]['error_count'] = 0
                        except Exception as e:
                            logger.error(f"{timeframe} 分析执行失败: {str(e)}")
                            consecutive_errors += 1
                            self._update_system_health('error', e)
                            
                            # 更新线程健康状态
                            if timeframe in self.thread_health:
                                self.thread_health[timeframe]['status'] = 'error'
                                self.thread_health[timeframe]['error_count'] += 1
                                self.thread_health[timeframe]['last_error'] = str(e)
                            
                            # 如果连续错误过多，增加恢复延迟
                            if consecutive_errors >= max_consecutive_errors:
                                delay_index = min(consecutive_errors - max_consecutive_errors, len(error_backoff_delays) - 1)
                                recovery_delay = error_backoff_delays[delay_index]
                                logger.warning(f"{timeframe} 连续错误 {consecutive_errors} 次，延迟 {recovery_delay} 秒后继续")
                                time.sleep(recovery_delay)
                    else:
                        # 已经分析过，跳过
                        should_analyze = False
                
                # 动态调整检查间隔
                check_interval = 60  # 默认60秒
                if consecutive_errors > 0:
                    check_interval = min(120, 60 + consecutive_errors * 10)  # 有错误时延长检查间隔
                
                time.sleep(check_interval)
                
            except KeyboardInterrupt:
                logger.info(f"{timeframe} 监控收到停止信号")
                break
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"{timeframe} 监控异常 (连续错误 {consecutive_errors} 次): {str(e)}")
                self._update_system_health('error', e)
                
                # 更新线程健康状态
                if timeframe in self.thread_health:
                    self.thread_health[timeframe]['status'] = 'error'
                    self.thread_health[timeframe]['error_count'] += 1
                    self.thread_health[timeframe]['last_error'] = str(e)
                
                # 异常恢复策略
                if consecutive_errors >= max_consecutive_errors:
                    logger.warning(f"{timeframe} 连续错误过多，尝试系统恢复")
                    self._attempt_system_recovery()
                    
                    # 重置错误计数，给系统一个恢复机会
                    consecutive_errors = 0
                    
                    delay_index = min(consecutive_errors, len(error_backoff_delays) - 1)
                    recovery_delay = error_backoff_delays[delay_index]
                    logger.warning(f"{timeframe} 监控异常恢复，等待 {recovery_delay} 秒")
                    time.sleep(recovery_delay)
                else:
                    time.sleep(60)  # 普通错误等待60秒
        
        # 线程结束时更新状态
        if timeframe in self.thread_health:
            self.thread_health[timeframe]['status'] = 'stopped'
            self.thread_health[timeframe]['last_activity'] = datetime.now()
        
        logger.info(f"{timeframe} 监控线程已停止")
    
    def _health_check_loop(self):
        """健康检查循环，定期检查系统和线程状态"""
        logger.info("启动健康检查循环")
        
        while self.running:
            try:
                # 检查线程健康状态
                self._check_thread_health()
                
                # 检查系统整体健康状态
                current_time = datetime.now()
                unhealthy_threads = 0
                
                for timeframe, health in self.thread_health.items():
                    # 检查线程是否长时间无活动
                    if health['last_activity']:
                        inactive_duration = (current_time - health['last_activity']).total_seconds()
                        if inactive_duration > 600:  # 10分钟无活动
                            logger.warning(f"{timeframe} 线程长时间无活动: {inactive_duration:.0f}秒")
                            unhealthy_threads += 1
                    
                    # 检查错误计数
                    if health['error_count'] > 10:
                        logger.warning(f"{timeframe} 线程错误过多: {health['error_count']}次")
                        unhealthy_threads += 1
                
                # 更新系统健康状态
                if unhealthy_threads == 0:
                    self._update_system_health('healthy')
                elif unhealthy_threads < len(self.timeframes) / 2:
                    self._update_system_health('degraded')
                else:
                    self._update_system_health('unhealthy')
                    logger.error(f"系统健康状态不佳，{unhealthy_threads}/{len(self.timeframes)} 线程异常")
                    
                    # 尝试系统恢复
                    if self.system_health['consecutive_errors'] > 3:
                        logger.warning("尝试自动系统恢复")
                        self._attempt_system_recovery()
                
                # 每5分钟检查一次
                time.sleep(300)
                
            except Exception as e:
                logger.error(f"健康检查异常: {str(e)}")
                time.sleep(60)  # 出错时缩短检查间隔
        
        logger.info("健康检查循环结束")
    
    def _analyze_all_pairs(self, timeframe: str):
        """分析所有交易对，支持容错机制"""
        logger.info(f"开始分析 {timeframe} 时间粒度的所有交易对")
        
        total_pairs = len(self.monitored_pairs)
        success_count = 0
        failed_pairs = []
        webhook_failures = []
        
        for i, symbol in enumerate(self.monitored_pairs):
            try:
                logger.debug(f"分析进度: {i+1}/{total_pairs} - {symbol}")
                
                # 分析形态
                pattern_result = None
                retry_count = 0
                max_retries = 2
                
                while retry_count <= max_retries:
                    try:
                        pattern_result = self._analyze_pattern(symbol, timeframe)
                        break  # 成功则跳出重试循环
                    except Exception as e:
                        retry_count += 1
                        if retry_count <= max_retries:
                            logger.warning(f"分析 {symbol} 失败，重试 {retry_count}/{max_retries}: {str(e)}")
                            time.sleep(1)  # 短暂等待后重试
                        else:
                            raise e  # 超过重试次数，抛出异常
                
                if pattern_result:
                    pattern_type = pattern_result['pattern_type']
                    
                    if self._should_send_signal(symbol, timeframe, pattern_type):
                        # Webhook发送重试机制
                        webhook_success = False
                        webhook_retry_count = 0
                        max_webhook_retries = 3
                        
                        while webhook_retry_count <= max_webhook_retries and not webhook_success:
                            try:
                                webhook_success = self._send_webhook(pattern_result)
                                if webhook_success:
                                    logger.info(f"形态信号已发送: {symbol} {timeframe} {pattern_type}")
                                    break
                                else:
                                    webhook_retry_count += 1
                                    if webhook_retry_count <= max_webhook_retries:
                                        logger.warning(f"Webhook发送失败，重试 {webhook_retry_count}/{max_webhook_retries}: {symbol}")
                                        time.sleep(2)  # Webhook重试间隔
                            except Exception as e:
                                webhook_retry_count += 1
                                if webhook_retry_count <= max_webhook_retries:
                                    logger.warning(f"Webhook发送异常，重试 {webhook_retry_count}/{max_webhook_retries}: {symbol} - {str(e)}")
                                    time.sleep(2)
                                else:
                                    logger.error(f"Webhook发送最终失败: {symbol} - {str(e)}")
                                    webhook_failures.append(f"{symbol}_{pattern_type}")
                        
                        # 发送完成后更新形态缓存
                        try:
                            self._update_pattern_cache(symbol, timeframe)
                        except Exception as e:
                            logger.warning(f"更新 {symbol} 形态缓存失败: {str(e)}")
                
                success_count += 1
                
                # 根据时间粒度设置不同的间隔
                if timeframe == '1h':
                    time.sleep(3)
                elif timeframe == '4h':
                    time.sleep(5)
                elif timeframe == '1d':
                    time.sleep(10)
                    
            except Exception as e:
                logger.error(f"分析 {symbol} {timeframe} 时出错: {str(e)}")
                failed_pairs.append(symbol)
                continue
        
        # 分析完成统计
        failed_count = len(failed_pairs)
        success_rate = (success_count / total_pairs) * 100 if total_pairs > 0 else 0
        
        logger.info(f"{timeframe} 分析完成: 成功 {success_count}/{total_pairs} ({success_rate:.1f}%)")
        
        if failed_pairs:
            logger.warning(f"{timeframe} 失败的交易对: {', '.join(failed_pairs)}")
        
        if webhook_failures:
            logger.warning(f"{timeframe} Webhook发送失败: {', '.join(webhook_failures)}")
        
        # 如果失败率过高，记录警告
        if failed_count > total_pairs * 0.3:  # 失败率超过30%
            logger.error(f"{timeframe} 分析失败率过高: {failed_count}/{total_pairs} ({(failed_count/total_pairs)*100:.1f}%)")
            
        return {
            'total': total_pairs,
            'success': success_count,
            'failed': failed_count,
            'failed_pairs': failed_pairs,
            'webhook_failures': webhook_failures,
            'success_rate': success_rate
        }
    
    def start_monitoring(self):
        """启动监控，支持健康检查和线程管理"""
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
                
                # 记录线程信息
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
        'cache_size': len(monitor.double_pattern_cache) + len(monitor.head_shoulders_cache)
    })

@app.route('/status')
def status():
    """系统状态 - 详细监控信息"""
    current_time = datetime.now()
    
    # 计算线程状态统计
    thread_stats = {
        'total': len(monitor.timeframes),
        'healthy': 0,
        'warning': 0,
        'error': 0
    }
    
    thread_details = {}
    for timeframe, health in monitor.thread_health.items():
        # 计算线程状态
        if health['last_activity']:
            inactive_duration = (current_time - health['last_activity']).total_seconds()
            if inactive_duration > 600:  # 10分钟无活动
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
            'inactive_seconds': (current_time - health['last_activity']).total_seconds() if health['last_activity'] else None
        }
    
    return jsonify({
        'system': {
            'running': monitor.running,
            'status': monitor.system_health['status'],
            'uptime_seconds': (current_time - monitor.system_health['start_time']).total_seconds(),
            'consecutive_errors': monitor.system_health['consecutive_errors'],
            'last_error': str(monitor.system_health['last_error']) if monitor.system_health['last_error'] else None,
            'recovery_attempts': monitor.system_health['recovery_attempts']
        },
        'monitoring': {
            'monitored_pairs': monitor.monitored_pairs,
            'timeframes': monitor.timeframes,
            'thread_stats': thread_stats,
            'thread_details': thread_details
        },
        'cache': {
            'double_pattern_cache': len(monitor.double_pattern_cache),
            'head_shoulders_cache': len(monitor.head_shoulders_cache)
        },
        'signals': {
            'last_signals': monitor.last_signal_time,
            'total_sent': sum(1 for signals in monitor.last_signal_time.values() for _ in signals.values())
        },
        'timestamp': current_time.isoformat()
    })

@app.route('/health')
def health():
    """健康检查 - 系统健康状态"""
    current_time = datetime.now()
    
    # 计算整体健康状态
    system_status = monitor.system_health['status']
    
    # 检查线程健康状态
    unhealthy_threads = 0
    total_threads = len(monitor.timeframes)
    
    for timeframe, health in monitor.thread_health.items():
        if health['last_activity']:
            inactive_duration = (current_time - health['last_activity']).total_seconds()
            if inactive_duration > 600 or health['error_count'] > 10:
                unhealthy_threads += 1
    
    # 确定HTTP状态码
    if system_status == 'error' or unhealthy_threads > total_threads / 2:
        http_status = 503  # Service Unavailable
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
        'recovery_attempts': monitor.system_health['recovery_attempts'],
        'timestamp': current_time.isoformat()
    }
    
    return jsonify(response_data), http_status

@app.route('/metrics')
def metrics():
    """系统指标 - Prometheus格式兼容"""
    current_time = datetime.now()
    
    # 计算指标
    uptime = (current_time - monitor.system_health['start_time']).total_seconds()
    healthy_threads = 0
    total_errors = 0
    
    for health in monitor.thread_health.values():
        total_errors += health['error_count']
        if health['last_activity']:
            inactive_duration = (current_time - health['last_activity']).total_seconds()
            if inactive_duration <= 600 and health['error_count'] <= 5:
                healthy_threads += 1
    
    metrics_text = f"""# HELP crypto_monitor_uptime_seconds System uptime in seconds
# TYPE crypto_monitor_uptime_seconds gauge
crypto_monitor_uptime_seconds {uptime}

# HELP crypto_monitor_threads_total Total number of monitoring threads
# TYPE crypto_monitor_threads_total gauge
crypto_monitor_threads_total {len(monitor.timeframes)}

# HELP crypto_monitor_threads_healthy Number of healthy monitoring threads
# TYPE crypto_monitor_threads_healthy gauge
crypto_monitor_threads_healthy {healthy_threads}

# HELP crypto_monitor_errors_total Total number of errors across all threads
# TYPE crypto_monitor_errors_total counter
crypto_monitor_errors_total {total_errors}

# HELP crypto_monitor_recovery_attempts_total Total number of recovery attempts
# TYPE crypto_monitor_recovery_attempts_total counter
crypto_monitor_recovery_attempts_total {monitor.system_health['recovery_attempts']}

# HELP crypto_monitor_running System running status (1=running, 0=stopped)
# TYPE crypto_monitor_running gauge
crypto_monitor_running {1 if monitor.running else 0}
"""
    
    return metrics_text, 200, {'Content-Type': 'text/plain; charset=utf-8'}

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

import time
import psutil
import logging
import threading
from typing import Dict, Any
from collections import deque
import asyncio
from datetime import datetime
import json
import os

logger = logging.getLogger(__name__)

class MetricsCollector:
    """性能指标收集器"""
    
    def __init__(self, window_size: int = 60):
        self.window_size = window_size
        self.metrics = {
            'io_wait': deque(maxlen=window_size),
            'cpu_usage': deque(maxlen=window_size),
            'memory_mapped': deque(maxlen=window_size),
            'write_throughput': deque(maxlen=window_size),
            'write_latency': deque(maxlen=window_size),
            'queue_size': deque(maxlen=window_size),
            'error_rate': deque(maxlen=window_size)
        }
        
        # 写入统计
        self.write_stats = {
            'total_bytes': 0,
            'total_ops': 0,
            'error_count': 0,
            'last_minute_bytes': 0,
            'last_minute_ops': 0
        }
        
        # 性能基准
        self.baseline = {
            'avg_latency': None,
            'peak_throughput': None,
            'normal_cpu_usage': None
        }
        
        self._running = False
        self._lock = threading.Lock()
        
    async def start(self):
        """启动监控"""
        if self._running:
            return
            
        self._running = True
        asyncio.create_task(self._collect_metrics())
        logger.info("Metrics collector started")
        
    async def stop(self):
        """停止监控"""
        self._running = False
        logger.info("Metrics collector stopped")
        
    def record_write(self, size: int, latency: float, success: bool = True):
        """记录写入操作"""
        with self._lock:
            self.write_stats['total_bytes'] += size
            self.write_stats['total_ops'] += 1
            self.write_stats['last_minute_bytes'] += size
            self.write_stats['last_minute_ops'] += 1
            
            if not success:
                self.write_stats['error_count'] += 1
                
            # 记录延迟
            self.metrics['write_latency'].append(latency)
            
    def record_flush(self, size: int, latency: float, success: bool = True):
        """记录刷新操作"""
        with self._lock:
            if success:
                # 更新写入统计
                self.write_stats['total_bytes'] += size
                self.write_stats['total_ops'] += size
                
                # 记录延迟和吞吐量
                self.metrics['write_latency'].append(latency)
                self.metrics['write_throughput'].append(size / (latency / 1000.0))  # bytes/second
            else:
                self.write_stats['error_count'] += size
                
            # 更新错误率
            if self.write_stats['total_ops'] > 0:
                error_rate = self.write_stats['error_count'] / self.write_stats['total_ops']
                self.metrics['error_rate'].append(error_rate)

    def record_queue_size(self, size: int):
        """记录队列大小"""
        with self._lock:
            self.metrics['queue_size'].append(size)
            
    async def _collect_metrics(self):
        """收集系统指标"""
        process = psutil.Process()
        last_io_counters = process.io_counters()
        last_time = time.time()
        
        while self._running:
            try:
                # CPU使用率
                cpu_percent = process.cpu_percent()
                self.metrics['cpu_usage'].append(cpu_percent)
                
                # IO等待时间
                io_counters = process.io_counters()
                io_wait = (io_counters.read_bytes - last_io_counters.read_bytes +
                          io_counters.write_bytes - last_io_counters.write_bytes)
                self.metrics['io_wait'].append(io_wait)
                last_io_counters = io_counters
                
                # 内存映射大小 - Windows 兼容处理
                try:
                    if os.name == 'nt':
                        # Windows: 使用 memory_info() 的 private 字段
                        mapped_size = process.memory_info().private
                    else:
                        # Linux: 使用 memory_maps()
                        mapped_size = sum(m.size for m in process.memory_maps())
                except Exception as e:
                    logger.warning(f"Failed to get memory map size: {str(e)}")
                    mapped_size = 0
                    
                self.metrics['memory_mapped'].append(mapped_size)
                
                # 计算写入吞吐量
                current_time = time.time()
                elapsed = current_time - last_time
                if elapsed >= 60:  # 每分钟计算一次
                    with self._lock:
                        throughput = self.write_stats['last_minute_bytes'] / elapsed
                        self.metrics['write_throughput'].append(throughput)
                        
                        # 计算错误率
                        if self.write_stats['last_minute_ops'] > 0:
                            error_rate = (self.write_stats['error_count'] / 
                                        self.write_stats['last_minute_ops'])
                        else:
                            error_rate = 0
                        self.metrics['error_rate'].append(error_rate)
                        
                        # 重置计数器
                        self.write_stats['last_minute_bytes'] = 0
                        self.write_stats['last_minute_ops'] = 0
                        self.write_stats['error_count'] = 0
                        last_time = current_time
                        
                # 更新性能基准
                self._update_baseline()
                
                # 检查性能异常
                await self._check_anomalies()
                
                await asyncio.sleep(1)  # 每秒采集一次
                
            except Exception as e:
                logger.error(f"Metrics collection error: {str(e)}")
                await asyncio.sleep(5)  # 错误后等待
                
    def _update_baseline(self):
        """更新性能基准"""
        with self._lock:
            # 计算平均延迟
            if self.metrics['write_latency']:
                avg_latency = sum(self.metrics['write_latency']) / len(self.metrics['write_latency'])
                if self.baseline['avg_latency'] is None:
                    self.baseline['avg_latency'] = avg_latency
                else:
                    self.baseline['avg_latency'] = (
                        0.9 * self.baseline['avg_latency'] + 0.1 * avg_latency
                    )
                    
            # 更新峰值吞吐量
            if self.metrics['write_throughput']:
                current_throughput = self.metrics['write_throughput'][-1]
                if (self.baseline['peak_throughput'] is None or
                    current_throughput > self.baseline['peak_throughput']):
                    self.baseline['peak_throughput'] = current_throughput
                    
            # 更新正常CPU使用率
            if self.metrics['cpu_usage']:
                cpu_usage = sum(self.metrics['cpu_usage']) / len(self.metrics['cpu_usage'])
                if self.baseline['normal_cpu_usage'] is None:
                    self.baseline['normal_cpu_usage'] = cpu_usage
                else:
                    self.baseline['normal_cpu_usage'] = (
                        0.9 * self.baseline['normal_cpu_usage'] + 0.1 * cpu_usage
                    )
                    
    async def _check_anomalies(self):
        """检查性能异常"""
        with self._lock:
            # 检查延迟异常
            if (self.baseline['avg_latency'] is not None and
                self.metrics['write_latency'] and
                self.metrics['write_latency'][-1] > self.baseline['avg_latency'] * 3):
                logger.warning(f"High latency detected: {self.metrics['write_latency'][-1]:.2f}ms")
                
            # 检查吞吐量异常
            if (self.baseline['peak_throughput'] is not None and
                self.metrics['write_throughput'] and
                self.metrics['write_throughput'][-1] < self.baseline['peak_throughput'] * 0.5):
                logger.warning(f"Low throughput detected: {self.metrics['write_throughput'][-1]:.2f} B/s")
                
            # 检查CPU使用率异常
            if (self.baseline['normal_cpu_usage'] is not None and
                self.metrics['cpu_usage'] and
                self.metrics['cpu_usage'][-1] > self.baseline['normal_cpu_usage'] * 2):
                logger.warning(f"High CPU usage detected: {self.metrics['cpu_usage'][-1]:.1f}%")
                
            # 检查IO等待异常
            if self.metrics['io_wait'] and self.metrics['io_wait'][-1] > 1000000:  # 1MB
                logger.warning(f"High IO wait detected: {self.metrics['io_wait'][-1]} bytes")
                
    def get_metrics(self) -> Dict[str, Any]:
        """获取监控指标"""
        with self._lock:
            current_metrics = {
                'timestamp': datetime.now().isoformat(),
                'io_wait': {
                    'current': self.metrics['io_wait'][-1] if self.metrics['io_wait'] else 0,
                    'avg': sum(self.metrics['io_wait']) / len(self.metrics['io_wait']) if self.metrics['io_wait'] else 0
                },
                'cpu_usage': {
                    'current': self.metrics['cpu_usage'][-1] if self.metrics['cpu_usage'] else 0,
                    'avg': sum(self.metrics['cpu_usage']) / len(self.metrics['cpu_usage']) if self.metrics['cpu_usage'] else 0
                },
                'memory_mapped': {
                    'current': self.metrics['memory_mapped'][-1] if self.metrics['memory_mapped'] else 0,
                    'avg': sum(self.metrics['memory_mapped']) / len(self.metrics['memory_mapped']) if self.metrics['memory_mapped'] else 0
                },
                'write_throughput': {
                    'current': self.metrics['write_throughput'][-1] if self.metrics['write_throughput'] else 0,
                    'peak': self.baseline['peak_throughput'] if self.baseline['peak_throughput'] is not None else 0
                },
                'write_latency': {
                    'current': self.metrics['write_latency'][-1] if self.metrics['write_latency'] else 0,
                    'avg': self.baseline['avg_latency'] if self.baseline['avg_latency'] is not None else 0
                },
                'queue_size': {
                    'current': self.metrics['queue_size'][-1] if self.metrics['queue_size'] else 0,
                    'avg': sum(self.metrics['queue_size']) / len(self.metrics['queue_size']) if self.metrics['queue_size'] else 0
                },
                'error_rate': {
                    'current': self.metrics['error_rate'][-1] if self.metrics['error_rate'] else 0,
                    'total_errors': self.write_stats['error_count']
                },
                'total_stats': {
                    'bytes_written': self.write_stats['total_bytes'],
                    'operations': self.write_stats['total_ops']
                }
            }
            
            return current_metrics
            
    def save_metrics(self, filepath: str):
        """保存监控指标到文件"""
        metrics = self.get_metrics()
        try:
            with open(filepath, 'w') as f:
                json.dump(metrics, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save metrics: {str(e)}")
            
    @classmethod
    def load_metrics(cls, filepath: str) -> Dict[str, Any]:
        """从文件加载监控指标"""
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load metrics: {str(e)}")
            return {}

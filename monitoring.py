from typing import Dict, Any
import time
from contextlib import contextmanager
import threading
from collections import defaultdict
import logging
import psutil
import asyncio
from datetime import datetime

logger = logging.getLogger(__name__)

class SystemMetrics:
    @staticmethod
    def get_system_metrics() -> Dict[str, float]:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return {
            'cpu_usage_percent': cpu_percent,
            'memory_usage_percent': memory.percent,
            'memory_available_gb': memory.available / (1024 ** 3),
            'disk_usage_percent': disk.percent,
            'disk_free_gb': disk.free / (1024 ** 3)
        }

class Metrics:
    def __init__(self):
        self._metrics = defaultdict(lambda: defaultdict(float))
        self._histograms = defaultdict(list)
        self._lock = threading.Lock()
        self.start_time = time.time()
    
    def record(self, metric_name: str, value: float, metric_type: str = "counter"):
        with self._lock:
            if metric_type == "counter":
                self._metrics[metric_type][metric_name] += value
            elif metric_type == "gauge":
                self._metrics[metric_type][metric_name] = value
            elif metric_type == "histogram":
                self._histograms[metric_name].append(value)
                if len(self._histograms[metric_name]) > 1000:  # 保持最近1000个样本
                    self._histograms[metric_name] = self._histograms[metric_name][-1000:]
    
    def get_histogram_stats(self, metric_name: str) -> Dict[str, float]:
        with self._lock:
            if not self._histograms[metric_name]:
                return {}
            values = sorted(self._histograms[metric_name])
            return {
                'min': values[0],
                'max': values[-1],
                'avg': sum(values) / len(values),
                'p50': values[len(values) // 2],
                'p95': values[int(len(values) * 0.95)],
                'p99': values[int(len(values) * 0.99)]
            }
    
    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            metrics_data = {
                metric_type: dict(metrics)
                for metric_type, metrics in self._metrics.items()
            }
            
            # 添加直方图统计
            metrics_data['histograms'] = {
                name: self.get_histogram_stats(name)
                for name in self._histograms.keys()
            }
            
            # 添加系统指标
            metrics_data['system'] = SystemMetrics.get_system_metrics()
            
            # 添加运行时间
            uptime = time.time() - self.start_time
            metrics_data['uptime_seconds'] = uptime
            
            return metrics_data

metrics = Metrics()

@contextmanager
def measure_time(operation_name: str):
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        metrics.record(f"{operation_name}_duration_seconds", duration, "histogram")
        metrics.record(f"{operation_name}_total", 1, "counter")
        logger.debug(f"{operation_name} took {duration:.3f} seconds")

async def start_metrics_collection():
    """启动定期指标收集"""
    while True:
        try:
            # 收集系统指标
            system_metrics = SystemMetrics.get_system_metrics()
            for name, value in system_metrics.items():
                metrics.record(f"system_{name}", value, "gauge")
            
            await asyncio.sleep(60)  # 每分钟收集一次
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            await asyncio.sleep(5)

"""Prometheus metrics collection for ObSave."""

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
)
import time
import psutil
from typing import Dict, Any
from threading import Lock
import logging

logger = logging.getLogger(__name__)

# 创建一个新的注册表
REGISTRY = CollectorRegistry()

# 请求相关指标
REQUEST_COUNT = Counter(
    'obsave_requests_total',
    'Total number of requests',
    ['method', 'endpoint', 'status'],
    registry=REGISTRY
)

REQUEST_LATENCY = Histogram(
    'obsave_request_latency_seconds',
    'Request latency in seconds',
    ['method', 'endpoint'],
    buckets=(0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0),
    registry=REGISTRY
)

ACTIVE_REQUESTS = Gauge(
    'obsave_active_requests',
    'Number of active requests',
    registry=REGISTRY
)

# 存储相关指标
STORAGE_SIZE = Gauge(
    'obsave_storage_size_bytes',
    'Total storage size in bytes',
    registry=REGISTRY
)

STORAGE_FILES = Gauge(
    'obsave_storage_files_total',
    'Total number of stored files',
    registry=REGISTRY
)

STORAGE_OPERATIONS = Counter(
    'obsave_storage_operations_total',
    'Number of storage operations',
    ['operation', 'status'],
    registry=REGISTRY
)

# 系统资源指标
MEMORY_USAGE = Gauge(
    'obsave_memory_usage_bytes',
    'Memory usage in bytes',
    ['type'],
    registry=REGISTRY
)

CPU_USAGE = Gauge(
    'obsave_cpu_usage_percent',
    'CPU usage percentage',
    registry=REGISTRY
)

OPEN_FILES = Gauge(
    'obsave_open_files',
    'Number of open files',
    registry=REGISTRY
)

THREAD_COUNT = Gauge(
    'obsave_threads',
    'Number of threads',
    registry=REGISTRY
)

class MetricsCollector:
    """指标收集器"""
    
    def __init__(self):
        self._lock = Lock()
        self._process = psutil.Process()
        self._start_time = time.time()
        
    def collect_system_metrics(self):
        """收集系统指标"""
        try:
            # 内存使用
            memory_info = self._process.memory_info()
            MEMORY_USAGE.labels('rss').set(memory_info.rss)
            MEMORY_USAGE.labels('vms').set(memory_info.vms)
            
            # CPU 使用
            CPU_USAGE.set(self._process.cpu_percent())
            
            # 打开的文件数
            OPEN_FILES.set(len(self._process.open_files()))
            
            # 线程数
            THREAD_COUNT.set(len(self._process.threads()))
            
        except Exception as e:
            logger.error(f"Error collecting system metrics: {str(e)}")
            
    def update_storage_metrics(self, stats: Dict[str, Any]):
        """更新存储指标"""
        try:
            STORAGE_SIZE.set(stats.get('total_size', 0))
            STORAGE_FILES.set(stats.get('total_files', 0))
        except Exception as e:
            logger.error(f"Error updating storage metrics: {str(e)}")
            
    def track_request(self, method: str, endpoint: str):
        """跟踪请求开始"""
        ACTIVE_REQUESTS.inc()
        return time.time()
        
    def track_request_end(self, start_time: float, method: str, endpoint: str, status: int):
        """跟踪请求结束"""
        try:
            duration = time.time() - start_time
            REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=status).inc()
            REQUEST_LATENCY.labels(method=method, endpoint=endpoint).observe(duration)
            ACTIVE_REQUESTS.dec()
        except Exception as e:
            logger.error(f"Error tracking request metrics: {str(e)}")
            
    def track_storage_operation(self, operation: str, success: bool):
        """跟踪存储操作"""
        status = 'success' if success else 'error'
        STORAGE_OPERATIONS.labels(operation=operation, status=status).inc()
        
# 创建全局指标收集器实例
metrics_collector = MetricsCollector()

async def metrics_endpoint():
    """Prometheus 指标端点处理函数"""
    # 收集最新的系统指标
    metrics_collector.collect_system_metrics()
    
    # 生成指标数据
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST

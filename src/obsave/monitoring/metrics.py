import os
import uuid
from typing import Dict, Any
from threading import Lock
import logging
import threading
import gc
from datetime import datetime
import time
import psutil
import platform
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
)
from fastapi import Response

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
    'obsave_storage_files',
    'Number of files in storage',
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
        self._system = platform.system().lower()
        
    def _safe_call(self, func, default=None, *args, **kwargs):
        """安全调用函数，如果失败返回默认值"""
        try:
            return func(*args, **kwargs)
        except (psutil.Error, OSError, AttributeError):
            return default
            
    def collect_system_metrics(self):
        """收集系统指标"""
        metrics = {}
        try:
            # CPU 指标
            cpu_times = self._safe_call(self._process.cpu_times)
            cpu_metrics = {
                'cpu_usage_percent': self._safe_call(self._process.cpu_percent),
                'cpu_count_physical': self._safe_call(lambda: psutil.cpu_count(logical=False), 0),
                'cpu_count_logical': self._safe_call(psutil.cpu_count, 0),
                'process_cpu_percent': self._safe_call(self._process.cpu_percent),
                'process_cpu_times': cpu_times._asdict() if cpu_times else {}
            }
            CPU_USAGE.set(cpu_metrics['cpu_usage_percent'] or 0.0)
            metrics['cpu'] = cpu_metrics

            # 内存指标
            memory = self._safe_call(psutil.virtual_memory)
            process_memory = self._safe_call(self._process.memory_info)
            memory_metrics = {
                'memory_usage_percent': getattr(memory, 'percent', 0.0),
                'memory_available_gb': getattr(memory, 'available', 0) / (1024 ** 3),
                'memory_total_gb': getattr(memory, 'total', 0) / (1024 ** 3),
            }
            
            if process_memory:
                memory_metrics.update({
                    'process_memory_rss_mb': getattr(process_memory, 'rss', 0) / (1024 ** 2),
                    'process_memory_vms_mb': getattr(process_memory, 'vms', 0) / (1024 ** 2)
                })
                MEMORY_USAGE.labels('rss').set(getattr(process_memory, 'rss', 0))
                MEMORY_USAGE.labels('vms').set(getattr(process_memory, 'vms', 0))
                
            metrics['memory'] = memory_metrics

            # 磁盘指标
            disk = self._safe_call(lambda: psutil.disk_usage('/'))
            disk_metrics = {
                'disk_usage_percent': getattr(disk, 'percent', 0.0),
                'disk_free_gb': getattr(disk, 'free', 0) / (1024 ** 3),
                'disk_total_gb': getattr(disk, 'total', 0) / (1024 ** 3)
            }
            metrics['disk'] = disk_metrics

            # 进程指标
            process_metrics = {
                'process_threads_count': len(self._safe_call(self._process.threads, [])),
                'process_open_files': len(self._safe_call(self._process.open_files, [])),
                'process_connections': len(self._safe_call(self._process.connections, [])),
                'process_children': len(self._safe_call(self._process.children, []))
            }
            
            # Windows 特有的句柄数
            if self._system == 'windows':
                process_metrics['process_handles'] = self._safe_call(self._process.num_handles)
                
            THREAD_COUNT.set(process_metrics['process_threads_count'])
            OPEN_FILES.set(process_metrics['process_open_files'])
            metrics['process'] = process_metrics

            # IO 指标
            io_counters = self._safe_call(self._process.io_counters)
            io_metrics = {}
            if io_counters:
                io_metrics = {
                    'process_io_read_mb': getattr(io_counters, 'read_bytes', 0) / (1024 ** 2),
                    'process_io_write_mb': getattr(io_counters, 'write_bytes', 0) / (1024 ** 2),
                    'process_io_read_count': getattr(io_counters, 'read_count', 0),
                    'process_io_write_count': getattr(io_counters, 'write_count', 0)
                }
            metrics['io'] = io_metrics

            # 运行时指标
            runtime_metrics = {
                'python_gc_counts': gc.get_count(),
                'python_threads_active': threading.active_count(),
                'uptime_seconds': time.time() - self._start_time
            }
            metrics['runtime'] = runtime_metrics

            # 网络连接指标
            connections = self._safe_call(self._process.connections, [])
            net_metrics = {
                'connections': {
                    'total': len(connections),
                    'established': len([c for c in connections if getattr(c, 'status', '') == 'ESTABLISHED']),
                    'listen': len([c for c in connections if getattr(c, 'status', '') == 'LISTEN']),
                    'time_wait': len([c for c in connections if getattr(c, 'status', '') == 'TIME_WAIT'])
                }
            }
            metrics['network'] = net_metrics

            # 文件系统指标
            fs_metrics = {
                'open_files': len(self._safe_call(self._process.open_files, [])),
            }
            
            # Windows 特有的句柄计数
            if self._system == 'windows':
                fs_metrics['handles'] = self._safe_call(self._process.num_handles)
            
            metrics['filesystem'] = fs_metrics

            # 基本系统信息
            metrics['system'] = {
                'platform': platform.system(),
                'platform_release': platform.release(),
                'python_version': platform.python_version(),
                'hostname': platform.node()
            }

            return metrics

        except Exception as e:
            logger.error(f"Error collecting system metrics: {str(e)}", exc_info=True)
            return {
                'error': str(e),
                'status': 'error',
                'timestamp': datetime.utcnow().isoformat()
            }
            
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
    try:
        return Response(
            generate_latest(REGISTRY),
            media_type=CONTENT_TYPE_LATEST
        )
    except Exception as e:
        logger.error(f"Error generating metrics: {str(e)}")
        return Response(
            content="Error generating metrics",
            status_code=500
        )

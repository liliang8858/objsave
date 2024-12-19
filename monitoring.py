from typing import Dict, Any
import time
from contextlib import contextmanager
import threading
from collections import defaultdict
import logging
import psutil
import asyncio
from datetime import datetime
import os
from queue import Queue
import gc

logger = logging.getLogger(__name__)

class SystemMetrics:
    @staticmethod
    def get_system_metrics() -> Dict[str, Any]:
        # 获取当前进程
        current_process = psutil.Process()
        
        # CPU指标
        cpu_metrics = {
            'cpu_usage_percent': psutil.cpu_percent(interval=1),
            'cpu_count_physical': psutil.cpu_count(logical=False),
            'cpu_count_logical': psutil.cpu_count(),
            'process_cpu_percent': current_process.cpu_percent(),
            'process_cpu_times': current_process.cpu_times()._asdict(),
        }
        
        # 内存指标
        memory = psutil.virtual_memory()
        memory_metrics = {
            'memory_usage_percent': memory.percent,
            'memory_available_gb': memory.available / (1024 ** 3),
            'memory_total_gb': memory.total / (1024 ** 3),
            'process_memory_rss_mb': current_process.memory_info().rss / (1024 * 1024),
            'process_memory_vms_mb': current_process.memory_info().vms / (1024 * 1024),
        }
        
        # 磁盘指标
        disk = psutil.disk_usage('/')
        disk_metrics = {
            'disk_usage_percent': disk.percent,
            'disk_free_gb': disk.free / (1024 ** 3),
            'disk_total_gb': disk.total / (1024 ** 3),
        }
        
        # 进程和线程指标
        process_metrics = {
            'process_threads_count': current_process.num_threads(),
            'process_open_files': len(current_process.open_files()),
            'process_connections': len(current_process.connections()),
            'process_handles': current_process.num_handles() if os.name == 'nt' else 0,
            'process_children': len(current_process.children()),
        }
        
        # IO指标
        io_counters = current_process.io_counters()
        io_metrics = {
            'process_io_read_mb': io_counters.read_bytes / (1024 * 1024),
            'process_io_write_mb': io_counters.write_bytes / (1024 * 1024),
            'process_io_read_count': io_counters.read_count,
            'process_io_write_count': io_counters.write_count,
        }
        
        # Python运行时指标
        gc_counts = gc.get_count()
        runtime_metrics = {
            'python_gc_counts': {
                'generation0': gc_counts[0],
                'generation1': gc_counts[1],
                'generation2': gc_counts[2],
            },
            'python_threads_active': threading.active_count(),
        }
        
        return {
            'cpu': cpu_metrics,
            'memory': memory_metrics,
            'disk': disk_metrics,
            'process': process_metrics,
            'io': io_metrics,
            'runtime': runtime_metrics,
        }

class QueueMetrics:
    """队列指标收集器"""
    def __init__(self):
        self._queues = {}
        self._lock = threading.Lock()
    
    def register_queue(self, name: str, queue: Queue):
        """注册队列以进行监控"""
        with self._lock:
            self._queues[name] = queue
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取所有队列的指标"""
        metrics = {}
        with self._lock:
            for name, queue in self._queues.items():
                metrics[name] = {
                    'qsize': queue.qsize(),
                    'empty': queue.empty(),
                    'full': queue.full(),
                    'maxsize': queue.maxsize,
                }
        return metrics

class WorkflowMetrics:
    """工作流指标收集器"""
    def __init__(self):
        self._workflows = defaultdict(lambda: {
            'total': 0,
            'success': 0,
            'failed': 0,
            'in_progress': 0,
            'durations': [],
        })
        self._lock = threading.Lock()
    
    def record_workflow_start(self, workflow_name: str):
        """记录工作流开始"""
        with self._lock:
            self._workflows[workflow_name]['total'] += 1
            self._workflows[workflow_name]['in_progress'] += 1
    
    def record_workflow_end(self, workflow_name: str, success: bool, duration: float):
        """记录工作流结束"""
        with self._lock:
            workflow = self._workflows[workflow_name]
            workflow['in_progress'] -= 1
            if success:
                workflow['success'] += 1
            else:
                workflow['failed'] += 1
            workflow['durations'].append(duration)
            # 只保留最近1000个duration样本
            if len(workflow['durations']) > 1000:
                workflow['durations'] = workflow['durations'][-1000:]
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取所有工作流指标"""
        with self._lock:
            metrics = {}
            for name, data in self._workflows.items():
                durations = data['durations']
                duration_stats = {}
                if durations:
                    sorted_durations = sorted(durations)
                    duration_stats = {
                        'min': min(durations),
                        'max': max(durations),
                        'avg': sum(durations) / len(durations),
                        'p50': sorted_durations[len(sorted_durations) // 2],
                        'p95': sorted_durations[int(len(sorted_durations) * 0.95)],
                        'p99': sorted_durations[int(len(sorted_durations) * 0.99)],
                    }
                
                metrics[name] = {
                    'total': data['total'],
                    'success': data['success'],
                    'failed': data['failed'],
                    'in_progress': data['in_progress'],
                    'success_rate': (data['success'] / data['total'] * 100) if data['total'] > 0 else 0,
                    'durations': duration_stats,
                }
            return metrics

class ThreadPoolMetrics:
    """线程池指标收集器"""
    def __init__(self):
        self._thread_pools = {}
        self._lock = threading.Lock()
    
    def register_thread_pool(self, name: str, pool):
        """注册线程池以进行监控"""
        with self._lock:
            self._thread_pools[name] = pool
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取所有线程池的指标"""
        metrics = {}
        with self._lock:
            for name, pool in self._thread_pools.items():
                metrics[name] = {
                    'workers': pool._max_workers,
                    'tasks_total': pool._work_queue.qsize(),
                    'threads_alive': len([t for t in pool._threads if t.is_alive()]),
                }
        return metrics

class StorageMetrics:
    """存储相关指标收集器"""
    def __init__(self):
        self._metrics = defaultdict(lambda: {
            'operations': defaultdict(int),
            'bytes': defaultdict(int),
            'latencies': defaultdict(list),
            'errors': defaultdict(int),
            'cache_hits': 0,
            'cache_misses': 0,
            'cache_evictions': 0,
            'flush_count': 0,
            'flush_bytes': 0,
            'flush_durations': [],
        })
        self._lock = threading.Lock()
    
    def record_operation(self, operation_type: str, storage_type: str, size: int, latency: float, success: bool = True):
        """记录存储操作"""
        with self._lock:
            metrics = self._metrics[storage_type]
            metrics['operations'][operation_type] += 1
            metrics['bytes'][operation_type] += size
            metrics['latencies'][operation_type].append(latency)
            if not success:
                metrics['errors'][operation_type] += 1
            
            # 保持最近1000个延迟样本
            if len(metrics['latencies'][operation_type]) > 1000:
                metrics['latencies'][operation_type] = metrics['latencies'][operation_type][-1000:]
    
    def record_cache_operation(self, storage_type: str, hit: bool):
        """记录缓存操作"""
        with self._lock:
            metrics = self._metrics[storage_type]
            if hit:
                metrics['cache_hits'] += 1
            else:
                metrics['cache_misses'] += 1
    
    def record_cache_eviction(self, storage_type: str):
        """记录缓存淘汰"""
        with self._lock:
            self._metrics[storage_type]['cache_evictions'] += 1
    
    def record_flush(self, storage_type: str, bytes_flushed: int, duration: float):
        """记录刷盘操作"""
        with self._lock:
            metrics = self._metrics[storage_type]
            metrics['flush_count'] += 1
            metrics['flush_bytes'] += bytes_flushed
            metrics['flush_durations'].append(duration)
            if len(metrics['flush_durations']) > 1000:
                metrics['flush_durations'] = metrics['flush_durations'][-1000:]
    
    def get_latency_stats(self, storage_type: str, operation_type: str) -> Dict[str, float]:
        """获取延迟统计"""
        with self._lock:
            latencies = self._metrics[storage_type]['latencies'][operation_type]
            if not latencies:
                return {}
            
            sorted_latencies = sorted(latencies)
            return {
                'min': min(latencies),
                'max': max(latencies),
                'avg': sum(latencies) / len(latencies),
                'p50': sorted_latencies[len(sorted_latencies) // 2],
                'p95': sorted_latencies[int(len(sorted_latencies) * 0.95)],
                'p99': sorted_latencies[int(len(sorted_latencies) * 0.99)],
            }
    
    def get_flush_stats(self, storage_type: str) -> Dict[str, Any]:
        """获取刷盘统计"""
        with self._lock:
            metrics = self._metrics[storage_type]
            durations = metrics['flush_durations']
            
            duration_stats = {}
            if durations:
                sorted_durations = sorted(durations)
                duration_stats = {
                    'min': min(durations),
                    'max': max(durations),
                    'avg': sum(durations) / len(durations),
                    'p50': sorted_durations[len(sorted_durations) // 2],
                    'p95': sorted_durations[int(len(sorted_durations) * 0.95)],
                    'p99': sorted_durations[int(len(sorted_durations) * 0.99)],
                }
            
            return {
                'count': metrics['flush_count'],
                'total_bytes': metrics['flush_bytes'],
                'durations': duration_stats
            }
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取所有存储指标"""
        with self._lock:
            result = {}
            for storage_type, metrics in self._metrics.items():
                storage_metrics = {
                    'operations': {
                        op_type: {
                            'count': count,
                            'bytes': metrics['bytes'][op_type],
                            'error_count': metrics['errors'][op_type],
                            'latencies': self.get_latency_stats(storage_type, op_type)
                        }
                        for op_type, count in metrics['operations'].items()
                    },
                    'cache': {
                        'hits': metrics['cache_hits'],
                        'misses': metrics['cache_misses'],
                        'hit_rate': (metrics['cache_hits'] / (metrics['cache_hits'] + metrics['cache_misses']) * 100) 
                            if (metrics['cache_hits'] + metrics['cache_misses']) > 0 else 0,
                        'evictions': metrics['cache_evictions']
                    },
                    'flush': self.get_flush_stats(storage_type)
                }
                result[storage_type] = storage_metrics
            return result

# 创建指标收集器实例
queue_metrics = QueueMetrics()
workflow_metrics = WorkflowMetrics()
thread_pool_metrics = ThreadPoolMetrics()
storage_metrics = StorageMetrics()

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
                if len(self._histograms[metric_name]) > 1000:
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
            
            # 添加队列指标
            metrics_data['queues'] = queue_metrics.get_metrics()
            
            # 添加工作流指标
            metrics_data['workflows'] = workflow_metrics.get_metrics()
            
            # 添加线程池指标
            metrics_data['thread_pools'] = thread_pool_metrics.get_metrics()
            
            # 添加存储指标
            metrics_data['storage'] = storage_metrics.get_metrics()
            
            # 添加运行时间
            uptime = time.time() - self.start_time
            metrics_data['uptime_seconds'] = uptime
            
            return metrics_data

metrics = Metrics()

@contextmanager
def measure_workflow(workflow_name: str):
    """测量工作流执行时间和结果的上下文管理器"""
    start_time = time.time()
    workflow_metrics.record_workflow_start(workflow_name)
    success = False
    try:
        yield
        success = True
    finally:
        duration = time.time() - start_time
        workflow_metrics.record_workflow_end(workflow_name, success, duration)

@contextmanager
def measure_time(operation_name: str):
    """测量操作执行时间的上下文管理器"""
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        metrics.record(f"{operation_name}_duration_seconds", duration, "histogram")
        metrics.record(f"{operation_name}_total", 1, "counter")
        logger.debug(f"{operation_name} took {duration:.3f} seconds")

@contextmanager
def measure_storage_operation(operation_type: str, storage_type: str, size: int = 0):
    """测量存储操作的上下文管理器"""
    start_time = time.time()
    success = True
    try:
        yield
    except Exception:
        success = False
        raise
    finally:
        duration = time.time() - start_time
        storage_metrics.record_operation(operation_type, storage_type, size, duration, success)

async def start_metrics_collection():
    """启动定期指标收集"""
    while True:
        try:
            # 收集系统指标
            system_metrics = SystemMetrics.get_system_metrics()
            for category, category_metrics in system_metrics.items():
                for name, value in category_metrics.items():
                    if isinstance(value, (int, float)):
                        metrics.record(f"system_{category}_{name}", value, "gauge")
            
            await asyncio.sleep(60)  # 每分钟收集一次
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            await asyncio.sleep(5)

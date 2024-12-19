import time
import os
import threading
from typing import Dict, Any, List, Deque
from datetime import datetime
import logging
from threading import Lock
from collections import deque
import psutil
import weakref
import asyncio

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """性能监控类"""
    def __init__(self, 
                 max_response_times: int = 100,
                 max_slow_requests: int = 10,
                 slow_request_threshold: float = 1.0):
        self._lock = Lock()
        self._max_response_times = max_response_times
        self._max_slow_requests = max_slow_requests
        self._slow_request_threshold = slow_request_threshold
        
        self._metrics = {
            'requests_total': 0,
            'requests_error': 0,
            'requests_success': 0,
            'requests_active': 0,
            'response_times': deque(maxlen=max_response_times),
            'slow_requests': deque(maxlen=max_slow_requests),
        }
        self._start_time = time.time()
        self._process = psutil.Process()
        
    def request_start(self):
        """记录请求开始"""
        with self._lock:
            self._metrics['requests_total'] += 1
            self._metrics['requests_active'] += 1
            
    def request_end(self, duration: float, path: str, status_code: int):
        """记录请求结束"""
        with self._lock:
            self._metrics['requests_active'] -= 1
            
            # 记录响应时间
            self._metrics['response_times'].append(duration)
            
            # 记录慢请求
            if duration > self._slow_request_threshold:
                self._metrics['slow_requests'].append({
                    'path': path,
                    'duration': duration,
                    'time': datetime.now().isoformat(),
                    'status_code': status_code
                })
                
            # 记录成功/失败
            if 200 <= status_code < 400:
                self._metrics['requests_success'] += 1
            else:
                self._metrics['requests_error'] += 1
                
    def get_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        with self._lock:
            uptime = time.time() - self._start_time
            
            # 计算响应时间统计
            response_times = list(self._metrics['response_times'])
            avg_response_time = sum(response_times) / len(response_times) if response_times else 0
            
            # 获取系统资源使用情况
            memory_info = self._process.memory_info()
            cpu_percent = self._process.cpu_percent()
            
            return {
                'uptime': round(uptime, 2),
                'requests': {
                    'total': self._metrics['requests_total'],
                    'success': self._metrics['requests_success'],
                    'error': self._metrics['requests_error'],
                    'active': self._metrics['requests_active'],
                },
                'response_time': {
                    'average': round(avg_response_time, 3),
                    'samples': len(response_times),
                },
                'slow_requests': list(self._metrics['slow_requests']),
                'system': {
                    'memory_rss': memory_info.rss,
                    'memory_vms': memory_info.vms,
                    'cpu_percent': cpu_percent,
                    'threads': len(self._process.threads()),
                }
            }
            
class ResourceManager:
    """资源管理类"""
    def __init__(self, max_workers: int = 4, max_memory_mb: int = 2048):
        self._lock = Lock()
        self._max_workers = max_workers
        self._max_memory_mb = max_memory_mb
        self._active_workers = 0
        self._process = psutil.Process()
        self._worker_refs = weakref.WeakSet()
        
    async def acquire_worker(self):
        """获取工作线程，带资源检查"""
        while True:
            with self._lock:
                if self._active_workers < self._max_workers:
                    memory_usage = self._process.memory_info().rss / (1024 * 1024)
                    if memory_usage < self._max_memory_mb:
                        self._active_workers += 1
                        worker_ref = weakref.ref(threading.current_thread())
                        self._worker_refs.add(worker_ref)
                        return True
            await asyncio.sleep(0.1)
            
    def release_worker(self):
        """释放工作线程"""
        with self._lock:
            if self._active_workers > 0:
                self._active_workers -= 1
                
    def get_stats(self) -> Dict[str, Any]:
        """获取资源使用统计"""
        with self._lock:
            memory_info = self._process.memory_info()
            return {
                'workers': {
                    'active': self._active_workers,
                    'max': self._max_workers,
                },
                'memory': {
                    'current_mb': memory_info.rss / (1024 * 1024),
                    'max_mb': self._max_memory_mb,
                },
                'threads': len(self._process.threads()),
            }
            
class AsyncLimiter:
    """异步并发限制器"""
    def __init__(self, max_concurrent: int = 100):
        self._sem = threading.Semaphore(max_concurrent)
        
    async def __aenter__(self):
        self._sem.acquire()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._sem.release()

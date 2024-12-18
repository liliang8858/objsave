import time
import os
import threading
from typing import Dict, Any
from datetime import datetime
import logging
from threading import Lock

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """性能监控类"""
    def __init__(self):
        self._lock = Lock()
        self._metrics = {
            'requests_total': 0,
            'requests_error': 0,
            'requests_success': 0,
            'requests_active': 0,
            'response_times': [],  # 最近100个请求的响应时间
            'slow_requests': [],   # 最近10个慢请求
        }
        self._start_time = time.time()
        
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
            if len(self._metrics['response_times']) > 100:
                self._metrics['response_times'].pop(0)
                
            # 记录慢请求 (>1s)
            if duration > 1.0:
                self._metrics['slow_requests'].append({
                    'path': path,
                    'duration': duration,
                    'time': datetime.now().isoformat()
                })
                if len(self._metrics['slow_requests']) > 10:
                    self._metrics['slow_requests'].pop(0)
                    
            # 记录成功/失败
            if 200 <= status_code < 400:
                self._metrics['requests_success'] += 1
            else:
                self._metrics['requests_error'] += 1
                
    def get_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        with self._lock:
            response_times = self._metrics['response_times']
            avg_response_time = sum(response_times) / len(response_times) if response_times else 0
            
            return {
                'uptime_seconds': int(time.time() - self._start_time),
                'requests': {
                    'total': self._metrics['requests_total'],
                    'success': self._metrics['requests_success'],
                    'error': self._metrics['requests_error'],
                    'active': self._metrics['requests_active'],
                },
                'response_time': {
                    'average': round(avg_response_time, 3),
                    'p95': sorted(response_times)[int(len(response_times) * 0.95)] if len(response_times) > 20 else None,
                },
                'slow_requests': self._metrics['slow_requests']
            }
            
class ResourceManager:
    """资源管理类"""
    def __init__(self, max_workers: int = 4, max_memory_mb: int = 2048):
        self._lock = Lock()
        self._max_workers = max_workers
        self._max_memory_mb = max_memory_mb
        self._active_workers = 0
        
    def acquire_worker(self) -> bool:
        """获取工作线程"""
        with self._lock:
            if self._active_workers >= self._max_workers:
                return False
            self._active_workers += 1
            return True
            
    def release_worker(self):
        """释放工作线程"""
        with self._lock:
            if self._active_workers > 0:
                self._active_workers -= 1
                
    def get_stats(self) -> Dict[str, Any]:
        """获取资源使用统计"""
        with self._lock:
            return {
                'workers': {
                    'active': self._active_workers,
                    'max': self._max_workers,
                    'available': self._max_workers - self._active_workers
                }
            }
            
class AsyncLimiter:
    """异步并发限制器"""
    def __init__(self, max_concurrent: int = 100):
        self._sem = threading.Semaphore(max_concurrent)
        
    async def __aenter__(self):
        acquired = self._sem.acquire(blocking=False)
        if not acquired:
            raise Exception("并发限制已达到最大值")
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._sem.release()

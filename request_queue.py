import asyncio
import logging
from typing import Dict, Any, Optional, Callable, Awaitable
from datetime import datetime
from collections import deque
import threading
from concurrent.futures import ThreadPoolExecutor
import time

logger = logging.getLogger(__name__)

class RequestQueue:
    """
    请求队列处理器
    - 异步处理请求
    - 自动扩展处理能力
    - 智能任务调度
    - 优先级处理
    """
    def __init__(self, max_workers: int = None):
        self.queue = asyncio.Queue()
        self.priority_queue = asyncio.PriorityQueue()
        self.processing = set()
        self.results = {}
        self.max_workers = max_workers or (threading.cpu_count() * 2)
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # 统计信息
        self.stats = {
            "total_requests": 0,
            "completed_requests": 0,
            "failed_requests": 0,
            "current_processing": 0,
            "avg_processing_time": 0,
            "peak_queue_size": 0
        }
        self.stats_lock = threading.Lock()
        
    async def start(self):
        """启动队列处理器"""
        # 启动处理器
        asyncio.create_task(self._process_queue())
        asyncio.create_task(self._process_priority_queue())
        
    async def _process_queue(self):
        """处理普通请求队列"""
        while True:
            try:
                task = await self.queue.get()
                asyncio.create_task(self._handle_task(task))
            except Exception as e:
                logger.error(f"Error processing queue: {str(e)}")
                await asyncio.sleep(0.1)
                
    async def _process_priority_queue(self):
        """处理优先级请求队列"""
        while True:
            try:
                _, task = await self.priority_queue.get()
                asyncio.create_task(self._handle_task(task))
            except Exception as e:
                logger.error(f"Error processing priority queue: {str(e)}")
                await asyncio.sleep(0.1)
                
    async def _handle_task(self, task: Dict):
        """处理单个任务"""
        task_id = task["id"]
        self.processing.add(task_id)
        
        start_time = time.time()
        with self.stats_lock:
            self.stats["current_processing"] += 1
            
        try:
            # 执行任务
            if task.get("is_cpu_bound"):
                # CPU密集型任务使用线程池
                result = await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool,
                    task["handler"],
                    *task.get("args", []),
                    **task.get("kwargs", {})
                )
            else:
                # IO密集型任务直接异步执行
                result = await task["handler"](
                    *task.get("args", []),
                    **task.get("kwargs", {})
                )
                
            self.results[task_id] = {
                "status": "completed",
                "result": result,
                "error": None
            }
            
            with self.stats_lock:
                self.stats["completed_requests"] += 1
                
        except Exception as e:
            logger.error(f"Task {task_id} failed: {str(e)}")
            self.results[task_id] = {
                "status": "failed",
                "result": None,
                "error": str(e)
            }
            
            with self.stats_lock:
                self.stats["failed_requests"] += 1
                
        finally:
            self.processing.remove(task_id)
            process_time = time.time() - start_time
            
            with self.stats_lock:
                self.stats["current_processing"] -= 1
                # 更新平均处理时间
                total_completed = self.stats["completed_requests"]
                current_avg = self.stats["avg_processing_time"]
                self.stats["avg_processing_time"] = (current_avg * (total_completed - 1) + process_time) / total_completed
                
    async def enqueue(
        self,
        handler: Callable[..., Awaitable[Any]],
        *args,
        priority: int = 0,
        is_cpu_bound: bool = False,
        **kwargs
    ) -> str:
        """
        将请求加入队列
        priority: 优先级(0-9)，数字越小优先级越高
        is_cpu_bound: 是否是CPU密集型任务
        """
        task_id = f"{int(time.time() * 1000)}_{id(handler)}"
        task = {
            "id": task_id,
            "handler": handler,
            "args": args,
            "kwargs": kwargs,
            "priority": priority,
            "is_cpu_bound": is_cpu_bound,
            "enqueue_time": datetime.now().isoformat()
        }
        
        with self.stats_lock:
            self.stats["total_requests"] += 1
            current_queue_size = len(self.processing) + self.queue.qsize() + self.priority_queue.qsize()
            self.stats["peak_queue_size"] = max(self.stats["peak_queue_size"], current_queue_size)
            
        if priority > 0:
            await self.priority_queue.put((priority, task))
        else:
            await self.queue.put(task)
            
        return task_id
        
    def get_result(self, task_id: str) -> Optional[Dict]:
        """获取任务结果"""
        return self.results.get(task_id)
        
    def get_stats(self) -> Dict[str, Any]:
        """获取队列统计信息"""
        with self.stats_lock:
            stats = self.stats.copy()
            stats.update({
                "queue_size": self.queue.qsize(),
                "priority_queue_size": self.priority_queue.qsize(),
                "processing_count": len(self.processing)
            })
            return stats

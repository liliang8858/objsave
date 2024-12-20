import psutil
import time
import asyncio
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from queue import Queue
import threading
from obsave.core.database import SessionLocal, Base
from obsave.core.models import ObjectStorage

logger = logging.getLogger(__name__)

class ResourceManager:
    """系统资源管理器"""
    def __init__(self):
        self.cpu_threshold = 90  # CPU使用率阈值
        self.memory_threshold = 90  # 内存使用率阈值
        self.last_check = time.time()
        self.check_interval = 1  # 资源检查间隔（秒）
        
    def get_system_resources(self) -> Dict[str, Any]:
        """获取系统资源使用情况"""
        current_time = time.time()
        
        # 限制检查频率
        if current_time - self.last_check < self.check_interval:
            return {}
            
        self.last_check = current_time
        
        try:
            return {
                'cpu_percent': psutil.cpu_percent(),
                'memory_percent': psutil.virtual_memory().percent,
                'connections': len(psutil.net_connections()),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting system resources: {str(e)}")
            return {}
            
    def check_resources(self) -> tuple[bool, str]:
        """检查系统资源是否超限"""
        resources = self.get_system_resources()
        
        if not resources:
            return True, ""
            
        if resources.get('cpu_percent', 0) > self.cpu_threshold:
            return False, "CPU负载过高"
            
        if resources.get('memory_percent', 0) > self.memory_threshold:
            return False, "内存不足"
            
        return True, ""

    def get_available_workers(self) -> int:
        """获取可用的工作线程数"""
        try:
            cpu_count = psutil.cpu_count() or 1
            return min(32, cpu_count * 4)  # 每个CPU最多4个工作线程
        except:
            return 4  # 默认值
            
    def get_memory_limit(self) -> int:
        """获取可用的最大内存限制（字节）"""
        try:
            total_memory = psutil.virtual_memory().total
            return int(total_memory * 0.4)  # 使用40%的系统内存
        except:
            return 2 * 1024 * 1024 * 1024  # 默认2GB

class WriteManager:
    """写入管理器，用于批量处理写入操作"""
    
    def __init__(self, batch_size: int = 100, flush_interval: float = 1.0, max_queue_size: int = 10000):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_queue_size = max_queue_size
        
        self.queue: Queue = Queue(maxsize=max_queue_size)
        self.buffer: List[Any] = []
        self.last_flush = time.time()
        self._running = False
        self._lock = threading.Lock()
        self._flush_event = threading.Event()
        
        self.stats = {
            'total_writes': 0,
            'total_flushes': 0,
            'failed_writes': 0,
            'queue_size': 0,
            'buffer_size': 0,
            'start_time': None
        }
        
    def start(self):
        """启动写入管理器"""
        if self._running:
            return
            
        self._running = True
        self.stats['start_time'] = time.time()
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        logger.info("Write manager started")
        
    def stop(self):
        """停止写入管理器"""
        if not self._running:
            return
            
        self._running = False
        self._flush_event.set()  # 触发最后一次刷新
        if hasattr(self, '_flush_thread'):
            self._flush_thread.join(timeout=5.0)
        logger.info("Write manager stopped")
        
    async def add_record(self, data: Any) -> bool:
        """异步添加记录"""
        return await self.write(data)
        
    async def write(self, data: Any) -> bool:
        """异步写入数据"""
        try:
            if not self._running:
                raise RuntimeError("Write manager is not running")
                
            if self.queue.qsize() >= self.max_queue_size:
                raise RuntimeError("Write queue is full")
                
            self.queue.put(data)
            self.stats['total_writes'] += 1
            self.stats['queue_size'] = self.queue.qsize()
            
            # 如果队列达到批处理大小，触发刷新
            if self.queue.qsize() >= self.batch_size:
                self._flush_event.set()
                
            return True
            
        except Exception as e:
            self.stats['failed_writes'] += 1
            logger.error(f"Write failed: {str(e)}")
            return False
            
    async def flush(self):
        """强制刷新缓冲区"""
        if not self._running:
            return
            
        self._flush_event.set()
        await asyncio.sleep(0.1)  # 给刷新线程一点时间
        
    def _flush_loop(self):
        """刷新循环"""
        while self._running or not self.queue.empty():
            try:
                # 等待触发刷新或达到刷新间隔
                self._flush_event.wait(timeout=self.flush_interval)
                self._flush_event.clear()
                
                # 获取队列中的所有数据
                with self._lock:
                    while not self.queue.empty() and len(self.buffer) < self.batch_size:
                        try:
                            item = self.queue.get_nowait()
                            self.buffer.append(item)
                        except Exception:
                            break
                            
                    # 如果缓冲区有数据，执行刷新
                    if self.buffer:
                        self._flush_buffer()
                        
            except Exception as e:
                logger.error(f"Flush error: {str(e)}")
                
    def _flush_buffer(self):
        """刷新缓冲区"""
        try:
            if not self.buffer:
                return
            
            # 创建数据库会话
            db = SessionLocal()
            try:
                for record in self.buffer:
                    # 创建数据库对象
                    db_object = ObjectStorage(
                        id=record['id'],
                        name=record['name'],
                        content=record['content'].encode('utf-8'),  # Convert to bytes for storage
                        content_type=record['content_type'],
                        type=record['type'],
                        size=record['size'],
                        created_at=datetime.fromisoformat(record['created_at']),
                        owner=record.get('owner')
                    )
                    db.add(db_object)
                
                # 提交所有更改
                db.commit()
                logger.debug(f"Successfully flushed {len(self.buffer)} records to database")
                
            except Exception as e:
                logger.error(f"Database error during flush: {str(e)}")
                db.rollback()
                raise
            finally:
                db.close()
                
            # 清空缓冲区
            self.buffer.clear()
            self.stats['total_flushes'] += 1
            self.stats['buffer_size'] = len(self.buffer)
            
        except Exception as e:
            self.stats['failed_writes'] += 1
            logger.error(f"Buffer flush failed: {str(e)}")
            
    def get_stats(self) -> Dict[str, Any]:
        """获取写入统计信息"""
        stats = dict(self.stats)
        if stats['start_time']:
            stats['uptime'] = time.time() - stats['start_time']
        return stats

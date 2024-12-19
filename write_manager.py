import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime
import json
from collections import deque
from sqlalchemy.orm import Session
from models import ObjectStorage
from database import SessionLocal
import threading
import time

logger = logging.getLogger(__name__)

class WriteManager:
    """写入管理器 - 使用Write-Behind策略"""
    
    def __init__(self, 
                 batch_size: int = 100,
                 flush_interval: float = 1.0,
                 max_queue_size: int = 10000):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_queue_size = max_queue_size
        
        # 写入队列 - 使用双端队列提高性能
        self.write_queue = deque(maxlen=max_queue_size)
        
        # 确认队列 - 存储已写入数据库的记录ID
        self.confirm_queue = set()
        
        # 内存缓存 - 用于快速查询
        self.cache = {}
        
        # 控制标志
        self._running = False
        self._flush_event = asyncio.Event()
        
        # 统计信息
        self.stats = {
            'total_writes': 0,
            'successful_writes': 0,
            'failed_writes': 0,
            'avg_write_time': 0
        }
        
        # 锁
        self._queue_lock = asyncio.Lock()
        self._cache_lock = asyncio.Lock()
        
    async def start(self):
        """启动写入管理器"""
        if self._running:
            return
            
        self._running = True
        asyncio.create_task(self._flush_loop())
        logger.info("Write manager started")
        
    async def stop(self):
        """停止写入管理器"""
        if not self._running:
            return
            
        self._running = False
        self._flush_event.set()
        await self._flush_to_db(force=True)  # 强制刷新所有数据
        logger.info("Write manager stopped")
        
    async def add_record(self, record: Dict[str, Any]) -> str:
        """添加记录到写入队列"""
        async with self._queue_lock:
            if len(self.write_queue) >= self.max_queue_size:
                await self._flush_to_db(force=True)
                
            # 添加到写入队列
            self.write_queue.append(record)
            
            # 更新内存缓存
            async with self._cache_lock:
                self.cache[record['id']] = record
                
            # 如果队列达到批处理大小，触发刷新
            if len(self.write_queue) >= self.batch_size:
                self._flush_event.set()
                
            return record['id']
            
    async def get_record(self, record_id: str) -> Dict[str, Any]:
        """从缓存获取记录"""
        async with self._cache_lock:
            return self.cache.get(record_id)
            
    async def _flush_loop(self):
        """后台刷新循环"""
        while self._running:
            try:
                # 等待刷新信号或超时
                await asyncio.wait_for(
                    self._flush_event.wait(),
                    timeout=self.flush_interval
                )
            except asyncio.TimeoutError:
                pass
                
            self._flush_event.clear()
            await self._flush_to_db()
            
    async def _flush_to_db(self, force: bool = False):
        """将数据刷新到数据库"""
        if not self.write_queue and not force:
            return
            
        async with self._queue_lock:
            # 获取要写入的批次
            batch_size = len(self.write_queue)
            if batch_size == 0:
                return
                
            if not force and batch_size < self.batch_size:
                return
                
            # 准备批量写入的记录
            records = []
            while self.write_queue and len(records) < self.batch_size:
                records.append(self.write_queue.popleft())
                
        if not records:
            return
            
        # 批量写入数据库
        start_time = time.time()
        try:
            session = SessionLocal()
            try:
                # 创建ORM对象
                db_objects = []
                for record in records:
                    obj = ObjectStorage(
                        id=record['id'],
                        name=record['name'],
                        content=record['content'],
                        content_type=record['content_type'],
                        type=record['type'],
                        size=record['size'],
                        created_at=datetime.fromisoformat(record['created_at']),
                        owner=record.get('owner')
                    )
                    db_objects.append(obj)
                    
                # 批量插入
                session.bulk_save_objects(db_objects)
                session.commit()
                
                # 更新确认队列
                for record in records:
                    self.confirm_queue.add(record['id'])
                    
                # 更新统计信息
                write_time = time.time() - start_time
                self.stats['total_writes'] += len(records)
                self.stats['successful_writes'] += len(records)
                self.stats['avg_write_time'] = (
                    self.stats['avg_write_time'] * (self.stats['total_writes'] - len(records)) +
                    write_time * len(records)
                ) / self.stats['total_writes']
                
                logger.info(f"Successfully wrote {len(records)} records to database in {write_time:.2f}s")
                
            except Exception as e:
                logger.error(f"Database write error: {str(e)}")
                session.rollback()
                
                # 写入失败，放回队列
                async with self._queue_lock:
                    for record in reversed(records):
                        self.write_queue.appendleft(record)
                        
                self.stats['failed_writes'] += len(records)
                raise
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Flush error: {str(e)}")
            # 触发重试
            self._flush_event.set()
            
    async def wait_for_confirm(self, record_id: str, timeout: float = 5.0) -> bool:
        """等待记录写入确认"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if record_id in self.confirm_queue:
                return True
            await asyncio.sleep(0.1)
        return False
        
    def get_stats(self) -> Dict[str, Any]:
        """获取写入统计信息"""
        return {
            'queue_size': len(self.write_queue),
            'cache_size': len(self.cache),
            'confirmed_writes': len(self.confirm_queue),
            **self.stats
        }

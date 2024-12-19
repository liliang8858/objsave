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
import os
import hashlib
from zero_copy import ZeroCopyWriter
from monitor import MetricsCollector
from async_io import AsyncIOManager

logger = logging.getLogger(__name__)

class WriteManager:
    """写入管理器 - 使用Write-Behind策略和零拷贝技术"""
    
    def __init__(self, 
                 batch_size: int = 100,
                 flush_interval: float = 1.0,
                 max_queue_size: int = 10000):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_queue_size = max_queue_size
        
        # 写入队列 - 使用双端队列提高性能
        self.write_queue = deque(maxlen=max_queue_size)
        
        # 内存缓存 - 用于快速查询
        self.cache = {}
        
        # WAL目录
        self.wal_dir = "./wal"
        os.makedirs(self.wal_dir, exist_ok=True)
        
        # 零拷贝写入器
        self.zero_copy = ZeroCopyWriter()
        
        # 性能监控器
        self.metrics = MetricsCollector()
        
        # 异步I/O管理器
        self.async_io = AsyncIOManager()
        
        # 控制标志
        self._running = False
        self._flush_event = asyncio.Event()
        
        # 锁
        self._queue_lock = asyncio.Lock()
        self._cache_lock = asyncio.Lock()
        
    async def start(self):
        """启动写入管理器"""
        if self._running:
            return
            
        self._running = True
        # 启动监控和I/O管理器
        await self.metrics.start()
        await self.async_io.start()
        # 恢复WAL
        await self._recover_from_wal()
        # 启动后台刷新任务
        asyncio.create_task(self._flush_loop())
        logger.info("Write manager started")
        
    async def stop(self):
        """停止写入管理器"""
        if not self._running:
            return
            
        self._running = False
        self._flush_event.set()
        await self._flush_to_db(force=True)  # 强制刷新所有数据
        await self.metrics.stop()  # 停止监控
        await self.async_io.stop()  # 停止I/O管理器
        self.zero_copy.cleanup()
        
        # 保存监控数据
        self.metrics.save_metrics("write_metrics.json")
        logger.info("Write manager stopped")
        
    async def add_record(self, record: Dict[str, Any]) -> str:
        """添加记录到写入队列"""
        start_time = time.time()
        try:
            # 写入WAL
            await self._write_to_wal(record)
            
            async with self._queue_lock:
                if len(self.write_queue) >= self.max_queue_size:
                    self._flush_event.set()  # 触发刷新，但不等待
                    
                # 添加到写入队列
                self.write_queue.append(record)
                
                # 更新内存缓存
                async with self._cache_lock:
                    self.cache[record['id']] = record
                    
                # 记录队列大小
                self.metrics.record_queue_size(len(self.write_queue))
                
            # 记录写入操作
            write_time = time.time() - start_time
            self.metrics.record_write(
                size=len(json.dumps(record).encode('utf-8')),
                latency=write_time * 1000,  # 转换为毫秒
                success=True
            )
            
            return record['id']
            
        except Exception as e:
            # 记录失败操作
            write_time = time.time() - start_time
            self.metrics.record_write(
                size=len(json.dumps(record).encode('utf-8')),
                latency=write_time * 1000,
                success=False
            )
            raise
            
    async def get_record(self, record_id: str) -> Dict[str, Any]:
        """从缓存获取记录"""
        async with self._cache_lock:
            return self.cache.get(record_id)
            
    async def _write_to_wal(self, record: Dict[str, Any]):
        """使用异步I/O写入WAL文件"""
        wal_entry = {
            'timestamp': time.time(),
            'record': record
        }
        
        # 使用记录ID和时间戳生成WAL文件名
        filename = f"{record['id']}_{int(time.time()*1000)}.wal"
        filepath = os.path.join(self.wal_dir, filename)
        
        # 使用异步I/O写入
        success = await self.async_io.write_json(filepath, wal_entry)
        if not success:
            raise Exception("Failed to write WAL file")
            
    async def _recover_from_wal(self):
        """从WAL恢复数据"""
        try:
            wal_files = sorted(os.listdir(self.wal_dir))
            for filename in wal_files:
                if not filename.endswith('.wal'):
                    continue
                    
                filepath = os.path.join(self.wal_dir, filename)
                try:
                    # 使用零拷贝读取
                    with open(filepath, 'rb') as f:
                        content = f.read()
                    wal_entry = json.loads(content.decode('utf-8'))
                    record = wal_entry['record']
                    
                    # 检查记录是否已存在
                    async with self._cache_lock:
                        if record['id'] not in self.cache:
                            self.write_queue.append(record)
                            self.cache[record['id']] = record
                    
                except Exception as e:
                    logger.error(f"Failed to recover WAL {filename}: {str(e)}")
                    
                # 处理完后删除WAL文件
                try:
                    os.remove(filepath)
                except Exception as e:
                    logger.error(f"Failed to delete WAL {filename}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"WAL recovery failed: {str(e)}")
            
    async def _flush_loop(self):
        """后台刷新循环"""
        while self._running:
            try:
                # 等待刷新信号或超时
                try:
                    await asyncio.wait_for(
                        self._flush_event.wait(),
                        timeout=self.flush_interval
                    )
                except asyncio.TimeoutError:
                    pass
                    
                self._flush_event.clear()
                await self._flush_to_db()
                
            except Exception as e:
                logger.error(f"Flush loop error: {str(e)}")
                await asyncio.sleep(1)  # 错误后等待
                
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
                
                # 删除对应的WAL文件
                for record in records:
                    wal_pattern = f"{record['id']}_*.wal"
                    for filename in os.listdir(self.wal_dir):
                        if filename.startswith(record['id']):
                            try:
                                os.remove(os.path.join(self.wal_dir, filename))
                            except Exception as e:
                                logger.error(f"Failed to delete WAL {filename}: {str(e)}")
                                
                # 更新统计信息
                write_time = time.time() - start_time
                self.metrics.record_flush(
                    size=len(records),
                    latency=write_time * 1000,  # 转换为毫秒
                    success=True
                )
                
                logger.info(f"Successfully wrote {len(records)} records to database in {write_time:.2f}s")
                
            except Exception as e:
                logger.error(f"Database write error: {str(e)}")
                session.rollback()
                
                # 写入失败，放回队列
                async with self._queue_lock:
                    for record in reversed(records):
                        self.write_queue.appendleft(record)
                        
                self.metrics.record_flush(
                    size=len(records),
                    latency=(time.time() - start_time) * 1000,
                    success=False
                )
                raise
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Flush error: {str(e)}")
            # 触发重试
            self._flush_event.set()
            
    def get_stats(self) -> Dict[str, Any]:
        """获取写入统计信息"""
        return {
            'queue_size': len(self.write_queue),
            'cache_size': len(self.cache),
            **self.metrics.get_metrics()
        }

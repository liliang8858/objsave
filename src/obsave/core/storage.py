"""ObSave core storage module."""

from typing import Optional, Dict, Any
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from threading import Lock
import time
from .exceptions import StorageError, ObjectNotFoundError
from obsave.monitoring.metrics import metrics_collector

logger = logging.getLogger(__name__)

class ObjectStorage:
    """对象存储核心类"""
    
    def __init__(self, 
                 base_path: str = "storage",
                 chunk_size: int = 64 * 1024,  # 64KB chunks
                 max_concurrent_ops: int = 32,
                 max_retries: int = 3,
                 retry_delay: float = 0.1):
        self.base_path = os.path.abspath(base_path)
        self.chunk_size = chunk_size
        self.max_concurrent_ops = max_concurrent_ops
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # 创建存储目录
        os.makedirs(self.base_path, exist_ok=True)
        
        # 统计信息
        self.stats = {
            "writes": 0,
            "reads": 0,
            "deletes": 0,
            "total_size": 0,
            "total_files": 0,
            "errors": 0,
            "retries": 0
        }
        self.stats_lock = Lock()
        
        # 并发控制
        self.semaphore = asyncio.Semaphore(max_concurrent_ops)
        self.thread_pool = ThreadPoolExecutor(
            max_workers=max_concurrent_ops,
            thread_name_prefix="storage_worker"
        )
        
        # 初始化时统计存储信息
        self._update_storage_stats()
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def close(self):
        """关闭存储管理器，清理资源"""
        if hasattr(self, 'thread_pool'):
            self.thread_pool.shutdown(wait=True)
            
    async def _retry_operation(self, operation, *args, **kwargs):
        """重试操作的包装器"""
        for attempt in range(self.max_retries):
            try:
                return await operation(*args, **kwargs)
            except Exception as e:
                with self.stats_lock:
                    self.stats["errors"] += 1
                    self.stats["retries"] += 1
                
                if attempt == self.max_retries - 1:
                    logger.error(f"Operation failed after {self.max_retries} attempts: {str(e)}")
                    raise StorageError(f"Operation failed: {str(e)}")
                    
                await asyncio.sleep(self.retry_delay * (attempt + 1))
                logger.warning(f"Retrying operation, attempt {attempt + 2}/{self.max_retries}")
                
    def _get_file_path(self, key: str) -> str:
        """获取文件的完整路径"""
        # 使用key的前4个字符作为子目录，减少单目录文件数
        if len(key) >= 4:
            subdir = key[:4]
            full_path = os.path.join(self.base_path, subdir)
            os.makedirs(full_path, exist_ok=True)
            return os.path.join(full_path, key)
        return os.path.join(self.base_path, key)
        
    async def store(self, key: str, data: bytes) -> bool:
        """存储数据"""
        try:
            result = await self._retry_operation(self._store, key, data)
            metrics_collector.track_storage_operation('store', result)
            return result
        except Exception as e:
            metrics_collector.track_storage_operation('store', False)
            raise
            
    async def _store(self, key: str, data: bytes) -> bool:
        try:
            async with self.semaphore:
                file_path = self._get_file_path(key)
                temp_path = f"{file_path}.tmp"
                
                def write_file():
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    with open(temp_path, 'wb') as f:
                        for i in range(0, len(data), self.chunk_size):
                            chunk = data[i:i + self.chunk_size]
                            f.write(chunk)
                    # 原子性重命名
                    os.replace(temp_path, file_path)
                        
                await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool,
                    write_file
                )
                
                with self.stats_lock:
                    self.stats["writes"] += 1
                    self.stats["total_size"] += len(data)
                    self.stats["total_files"] += 1
                    
                return True
                
        except Exception as e:
            logger.error(f"Error storing data for key {key}: {str(e)}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            return False
            
    async def get(self, key: str) -> Optional[bytes]:
        """获取数据"""
        try:
            result = await self._retry_operation(self._get, key)
            metrics_collector.track_storage_operation('get', result is not None)
            return result
        except Exception as e:
            metrics_collector.track_storage_operation('get', False)
            raise
            
    async def _get(self, key: str) -> Optional[bytes]:
        try:
            async with self.semaphore:
                file_path = self._get_file_path(key)
                if not os.path.exists(file_path):
                    raise ObjectNotFoundError(key)
                    
                def read_file():
                    with open(file_path, 'rb') as f:
                        data = bytearray()
                        while True:
                            chunk = f.read(self.chunk_size)
                            if not chunk:
                                break
                            data.extend(chunk)
                        return bytes(data)
                        
                data = await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool,
                    read_file
                )
                
                with self.stats_lock:
                    self.stats["reads"] += 1
                    
                return data
                
        except ObjectNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error reading data for key {key}: {str(e)}")
            raise StorageError(f"Failed to read object: {str(e)}")
            
    async def delete(self, key: str) -> bool:
        """删除数据"""
        try:
            result = await self._retry_operation(self._delete, key)
            metrics_collector.track_storage_operation('delete', result)
            return result
        except Exception as e:
            metrics_collector.track_storage_operation('delete', False)
            raise
            
    async def _delete(self, key: str) -> bool:
        try:
            async with self.semaphore:
                file_path = self._get_file_path(key)
                if not os.path.exists(file_path):
                    return False
                    
                file_size = os.path.getsize(file_path)
                
                def delete_file():
                    os.remove(file_path)
                    # 如果目录为空，删除目录
                    dir_path = os.path.dirname(file_path)
                    if not os.listdir(dir_path):
                        os.rmdir(dir_path)
                        
                await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool,
                    delete_file
                )
                
                with self.stats_lock:
                    self.stats["deletes"] += 1
                    self.stats["total_size"] -= file_size
                    self.stats["total_files"] -= 1
                    
                return True
                
        except Exception as e:
            logger.error(f"Error deleting data for key {key}: {str(e)}")
            return False
            
    def _update_storage_stats(self):
        """更新存储统计信息"""
        total_size = 0
        total_files = 0
        
        for root, _, files in os.walk(self.base_path):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    total_size += os.path.getsize(file_path)
                    total_files += 1
                except OSError:
                    continue
                    
        with self.stats_lock:
            self.stats["total_size"] = total_size
            self.stats["total_files"] = total_files
            
        # 更新 Prometheus 指标
        metrics_collector.update_storage_metrics(self.stats)
        
    def get_stats(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        with self.stats_lock:
            return {
                "total_files": self.stats["total_files"],
                "total_size": self.stats["total_size"],
                "total_size_mb": round(self.stats["total_size"] / (1024 * 1024), 2),
                "writes": self.stats["writes"],
                "reads": self.stats["reads"],
                "deletes": self.stats["deletes"],
                "errors": self.stats["errors"],
                "retries": self.stats["retries"]
            }

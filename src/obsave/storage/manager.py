import os
import shutil
from typing import Dict, Any, Optional
import logging
from threading import Lock
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class StorageManager:
    """
    存储管理器
    - 文件系统存储
    - 并发操作支持
    - 自动清理
    - 性能监控
    """
    def __init__(self, 
                 base_path: str = "storage",
                 chunk_size: int = 64 * 1024,  # 64KB chunks
                 max_concurrent_ops: int = 32):
        self.base_path = os.path.abspath(base_path)
        self.chunk_size = chunk_size
        self.max_concurrent_ops = max_concurrent_ops
        
        # 创建存储目录
        os.makedirs(self.base_path, exist_ok=True)
        
        # 统计信息
        self.stats = {
            "writes": 0,
            "reads": 0,
            "deletes": 0,
            "total_size": 0,
            "total_files": 0
        }
        self.stats_lock = Lock()
        
        # 并发控制
        self.semaphore = asyncio.Semaphore(max_concurrent_ops)
        self.thread_pool = ThreadPoolExecutor(max_workers=max_concurrent_ops)
        
        # 初始化时统计存储信息
        self._update_storage_stats()
        
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
            
    def _get_file_path(self, key: str) -> str:
        """获取文件的完整路径"""
        # 使用key的前4个字符作为子目录，减少单目录文件数
        if len(key) >= 4:
            subdir = key[:4]
            full_path = os.path.join(self.base_path, subdir)
            os.makedirs(full_path, exist_ok=True)
            return os.path.join(full_path, key)
        return os.path.join(self.base_path, key)
        
    async def set(self, key: str, data: bytes) -> bool:
        """异步存储数据"""
        try:
            async with self.semaphore:
                file_path = self._get_file_path(key)
                
                def write_file():
                    with open(file_path, 'wb') as f:
                        for i in range(0, len(data), self.chunk_size):
                            chunk = data[i:i + self.chunk_size]
                            f.write(chunk)
                            
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
            return False
            
    async def get(self, key: str) -> Optional[bytes]:
        """异步获取数据"""
        try:
            async with self.semaphore:
                file_path = self._get_file_path(key)
                if not os.path.exists(file_path):
                    return None
                    
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
                
        except Exception as e:
            logger.error(f"Error reading data for key {key}: {str(e)}")
            return None
            
    async def delete(self, key: str) -> bool:
        """异步删除数据"""
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
            
    def clear(self) -> None:
        """清空存储"""
        try:
            if os.path.exists(self.base_path):
                shutil.rmtree(self.base_path)
            os.makedirs(self.base_path, exist_ok=True)
            
            with self.stats_lock:
                self.stats["total_size"] = 0
                self.stats["total_files"] = 0
                
        except Exception as e:
            logger.error(f"Error clearing storage: {str(e)}")
            
    def get_stats(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        with self.stats_lock:
            return {
                "total_files": self.stats["total_files"],
                "total_size": self.stats["total_size"],
                "total_size_mb": round(self.stats["total_size"] / (1024 * 1024), 2),
                "writes": self.stats["writes"],
                "reads": self.stats["reads"],
                "deletes": self.stats["deletes"]
            }

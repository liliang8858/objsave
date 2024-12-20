import os
import asyncio
import selectors
import logging
import threading
from typing import Dict, Any, Optional, Callable, List
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import json
import time
import msvcrt
import win32file
import win32event
import pywintypes
import win32con

logger = logging.getLogger(__name__)

class AsyncPool:
    """异步任务池，用于管理和执行异步任务"""
    
    def __init__(self, max_workers: Optional[int] = None):
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.tasks: List[asyncio.Task] = []
        self.loop = asyncio.get_event_loop()
        
    async def submit(self, func: Callable, *args, **kwargs) -> Any:
        """提交一个任务到线程池执行"""
        return await self.loop.run_in_executor(
            self.executor,
            lambda: func(*args, **kwargs)
        )
        
    def submit_sync(self, func: Callable, *args, **kwargs) -> asyncio.Task:
        """同步提交一个异步任务"""
        task = self.loop.create_task(self.submit(func, *args, **kwargs))
        self.tasks.append(task)
        task.add_done_callback(lambda t: self.tasks.remove(t))
        return task
    
    async def wait_all(self):
        """等待所有任务完成"""
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
            
    def shutdown(self):
        """关闭线程池"""
        self.executor.shutdown(wait=True)

class WindowsIOManager:
    """Windows专用I/O管理器"""
    
    def __init__(self):
        self.pending_ops = {}
        self.callbacks = {}
        self._running = False
        self.stats = {
            'total_ops': 0,
            'pending_ops': 0,
            'completed_ops': 0,
            'failed_ops': 0
        }
        
    def start(self):
        self._running = True
        
    def stop(self):
        self._running = False
        
    def write_file(self, file_path: str, data: bytes):
        """异步写入文件"""
        try:
            # 创建或打开文件
            handle = win32file.CreateFile(
                file_path,
                win32file.GENERIC_WRITE,
                0,  # 不共享
                None,  # 默认安全属性
                win32file.CREATE_ALWAYS,
                win32file.FILE_FLAG_OVERLAPPED,
                None
            )
            
            # 创建一个重叠I/O事件
            event = win32event.CreateEvent(None, True, False, None)
            overlapped = pywintypes.OVERLAPPED()
            overlapped.hEvent = event
            
            # 写入数据
            win32file.WriteFile(handle, data, overlapped)
            
            # 等待操作完成
            win32event.WaitForSingleObject(event, win32event.INFINITE)
            
            # 获取结果
            bytes_written = win32file.GetOverlappedResult(handle, overlapped, True)
            
            # 关闭句柄
            handle.Close()
            
            self.stats['total_ops'] += 1
            self.stats['completed_ops'] += 1
            
            return bytes_written
            
        except Exception as e:
            self.stats['failed_ops'] += 1
            logger.error(f"Failed to write file {file_path}: {str(e)}")
            raise

class UnixIOManager:
    """Unix系统I/O管理器"""
    
    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self.write_buffers = {}
        self.callbacks = {}
        self._running = False
        self.stats = {
            'total_ops': 0,
            'pending_ops': 0,
            'completed_ops': 0,
            'failed_ops': 0
        }
        
    def start(self):
        self._running = True
        
    def stop(self):
        self._running = False
        self.selector.close()
        
    def write_file(self, file_path: str, data: bytes):
        """异步写入文件"""
        try:
            # Unix系统使用普通文件I/O
            with open(file_path, 'wb') as f:
                bytes_written = f.write(data)
                f.flush()
                os.fsync(f.fileno())
                
            self.stats['total_ops'] += 1
            self.stats['completed_ops'] += 1
            
            return bytes_written
            
        except Exception as e:
            self.stats['failed_ops'] += 1
            logger.error(f"Failed to write file {file_path}: {str(e)}")
            raise

class AsyncIOManager:
    """跨平台异步I/O管理器"""
    
    def __init__(self, max_workers: Optional[int] = None):
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers or min(32, (os.cpu_count() or 1) + 4)
        )
        # 根据操作系统选择合适的I/O管理器
        if os.name == 'nt':
            self.io_manager = WindowsIOManager()
        else:
            self.io_manager = UnixIOManager()
            
        self._running = False
        self.stats = {
            'start_time': time.time(),
            'total_bytes_written': 0,
            'total_files_written': 0
        }
        
    def start(self):
        """启动I/O管理器"""
        self._running = True
        self.io_manager.start()
        
    def stop(self):
        """停止I/O管理器"""
        self._running = False
        self.io_manager.stop()
        self.executor.shutdown(wait=True)
        
    async def write_file(self, file_path: str, data: bytes):
        """异步写入文件"""
        if not self._running:
            raise RuntimeError("AsyncIOManager is not running")
            
        loop = asyncio.get_event_loop()
        bytes_written = await loop.run_in_executor(
            self.executor,
            self.io_manager.write_file,
            file_path,
            data
        )
        
        self.stats['total_bytes_written'] += bytes_written
        self.stats['total_files_written'] += 1
        
        return bytes_written
        
    async def write_json(self, file_path: str, data: Dict[str, Any]):
        """异步写入JSON数据"""
        json_data = json.dumps(data, ensure_ascii=False).encode('utf-8')
        return await self.write_file(file_path, json_data)
        
    def get_stats(self):
        """获取I/O统计信息"""
        stats = {
            **self.stats,
            'uptime': time.time() - self.stats['start_time'],
            'io_manager_stats': self.io_manager.stats
        }
        return stats

import os
import asyncio
import selectors
import logging
import threading
from typing import Dict, Any, Optional, Callable
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

class WindowsIOManager:
    """Windows专用I/O管理器"""
    
    def __init__(self):
        self.pending_ops = {}
        self.callbacks = {}
        self._running = False
        
    def start(self):
        self._running = True
        
    def stop(self):
        self._running = False
        
    async def write_file(self, file_path: str, data: bytes) -> bool:
        try:
            # 创建或打开文件
            handle = win32file.CreateFile(
                file_path,
                win32file.GENERIC_WRITE,
                0,  # 不共享
                None,  # 默认安全属性
                win32file.CREATE_ALWAYS,
                win32file.FILE_FLAG_OVERLAPPED,  # 异步I/O
                None
            )
            
            try:
                # 创建一个事件对象
                event = win32event.CreateEvent(None, True, False, None)
                overlapped = pywintypes.OVERLAPPED()
                overlapped.hEvent = event
                
                # 写入数据
                win32file.WriteFile(handle, data, overlapped)
                
                # 等待完成
                rc = win32event.WaitForSingleObject(event, 5000)  # 5秒超时
                if rc == win32event.WAIT_TIMEOUT:
                    raise TimeoutError("Write operation timed out")
                    
                # 获取结果
                bytes_written = win32file.GetOverlappedResult(handle, overlapped, True)
                return bytes_written == len(data)
                
            finally:
                handle.Close()
                
        except Exception as e:
            logger.error(f"Windows write error: {str(e)}")
            return False
            
class UnixIOManager:
    """Unix系统I/O管理器"""
    
    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self.write_buffers = {}
        self.callbacks = {}
        self._running = False
        
    def start(self):
        self._running = True
        
    def stop(self):
        self._running = False
        self.selector.close()
        
    async def write_file(self, file_path: str, data: bytes) -> bool:
        try:
            # 以非阻塞模式打开文件
            fd = os.open(file_path, os.O_WRONLY | os.O_CREAT | os.O_NONBLOCK)
            try:
                # 注册写事件
                self.selector.register(fd, selectors.EVENT_WRITE)
                
                # 写入数据
                total_written = 0
                while total_written < len(data):
                    events = self.selector.select(timeout=5.0)  # 5秒超时
                    if not events:
                        raise TimeoutError("Write operation timed out")
                        
                    for key, mask in events:
                        if mask & selectors.EVENT_WRITE:
                            remaining = data[total_written:]
                            written = os.write(fd, remaining)
                            total_written += written
                            
                return total_written == len(data)
                
            finally:
                self.selector.unregister(fd)
                os.close(fd)
                
        except Exception as e:
            logger.error(f"Unix write error: {str(e)}")
            return False
            
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
        
    async def start(self):
        """启动I/O管理器"""
        if self._running:
            return
            
        self._running = True
        self.io_manager.start()
        logger.info("AsyncIO manager started")
        
    async def stop(self):
        """停止I/O管理器"""
        if not self._running:
            return
            
        self._running = False
        self.io_manager.stop()
        self.executor.shutdown(wait=False)
        logger.info("AsyncIO manager stopped")
        
    async def write_file(self, file_path: str, data: bytes) -> bool:
        """异步写入文件"""
        if not self._running:
            return False
            
        loop = asyncio.get_event_loop()
        try:
            # 在线程池中执行I/O操作
            return await loop.run_in_executor(
                self.executor,
                lambda: self.io_manager.write_file(file_path, data)
            )
        except Exception as e:
            logger.error(f"Write failed for {file_path}: {str(e)}")
            return False
            
    async def write_json(self, file_path: str, data: Dict[str, Any]) -> bool:
        """异步写入JSON数据"""
        try:
            json_data = json.dumps(data).encode('utf-8')
            return await self.write_file(file_path, json_data)
        except Exception as e:
            logger.error(f"JSON write failed for {file_path}: {str(e)}")
            return False
            
    def get_stats(self) -> Dict[str, Any]:
        """获取I/O统计信息"""
        return {
            "thread_pool_size": self.executor._max_workers,
            "platform": "windows" if os.name == 'nt' else "unix"
        }

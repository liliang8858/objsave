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

logger = logging.getLogger(__name__)

class AsyncIOManager:
    """跨平台异步I/O管理器"""
    
    def __init__(self, max_workers: Optional[int] = None):
        self.selector = selectors.DefaultSelector()
        self.loop = asyncio.get_event_loop()
        self.write_queue = Queue()
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers or min(32, (os.cpu_count() or 1) + 4)
        )
        self.write_buffers: Dict[int, bytearray] = {}
        self.callbacks: Dict[int, Callable] = {}
        self._running = False
        self._worker_thread = None
        
    async def start(self):
        """启动I/O管理器"""
        if self._running:
            return
            
        self._running = True
        self._worker_thread = threading.Thread(
            target=self._io_worker,
            daemon=True
        )
        self._worker_thread.start()
        logger.info("AsyncIO manager started")
        
    async def stop(self):
        """停止I/O管理器"""
        if not self._running:
            return
            
        self._running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=5.0)
        self.executor.shutdown(wait=False)
        self.selector.close()
        logger.info("AsyncIO manager stopped")
        
    def _io_worker(self):
        """I/O工作线程"""
        while self._running:
            try:
                events = self.selector.select(timeout=0.1)
                for key, mask in events:
                    fd = key.fd
                    if mask & selectors.EVENT_WRITE:
                        self._handle_write(fd)
                    if mask & selectors.EVENT_READ:
                        self._handle_read(fd)
                        
                # 处理写入队列
                while not self.write_queue.empty() and self._running:
                    fd, data, callback = self.write_queue.get_nowait()
                    self._register_write(fd, data, callback)
                    
            except Exception as e:
                logger.error(f"IO worker error: {str(e)}")
                time.sleep(0.1)
                
    def _register_write(self, fd: int, data: bytes, callback: Optional[Callable] = None):
        """注册写入操作"""
        try:
            self.write_buffers[fd] = bytearray(data)
            self.callbacks[fd] = callback
            self.selector.register(fd, selectors.EVENT_WRITE)
        except Exception as e:
            logger.error(f"Failed to register write for fd {fd}: {str(e)}")
            if callback:
                callback(False, str(e))
                
    def _handle_write(self, fd: int):
        """处理写入事件"""
        try:
            if fd not in self.write_buffers:
                return
                
            buffer = self.write_buffers[fd]
            while buffer:
                try:
                    sent = os.write(fd, buffer)
                    del buffer[:sent]
                except BlockingIOError:
                    break
                except Exception as e:
                    logger.error(f"Write error for fd {fd}: {str(e)}")
                    self._cleanup_fd(fd, success=False, error=str(e))
                    return
                    
            if not buffer:
                self._cleanup_fd(fd, success=True)
                
        except Exception as e:
            logger.error(f"Write handler error for fd {fd}: {str(e)}")
            self._cleanup_fd(fd, success=False, error=str(e))
            
    def _cleanup_fd(self, fd: int, success: bool, error: str = None):
        """清理文件描述符相关资源"""
        try:
            self.selector.unregister(fd)
        except Exception:
            pass
            
        callback = self.callbacks.pop(fd, None)
        self.write_buffers.pop(fd, None)
        
        if callback:
            try:
                callback(success, error)
            except Exception as e:
                logger.error(f"Callback error for fd {fd}: {str(e)}")
                
    async def write_file(self, file_path: str, data: bytes) -> bool:
        """异步写入文件"""
        future = self.loop.create_future()
        
        def write_callback(success: bool, error: Optional[str] = None):
            if success:
                self.loop.call_soon_threadsafe(future.set_result, True)
            else:
                self.loop.call_soon_threadsafe(
                    future.set_exception,
                    Exception(error or "Write failed")
                )
                
        try:
            # 以非阻塞模式打开文件
            flags = os.O_WRONLY | os.O_CREAT | os.O_NONBLOCK
            if os.name == 'nt':
                flags |= os.O_BINARY
                
            fd = os.open(file_path, flags)
            
            # 将写入请求加入队列
            self.write_queue.put((fd, data, write_callback))
            
            # 等待写入完成
            try:
                await future
                return True
            except Exception as e:
                logger.error(f"Write failed for {file_path}: {str(e)}")
                return False
            finally:
                try:
                    os.close(fd)
                except Exception:
                    pass
                    
        except Exception as e:
            logger.error(f"Failed to open file {file_path}: {str(e)}")
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
            "pending_writes": self.write_queue.qsize(),
            "active_fds": len(self.write_buffers),
            "thread_pool_size": self.executor._max_workers
        }

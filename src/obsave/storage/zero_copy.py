import os
import mmap
import ctypes
import logging
from typing import Optional
import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Windows specific
if os.name == 'nt':
    import msvcrt
    
    kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
    
    class OVERLAPPED(ctypes.Structure):
        _fields_ = [
            ('Internal', ctypes.c_void_p),
            ('InternalHigh', ctypes.c_void_p),
            ('Offset', ctypes.c_ulong),
            ('OffsetHigh', ctypes.c_ulong),
            ('hEvent', ctypes.c_void_p)
        ]

class ZeroCopyWriter:
    """零拷贝写入管理器"""
    
    def __init__(self, max_workers: int = None):
        self.thread_pool = ThreadPoolExecutor(
            max_workers=max_workers or min(32, (os.cpu_count() or 1) * 2),
            thread_name_prefix="zerocopy_worker"
        )
        self._local = threading.local()
        
    @contextmanager
    def _get_mmap(self, file_path: str, size: int) -> mmap.mmap:
        """获取内存映射"""
        if not hasattr(self._local, 'mmap_cache'):
            self._local.mmap_cache = {}
            
        if file_path in self._local.mmap_cache:
            yield self._local.mmap_cache[file_path]
            return
            
        # 创建或打开文件
        if os.name == 'nt':
            flags = os.O_RDWR | os.O_CREAT | os.O_BINARY
        else:
            flags = os.O_RDWR | os.O_CREAT
            
        fd = os.open(file_path, flags)
        try:
            # 设置文件大小
            os.ftruncate(fd, size)
            
            # 创建内存映射
            if os.name == 'nt':
                map_obj = mmap.mmap(fd, size, access=mmap.ACCESS_WRITE)
            else:
                map_obj = mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_WRITE)
                
            self._local.mmap_cache[file_path] = map_obj
            yield map_obj
            
        finally:
            os.close(fd)
            
    def _write_with_mmap(self, file_path: str, data: bytes, offset: int = 0):
        """使用mmap写入数据"""
        size = len(data) + offset
        
        with self._get_mmap(file_path, size) as mm:
            mm.seek(offset)
            mm.write(data)
            mm.flush()  # 强制刷新到磁盘
            
    async def write_file(self, file_path: str, data: bytes, offset: int = 0):
        """异步写入文件"""
        try:
            await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                self._write_with_mmap,
                file_path,
                data,
                offset
            )
        except Exception as e:
            logger.error(f"Zero-copy write failed: {str(e)}")
            raise
            
    def _copy_file_windows(self, src_path: str, dst_path: str):
        """Windows下的零拷贝文件复制"""
        try:
            # 打开源文件
            src_handle = kernel32.CreateFileW(
                src_path, 0x80000000, 0, None, 3, 0x80, None
            )
            if src_handle == -1:
                raise OSError(f"Failed to open source file: {src_path}")
                
            # 打开目标文件
            dst_handle = kernel32.CreateFileW(
                dst_path, 0x40000000, 0, None, 2, 0x80, None
            )
            if dst_handle == -1:
                kernel32.CloseHandle(src_handle)
                raise OSError(f"Failed to open destination file: {dst_path}")
                
            try:
                # 获取文件大小
                size = kernel32.GetFileSize(src_handle, None)
                if size == 0xFFFFFFFF:
                    raise OSError("Failed to get file size")
                    
                # 创建重叠结构
                overlapped = OVERLAPPED()
                overlapped.Offset = 0
                overlapped.OffsetHigh = 0
                
                # 执行传输
                result = kernel32.TransmitFile(
                    dst_handle, src_handle, size, 0, ctypes.byref(overlapped),
                    None, 0
                )
                if not result:
                    raise OSError("TransmitFile failed")
                    
            finally:
                kernel32.CloseHandle(src_handle)
                kernel32.CloseHandle(dst_handle)
                
        except Exception as e:
            logger.error(f"Windows zero-copy failed: {str(e)}")
            raise
            
    def _copy_file_linux(self, src_path: str, dst_path: str):
        """Linux下的零拷贝文件复制"""
        try:
            src_fd = os.open(src_path, os.O_RDONLY)
            dst_fd = os.open(dst_path, os.O_WRONLY | os.O_CREAT)
            
            try:
                # 获取文件大小
                size = os.fstat(src_fd).st_size
                
                # 使用sendfile
                os.sendfile(dst_fd, src_fd, 0, size)
                
            finally:
                os.close(src_fd)
                os.close(dst_fd)
                
        except Exception as e:
            logger.error(f"Linux zero-copy failed: {str(e)}")
            raise
            
    async def copy_file(self, src_path: str, dst_path: str):
        """异步零拷贝文件复制"""
        copy_func = self._copy_file_windows if os.name == 'nt' else self._copy_file_linux
        
        try:
            await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                copy_func,
                src_path,
                dst_path
            )
        except Exception as e:
            logger.error(f"File copy failed: {str(e)}")
            raise
            
    def cleanup(self):
        """清理资源"""
        if hasattr(self._local, 'mmap_cache'):
            for mm in self._local.mmap_cache.values():
                mm.close()
            self._local.mmap_cache.clear()
        self.thread_pool.shutdown(wait=True)

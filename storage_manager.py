import os
import json
import logging
from typing import Any, Dict, Optional
from datetime import datetime
import tempfile
import pickle
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

class StorageManager:
    """存储管理器，负责处理文件的持久化存储"""
    
    def __init__(self, base_dir: str = None):
        """
        初始化存储管理器
        base_dir: 基础存储目录，如果不指定则使用系统临时目录
        """
        if base_dir:
            self.base_dir = Path(base_dir)
        else:
            self.base_dir = Path(tempfile.gettempdir()) / "obsave_storage"
            
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir = self.base_dir / "temp"
        self.temp_dir.mkdir(exist_ok=True)
        self.data_dir = self.base_dir / "data"
        self.data_dir.mkdir(exist_ok=True)
        
        # 统计信息
        self._stats = {
            "total_files": 0,
            "total_size": 0,
            "writes": 0,
            "reads": 0,
            "deletes": 0
        }
        
        # 初始化时统计现有文件
        self._update_stats()
        
    def _update_stats(self):
        """更新存储统计信息"""
        total_size = 0
        total_files = 0
        
        for path in self.data_dir.rglob("*"):
            if path.is_file():
                total_files += 1
                total_size += path.stat().st_size
                
        self._stats["total_files"] = total_files
        self._stats["total_size"] = total_size
        
    def _get_path(self, key: str) -> Path:
        """获取存储路径"""
        # 使用key的前4个字符作为子目录，避免单目录文件过多
        if len(key) >= 4:
            subdir = key[:4]
        else:
            subdir = key
            
        path = self.data_dir / subdir
        path.mkdir(exist_ok=True)
        return path / key
        
    def set(self, key: str, value: bytes) -> bool:
        """
        存储数据
        key: 唯一标识符
        value: 要存储的二进制数据
        """
        try:
            path = self._get_path(key)
            
            # 先写入临时文件
            temp_path = self.temp_dir / f"{key}.tmp"
            with open(temp_path, "wb") as f:
                f.write(value)
                
            # 移动到最终位置
            shutil.move(str(temp_path), str(path))
            
            # 更新统计信息
            self._stats["writes"] += 1
            self._stats["total_files"] += 1
            self._stats["total_size"] += len(value)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to store data for key {key}: {str(e)}")
            return False
            
    def get(self, key: str) -> Optional[bytes]:
        """
        获取数据
        key: 唯一标识符
        """
        try:
            path = self._get_path(key)
            if not path.exists():
                return None
                
            with open(path, "rb") as f:
                data = f.read()
                
            self._stats["reads"] += 1
            return data
            
        except Exception as e:
            logger.error(f"Failed to read data for key {key}: {str(e)}")
            return None
            
    def delete(self, key: str) -> bool:
        """
        删除数据
        key: 唯一标识符
        """
        try:
            path = self._get_path(key)
            if not path.exists():
                return False
                
            size = path.stat().st_size
            path.unlink()
            
            # 更新统计信息
            self._stats["deletes"] += 1
            self._stats["total_files"] -= 1
            self._stats["total_size"] -= size
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete data for key {key}: {str(e)}")
            return False
            
    def clear(self):
        """清空所有数据"""
        try:
            shutil.rmtree(str(self.data_dir))
            self.data_dir.mkdir(exist_ok=True)
            
            # 重置统计信息
            self._stats["total_files"] = 0
            self._stats["total_size"] = 0
            
        except Exception as e:
            logger.error(f"Failed to clear storage: {str(e)}")
            
    def get_stats(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        return {
            "total_files": self._stats["total_files"],
            "total_size": self._stats["total_size"],
            "total_size_mb": round(self._stats["total_size"] / (1024 * 1024), 2),
            "writes": self._stats["writes"],
            "reads": self._stats["reads"],
            "deletes": self._stats["deletes"]
        }

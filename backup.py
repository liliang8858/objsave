import os
import shutil
import json
import gzip
import logging
from datetime import datetime
import asyncio
from typing import Optional, List
from pathlib import Path
import aiofiles
import aiofiles.os
from config import settings

logger = logging.getLogger(__name__)

class BackupManager:
    def __init__(self, backup_dir: str = "backups"):
        self.backup_dir = backup_dir
        self.metadata_file = os.path.join(backup_dir, "backup_metadata.json")
        os.makedirs(backup_dir, exist_ok=True)
    
    async def create_backup(self, backup_name: Optional[str] = None) -> str:
        """创建一个新的备份"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = backup_name or f"backup_{timestamp}"
            backup_path = os.path.join(self.backup_dir, backup_name)
            os.makedirs(backup_path, exist_ok=True)
            
            # 备份存储目录
            storage_backup_path = os.path.join(backup_path, "storage")
            await self._backup_directory(settings.STORAGE_BASE_PATH, storage_backup_path)
            
            # 备份数据库
            db_backup_path = os.path.join(backup_path, "database.db")
            await self._backup_database(db_backup_path)
            
            # 记录备份元数据
            metadata = {
                "backup_name": backup_name,
                "timestamp": timestamp,
                "storage_size": await self._get_dir_size(storage_backup_path),
                "database_size": os.path.getsize(db_backup_path) if os.path.exists(db_backup_path) else 0
            }
            await self._update_metadata(backup_name, metadata)
            
            logger.info(f"Backup created successfully: {backup_name}")
            return backup_name
            
        except Exception as e:
            logger.error(f"Failed to create backup: {str(e)}")
            raise
    
    async def restore_backup(self, backup_name: str) -> bool:
        """从备份恢复数据"""
        try:
            backup_path = os.path.join(self.backup_dir, backup_name)
            if not os.path.exists(backup_path):
                raise ValueError(f"Backup {backup_name} not found")
            
            # 恢复存储目录
            storage_backup_path = os.path.join(backup_path, "storage")
            if os.path.exists(storage_backup_path):
                await self._restore_directory(storage_backup_path, settings.STORAGE_BASE_PATH)
            
            # 恢复数据库
            db_backup_path = os.path.join(backup_path, "database.db")
            if os.path.exists(db_backup_path):
                await self._restore_database(db_backup_path)
            
            logger.info(f"Backup {backup_name} restored successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore backup: {str(e)}")
            raise
    
    async def list_backups(self) -> List[dict]:
        """列出所有可用的备份"""
        try:
            if not os.path.exists(self.metadata_file):
                return []
            
            async with aiofiles.open(self.metadata_file, 'r') as f:
                content = await f.read()
                return json.loads(content)
        except Exception as e:
            logger.error(f"Failed to list backups: {str(e)}")
            return []
    
    async def _backup_directory(self, src: str, dst: str):
        """备份目录（异步）"""
        def copy_directory():
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
        
        await asyncio.to_thread(copy_directory)
    
    async def _restore_directory(self, src: str, dst: str):
        """恢复目录（异步）"""
        def restore_directory():
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
        
        await asyncio.to_thread(restore_directory)
    
    async def _backup_database(self, backup_path: str):
        """备份数据库（异步）"""
        def copy_database():
            if os.path.exists("objsave.db"):
                shutil.copy2("objsave.db", backup_path)
        
        await asyncio.to_thread(copy_database)
    
    async def _restore_database(self, backup_path: str):
        """恢复数据库（异步）"""
        def restore_database():
            if os.path.exists(backup_path):
                if os.path.exists("objsave.db"):
                    os.remove("objsave.db")
                shutil.copy2(backup_path, "objsave.db")
        
        await asyncio.to_thread(restore_database)
    
    async def _get_dir_size(self, path: str) -> int:
        """获取目录大小（异步）"""
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                total_size += await aiofiles.os.path.getsize(file_path)
        return total_size
    
    async def _update_metadata(self, backup_name: str, metadata: dict):
        """更新备份元数据"""
        try:
            existing_metadata = []
            if os.path.exists(self.metadata_file):
                async with aiofiles.open(self.metadata_file, 'r') as f:
                    content = await f.read()
                    existing_metadata = json.loads(content)
            
            # 更新或添加新的元数据
            updated = False
            for i, item in enumerate(existing_metadata):
                if item["backup_name"] == backup_name:
                    existing_metadata[i] = metadata
                    updated = True
                    break
            
            if not updated:
                existing_metadata.append(metadata)
            
            async with aiofiles.open(self.metadata_file, 'w') as f:
                await f.write(json.dumps(existing_metadata, indent=2))
                
        except Exception as e:
            logger.error(f"Failed to update backup metadata: {str(e)}")
            raise

backup_manager = BackupManager()

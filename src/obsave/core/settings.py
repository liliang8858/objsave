"""Global settings for ObSave."""

import os
from typing import Optional
from pydantic import BaseModel, ConfigDict
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """应用程序设置"""
    # 存储设置
    BASE_PATH: str = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    STORAGE_BASE_PATH: str = os.path.join(BASE_PATH, "data")
    MAX_WORKERS: int = min(32, (os.cpu_count() or 1) + 4)
    CHUNK_SIZE: int = 8192  # 8KB
    MAX_UPLOAD_SIZE: int = 100 * 1024 * 1024  # 100MB
    
    # 数据库设置
    DATABASE_URL: str = f"sqlite:///{os.path.join(STORAGE_BASE_PATH, 'obsave.db')}"
    
    # API设置
    API_TITLE: str = "ObjSave API"
    API_DESCRIPTION: str = "对象存储服务 API"
    API_VERSION: str = "0.1.0"
    API_PREFIX: str = "/objsave"
    
    # 缓存设置
    CACHE_TTL: int = 300  # 5分钟
    CACHE_MAX_SIZE: int = 1000  # 最多缓存1000个对象
    CACHE_SHARDS: int = 32  # 缓存分片数
    
    # 安全设置
    SECRET_KEY: str = "your-secret-key-here"  # 请在生产环境中修改
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # 日志设置
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(levelname)s - %(message)s"
    LOG_DIR: str = os.path.join(BASE_PATH, "logs")
    LOG_FILE: str = os.path.join(LOG_DIR, "obsave.log")
    LOG_MAX_BYTES: int = 10 * 1024 * 1024  # 10MB
    LOG_BACKUP_COUNT: int = 5
    
    model_config = ConfigDict(env_file=".env", case_sensitive=True)

# 创建全局设置实例
settings = Settings()

# 导出所有设置变量
MAX_WORKERS = settings.MAX_WORKERS
CHUNK_SIZE = settings.CHUNK_SIZE
MAX_UPLOAD_SIZE = settings.MAX_UPLOAD_SIZE
STORAGE_BASE_PATH = settings.STORAGE_BASE_PATH
DATABASE_URL = settings.DATABASE_URL
API_TITLE = settings.API_TITLE
API_DESCRIPTION = settings.API_DESCRIPTION
API_VERSION = settings.API_VERSION
API_PREFIX = settings.API_PREFIX
CACHE_TTL = settings.CACHE_TTL
CACHE_MAX_SIZE = settings.CACHE_MAX_SIZE
CACHE_SHARDS = settings.CACHE_SHARDS
SECRET_KEY = settings.SECRET_KEY
ALGORITHM = settings.ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES

# 日志设置
LOG_LEVEL = settings.LOG_LEVEL
LOG_FORMAT = settings.LOG_FORMAT
LOG_DIR = settings.LOG_DIR
LOG_FILE = settings.LOG_FILE
LOG_MAX_BYTES = settings.LOG_MAX_BYTES
LOG_BACKUP_COUNT = settings.LOG_BACKUP_COUNT

# Create necessary directories
for directory in [STORAGE_BASE_PATH, LOG_DIR]:
    os.makedirs(directory, exist_ok=True)

"""Global settings for ObSave."""

import os
from typing import Optional
from pydantic import BaseSettings

class Settings(BaseSettings):
    """应用程序设置"""
    # 存储设置
    STORAGE_BASE_PATH: str = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "data")
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
    
    # 安全设置
    SECRET_KEY: str = "your-secret-key-here"  # 请在生产环境中修改
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    class Config:
        env_file = ".env"
        case_sensitive = True

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
SECRET_KEY = settings.SECRET_KEY
ALGORITHM = settings.ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES

# Create necessary directories
for directory in [STORAGE_BASE_PATH]:
    os.makedirs(directory, exist_ok=True)

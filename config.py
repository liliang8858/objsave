from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    # 系统配置
    MAX_UPLOAD_SIZE: int = 50 * 1024 * 1024  # 50MB
    MAX_WORKERS: int = 32
    CHUNK_SIZE: int = 64 * 1024  # 64KB chunks
    
    # 缓存配置
    CACHE_MAX_ITEMS: int = 20000
    CACHE_SHARDS: int = 32
    CACHE_TTL: int = 3600
    
    # 存储配置
    STORAGE_BASE_PATH: str = "storage"
    
    # 写入管理器配置
    WRITE_BATCH_SIZE: int = 100
    WRITE_FLUSH_INTERVAL: float = 1.0
    WRITE_MAX_QUEUE_SIZE: int = 10000
    
    # 数据库配置
    DATABASE_URL: str = "sqlite:///./objsave.db"
    
    class Config:
        env_file = ".env"

settings = Settings()

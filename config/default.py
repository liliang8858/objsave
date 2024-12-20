from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    # 系统配置
    MAX_UPLOAD_SIZE: int = 50 * 1024 * 1024  # 50MB
    MAX_WORKERS: int = 32
    CHUNK_SIZE: int = 64 * 1024  # 64KB chunks
    
    # 缓存配置
    CACHE_MAX_SIZE: int = 20000
    CACHE_SHARDS: int = 32
    CACHE_TTL: int = 3600
    
    # 存储配置
    STORAGE_BASE_PATH: str = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "storage"))
    
    # 写入管理器配置
    WRITE_BATCH_SIZE: int = 100
    WRITE_FLUSH_INTERVAL: float = 1.0
    WRITE_MAX_QUEUE_SIZE: int = 10000
    
    # 数据库配置
    DATABASE_URL: str = f"sqlite:///{os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'objsave.db')}"
    
    # API配置
    API_TITLE: str = "ObjSave API"
    API_DESCRIPTION: str = "对象存储服务 API"
    API_VERSION: str = "0.1.0"
    API_PREFIX: str = "/objsave"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 确保存储目录存在
        os.makedirs(self.STORAGE_BASE_PATH, exist_ok=True)
    
    class Config:
        env_file = ".env"

settings = Settings()

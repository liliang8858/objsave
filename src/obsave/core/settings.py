"""Global settings for ObSave."""

import os
import multiprocessing

# System settings
MAX_WORKERS = min(32, multiprocessing.cpu_count() * 4)
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB
REQUEST_TIMEOUT = 30  # seconds
CHUNK_SIZE = 64 * 1024  # 64KB chunks

# Cache settings
CACHE_MAX_ITEMS = 20000
CACHE_SHARDS = 32
CACHE_TTL = 3600  # 1 hour

# Storage settings
STORAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'storage')
BACKUP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'backups')
STORAGE_BASE_PATH = STORAGE_DIR

# Database settings
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///./objsave.db')

# API settings
API_PREFIX = '/objsave'
API_TITLE = 'ObSave API'
API_DESCRIPTION = """
# ObjSave 对象存储服务

ObjSave是一个高性能的对象存储服务，提供以下主要功能：

## 核心功能
* 文件上传和下载
* JSON对象存储和查询
* 数据备份和恢复
* 性能监控

## 技术特点
* 高并发处理
* 异步IO操作
* 缓存优化
* 实时监控

## 使用说明
所有API端点都以 `/objsave` 为前缀。详细的API文档请参考下面的接口说明。
"""
API_VERSION = '1.0.0'
API_DOCS_URL = '/objsave/docs'
API_REDOC_URL = '/objsave/redoc'
API_OPENAPI_URL = '/objsave/openapi.json'

# Write Manager settings
WRITE_BATCH_SIZE = 100
WRITE_FLUSH_INTERVAL = 1.0  # seconds
WRITE_MAX_QUEUE_SIZE = 10000

# Logging settings
LOG_LEVEL = os.getenv('LOG_LEVEL', 'WARNING')
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'objsave.log')
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
LOG_BACKUP_COUNT = 5

# Create necessary directories
for directory in [STORAGE_DIR, BACKUP_DIR, LOG_DIR]:
    os.makedirs(directory, exist_ok=True)

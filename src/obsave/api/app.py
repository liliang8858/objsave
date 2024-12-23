import os
import uuid
from typing import List, Optional, Dict, Any, Union, Set
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException, APIRouter, Request, Response, BackgroundTasks
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel, ConfigDict
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from contextlib import asynccontextmanager
from jsonpath_ng import parse as parse_jsonpath
from fastapi import FastAPI, HTTPException, Depends, File, UploadFile, Query, Body
from fastapi.responses import FileResponse, JSONResponse
from typing import List, Optional, Dict, Any, Union, Set
from sqlalchemy.orm import Session
from sqlalchemy import text, or_
import json
import uuid
import logging
import os
from cachetools import TTLCache
from datetime import datetime, timedelta
from jsonpath_ng import parse as parse_jsonpath
from sqlalchemy.exc import SQLAlchemyError
import threading
from queue import Queue, Empty
import threading

from obsave.core.storage import ObjectStorage
from obsave.core.exceptions import StorageError, ObjectNotFoundError, ObjSaveException
from obsave.core.database import get_db, init_db
from obsave.core.settings import settings  
from obsave.monitoring.metrics import metrics_collector
from obsave.monitoring.middleware import PrometheusMiddleware
from obsave.utils import CacheManager, RequestQueue, WriteManager, AsyncIOManager
from obsave.core.models import ObjectMetadata, JSONObjectModel, ObjectStorage as ObjectStorageModel

# 配置日志记录
logger = logging.getLogger("obsave")
logger.setLevel(getattr(logging, settings.LOG_LEVEL))  

# 创建日志处理器
file_handler = RotatingFileHandler(
    settings.LOG_FILE,  
    maxBytes=settings.LOG_MAX_BYTES,  
    backupCount=settings.LOG_BACKUP_COUNT  
)
file_handler.setFormatter(logging.Formatter(settings.LOG_FORMAT))  
logger.addHandler(file_handler)

# 如果不是生产环境，也添加控制台输出
if settings.LOG_LEVEL == "DEBUG":  
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(settings.LOG_FORMAT))  
    logger.addHandler(console_handler)

# 系统配置
executor = ThreadPoolExecutor(max_workers=settings.MAX_WORKERS)  

# 创建线程池
thread_pool = ThreadPoolExecutor(
    max_workers=settings.MAX_WORKERS * 2,  
    thread_name_prefix="db_worker"
)

# 创建上传专用线程池
upload_thread_pool = ThreadPoolExecutor(
    max_workers=settings.MAX_WORKERS,  
    thread_name_prefix="upload_worker"
)

# 创建I/O管理器实例
io_manager = AsyncIOManager(max_workers=settings.MAX_WORKERS)  

# 初始化存储实例
storage = ObjectStorage(
    base_path=settings.STORAGE_BASE_PATH,  
    chunk_size=settings.CHUNK_SIZE,  
    max_concurrent_ops=settings.MAX_WORKERS  
)

# 缓存配置
CACHE_CONFIG = {
    'DEFAULT_TTL': 60,  # 默认缓存时间（秒）
    'MAX_SIZE': 1000,   # 最大缓存条目数
    'QUERY_TTL': 30,    # 查询结果缓存时间
    'LIST_TTL': 60,     # 列表缓存时间
    'METADATA_TTL': 300 # 元数据缓存时间
}

class CacheKeyManager:
    """缓存键管理器，用于跟踪缓存键之间的关系"""
    
    def __init__(self):
        # 存储对象ID到缓存键的映射
        self._object_cache_keys = {}
        # 存储查询结果中包含的对象ID
        self._query_objects = {}
        # 用于存储列表缓存键
        self._list_cache_keys = set()
        self._lock = threading.RLock()  # 使用可重入锁
        
    def add_object_cache_key(self, object_id: str, cache_key: str):
        """添加对象相关的缓存键"""
        try:
            with self._lock:
                if object_id not in self._object_cache_keys:
                    self._object_cache_keys[object_id] = set()
                self._object_cache_keys[object_id].add(cache_key)
        except Exception as e:
            logger.error(f"Error adding object cache key: {str(e)}")
            
    def add_query_result(self, cache_key: str, object_ids: List[str]):
        """记录查询结果中包含的对象ID"""
        if not object_ids:
            return
            
        try:
            with self._lock:
                self._query_objects[cache_key] = set(object_ids)
                # 为每个对象添加反向引用
                for obj_id in object_ids:
                    if obj_id not in self._object_cache_keys:
                        self._object_cache_keys[obj_id] = set()
                    self._object_cache_keys[obj_id].add(cache_key)
        except Exception as e:
            logger.error(f"Error adding query result: {str(e)}")
            
    def add_list_cache_key(self, cache_key: str):
        """添加列表缓存键"""
        try:
            with self._lock:
                self._list_cache_keys.add(cache_key)
        except Exception as e:
            logger.error(f"Error adding list cache key: {str(e)}")
            
    def get_related_cache_keys(self, object_id: str) -> Set[str]:
        """获取与对象相关的所有缓存键"""
        try:
            with self._lock:
                # 直接相关的缓存键
                related_keys = self._object_cache_keys.get(object_id, set()).copy()
                # 包含该对象的查询结果缓存键
                for query_key, obj_ids in self._query_objects.items():
                    if object_id in obj_ids:
                        related_keys.add(query_key)
                return related_keys
        except Exception as e:
            logger.error(f"Error getting related cache keys: {str(e)}")
            return set()
            
    def clear_object_cache_keys(self, object_id: str) -> Set[str]:
        """清除对象相关的所有缓存键记录"""
        try:
            with self._lock:
                # 获取所有相关的缓存键
                related_keys = self.get_related_cache_keys(object_id)
                # 从所有映射中删除这些键
                self._object_cache_keys.pop(object_id, None)
                keys_to_remove = []
                for key in self._query_objects:
                    if key in related_keys:
                        keys_to_remove.append(key)
                for key in keys_to_remove:
                    self._query_objects.pop(key, None)
                return related_keys
        except Exception as e:
            logger.error(f"Error clearing object cache keys: {str(e)}")
            return set()
            
    def clear_all(self):
        """清除所有缓存键记录"""
        try:
            with self._lock:
                self._object_cache_keys.clear()
                self._query_objects.clear()
                self._list_cache_keys.clear()
        except Exception as e:
            logger.error(f"Error clearing all cache keys: {str(e)}")

class AsyncCacheInvalidator:
    """异步缓存清理器"""
    
    def __init__(self):
        self._queue = Queue()
        self._thread = threading.Thread(target=self._process_queue, daemon=True)
        self._running = True
        self._thread.start()
        
    def _process_queue(self):
        """处理缓存清理队列"""
        while self._running:
            try:
                # 从队列获取任务，最多等待1秒
                task = self._queue.get(timeout=1)
                if task is None:
                    continue
                    
                object_id, operation = task
                logger.debug(f"Processing async cache invalidation for object {object_id}, operation: {operation}")
                
                try:
                    # 获取所有相关的缓存键
                    related_keys = cache_key_manager.get_related_cache_keys(object_id)
                    
                    # 删除对象的直接缓存
                    safe_cache_pop(query_cache, f"content:{object_id}")
                    safe_cache_pop(metadata_cache, f"metadata:{object_id}")
                    
                    # 删除所有相关的查询缓存
                    for key in related_keys:
                        safe_cache_pop(query_cache, key)
                        safe_cache_pop(list_cache, key)
                    
                    # 清除缓存键记录
                    cache_key_manager.clear_object_cache_keys(object_id)
                    logger.info(f"Async cache invalidation completed for object {object_id}")
                    
                except Exception as e:
                    logger.error(f"Error in async cache invalidation for object {object_id}: {str(e)}")
                    
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in cache invalidation thread: {str(e)}")
                
    def invalidate(self, object_id: str, operation: str):
        """添加缓存失效任务到队列"""
        try:
            self._queue.put_nowait((object_id, operation))
            logger.debug(f"Queued cache invalidation for object {object_id}, operation: {operation}")
        except Exception as e:
            logger.error(f"Error queuing cache invalidation: {str(e)}")
            
    def stop(self):
        """停止缓存清理线程"""
        self._running = False
        self._thread.join(timeout=5)

def safe_cache_pop(cache, key: str) -> None:
    """安全地删除缓存值"""
    try:
        cache.pop(key, None)
    except Exception as e:
        logger.error(f"Error popping cache for key {key}: {str(e)}")

def invalidate_cache(object_id: Optional[str] = None):
    """使缓存失效"""
    try:
        if object_id:
            # 获取所有相关的缓存键
            related_keys = cache_key_manager.get_related_cache_keys(object_id)
            
            # 删除对象的直接缓存
            try:
                query_cache.pop(f"content:{object_id}", None)
            except Exception:
                pass
                
            try:
                metadata_cache.pop(f"metadata:{object_id}", None)
            except Exception:
                pass
            
            # 删除所有相关的查询缓存
            for key in related_keys:
                try:
                    query_cache.pop(key, None)
                except Exception:
                    pass
                try:
                    list_cache.pop(key, None)
                except Exception:
                    pass
            
            # 清除缓存键记录
            cache_key_manager.clear_object_cache_keys(object_id)
            logger.info(f"Cache invalidated for object {object_id} and related queries")
        else:
            # 删除所有缓存
            try:
                query_cache.clear()
                list_cache.clear()
                metadata_cache.clear()
                cache_key_manager.clear_all()
                logger.info("All caches invalidated")
            except Exception as e:
                logger.error(f"Error clearing all caches: {str(e)}")
    except Exception as e:
        logger.error(f"Error in invalidate_cache: {str(e)}")

def safe_cache_set(cache, key: str, value: Any):
    """安全地设置缓存值"""
    try:
        cache[key] = value
        return True
    except Exception as e:
        logger.error(f"Error setting cache for key {key}: {str(e)}")
        return False

def safe_cache_get(cache, key: str) -> Optional[Any]:
    """安全地获取缓存值"""
    try:
        return cache.get(key)
    except Exception as e:
        logger.error(f"Error getting cache for key {key}: {str(e)}")
        return None

# 初始化缓存和缓存键管理器
query_cache = TTLCache(maxsize=CACHE_CONFIG['MAX_SIZE'], ttl=CACHE_CONFIG['QUERY_TTL'])
list_cache = TTLCache(maxsize=CACHE_CONFIG['MAX_SIZE'], ttl=CACHE_CONFIG['LIST_TTL'])
metadata_cache = TTLCache(maxsize=CACHE_CONFIG['MAX_SIZE'], ttl=CACHE_CONFIG['METADATA_TTL'])
cache_key_manager = CacheKeyManager()

# 初始化缓存管理器
cache = CacheManager(
    max_items=settings.CACHE_MAX_SIZE,  
    shards=settings.CACHE_SHARDS,  
    ttl=settings.CACHE_TTL  
)

# 创建请求队列实例
request_queue = RequestQueue(
    max_workers=settings.MAX_WORKERS,  
    max_queue_size=10000
)

# 全局写入管理器
write_manager = WriteManager(
    batch_size=100,
    flush_interval=1.0,
    max_queue_size=10000
)

# 创建异步IO管理器实例
io_manager = AsyncIOManager(
    max_workers=settings.MAX_WORKERS  
)

# 初始化缓存
# maxsize: 最大缓存条目数
# ttl: 缓存过期时间（秒）
cache = TTLCache(maxsize=1000, ttl=60)

def validate_jsonpath(jsonpath: str) -> bool:
    """验证 JSONPath 表达式的有效性"""
    try:
        parse_jsonpath(jsonpath)
        return True
    except Exception:
        return False

def clear_cache():
    """清除所有缓存"""
    cache.clear()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up server...")
    logger.info(f"Available workers: {settings.MAX_WORKERS}")  
    
    # 初始化数据库
    init_db()
    
    # 启动I/O管理器
    io_manager.start()
    
    # 启动写入管理器
    write_manager.start()
    
    # 启动请求队列处理器
    await request_queue.start()
    
    yield
    
    # Shutdown
    logger.info("Shutting down...")
    await write_manager.flush()  
    write_manager.stop()
    io_manager.stop()
    thread_pool.shutdown(wait=False)
    upload_thread_pool.shutdown(wait=False)

# 创建FastAPI应用
app = FastAPI(
    title=settings.API_TITLE,  
    description=settings.API_DESCRIPTION,  
    version="1.0.0",
    docs_url="/objsave/docs",
    openapi_url="/objsave/openapi.json",
    redoc_url="/objsave/redoc",
    lifespan=lifespan
)

# CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GZip中间件
app.add_middleware(GZipMiddleware, minimum_size=1000)

# 异常处理中间件
@app.exception_handler(ObjSaveException)
async def objsave_exception_handler(request: Request, exc: ObjSaveException):
    logger.error(f"Request failed: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error_code": exc.error_code,
            "detail": exc.detail
        }
    )

# 全局异常处理
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception occurred")
    return JSONResponse(
        status_code=500,
        content={
            "error_code": "INTERNAL_ERROR",
            "detail": "An internal server error occurred"
        }
    )

# 请求信号量，限制并发请求数
request_semaphore = asyncio.Semaphore(settings.MAX_WORKERS)  

# 请求ID中间件
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    try:
        async with request_semaphore:
            response = await call_next(request)
            return response
    except Exception as e:
        logger.error(f"Request {request_id} failed: {str(e)}")
        raise

# 超时中间件
@app.middleware("http")
async def timeout_middleware(request: Request, call_next):
    try:
        return await asyncio.wait_for(
            call_next(request),
            timeout=30
        )
    except asyncio.TimeoutError:
        logger.error("Request timeout")
        return JSONResponse(
            status_code=408,
            content={"detail": "Request timeout"}
        )

# HTTP指标中间件
@app.middleware("http")
async def http_metrics_middleware(request: Request, call_next):
    """HTTP请求指标收集中间件"""
    start_time = time.time()
    
    # 记录请求开始
    metrics_collector.track_request(
        method=request.method,
        endpoint=request.url.path
    )
    
    try:
        response = await call_next(request)
        
        # 记录请求结束
        metrics_collector.track_request_end(
            start_time=start_time,
            method=request.method,
            endpoint=request.url.path,
            status=response.status_code
        )
        
        return response
        
    except Exception as e:
        # 记录失败请求
        metrics_collector.track_request_end(
            start_time=start_time,
            method=request.method,
            endpoint=request.url.path,
            status=500
        )
        raise

# 创建路由器
router = APIRouter(
    prefix=settings.API_PREFIX,  
    tags=["object-storage"]
)

# 对象元数据模型
class ObjectMetadata(BaseModel):
    id: str
    name: str
    content_type: str
    size: int
    created_at: str
    type: Optional[str] = None
    owner: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

# JSON对象模型
class JSONObjectModel(BaseModel):
    id: Optional[str] = None
    type: str
    content: Dict[str, Any]
    name: Optional[str] = None
    content_type: str = "application/json"

    model_config = ConfigDict(from_attributes=True)

# JSON对象响应模型
class JSONObjectResponse(BaseModel):
    id: str
    name: str
    content_type: str
    size: int
    created_at: str
    type: Optional[str] = None
    content: Dict[str, Any]

    model_config = ConfigDict(from_attributes=True)

# JSON 查询模型
class JSONQueryModel(BaseModel):
    """JSON查询模型，用于定义JSON对象的查询条件"""
    jsonpath: str
    value: Optional[Any] = None
    operator: Optional[str] = 'eq'
    type: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

# 健康检查接口
@router.get("/health",
    summary="系统健康检查",
    description="检查系统健康状态并返回所有性能指标",
    response_description="返回系统状态和详细指标",
    tags=["monitoring"])
async def health_check():
    try:
        # 获取系统指标
        system_metrics = metrics_collector.collect_system_metrics()
        
        # 构建详细的健康状态响应
        health_status = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'system': system_metrics
        }
        
        return JSONResponse(
            content=health_status,
            status_code=200
        )
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return JSONResponse(
            content={
                'status': 'unhealthy',
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            },
            status_code=500
        )

# 上传对象接口
@router.post("/objects", response_model=ObjectMetadata)
async def upload_object(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    async with measure_time("upload_object"):
        if file.size and file.size > settings.MAX_UPLOAD_SIZE:  
            raise ObjSaveException(
                status_code=413,
                detail=f"File too large. Maximum size is {settings.MAX_UPLOAD_SIZE} bytes",  
                error_code="FILE_TOO_LARGE"
            )
        
        try:
            object_id = str(uuid.uuid4())
            
            # 异步存储文件
            storage_task = asyncio.create_task(
                storage.store_file(object_id, file)
            )
            
            # 创建元数据记录
            metadata = ObjectMetadata(
                id=object_id,
                name=file.filename,
                content_type=file.content_type,
                size=file.size,
                created_at=datetime.utcnow().isoformat()
            )
            
            # 等待存储完成
            await storage_task
            
            # 写入数据库
            await write_manager.write(metadata)
            metrics.record("objects_uploaded_total", 1)
            metrics.record("bytes_uploaded_total", file.size)
            
            # 异步触发缓存清理
            cache_invalidator.invalidate(object_id, "create")
            
            return metadata
            
        except Exception as e:
            logger.exception("Failed to upload object")
            await storage.cleanup(object_id)
            raise ObjSaveException(
                status_code=500,
                detail="Failed to upload object",
                error_code="UPLOAD_FAILED"
            ) from e

# 下载对象接口
@router.get("/download/{object_id}")
async def download_object(
    object_id: str,
    db: Session = Depends(get_db)
):
    """下载指定对象"""
    request_id = str(uuid.uuid4())
    logger.debug(f"[{request_id}] Starting download for object: {object_id}")
    
    try:
        # 查询数据库获取对象元数据
        db_obj = db.query(ObjectStorageModel).filter(ObjectStorageModel.id == object_id).first()
        if not db_obj:
            logger.warning(f"[{request_id}] Object not found in database: {object_id}")
            raise HTTPException(status_code=404, detail="Object not found")
            
        # 返回JSON响应
        return Response(
            content=db_obj.content,  
            media_type=db_obj.content_type or 'application/json',
            headers={
                "Content-Disposition": f'attachment; filename="{db_obj.name}"',
                "Content-Length": str(db_obj.size),
                "Cache-Control": "max-age=3600"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Download failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")

# 列出所有对象接口
@router.get("/list", response_model=List[ObjectMetadata])
async def list_objects(
    db: Session = Depends(get_db),
    limit: Optional[int] = 100,
    offset: Optional[int] = 0,
    last_id: Optional[str] = None  
):
    """列出存储的对象"""
    request_id = str(uuid.uuid4())
    logger.debug(f"[{request_id}] Listing objects: limit={limit}, offset={offset}, last_id={last_id}")
    
    try:
        cache_key = f"list:last_id={last_id}:limit={limit}:offset={offset}"
        cached_data = list_cache.get(cache_key)
        if cached_data:
            logger.debug(f"[{request_id}] Cache hit for {cache_key}")
            return cached_data
            
        # 直接在当前线程执行数据库操作
        query = db.query(ObjectStorageModel).order_by(ObjectStorageModel.created_at.desc())
        if last_id:
            query = query.filter(ObjectStorageModel.id > last_id)
            
        objects = query.offset(offset).limit(limit).all()
        
        result = [
            ObjectMetadata(
                id=obj.id,
                name=obj.name,
                content_type=obj.content_type,
                size=obj.size,
                created_at=obj.created_at.isoformat(),
                type=obj.type,
                owner=obj.owner
            ) for obj in objects
        ]
        
        # 缓存结果
        list_cache.set(cache_key, result, ttl=CACHE_CONFIG['LIST_TTL'])  
        cache_key_manager.add_list_cache_key(cache_key)
        cache_key_manager.add_query_result(cache_key, [obj.id for obj in objects])
        logger.debug(f"[{request_id}] Successfully listed {len(result)} objects")
        
        return result
        
    except Exception as e:
        logger.error(f"[{request_id}] Failed to list objects: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list objects: {str(e)}")

# JSON对象上传接口
@router.post("/upload/json", response_model=ObjectMetadata)
async def upload_json_object(
    request: Request,
    json_data: JSONObjectModel,
    db: Session = Depends(get_db)
):
    """上传JSON对象"""
    request_id = request.state.request_id
    logger.debug(f"[{request_id}] Processing JSON upload")
    
    try:
        # 解析请求体
        content_str = json.dumps(json_data.content)
        content_size = len(content_str.encode('utf-8'))
        logger.debug(f"[{request_id}] Content size: {content_size} bytes")
        
        # 检查大小限制
        if content_size > 10 * 1024 * 1024:
            logger.warning(f"[{request_id}] Content too large")
            raise HTTPException(status_code=413, detail="Content too large")
            
        # 准备数据
        object_id = str(uuid.uuid4())
        current_time = datetime.now()
        
        # 创建记录
        record = {
            'id': object_id,
            'name': json_data.name or object_id,
            'content': content_str,
            'content_type': 'application/json',
            'type': json_data.type,
            'size': content_size,
            'created_at': current_time.isoformat(),
            'owner': None
        }
        
        # 添加到写入队列 - 不等待确认
        try:
            await write_manager.add_record(record)
            logger.debug(f"[{request_id}] Record queued for writing")
        except Exception as e:
            logger.error(f"[{request_id}] Write queue error: {str(e)}")
            raise HTTPException(status_code=500, detail="Write queue error")
            
        # 准备响应
        metadata = ObjectMetadata(
            id=object_id,
            name=record['name'],
            content_type=record['content_type'],
            size=record['size'],
            created_at=record['created_at'],
            type=record['type']
        )
        
        logger.info(f"[{request_id}] Upload queued successfully")
        
        # 异步触发缓存清理
        cache_invalidator.invalidate(object_id, "create")
        
        return metadata
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# JSON对象批量上传接口
@router.post("/upload/json/batch", response_model=List[ObjectMetadata])
async def upload_json_objects_batch(
    json_objects: List[JSONObjectModel], 
    db: Session = Depends(get_db)
):
    """批量上传JSON对象"""
    request_id = str(uuid.uuid4())
    logger.debug(f"[{request_id}] Starting batch upload of {len(json_objects)} objects")
    
    try:
        metadata_list = []
        db_objects = []
        
        # 准备所有对象
        for json_data in json_objects:
            content_str = json.dumps(json_data.content)
            content_size = len(content_str.encode('utf-8'))
            
            if content_size > 10 * 1024 * 1024:  
                logger.warning(f"[{request_id}] Object too large: {content_size} bytes")
                continue
                
            db_object = ObjectStorageModel(
                id=str(uuid.uuid4()),
                name=json_data.name or f"json_object_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                content=content_str,
                content_type="application/json",
                type=json_data.type,
                size=content_size,
                created_at=datetime.now().isoformat()
            )
            db_objects.append(db_object)
        
        # 数据库操作
        logger.debug(f"[{request_id}] Starting database transaction")
        try:
            db.add_all(db_objects)
            await asyncio.to_thread(db.commit)
            logger.debug(f"[{request_id}] Database transaction successful")
            
            # 创建元数据列表
            for obj in db_objects:
                metadata = ObjectMetadata(
                    id=obj.id,
                    name=obj.name,
                    content_type=obj.content_type,
                    size=obj.size,
                    created_at=str(obj.created_at)
                )
                metadata_list.append(metadata)
                
                # 异步缓存
                try:
                    metadata_cache.set(f"metadata:{obj.id}", metadata.dict())
                except Exception as e:
                    logger.error(f"[{request_id}] Cache error for object {obj.id}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"[{request_id}] Database error: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=500, detail="Database error")
            
        logger.info(f"[{request_id}] Successfully uploaded {len(metadata_list)} objects")
        
        # 异步触发缓存清理
        for obj in db_objects:
            cache_invalidator.invalidate(obj.id, "create")
        
        return metadata_list
        
    except Exception as e:
        logger.error(f"[{request_id}] Batch upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# JSON对象更新接口
@router.put("/update/json/{object_id}", response_model=ObjectMetadata)
async def update_json_object(
    object_id: str,
    json_data: JSONObjectModel,
    db: Session = Depends(get_db)
):
    """更新指定ID的JSON对象"""
    request_id = str(uuid.uuid4())
    logger.debug(f"[{request_id}] Updating object {object_id}")
    
    try:
        # 查找对象
        db_object = db.query(ObjectStorageModel).filter(ObjectStorageModel.id == object_id).first()
        if not db_object:
            logger.warning(f"[{request_id}] Object {object_id} not found")
            raise HTTPException(status_code=404, detail="Object not found")
            
        # 更新对象
        content_str = json.dumps(json_data.content)
        content_size = len(content_str.encode('utf-8'))
        
        if content_size > 10 * 1024 * 1024:  
            logger.warning(f"[{request_id}] Content too large: {content_size} bytes")
            raise HTTPException(status_code=413, detail="Content too large")
            
        try:
            db_object.content = content_str
            db_object.name = json_data.name or db_object.name
            db_object.size = content_size
            db_object.type = json_data.type
            
            await asyncio.to_thread(db.commit)
            logger.debug(f"[{request_id}] Database update successful")
            
        except Exception as e:
            logger.error(f"[{request_id}] Database error: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=500, detail="Database error")
            
        # 更新缓存
        metadata = ObjectMetadata(
            id=db_object.id,
            name=db_object.name,
            content_type=db_object.content_type,
            size=db_object.size,
            created_at=str(db_object.created_at)
        )
        
        try:
            metadata_cache.set(f"metadata:{object_id}", metadata.dict())
            query_cache.set(f"content:{object_id}", json_data.content)
        except Exception as e:
            logger.error(f"[{request_id}] Cache error: {str(e)}")
            
        logger.info(f"[{request_id}] Successfully updated object {object_id}")
        
        # 异步触发缓存清理
        cache_invalidator.invalidate(object_id, "update")
        
        return metadata
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Update failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# JSON对象更新接口
@router.put("/update/json/{object_id}", response_model=ObjectMetadata)
def update_json_object(
    object_id: str,
    content: Dict[str, Any],
    db: Session = Depends(get_db)
):
    """更新 JSON 对象的内容"""
    try:
        # 查找对象
        db_object = db.query(ObjectStorageModel).filter(ObjectStorageModel.id == object_id).first()
        if not db_object:
            raise HTTPException(status_code=404, detail="Object not found")
            
        # 验证内容类型
        if db_object.content_type != "application/json":
            raise HTTPException(status_code=400, detail="Object is not a JSON object")
            
        # 更新内容
        db_object.content = json.dumps(content)
        db_object.size = len(db_object.content)
        db_object.updated_at = datetime.utcnow()
        
        try:
            # 提交更改
            db.commit()
            # 清除会话缓存
            db.expire_all()
            # 刷新对象
            db.refresh(db_object)
            # 异步触发缓存清理
            cache_invalidator.invalidate(object_id, "update")
            
            logger.info(f"Successfully updated JSON object {object_id}")
            return db_object
            
        except SQLAlchemyError as e:
            db.rollback()
            logger.error(f"Database error while updating object {object_id}: {str(e)}")
            raise HTTPException(status_code=500, detail="Database error")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating JSON object {object_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# JSON对象查询接口
@router.post("/query/json", response_model=Dict[str, Any])
def query_json_objects(
    query: JSONQueryModel,
    db: Session = Depends(get_db),
    limit: Optional[int] = Query(default=100, ge=1, le=5000),
    offset: Optional[int] = Query(default=0, ge=0)
):
    """根据 JSONPath 查询和过滤 JSON 对象"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Starting query request with limit={limit}, offset={offset}")
    
    try:
        # 确保会话是干净的
        db.expire_all()
        
        # 确保 JSONPath 以 $ 开头
        if not query.jsonpath or not isinstance(query.jsonpath, str):
            query.jsonpath = '$'
        elif not query.jsonpath.startswith('$'):
            query.jsonpath = '$.' + query.jsonpath
            
        logger.info(f"[{request_id}] Query parameters: path={query.jsonpath}, type={query.type}, value={query.value}, operator={query.operator}")
        
        # 验证 JSONPath
        try:
            jsonpath_expr = parse_jsonpath(query.jsonpath)
            logger.debug(f"[{request_id}] JSONPath parsed successfully")
        except Exception as e:
            logger.warning(f"[{request_id}] Invalid JSONPath: {query.jsonpath}, error: {str(e)}")
            raise HTTPException(
                status_code=422, 
                detail={
                    "error": "Invalid JSONPath",
                    "message": f"Invalid JSONPath expression: {str(e)}"
                }
            )
            
        # 生成缓存键
        cache_key = f"query:{hash((query.jsonpath, query.type, query.value, query.operator, offset, limit))}"
        logger.debug(f"[{request_id}] Cache key generated: {cache_key}")
        
        # 尝试从缓存获取结果
        cached_result = safe_cache_get(query_cache, cache_key)
        if cached_result:
            logger.info(f"[{request_id}] Cache hit, returning cached result")
            return cached_result
            
        # 基础查询
        try:
            base_query = db.query(ObjectStorageModel).filter(
                ObjectStorageModel.content_type == "application/json"
            ).order_by(ObjectStorageModel.created_at.desc())  # 按创建时间倒序排序
            
            # 添加类型过滤
            if query.type:
                base_query = base_query.filter(ObjectStorageModel.type == query.type)
                
            logger.debug(f"[{request_id}] Base query constructed with time-based sorting")
        except Exception as e:
            logger.error(f"[{request_id}] Error constructing base query: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")
        
        # 使用流式处理来减少内存使用
        chunk_size = min(100, limit)  # 每次处理的记录数
        filtered_objects = []
        total_processed = 0
        
        def process_chunk(objects):
            nonlocal total_processed
            for obj in objects:
                try:
                    if not obj.content:
                        logger.warning(f"[{request_id}] Empty content for object {obj.id}")
                        continue
                        
                    content = json.loads(obj.content)
                    matches = [match.value for match in jsonpath_expr.find(content)]
                    
                    if matches:
                        if query.value is not None:
                            match_found = False
                            for x in matches:
                                if compare_values(x, query.value, query.operator):
                                    match_found = True
                                    break
                            if not match_found:
                                continue
                        
                        filtered_objects.append((obj, content))
                        
                except json.JSONDecodeError as je:
                    logger.warning(f"[{request_id}] Invalid JSON in object {obj.id}: {str(je)}")
                    continue
                except Exception as e:
                    logger.error(f"[{request_id}] Error processing object {obj.id}: {str(e)}")
                    continue
                    
                total_processed += 1
                
        def compare_values(x, value, operator):
            if operator is None:
                return True
                
            try:
                if operator == 'eq':
                    return x == value
                elif operator == 'ne':
                    return x != value
                elif operator == 'gt':
                    return float(x) > float(value)
                elif operator == 'lt':
                    return float(x) < float(value)
                elif operator == 'gte':
                    return float(x) >= float(value)
                elif operator == 'lte':
                    return float(x) <= float(value)
                return False
            except (ValueError, TypeError) as e:
                logger.warning(f"[{request_id}] Value comparison error: {str(e)}, x={x}, value={value}, operator={operator}")
                return False
        
        # 分块处理数据
        try:
            while True:
                chunk = base_query.limit(chunk_size).offset(total_processed).all()
                if not chunk:
                    logger.debug(f"[{request_id}] No more chunks to process")
                    break
                    
                logger.debug(f"[{request_id}] Processing chunk of size {len(chunk)}")
                process_chunk(chunk)
                
                # 如果已经找到足够的结果，可以提前退出
                if len(filtered_objects) >= offset + limit:
                    logger.debug(f"[{request_id}] Found enough results, stopping early")
                    break
                    
                # 避免无限循环
                if len(chunk) < chunk_size:
                    logger.debug(f"[{request_id}] Last chunk processed (size < chunk_size)")
                    break
                    
        except Exception as e:
            logger.error(f"[{request_id}] Error during chunk processing: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")
        
        # 计算总数和分页
        total_count = len(filtered_objects)
        start_idx = min(offset, total_count)
        end_idx = min(offset + limit, total_count)
        paginated_objects = filtered_objects[start_idx:end_idx]
        
        logger.info(f"[{request_id}] Found {total_count} matching objects, returning {len(paginated_objects)} items")
        
        # 构建响应
        try:
            results = []
            for obj, content in paginated_objects:
                results.append(JSONObjectResponse(
                    id=obj.id,
                    name=obj.name,
                    content_type=obj.content_type,
                    size=obj.size,
                    created_at=str(obj.created_at),
                    type=obj.type,
                    content=content
                ))
                
            # 构建响应数据
            response_data = {
                "total": total_count,
                "offset": offset,
                "limit": limit,
                "items": results
            }
            
            # 缓存结果
            if total_count > 0:
                if safe_cache_set(query_cache, cache_key, response_data):
                    # 只在缓存成功时记录缓存关系
                    cache_key_manager.add_query_result(
                        cache_key,
                        [obj.id for obj, _ in filtered_objects]
                    )
                    logger.debug(f"[{request_id}] Results cached with key {cache_key}")
            
            return response_data
            
        except Exception as e:
            logger.error(f"[{request_id}] Error building response: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error building response: {str(e)}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Unhandled error in query_json_objects: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# 搜索接口
@router.get("/search", response_model=List[ObjectMetadata])
async def search_objects(
    query: str,
    skip: int = 0,
    limit: int = 100,
    type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """搜索对象"""
    cache_key = f"search:query={query}:skip={skip}:limit={limit}:type={type}"
    cached_result = list_cache.get(cache_key)
    if cached_result:
        return cached_result
        
    search_query = db.query(ObjectStorageModel).order_by(ObjectStorageModel.created_at.desc())
    
    # 添加搜索条件
    if query:
        search_query = search_query.filter(
            or_(
                ObjectStorageModel.name.ilike(f"%{query}%"),
                ObjectStorageModel.content.ilike(f"%{query}%")
            )
        )
    
    if type:
        search_query = search_query.filter(ObjectStorageModel.type == type)
    
    objects = search_query.offset(skip).limit(limit).all()
    
    # 缓存结果
    if objects:
        list_cache[cache_key] = objects
        cache_key_manager.add_list_cache_key(cache_key)
        cache_key_manager.add_query_result(cache_key, [obj.id for obj in objects])
        
    return objects

# 监控相关API
@router.get("/metrics", 
    summary="获取系统指标",
    description="获取系统性能指标，包括CPU使用率、内存使用情况、存储状态等",
    response_description="返回所有收集的指标数据",
    tags=["monitoring"])
async def get_metrics():
    """
    获取系统性能指标
    
    返回以下指标：
    * CPU使用率
    * 内存使用情况
    * 磁盘使用状态
    * 请求处理统计
    * 操作耗时统计
    """
    return metrics.get_metrics()

# 备份相关API
@router.post("/backups", 
    summary="创建新备份",
    description="创建系统数据的完整备份，包括存储的对象和数据库",
    response_description="返回创建的备份信息",
    tags=["backup"])
async def create_backup(
    background_tasks: BackgroundTasks,
    backup_name: Optional[str] = None
):
    """
    创建新的系统备份
    
    - **backup_name**: 可选的备份名称，如果不提供将自动生成
    """
    try:
        backup_name = await backup_manager.create_backup(backup_name)
        return {"message": "Backup started", "backup_name": backup_name}
    except Exception as e:
        raise ObjSaveException(
            status_code=500,
            detail=f"Failed to create backup: {str(e)}",
            error_code="BACKUP_FAILED"
        )

@router.get("/backups", 
    summary="列出所有备份",
    description="获取所有可用备份的列表",
    response_description="返回备份列表",
    tags=["backup"])
async def list_backups():
    """
    获取所有可用的备份列表
    
    返回每个备份的以下信息：
    * 备份名称
    * 创建时间
    * 备份大小
    * 状态
    """
    return await backup_manager.list_backups()

@router.post("/backups/{backup_name}/restore", 
    summary="恢复备份",
    description="从指定的备份恢复系统数据",
    response_description="返回恢复操作的结果",
    tags=["backup"])
async def restore_backup(backup_name: str):
    """
    从指定备份恢复系统
    
    - **backup_name**: 要恢复的备份名称
    """
    try:
        success = await backup_manager.restore_backup(backup_name)
        return {"message": "Backup restored successfully", "success": success}
    except Exception as e:
        raise ObjSaveException(
            status_code=500,
            detail=f"Failed to restore backup: {str(e)}",
            error_code="RESTORE_FAILED"
        )

# 添加监控端点
@router.get("/stats")
async def get_write_stats():
    """获取写入统计信息"""
    return write_manager.get_stats()

# 清除缓存接口
@router.post("/clear-cache")
async def clear_query_cache():
    """清除所有缓存"""
    clear_all_caches()
    return {"message": "All caches cleared successfully"}

# 注册路由
app.include_router(router)

# 在应用启动时开始收集指标
@app.on_event("startup")
async def start_metrics_collector():
    asyncio.create_task(start_metrics_collection())

# 初始化异步缓存清理器
cache_invalidator = AsyncCacheInvalidator()

# 在应用关闭时停止缓存清理线程
@app.on_event("shutdown")
def shutdown_event():
    cache_invalidator.stop()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=settings.MAX_WORKERS  
    )

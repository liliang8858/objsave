import os
import uuid
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException, APIRouter, Request, Response, BackgroundTasks
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel, ConfigDict
import json
import jsonpath
import logging
from datetime import datetime
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from contextlib import asynccontextmanager
from request_queue import RequestQueue
from storage_manager import StorageManager
from cache_manager import CacheManager
from fastapi.middleware.gzip import GZipMiddleware
from models import ObjectStorage, ObjectMetadata
from database import get_db, init_db
from write_manager import WriteManager
from config import settings
from exceptions import ObjSaveException, ObjectNotFoundError, InvalidJSONPathError
from monitoring import metrics, measure_time
from backup import backup_manager

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s:%(pathname)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('objsave.log')
    ]
)
logger = logging.getLogger(__name__)

# 系统配置
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB
MAX_WORKERS = min(32, multiprocessing.cpu_count() * 4)
CHUNK_SIZE = 64 * 1024  # 64KB chunks
CACHE_MAX_ITEMS = 20000
CACHE_SHARDS = 32
CACHE_TTL = 3600

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

# 创建存储管理器实例
storage = StorageManager(
    base_path=settings.STORAGE_BASE_PATH,
    chunk_size=settings.CHUNK_SIZE,
    max_concurrent_ops=settings.MAX_WORKERS
)

# 创建缓存管理器实例
cache = CacheManager(
    max_items=settings.CACHE_MAX_ITEMS,
    shards=settings.CACHE_SHARDS,
    ttl=settings.CACHE_TTL
)

# 创建请求队列实例
request_queue = RequestQueue(
    max_workers=settings.MAX_WORKERS
)

# 全局写入管理器
write_manager = WriteManager(
    batch_size=100,      # 每批次100条记录
    flush_interval=1.0,  # 每秒刷新一次
    max_queue_size=10000 # 最大队列大小
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up server...")
    logger.info(f"Available workers: {settings.MAX_WORKERS}")
    logger.info(f"Cache config: max_items={settings.CACHE_MAX_ITEMS}, shards={settings.CACHE_SHARDS}, ttl={settings.CACHE_TTL}s")
    
    # 初始化数据库
    init_db()
    
    # 启动请求队列处理器
    await request_queue.start()
    
    # 启动写入管理器
    await write_manager.start()
    
    yield
    
    # Shutdown
    logger.info("Shutting down...")
    thread_pool.shutdown(wait=False)
    upload_thread_pool.shutdown(wait=False)
    
    # 关闭写入管理器
    await write_manager.stop()

# 创建FastAPI应用
app = FastAPI(
    title="ObjSave API",
    description="""
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
    """,
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/objsave/docs",
    redoc_url="/objsave/redoc",
    openapi_url="/objsave/openapi.json"
)

# 请求信号量，限制并发请求数
request_semaphore = asyncio.Semaphore(settings.MAX_WORKERS)

# 请求ID中间件
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    logger.info(f"[{request_id}] Request started: {request.method} {request.url.path}")
    
    try:
        async with request_semaphore:
            response = await call_next(request)
            logger.info(f"[{request_id}] Request completed")
            return response
    except Exception as e:
        logger.error(f"[{request_id}] Request failed: {str(e)}")
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

# 创建路由器
router = APIRouter(
    prefix="/objsave",
    tags=["object-storage"]
)

# 对象元数据模型
class ObjectMetadata(BaseModel):
    id: str
    name: str
    content_type: str
    size: int
    created_at: str

    model_config = ConfigDict(from_attributes=True)

# JSON对象模型
class JSONObjectModel(BaseModel):
    id: Optional[str] = None
    type: str
    content: Dict[str, Any]
    name: Optional[str] = None
    content_type: str = "application/json"

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

# 健康检查接口
@router.get("/health")
async def health_check():
    """健康检查接口，返回系统状态和性能指标"""
    try:
        # 获取写入管理器状态
        write_stats = write_manager.get_stats()
        metrics = write_manager.metrics.get_metrics()
        
        # 系统状态评估
        status = "healthy"
        alerts = []
        
        # 检查写入延迟
        current_latency = metrics['write_latency']['current']
        avg_latency = metrics['write_latency']['avg']
        if current_latency > avg_latency * 3:
            alerts.append({
                "level": "warning",
                "message": f"High write latency: {current_latency:.2f}ms (avg: {avg_latency:.2f}ms)"
            })
            status = "degraded"
            
        # 检查吞吐量
        current_throughput = metrics['write_throughput']['current']
        peak_throughput = metrics['write_throughput']['peak']
        if current_throughput < peak_throughput * 0.5:
            alerts.append({
                "level": "warning",
                "message": f"Low throughput: {current_throughput:.2f}B/s (peak: {peak_throughput:.2f}B/s)"
            })
            status = "degraded"
            
        # 检查CPU使用率
        cpu_usage = metrics['cpu_usage']['current']
        if cpu_usage > 80:
            alerts.append({
                "level": "warning",
                "message": f"High CPU usage: {cpu_usage:.1f}%"
            })
            status = "degraded"
            
        # 检查队列积压
        queue_size = metrics['queue_size']['current']
        if queue_size > write_manager.max_queue_size * 0.8:
            alerts.append({
                "level": "warning",
                "message": f"Queue near capacity: {queue_size}/{write_manager.max_queue_size}"
            })
            status = "degraded"
            
        # 检查错误率
        error_rate = metrics['error_rate']['current']
        if error_rate > 0.05:  # 5%错误率阈值
            alerts.append({
                "level": "critical",
                "message": f"High error rate: {error_rate*100:.1f}%"
            })
            status = "unhealthy"
            
        # 构建健康检查响应
        health_response = {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0",  # 应用版本
            
            # 系统状态
            "system": {
                "cpu_usage": metrics['cpu_usage'],
                "memory_mapped": {
                    "current": metrics['memory_mapped']['current'] / (1024*1024),  # 转换为MB
                    "avg": metrics['memory_mapped']['avg'] / (1024*1024)
                },
                "io_wait": metrics['io_wait']
            },
            
            # 写入性能
            "write_performance": {
                "latency_ms": metrics['write_latency'],
                "throughput_bps": metrics['write_throughput'],
                "queue_size": metrics['queue_size'],
                "error_rate": metrics['error_rate']
            },
            
            # 累计统计
            "statistics": {
                "total_bytes_written": metrics['total_stats']['bytes_written'],
                "total_operations": metrics['total_stats']['operations'],
                "total_errors": metrics['error_rate']['total_errors']
            },
            
            # 告警信息
            "alerts": alerts,
            
            # 缓存状态
            "cache": {
                "size": len(write_manager.cache),
                "max_size": write_manager.max_queue_size
            }
        }
        
        # 设置响应状态码
        status_code = 200 if status == "healthy" else 503 if status == "unhealthy" else 200
        
        return JSONResponse(
            content=health_response,
            status_code=status_code
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return JSONResponse(
            content={
                "status": "error",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
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
    try:
        async def download_task():
            db_obj = db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
            if not db_obj:
                raise HTTPException(status_code=404, detail="Object not found")
                
            # 尝试从缓存获取
            content = cache.get(object_id)
            if content is None:
                content = storage.get(object_id)
                if content is None:
                    raise HTTPException(status_code=404, detail="Object content not found")
                    
            return {
                "content": content,
                "media_type": db_obj.content_type,
                "filename": db_obj.name
            }
            
        # 将下载任务加入队列
        task_id = await request_queue.enqueue(
            download_task,
            priority=0,  # 下载请求优先处理
            is_cpu_bound=False
        )
        
        # 等待结果
        while True:
            result = request_queue.get_result(task_id)
            if result:
                if result["status"] == "completed":
                    data = result["result"]
                    return Response(
                        content=data["content"],
                        media_type=data["media_type"],
                        headers={
                            "Content-Disposition": f'attachment; filename="{data["filename"]}"',
                            "Cache-Control": "max-age=3600"
                        }
                    )
                elif result["status"] == "failed":
                    raise HTTPException(status_code=500, detail=result["error"])
            await asyncio.sleep(0.1)
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Download failed")

# 列出所有对象接口
@router.get("/list", response_model=List[ObjectMetadata])
async def list_objects(
    db: Session = Depends(get_db),
    limit: Optional[int] = 100,
    offset: Optional[int] = 0,
    last_id: Optional[str] = None  # 游标分页
):
    """列出存储的对象"""
    try:
        cache_key = f"list:last_id={last_id}:limit={limit}:offset={offset}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
            
        def db_operation():
            query = db.query(ObjectStorage)
            if last_id:
                query = query.filter(ObjectStorage.id > last_id)
            return query.offset(offset).limit(limit).all()
            
        objects = await asyncio.get_event_loop().run_in_executor(
            thread_pool, 
            db_operation
        )
        
        result = [
            ObjectMetadata(
                id=obj.id,
                name=obj.name,
                content_type=obj.content_type,
                size=obj.size,
                created_at=str(obj.created_at)
            ) for obj in objects
        ]
        
        # 缓存结果，设置较短的TTL
        cache.set(cache_key, result, ttl=60)  # 缓存1分钟
        
        return result
        
    except Exception as e:
        logger.error(f"Error listing objects: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取列表失败: {str(e)}")

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
            
            if content_size > 10 * 1024 * 1024:  # 10MB限制
                logger.warning(f"[{request_id}] Object too large: {content_size} bytes")
                continue
                
            db_object = ObjectStorage(
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
                    cache.set(f"metadata:{obj.id}", metadata.dict())
                except Exception as e:
                    logger.error(f"[{request_id}] Cache error for object {obj.id}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"[{request_id}] Database error: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=500, detail="Database error")
            
        logger.info(f"[{request_id}] Successfully uploaded {len(metadata_list)} objects")
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
        db_object = db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
        if not db_object:
            logger.warning(f"[{request_id}] Object {object_id} not found")
            raise HTTPException(status_code=404, detail="Object not found")
            
        # 更新对象
        content_str = json.dumps(json_data.content)
        content_size = len(content_str.encode('utf-8'))
        
        if content_size > 10 * 1024 * 1024:  # 10MB限制
            logger.warning(f"[{request_id}] Content too large: {content_size} bytes")
            raise HTTPException(status_code=413, detail="Content too large")
            
        try:
            db_object.content = content_str
            db_object.name = json_data.name or db_object.name
            db_object.size = content_size
            db_object.type = json_data.type
            
            await asyncio.to_thread(db.commit)
            db.refresh(db_object)
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
            cache.set(f"metadata:{object_id}", metadata.dict())
            cache.set(f"content:{object_id}", json_data.content)
        except Exception as e:
            logger.error(f"[{request_id}] Cache error: {str(e)}")
            
        logger.info(f"[{request_id}] Successfully updated object {object_id}")
        return metadata
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Update failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# JSON 查询模型
class JSONQueryModel(BaseModel):
    jsonpath: str
    value: Optional[Any] = None
    operator: Optional[str] = 'eq'
    type: Optional[str] = None

def validate_jsonpath(path: str) -> bool:
    """验证 JSONPath 表达式的基本格式"""
    if not path:
        return False
    if not path.startswith('$'):
        return False
    return True

# JSON对象查询接口
@router.post("/query/json", response_model=List[JSONObjectResponse])
async def query_json_objects(
    query: JSONQueryModel,
    db: Session = Depends(get_db),
    limit: Optional[int] = 100,
    offset: Optional[int] = 0
):
    """根据 JSONPath 查询和过滤 JSON 对象"""
    request_id = str(uuid.uuid4())
    logger.debug(f"[{request_id}] Starting query with path: {query.jsonpath}")
    
    try:
        # 验证 JSONPath
        if not validate_jsonpath(query.jsonpath):
            logger.warning(f"[{request_id}] Invalid JSONPath: {query.jsonpath}")
            raise HTTPException(status_code=422, detail="Invalid JSONPath")
            
        # 基础查询
        base_query = db.query(ObjectStorage).filter(
            ObjectStorage.content_type == "application/json"
        )
        
        # 添加类型过滤
        if query.type:
            base_query = base_query.filter(ObjectStorage.type == query.type)
            
        # 执行查询
        try:
            objects = base_query.offset(offset).limit(limit).all()
            logger.debug(f"[{request_id}] Found {len(objects)} objects")
        except Exception as e:
            logger.error(f"[{request_id}] Database error: {str(e)}")
            raise HTTPException(status_code=500, detail="Database error")
            
        # 处理结果
        results = []
        for obj in objects:
            try:
                content = json.loads(obj.content)
                # 使用 jsonpath 过滤
                matches = jsonpath.jsonpath(content, query.jsonpath)
                if matches:
                    results.append(JSONObjectResponse(
                        id=obj.id,
                        name=obj.name,
                        content_type=obj.content_type,
                        size=obj.size,
                        created_at=str(obj.created_at),
                        type=obj.type,
                        content=content
                    ))
            except json.JSONDecodeError:
                logger.warning(f"[{request_id}] Invalid JSON in object {obj.id}")
                continue
            except Exception as e:
                logger.error(f"[{request_id}] Error processing object {obj.id}: {str(e)}")
                continue
                
        logger.info(f"[{request_id}] Query returned {len(results)} matches")
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Query failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

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

# 注册路由
app.include_router(router)

# 在应用启动时开始收集指标
@app.on_event("startup")
async def start_metrics_collector():
    asyncio.create_task(start_metrics_collection())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=settings.MAX_WORKERS
    )

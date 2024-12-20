import os
import uuid
from typing import List, Optional, Dict, Any
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

from obsave.core.storage import ObjectStorage
from obsave.core.exceptions import StorageError, ObjectNotFoundError, ObjSaveException
from obsave.core.database import get_db, init_db
from obsave.core.settings import settings  # Corrected import statement
from obsave.monitoring.metrics import metrics_collector
from obsave.monitoring.middleware import PrometheusMiddleware
from obsave.utils import CacheManager, RequestQueue, WriteManager, AsyncIOManager
from obsave.core.models import ObjectMetadata, JSONObjectModel, ObjectStorage as ObjectStorageModel

# 配置日志记录
logger = logging.getLogger("obsave")
logger.setLevel(getattr(logging, settings.LOG_LEVEL))  # Corrected settings usage

# 创建日志处理器
file_handler = RotatingFileHandler(
    settings.LOG_FILE,  # Corrected settings usage
    maxBytes=settings.LOG_MAX_BYTES,  # Corrected settings usage
    backupCount=settings.LOG_BACKUP_COUNT  # Corrected settings usage
)
file_handler.setFormatter(logging.Formatter(settings.LOG_FORMAT))  # Corrected settings usage
logger.addHandler(file_handler)

# 如果不是生产环境，也添加控制台输出
if settings.LOG_LEVEL == "DEBUG":  # Corrected settings usage
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(settings.LOG_FORMAT))  # Corrected settings usage
    logger.addHandler(console_handler)

# 系统配置
executor = ThreadPoolExecutor(max_workers=settings.MAX_WORKERS)  # Corrected settings usage

# 创建线程池
thread_pool = ThreadPoolExecutor(
    max_workers=settings.MAX_WORKERS * 2,  # Corrected settings usage
    thread_name_prefix="db_worker"
)

# 创建上传专用线程池
upload_thread_pool = ThreadPoolExecutor(
    max_workers=settings.MAX_WORKERS,  # Corrected settings usage
    thread_name_prefix="upload_worker"
)

# 创建I/O管理器实例
io_manager = AsyncIOManager(max_workers=settings.MAX_WORKERS)  # Corrected settings usage

# 初始化存储实例
storage = ObjectStorage(
    base_path=settings.STORAGE_BASE_PATH,  # Corrected settings usage
    chunk_size=settings.CHUNK_SIZE,  # Corrected settings usage
    max_concurrent_ops=settings.MAX_WORKERS  # Corrected settings usage
)

# 初始化缓存管理器
cache = CacheManager(
    max_items=settings.CACHE_MAX_SIZE,  # Corrected settings usage
    shards=settings.CACHE_SHARDS,  # Corrected settings usage
    ttl=settings.CACHE_TTL  # Corrected settings usage
)

# 创建请求队列实例
request_queue = RequestQueue(
    max_workers=settings.MAX_WORKERS,  # Corrected settings usage
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
    max_workers=settings.MAX_WORKERS  # Corrected settings usage
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up server...")
    logger.info(f"Available workers: {settings.MAX_WORKERS}")  # Corrected settings usage
    logger.info(f"Cache config: max_items={settings.CACHE_MAX_SIZE}, shards=32, ttl={settings.CACHE_TTL}s")  # Corrected settings usage
    
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
    await write_manager.flush()  # 确保所有待写入的数据都已保存
    write_manager.stop()
    io_manager.stop()
    thread_pool.shutdown(wait=False)
    upload_thread_pool.shutdown(wait=False)

# 创建FastAPI应用
app = FastAPI(
    title=settings.API_TITLE,  # Corrected settings usage
    description=settings.API_DESCRIPTION,  # Corrected settings usage
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
request_semaphore = asyncio.Semaphore(settings.MAX_WORKERS)  # Corrected settings usage

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
    prefix=settings.API_PREFIX,  # Corrected settings usage
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

def validate_jsonpath(path: str) -> bool:
    """验证 JSONPath 表达式的基本格式"""
    if not path:
        return False
    try:
        parse_jsonpath(path)
        return True
    except Exception as e:
        logger.warning(f"Invalid JSONPath: {path}, error: {str(e)}")
        return False

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
        if file.size and file.size > settings.MAX_UPLOAD_SIZE:  # Corrected settings usage
            raise ObjSaveException(
                status_code=413,
                detail=f"File too large. Maximum size is {settings.MAX_UPLOAD_SIZE} bytes",  # Corrected settings usage
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
            content=db_obj.content,  # Content is already in correct format (bytes)
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
    last_id: Optional[str] = None  # 游标分页
):
    """列出存储的对象"""
    request_id = str(uuid.uuid4())
    logger.debug(f"[{request_id}] Listing objects: limit={limit}, offset={offset}, last_id={last_id}")
    
    try:
        cache_key = f"list:last_id={last_id}:limit={limit}:offset={offset}"
        cached_data = cache.get(cache_key)
        if cached_data:
            logger.debug(f"[{request_id}] Cache hit for {cache_key}")
            return cached_data
            
        # 直接在当前线程执行数据库操作
        query = db.query(ObjectStorageModel)
        if last_id:
            query = query.filter(ObjectStorageModel.id > last_id)
            
        objects = query.order_by(ObjectStorageModel.created_at.desc()).offset(offset).limit(limit).all()
        
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
        cache.set(cache_key, result, ttl=60)  # 缓存1分钟
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
        db_object = db.query(ObjectStorageModel).filter(ObjectStorageModel.id == object_id).first()
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
            
        # 编译 JSONPath 表达式
        jsonpath_expr = parse_jsonpath(query.jsonpath)
            
        # 基础查询
        base_query = db.query(ObjectStorageModel).filter(
            ObjectStorageModel.content_type == "application/json"
        )
        
        # 添加类型过滤
        if query.type:
            base_query = base_query.filter(ObjectStorageModel.type == query.type)
            
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
                matches = [match.value for match in jsonpath_expr.find(content)]
                if matches:
                    # 如果指定了值和操作符，进行进一步过滤
                    if query.value is not None:
                        if query.operator == 'eq' and not any(x == query.value for x in matches):
                            continue
                        elif query.operator == 'ne' and not any(x != query.value for x in matches):
                            continue
                        elif query.operator == 'gt' and not any(x > query.value for x in matches):
                            continue
                        elif query.operator == 'lt' and not any(x < query.value for x in matches):
                            continue
                        elif query.operator == 'gte' and not any(x >= query.value for x in matches):
                            continue
                        elif query.operator == 'lte' and not any(x <= query.value for x in matches):
                            continue
                            
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
        workers=settings.MAX_WORKERS  # Corrected settings usage
    )

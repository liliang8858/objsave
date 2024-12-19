import os
import uuid
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException, APIRouter, Request, Response
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

# 配置日志记录
logging.basicConfig(
    level=logging.DEBUG,  # 改为DEBUG级别
    format='%(asctime)s - %(levelname)s - [%(pathname)s:%(lineno)d] - %(message)s'
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
    max_workers=MAX_WORKERS * 2,
    thread_name_prefix="db_worker"
)

# 创建上传专用线程池
upload_thread_pool = ThreadPoolExecutor(
    max_workers=MAX_WORKERS,
    thread_name_prefix="upload_worker"
)

# 创建存储管理器实例
storage = StorageManager(
    base_path="storage",
    chunk_size=CHUNK_SIZE,
    max_concurrent_ops=MAX_WORKERS
)

# 创建缓存管理器实例
cache = CacheManager(
    max_items=CACHE_MAX_ITEMS,
    shards=CACHE_SHARDS,
    ttl=CACHE_TTL
)

# 创建请求队列实例
request_queue = RequestQueue(
    max_workers=MAX_WORKERS
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up server...")
    logger.info(f"Available workers: {MAX_WORKERS}")
    logger.info(f"Cache config: max_items={CACHE_MAX_ITEMS}, shards={CACHE_SHARDS}, ttl={CACHE_TTL}s")
    
    # 初始化数据库
    init_db()
    
    # 启动请求队列处理器
    await request_queue.start()
    yield
    
    # Shutdown
    logger.info("Shutting down...")
    thread_pool.shutdown(wait=False)
    upload_thread_pool.shutdown(wait=False)

# 创建FastAPI应用
app = FastAPI(
    title="ObjSave API",
    description="高性能对象存储服务",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/objsave/docs",
    redoc_url="/objsave/redoc",
    openapi_url="/objsave/openapi.json"
)

# 请求信号量，限制并发请求数
request_semaphore = asyncio.Semaphore(MAX_WORKERS)

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
    """系统健康检查"""
    try:
        cache_stats = cache.get_stats()
        storage_stats = storage.get_stats()
        queue_stats = request_queue.get_stats()
        
        return {
            "status": "healthy",
            "cache": cache_stats,
            "storage": storage_stats,
            "queue": queue_stats,
            "system": {
                "workers": MAX_WORKERS
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Health check failed")

# 上传对象接口
@router.post("/upload")
async def upload_object(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """上传对象到存储服务"""
    try:
        # 检查文件大小
        if file.size and file.size > MAX_UPLOAD_SIZE:
            raise HTTPException(status_code=413, detail="File too large")
            
        content = await file.read()
        
        # 将上传任务加入队列
        async def upload_task():
            object_id = str(uuid.uuid4())
            
            # 尝试存储到缓存
            cache_stats = cache.get_stats()
            if float(cache_stats['capacity_usage'].rstrip('%')) <= 90:
                cache.set(object_id, content)
                storage_type = "cache"
            else:
                storage.set(object_id, content)
                storage_type = "storage"
                
            metadata = ObjectMetadata(
                id=object_id,
                name=file.filename,
                content_type=file.content_type,
                size=len(content),
                created_at=datetime.now().isoformat()
            )
            
            # 存储元数据到数据库
            db_obj = ObjectStorage(
                id=metadata.id,
                name=metadata.name,
                content_type=metadata.content_type,
                size=metadata.size,
                created_at=metadata.created_at
            )
            
            db.add(db_obj)
            db.commit()
            
            logger.info(f"Object {object_id} uploaded using {storage_type}")
            return metadata
            
        # 将任务加入队列，优先处理小文件
        priority = 0 if len(content) < 1024 * 1024 else 1  # 1MB以下的文件优先处理
        task_id = await request_queue.enqueue(
            upload_task,
            priority=priority,
            is_cpu_bound=len(content) > 5 * 1024 * 1024  # 5MB以上的文件使用线程池处理
        )
        
        # 等待结果
        while True:
            result = request_queue.get_result(task_id)
            if result:
                if result["status"] == "completed":
                    return result["result"]
                elif result["status"] == "failed":
                    raise HTTPException(status_code=500, detail=result["error"])
            await asyncio.sleep(0.1)
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Upload failed")

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
        
        # 使用超时保护的数据库操作
        try:
            async with asyncio.timeout(5):  # 5秒超时
                obj = ObjectStorage(
                    id=object_id,
                    name=json_data.name or object_id,
                    content=content_str,
                    content_type='application/json',
                    type=json_data.type,
                    size=content_size,
                    created_at=current_time,
                    owner=None
                )
                db.add(obj)
                await asyncio.to_thread(db.commit)
                db.refresh(obj)
                logger.debug(f"[{request_id}] Database operation completed")
                
        except asyncio.TimeoutError:
            logger.error(f"[{request_id}] Database operation timed out")
            db.rollback()
            raise HTTPException(status_code=408, detail="Database timeout")
        except Exception as e:
            logger.error(f"[{request_id}] Database error: {str(e)}")
            db.rollback()
            raise HTTPException(status_code=500, detail="Database error")
            
        # 准备响应
        metadata = ObjectMetadata(
            id=obj.id,
            name=obj.name,
            content_type=obj.content_type,
            size=obj.size,
            created_at=obj.created_at.isoformat(),
            type=obj.type
        )
        
        # 异步缓存操作
        try:
            logger.debug(f"[{request_id}] Starting cache operation")
            async with asyncio.timeout(2):  # 2秒缓存超时
                await asyncio.gather(
                    asyncio.to_thread(lambda: cache.set(f"metadata:{object_id}", metadata.dict())),
                    asyncio.to_thread(lambda: cache.set(f"content:{object_id}", json_data.content))
                )
            logger.debug(f"[{request_id}] Cache operation completed")
        except asyncio.TimeoutError:
            logger.warning(f"[{request_id}] Cache operation timed out")
        except Exception as e:
            logger.error(f"[{request_id}] Cache error: {str(e)}")
        
        logger.info(f"[{request_id}] Upload completed successfully")
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

# 注册路由
app.include_router(router)

# 启动配置
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        workers=1,  # 开发模式使用单进程
        log_level="debug",
        timeout_keep_alive=30,
        loop="asyncio",
        reload=True  # 开发模式启用热重载
    )

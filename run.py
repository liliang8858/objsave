import os
import uuid
import json
import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException, APIRouter, Request, Response, BackgroundTasks
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel, ConfigDict
import time
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from contextlib import asynccontextmanager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 配置常量
MAX_UPLOAD_SIZE = 100 * 1024 * 1024  # 100MB
MAX_WORKERS = min(32, (multiprocessing.cpu_count() or 1) * 5)  # 最大工作线程数

# 创建线程池
thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)

from obsave.core.storage import ObjectStorage
from obsave.core.exceptions import StorageError, ObjectNotFoundError
from obsave.core.database import get_db, init_db
from obsave.monitoring.metrics import metrics_collector
from obsave.monitoring.middleware import PrometheusMiddleware

# 系统配置
CHUNK_SIZE = 64 * 1024  # 64KB chunks

# 创建存储管理器实例
storage = ObjectStorage(
    base_path="storage",
    chunk_size=CHUNK_SIZE,
    max_concurrent_ops=MAX_WORKERS
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up server...")
    logger.info(f"Available workers: {MAX_WORKERS}")
    
    # 初始化数据库
    init_db()
    
    yield
    
    # Shutdown
    logger.info("Shutting down...")
    thread_pool.shutdown(wait=False)

# 创建FastAPI应用
app = FastAPI(
    title="ObjSave API",
    description="""
    对象存储API服务
    
    提供以下功能:
    - 文件上传和下载
    - 对象元数据管理
    - 存储统计信息
    """,
    version="1.0.0",
    lifespan=lifespan
)

# 创建请求信号量，限制并发请求数
request_semaphore = asyncio.Semaphore(100)  # 最多同时处理100个请求

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
    start_time = metrics_collector.track_request(
        method=request.method,
        endpoint=request.url.path
    )
    
    response = await call_next(request)
    
    # 记录请求指标
    metrics_collector.track_request_end(
        start_time=start_time,
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code
    )
    
    return response

# CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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

# 健康检查接口
@router.get("/health", response_model=Dict[str, Any])
async def health_check():
    """系统健康检查"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

# 上传对象接口
@router.post("/upload", response_model=ObjectMetadata)
async def upload_object(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """上传文件对象"""
    if file.size and file.size > MAX_UPLOAD_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum size is {MAX_UPLOAD_SIZE} bytes"
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
        
        return metadata
        
    except Exception as e:
        logger.exception("Failed to upload object")
        await storage.cleanup(object_id)
        raise HTTPException(
            status_code=500,
            detail="Failed to upload object"
        ) from e

# 上传JSON数据接口
@router.post("/upload/json", response_model=ObjectMetadata)
async def upload_json(
    data: Dict[str, Any],
    db: Session = Depends(get_db)
):
    """上传JSON数据"""
    object_id = str(uuid.uuid4())
    try:
        json_content = json.dumps(data, ensure_ascii=False).encode('utf-8')
        
        if len(json_content) > MAX_UPLOAD_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"JSON too large. Maximum size is {MAX_UPLOAD_SIZE} bytes"
            )
        
        # 异步存储JSON
        success = await storage.store(object_id, json_content)
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to store JSON data"
            )
        
        # 创建元数据记录
        metadata = ObjectMetadata(
            id=object_id,
            name=f"{object_id}.json",
            content_type="application/json",
            size=len(json_content),
            created_at=datetime.utcnow().isoformat()
        )
        
        return metadata
        
    except HTTPException:
        await storage.delete(object_id)
        raise
    except Exception as e:
        await storage.delete(object_id)
        logger.exception("Failed to upload JSON")
        raise HTTPException(
            status_code=500,
            detail="Failed to upload JSON"
        ) from e

# 下载对象接口
@router.get("/download/{object_id}")
async def download_object(
    object_id: str,
    db: Session = Depends(get_db)
):
    """下载指定对象"""
    try:
        # 获取对象内容
        content = await storage.get_file(object_id)
        if content is None:
            raise HTTPException(status_code=404, detail="Object not found")
            
        return Response(
            content=content,
            media_type="application/octet-stream"
        )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Download failed")

# 列出所有对象接口
@router.get("/list", response_model=List[ObjectMetadata])
async def list_objects(
    limit: Optional[int] = 100,
    offset: Optional[int] = 0
):
    """列出存储的对象"""
    try:
        # 验证参数
        if limit < 0 or offset < 0:
            raise HTTPException(
                status_code=400,
                detail="Limit and offset must be non-negative"
            )
        
        if limit > 1000:
            raise HTTPException(
                status_code=400,
                detail="Maximum limit is 1000"
            )
            
        objects = await storage.list_files(limit=limit, offset=offset)
        return objects
        
    except StorageError as e:
        logger.error(f"Storage error while listing objects: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to list objects"
        ) from e
    except Exception as e:
        logger.error(f"Error listing objects: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        ) from e

# 注册路由
app.include_router(router)

# 在应用启动时开始收集指标
@app.on_event("startup")
async def start_metrics_collector():
    metrics_collector.start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "run:app",  # 使用模块路径
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=MAX_WORKERS
    )

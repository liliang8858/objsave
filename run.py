import os
import uuid
import json
import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException, APIRouter, Request, Response, BackgroundTasks
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import desc, asc
from pydantic import BaseModel, ConfigDict
import time
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from functools import lru_cache
from collections import deque
import threading
import mmap
import weakref

from obsave.core.storage import ObjectStorage as StorageManager
from obsave.core.exceptions import StorageError, ObjectNotFoundError
from obsave.core.database import get_db, init_db, Base
from obsave.core.models import ObjectStorage
from obsave.monitoring.metrics import metrics_collector
from obsave.monitoring.middleware import PrometheusMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, REGISTRY

# 配置日志记录
logging.basicConfig(
    level=logging.WARNING,  # 生产环境使用WARNING级别
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            'objsave.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
    ]
)
logger = logging.getLogger(__name__)

# 系统配置
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB
MAX_WORKERS = min(32, multiprocessing.cpu_count() * 4)
CHUNK_SIZE = 64 * 1024  # 64KB chunks
CACHE_MAX_ITEMS = 20000
CACHE_SHARDS = 32
CACHE_TTL = 3600  # 1 hour
WRITE_BATCH_SIZE = 100
WRITE_FLUSH_INTERVAL = 1.0  # 1 second
MAX_QUEUE_SIZE = 10000

class CacheManager:
    """高性能分片缓存管理器"""
    
    def __init__(self, max_items: int, shards: int, ttl: int):
        self.ttl = ttl
        self.shards = [{} for _ in range(shards)]
        self.locks = [threading.Lock() for _ in range(shards)]
        self.max_items_per_shard = max_items // shards
        
    def _get_shard(self, key: str) -> int:
        return hash(key) % len(self.shards)
        
    def get(self, key: str) -> Optional[Any]:
        shard_id = self._get_shard(key)
        with self.locks[shard_id]:
            if key in self.shards[shard_id]:
                value, timestamp = self.shards[shard_id][key]
                if time.time() - timestamp <= self.ttl:
                    return value
                else:
                    del self.shards[shard_id][key]
        return None
        
    def set(self, key: str, value: Any):
        shard_id = self._get_shard(key)
        with self.locks[shard_id]:
            if len(self.shards[shard_id]) >= self.max_items_per_shard:
                # 清理过期项
                now = time.time()
                expired = [k for k, (_, ts) in self.shards[shard_id].items() 
                          if now - ts > self.ttl]
                for k in expired:
                    del self.shards[shard_id][k]
                    
                # 如果仍然满了，删除最旧的项
                if len(self.shards[shard_id]) >= self.max_items_per_shard:
                    oldest = min(self.shards[shard_id].items(), 
                               key=lambda x: x[1][1])
                    del self.shards[shard_id][oldest[0]]
                    
            self.shards[shard_id][key] = (value, time.time())

class RequestQueue:
    """高性能请求队列"""
    
    def __init__(self, max_workers: int):
        self.queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self.max_workers = max_workers
        self.workers = []
        self.running = False
        
    async def start(self):
        """启动工作线程"""
        self.running = True
        for _ in range(self.max_workers):
            worker = asyncio.create_task(self._worker())
            self.workers.append(worker)
            
    async def stop(self):
        """停止所有工作线程"""
        self.running = False
        await self.queue.join()
        for worker in self.workers:
            worker.cancel()
            
    async def _worker(self):
        """工作线程处理函数"""
        while self.running:
            try:
                request = await self.queue.get()
                await self._process_request(request)
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in request worker: {str(e)}")
                await asyncio.sleep(1)
                
    async def _process_request(self, request: Dict[str, Any]):
        """处理单个请求"""
        try:
            if request['type'] == 'upload':
                await write_manager.add(request['data'])
            elif request['type'] == 'delete':
                await storage.delete(request['id'])
        except Exception as e:
            logger.error(f"Error processing request: {str(e)}")
            
    async def put(self, request: Dict[str, Any]):
        """添加请求到队列"""
        await self.queue.put(request)

class WriteManager:
    """高性能写入管理器"""
    
    def __init__(self, batch_size: int, flush_interval: float, max_queue_size: int):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.queue = asyncio.Queue(maxsize=max_queue_size)
        self.batch = []
        self.last_flush = time.time()
        self.lock = asyncio.Lock()
        self.running = False
        self.write_buffer = mmap.mmap(-1, MAX_UPLOAD_SIZE)
        self.stats = {
            'total_writes': 0,
            'batch_writes': 0,
            'errors': 0,
            'avg_latency': 0
        }
        
    async def start(self):
        """启动写入管理器"""
        self.running = True
        asyncio.create_task(self._flush_loop())
        
    async def stop(self):
        """停止写入管理器"""
        self.running = False
        await self.flush()
        self.write_buffer.close()
        
    async def add(self, item: Dict[str, Any]):
        """添加项到写入队列"""
        await self.queue.put(item)
        
    async def _flush_loop(self):
        """定期刷新写入批次"""
        while self.running:
            try:
                if self.batch and (
                    len(self.batch) >= self.batch_size or
                    time.time() - self.last_flush >= self.flush_interval
                ):
                    await self.flush()
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in flush loop: {str(e)}")
                
    async def flush(self):
        """刷新当前批次到存储"""
        async with self.lock:
            if not self.batch:
                return
                
            start_time = time.time()
            try:
                # 批量写入
                for item in self.batch:
                    self.write_buffer.seek(0)
                    self.write_buffer.write(item['data'])
                    await storage.store(item['id'], self.write_buffer.read())
                    
                self.stats['total_writes'] += len(self.batch)
                self.stats['batch_writes'] += 1
                
                # 更新平均延迟
                latency = time.time() - start_time
                self.stats['avg_latency'] = (
                    self.stats['avg_latency'] * (self.stats['batch_writes'] - 1) +
                    latency
                ) / self.stats['batch_writes']
                
            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"Error in batch flush: {str(e)}")
                raise
            finally:
                self.batch.clear()
                self.last_flush = time.time()
                
    def get_stats(self) -> Dict[str, Union[int, float]]:
        """获取写入统计信息"""
        return {
            'total_writes': self.stats['total_writes'],
            'batch_writes': self.stats['batch_writes'],
            'errors': self.stats['errors'],
            'avg_latency': round(self.stats['avg_latency'] * 1000, 2),  # ms
            'queue_size': self.queue.qsize(),
            'batch_size': len(self.batch)
        }

# 创建管理器实例
cache = CacheManager(
    max_items=CACHE_MAX_ITEMS,
    shards=CACHE_SHARDS,
    ttl=CACHE_TTL
)

request_queue = RequestQueue(
    max_workers=MAX_WORKERS
)

write_manager = WriteManager(
    batch_size=WRITE_BATCH_SIZE,
    flush_interval=WRITE_FLUSH_INTERVAL,
    max_queue_size=MAX_QUEUE_SIZE
)

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动
    logger.info("Starting up server...")
    logger.info(f"Available workers: {MAX_WORKERS}")
    logger.info(f"Cache config: max_items={CACHE_MAX_ITEMS}, shards={CACHE_SHARDS}, ttl={CACHE_TTL}s")
    
    # 初始化数据库
    init_db()
    
    # 启动请求队列处理器
    await request_queue.start()
    
    # 启动写入管理器
    await write_manager.start()
    
    yield
    
    # 关闭
    logger.info("Shutting down server...")
    
    # 停止请求队列处理器
    await request_queue.stop()
    
    # 停止写入管理器
    await write_manager.stop()
    
    # 关闭数据库连接
    # TODO: 实现数据库连接关闭逻辑

# 创建 FastAPI 应用
app = FastAPI(
    title="ObjSave API",
    description="""
    ObjSave 是一个高性能的对象存储服务。
    
    ## 功能特点
    
    * 支持文件对象和 JSON 对象的存储
    * 提供高性能的查询和检索功能
    * 内置缓存和批处理机制
    * 支持系统备份和恢复
    * 提供完整的监控指标
    
    ## API 文档
    
    * `/objsave/docs` - Swagger UI 文档（当前页面）
    * `/objsave/redoc` - ReDoc 文档（更适合阅读）
    * `/objsave/openapi.json` - OpenAPI 规范
    
    ## 认证
    
    目前支持以下认证方式：
    * API Key（请在请求头中添加 `X-API-Key`）
    
    ## 错误处理
    
    所有错误响应都遵循统一的格式：
    ```json
    {
        "detail": "错误信息"
    }
    ```
    
    ## 性能监控
    
    可以通过 `/objsave/metrics` 端点获取系统性能指标。
    """,
    version="1.0.0",
    docs_url="/objsave/docs",
    redoc_url="/objsave/redoc",
    openapi_url="/objsave/openapi.json",
    openapi_tags=[
        {
            "name": "object-storage",
            "description": "对象存储相关操作，包括上传、下载和查询",
        },
        {
            "name": "monitoring",
            "description": "系统监控相关接口，包括性能指标和统计信息",
        },
        {
            "name": "backup",
            "description": "系统备份和恢复相关操作",
        }
    ],
    contact={
        "name": "ObjSave Team",
        "url": "https://github.com/yourusername/objsave",
        "email": "support@objsave.com",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    }
)

# 更新应用配置，使用新的 lifespan
app.router.lifespan_context = lifespan

# 对象元数据模型
class ObjectMetadata(BaseModel):
    id: str
    name: str
    content_type: str
    size: int
    created_at: str

    model_config = ConfigDict(from_attributes=True)

# 创建路由器
router = APIRouter(
    prefix="/objsave",
    tags=["object-storage"]
)

# 健康检查接口
@router.get("/health", response_model=Dict[str, Any])
async def health_check():
    """系统健康检查"""
    stats = storage.get_stats()
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0",
        "stats": stats
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
    
    object_id = str(uuid.uuid4())
    try:
        # 读取文件内容
        content = await file.read()
        
        # 通过请求队列异步处理上传
        await request_queue.put({
            'type': 'upload',
            'data': {
                'id': object_id,
                'data': content
            }
        })
        
        # 创建元数据记录
        metadata = ObjectMetadata(
            id=object_id,
            name=file.filename,
            content_type=file.content_type,
            size=len(content),
            created_at=datetime.utcnow().isoformat()
        )
        
        return metadata
        
    except Exception as e:
        logger.exception("Failed to upload object")
        await storage.delete(object_id)
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
        
        # 通过请求队列异步处理上传
        await request_queue.put({
            'type': 'upload',
            'data': {
                'id': object_id,
                'data': json_content
            }
        })
        
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
@lru_cache(maxsize=CACHE_MAX_ITEMS)
async def download_object(
    object_id: str,
    db: Session = Depends(get_db)
):
    """下载指定对象"""
    try:
        # 获取对象内容
        content = await storage.get(object_id)
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
    db: Session = Depends(get_db),
    limit: Optional[int] = 100,
    offset: Optional[int] = 0,
    last_id: Optional[str] = None,  # 游标分页
    type: Optional[str] = None,     # 按类型过滤
    sort: Optional[str] = "desc"    # 排序方向
):
    """列出存储的对象，支持分页、过滤和排序"""
    try:
        # 1. 参数验证和规范化
        limit = min(max(1, limit), 1000)  # 限制范围 1-1000
        offset = max(0, offset)
        sort = sort.lower() if sort else "desc"
        
        # 2. 构建缓存键
        cache_key = f"list:{last_id}:{limit}:{offset}:{type}:{sort}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
            
        # 3. 构建高性能查询
        def db_operation():
            try:
                query = db.query(ObjectStorage)
                
                # 添加过滤条件
                if last_id:
                    query = query.filter(
                        ObjectStorage.id > last_id if sort == "asc" 
                        else ObjectStorage.id < last_id
                    )
                if type:
                    query = query.filter(ObjectStorage.type == type)
                    
                # 优化排序
                query = query.order_by(
                    asc(ObjectStorage.id) if sort == "asc"
                    else desc(ObjectStorage.id)
                )
                
                # 使用 LIMIT/OFFSET 优化
                if not last_id:
                    query = query.offset(offset)
                query = query.limit(limit)
                
                # 执行查询并获取结果
                return query.all()
            except Exception as e:
                logger.error(f"Database query error: {str(e)}")
                raise
            
        # 4. 异步执行查询
        objects = await asyncio.get_event_loop().run_in_executor(
            thread_pool,
            db_operation
        )
        
        # 5. 高效转换结果
        result = [
            ObjectMetadata(
                id=obj.id,
                name=obj.name,
                content_type=obj.content_type,
                size=obj.size,
                created_at=str(obj.created_at)
            )
            for obj in objects
        ]
        
        # 6. 智能缓存
        if len(result) > 0:
            # 只缓存有数据的结果
            ttl = 60 if len(result) < 100 else 30  # 根据结果大小调整缓存时间
            cache.set(cache_key, result, ttl=ttl)
        
        return result
        
    except Exception as e:
        logger.error(f"Error listing objects: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={"message": "获取列表失败", "error": str(e)}
        )

# 监控相关API
@router.get("/metrics", tags=["monitoring"])
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
    try:
        # 收集系统指标
        metrics_collector.collect_system_metrics()
        return Response(
            content=generate_latest(REGISTRY),
            media_type=CONTENT_TYPE_LATEST
        )
    except Exception as e:
        logger.error(f"Error collecting metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 创建请求信号量，限制并发请求数
request_semaphore = asyncio.Semaphore(MAX_WORKERS)

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

# 注册路由
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "run:app",  # 使用模块路径
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=MAX_WORKERS
    )

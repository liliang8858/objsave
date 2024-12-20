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

from obsave.core.storage import ObjectStorage
from obsave.core.exceptions import StorageError, ObjectNotFoundError, ObjSaveException
from obsave.core.database import get_db, init_db
from obsave.core.settings import *
from obsave.monitoring.metrics import metrics_collector
from obsave.monitoring.middleware import PrometheusMiddleware
from obsave.utils import CacheManager, RequestQueue, WriteManager, AsyncIOManager
from obsave.core.models import ObjectMetadata, JSONObjectModel, ObjectStorage as ObjectStorageModel

# 配置日志记录
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding='utf-8'
        )
    ]
)
logger = logging.getLogger(__name__)

# 系统配置
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

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

# 创建I/O管理器实例
io_manager = AsyncIOManager(max_workers=MAX_WORKERS)

# 创建存储管理器实例
storage = ObjectStorage(
    base_path=STORAGE_BASE_PATH,
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

# 全局写入管理器
write_manager = WriteManager(
    batch_size=WRITE_BATCH_SIZE,
    flush_interval=WRITE_FLUSH_INTERVAL,
    max_queue_size=WRITE_MAX_QUEUE_SIZE
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up server...")
    logger.info(f"Available workers: {MAX_WORKERS}")
    logger.info(f"Cache config: max_items={CACHE_MAX_ITEMS}, shards={CACHE_SHARDS}, ttl={CACHE_TTL}s")
    
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
    title=API_TITLE,
    description=API_DESCRIPTION,
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
    prefix=API_PREFIX,
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
    if not path.startswith('$'):
        return False
    return True

# 健康检查接口
@router.get("/health",
    summary="系统健康检查",
    description="检查系统健康状态并返回所有性能指标",
    response_description="返回系统状态和详细指标",
    tags=["monitoring"])
async def health_check():
    try:
        # 获取所有指标
        all_metrics = metrics.get_metrics()
        
        # 提取系统指标
        system = all_metrics['system']
        
        # 构建详细的健康状态响应
        health_status = {
            'status': 'initializing',
            'timestamp': datetime.utcnow().isoformat(),
            'uptime_seconds': all_metrics.get('uptime_seconds', 0),
            
            # 详细的系统指标
            'system': {
                'cpu': {
                    'usage_percent': system['cpu']['cpu_usage_percent'],
                    'count_physical': system['cpu']['cpu_count_physical'],
                    'count_logical': system['cpu']['cpu_count_logical'],
                    'process_cpu_percent': system['cpu']['process_cpu_percent'],
                    'process_cpu_times': system['cpu']['process_cpu_times'],
                },
                'memory': {
                    'usage_percent': system['memory']['memory_usage_percent'],
                    'available_gb': system['memory']['memory_available_gb'],
                    'total_gb': system['memory']['memory_total_gb'],
                    'process_rss_mb': system['memory']['process_memory_rss_mb'],
                    'process_vms_mb': system['memory']['process_memory_vms_mb'],
                },
                'disk': {
                    'usage_percent': system['disk']['disk_usage_percent'],
                    'free_gb': system['disk']['disk_free_gb'],
                    'total_gb': system['disk']['disk_total_gb'],
                },
                'process': {
                    'threads_count': system['process']['process_threads_count'],
                    'open_files': system['process']['process_open_files'],
                    'connections': system['process']['process_connections'],
                    'handles': system['process']['process_handles'],
                    'children': system['process']['process_children'],
                },
                'io': {
                    'read_mb': system['io']['process_io_read_mb'],
                    'write_mb': system['io']['process_io_write_mb'],
                    'read_count': system['io']['process_io_read_count'],
                    'write_count': system['io']['process_io_write_count'],
                },
                'runtime': {
                    'gc_counts': system['runtime']['python_gc_counts'],
                    'threads_active': system['runtime']['python_threads_active'],
                }
            },
            
            # 存储系统详细指标
            'storage': {},
            
            # 队列详细指标
            'queues': {},
            
            # 工作流详细指标
            'workflows': {},
            
            # 线程池详细指标
            'thread_pools': {},
            
            # 直方图统计
            'histograms': all_metrics.get('histograms', {}),
            
            # 计数器指标
            'counters': all_metrics.get('counter', {}),
            
            # 仪表盘指标
            'gauges': all_metrics.get('gauge', {}),
            
            # 告警信息
            'alerts': [],
            
            # 性能指标
            'performance': {
                'request_rates': {},
                'error_rates': {},
                'latencies': {},
            },
            
            # HTTP指标
            'http': {
                'summary': {
                    'total_endpoints': len(all_metrics.get('http', {})),
                    'total_requests': sum(
                        endpoint['total_requests']
                        for endpoint in all_metrics.get('http', {}).values()
                    ),
                    'total_errors': sum(
                        endpoint['error_count']
                        for endpoint in all_metrics.get('http', {}).values()
                    ),
                    'requests_per_minute': sum(
                        endpoint['requests_per_minute']
                        for endpoint in all_metrics.get('http', {}).values()
                    ),
                    'errors_per_minute': sum(
                        endpoint['errors_per_minute']
                        for endpoint in all_metrics.get('http', {}).values()
                    ),
                },
                'endpoints': all_metrics.get('http', {})
            },
        }
        
        # 处理存储指标
        storage_stats = all_metrics.get('storage', {})
        for storage_type, stats in storage_stats.items():
            storage_metrics = {
                'operations': {},
                'cache': {},
                'flush': {}
            }
            
            # 处理操作指标
            for op_type, op_stats in stats.get('operations', {}).items():
                storage_metrics['operations'][op_type] = {
                    'count': op_stats['count'],
                    'bytes': op_stats['bytes'],
                    'error_count': op_stats['error_count'],
                    'error_rate': (op_stats['error_count'] / op_stats['count'] * 100) if op_stats['count'] > 0 else 0,
                    'latencies': op_stats['latencies'],
                    'throughput_mbps': op_stats['bytes'] / (all_metrics.get('uptime_seconds', 1) * 1024 * 1024)
                }
            
            # 处理缓存指标
            cache_stats = stats.get('cache', {})
            storage_metrics['cache'] = {
                'hits': cache_stats.get('hits', 0),
                'misses': cache_stats.get('misses', 0),
                'hit_rate': cache_stats.get('hit_rate', 0),
                'evictions': cache_stats.get('evictions', 0),
                'eviction_rate': (cache_stats.get('evictions', 0) / 
                    (cache_stats.get('hits', 0) + cache_stats.get('misses', 0)) * 100
                    if (cache_stats.get('hits', 0) + cache_stats.get('misses', 0)) > 0 else 0)
            }
            
            # 处理刷盘指标
            flush_stats = stats.get('flush', {})
            storage_metrics['flush'] = {
                'count': flush_stats.get('count', 0),
                'total_bytes': flush_stats.get('total_bytes', 0),
                'avg_flush_size': (flush_stats.get('total_bytes', 0) / flush_stats.get('count', 1)
                    if flush_stats.get('count', 0) > 0 else 0),
                'durations': flush_stats.get('durations', {}),
                'flush_rate': flush_stats.get('count', 0) / all_metrics.get('uptime_seconds', 1)
            }
            
            health_status['storage'][storage_type] = storage_metrics
        
        # 处理队列指标
        queue_stats = all_metrics.get('queues', {})
        for queue_name, stats in queue_stats.items():
            health_status['queues'][queue_name] = {
                'current_size': stats['qsize'],
                'is_empty': stats['empty'],
                'is_full': stats['full'],
                'capacity': stats['maxsize'],
                'utilization': (stats['qsize'] / stats['maxsize'] * 100) if stats['maxsize'] > 0 else 0
            }
        
        # 处理工作流指标
        workflow_stats = all_metrics.get('workflows', {})
        for workflow_name, stats in workflow_stats.items():
            health_status['workflows'][workflow_name] = {
                'total_executions': stats['total'],
                'successful': stats['success'],
                'failed': stats['failed'],
                'in_progress': stats['in_progress'],
                'success_rate': stats['success_rate'],
                'failure_rate': (stats['failed'] / stats['total'] * 100) if stats['total'] > 0 else 0,
                'average_concurrency': stats['in_progress'] / all_metrics.get('uptime_seconds', 1),
                'durations': stats.get('durations', {})
            }
        
        # 处理线程池指标
        thread_pool_stats = all_metrics.get('thread_pools', {})
        for pool_name, stats in thread_pool_stats.items():
            health_status['thread_pools'][pool_name] = {
                'workers': stats['workers'],
                'tasks_total': stats['tasks_total'],
                'threads_alive': stats['threads_alive'],
                'utilization': (stats['threads_alive'] / stats['workers'] * 100) if stats['workers'] > 0 else 0,
                'task_per_worker': stats['tasks_total'] / stats['workers'] if stats['workers'] > 0 else 0
            }
        
        # 计算性能指标
        for op_type in ['read', 'write', 'delete']:
            total_ops = sum(
                stats['operations'].get(op_type, {}).get('count', 0)
                for stats in storage_stats.values()
            )
            total_errors = sum(
                stats['operations'].get(op_type, {}).get('error_count', 0)
                for stats in storage_stats.values()
            )
            uptime = all_metrics.get('uptime_seconds', 1)
            
            health_status['performance']['request_rates'][op_type] = total_ops / uptime
            health_status['performance']['error_rates'][op_type] = (
                total_errors / total_ops * 100 if total_ops > 0 else 0
            )
        
        # 评估系统整体健康状态
        status_checks = {
            'cpu_healthy': system['cpu']['cpu_usage_percent'] < 90,
            'memory_healthy': system['memory']['memory_usage_percent'] < 90,
            'disk_healthy': system['disk']['disk_usage_percent'] < 90,
            'storage_healthy': all(
                all(op['error_rate'] < 5 for op in stats['operations'].values())
                for stats in health_status['storage'].values()
            ),
            'cache_healthy': all(
                stats['cache']['hit_rate'] > 60
                for stats in health_status['storage'].values()
            ),
            'queues_healthy': all(
                not stats['is_full']
                for stats in health_status['queues'].values()
            ),
            'workflows_healthy': all(
                stats['success_rate'] > 95
                for stats in health_status['workflows'].values()
            ),
            'http_healthy': all(
                endpoint['error_rate_per_minute'] < 5  # 每分钟错误率小于5%
                for endpoint in all_metrics.get('http', {}).values()
                if endpoint['requests_per_minute'] > 0  # 只检查有流量的端点
            ),
        }
        
        # 更新系统状态
        if all(status_checks.values()):
            health_status['status'] = 'healthy'
        elif any(not check for check in [
            status_checks['cpu_healthy'],
            status_checks['memory_healthy'],
            status_checks['disk_healthy']
        ]):
            health_status['status'] = 'critical'
        else:
            health_status['status'] = 'degraded'
        
        # 添加告警信息
        if not status_checks['cpu_healthy']:
            health_status['alerts'].append({
                'level': 'critical',
                'message': f'Critical CPU usage: {system["cpu"]["cpu_usage_percent"]}%'
            })
        elif system['cpu']['cpu_usage_percent'] > 80:
            health_status['alerts'].append({
                'level': 'warning',
                'message': f'High CPU usage: {system["cpu"]["cpu_usage_percent"]}%'
            })
        
        if not status_checks['memory_healthy']:
            health_status['alerts'].append({
                'level': 'critical',
                'message': f'Critical memory usage: {system["memory"]["memory_usage_percent"]}%'
            })
        elif system['memory']['memory_usage_percent'] > 80:
            health_status['alerts'].append({
                'level': 'warning',
                'message': f'High memory usage: {system["memory"]["memory_usage_percent"]}%'
            })
        
        if not status_checks['disk_healthy']:
            health_status['alerts'].append({
                'level': 'critical',
                'message': f'Critical disk usage: {system["disk"]["disk_usage_percent"]}%'
            })
        elif system['disk']['disk_usage_percent'] > 80:
            health_status['alerts'].append({
                'level': 'warning',
                'message': f'High disk usage: {system["disk"]["disk_usage_percent"]}%'
            })
        
        # 存储系统告警
        for storage_type, stats in health_status['storage'].items():
            for op_type, op_stats in stats['operations'].items():
                if op_stats['error_rate'] > 5:
                    health_status['alerts'].append({
                        'level': 'critical',
                        'message': f'High error rate in {storage_type} {op_type}: {op_stats["error_rate"]}%'
                    })
                elif op_stats['error_rate'] > 1:
                    health_status['alerts'].append({
                        'level': 'warning',
                        'message': f'Elevated error rate in {storage_type} {op_type}: {op_stats["error_rate"]}%'
                    })
            
            if stats['cache']['hit_rate'] < 50:
                health_status['alerts'].append({
                    'level': 'warning',
                    'message': f'Low cache hit rate in {storage_type}: {stats["cache"]["hit_rate"]}%'
                })
        
        # 队列告警
        for queue_name, stats in health_status['queues'].items():
            if stats['is_full']:
                health_status['alerts'].append({
                    'level': 'critical',
                    'message': f'Queue {queue_name} is full'
                })
            elif stats['utilization'] > 80:
                health_status['alerts'].append({
                    'level': 'warning',
                    'message': f'High queue utilization in {queue_name}: {stats["utilization"]}%'
                })
        
        # 工作流告警
        for workflow_name, stats in health_status['workflows'].items():
            if stats['failure_rate'] > 5:
                health_status['alerts'].append({
                    'level': 'critical',
                    'message': f'High failure rate in workflow {workflow_name}: {stats["failure_rate"]}%'
                })
            elif stats['failure_rate'] > 1:
                health_status['alerts'].append({
                    'level': 'warning',
                    'message': f'Elevated failure rate in workflow {workflow_name}: {stats["failure_rate"]}%'
                })
        
        # 线程池告警
        for pool_name, stats in health_status['thread_pools'].items():
            if stats['utilization'] > 90:
                health_status['alerts'].append({
                    'level': 'warning',
                    'message': f'High thread pool utilization in {pool_name}: {stats["utilization"]}%'
                })
        
        # HTTP相关告警
        for endpoint, stats in all_metrics.get('http', {}).items():
            # 高错误率告警
            if stats['requests_per_minute'] > 0:  # 只检查有流量的端点
                if stats['error_rate_per_minute'] > 10:
                    health_status['alerts'].append({
                        'level': 'critical',
                        'message': f'High error rate on {endpoint}: {stats["error_rate_per_minute"]:.1f}% errors/min'
                    })
                elif stats['error_rate_per_minute'] > 5:
                    health_status['alerts'].append({
                        'level': 'warning',
                        'message': f'Elevated error rate on {endpoint}: {stats["error_rate_per_minute"]:.1f}% errors/min'
                    })
            
            # 高延迟告警
            if 'latencies' in stats and 'p99' in stats['latencies']:
                if stats['latencies']['p99'] > 1000:  # p99延迟超过1秒
                    health_status['alerts'].append({
                        'level': 'warning',
                        'message': f'High latency on {endpoint}: p99={stats["latencies"]["p99"]:.0f}ms'
                    })
        
        return health_status
        
    except Exception as e:
        logger.exception("Health check failed")
        raise ObjSaveException(
            status_code=500,
            detail="Health check failed",
            error_code="HEALTH_CHECK_FAILED"
        )

# 上传对象接口
@router.post("/objects", response_model=ObjectMetadata)
async def upload_object(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    async with measure_time("upload_object"):
        if file.size and file.size > MAX_UPLOAD_SIZE:
            raise ObjSaveException(
                status_code=413,
                detail=f"File too large. Maximum size is {MAX_UPLOAD_SIZE} bytes",
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
            db_obj = db.query(ObjectStorageModel).filter(ObjectStorageModel.id == object_id).first()
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
            query = db.query(ObjectStorageModel)
            if last_id:
                query = query.filter(ObjectStorageModel.id > last_id)
            return query.order_by(ObjectStorageModel.created_at.desc()).offset(offset).limit(limit).all()
            
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
                created_at=obj.created_at.isoformat(),
                type=obj.type,
                owner=obj.owner
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
        workers=MAX_WORKERS
    )

import os
import uuid
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException, APIRouter, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
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

from db import init_db, get_db, ObjectStorage
from cache_manager import CacheManager
from resource_manager import ResourceManager

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,  # 生产环境推荐INFO级别
    format='%(asctime)s - %(levelname)s - [%(pathname)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 系统配置
MAX_UPLOAD_SIZE = 50 * 1024 * 1024  # 50MB
MAX_CONCURRENT_REQUESTS = 500
CHUNK_SIZE = 8192  # 8KB

# 创建资源管理器
resource_manager = ResourceManager()

# 创建缓存管理器实例
cache = CacheManager(
    max_size=10000,
    default_ttl=3600,
    max_memory_mb=int(resource_manager.get_memory_limit() / (1024 * 1024))
)

# 创建线程池
thread_pool = ThreadPoolExecutor(
    max_workers=resource_manager.get_available_workers()
)

# 创建FastAPI应用
app = FastAPI(
    title="对象存储服务",
    description="轻量级本地对象存储HTTP服务",
    openapi_url="/objsave/openapi.json",
    docs_url="/objsave/docs",
    redoc_url="/objsave/redoc"
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 创建路由前缀
api_router = APIRouter(prefix="/objsave")

# 初始化数据库
init_db()

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

# JSON 查询模型
class JSONQueryModel(BaseModel):
    jsonpath: str  # JSONPath查询表达式
    value: Optional[Any] = None  # 可选的精确匹配值
    operator: Optional[str] = 'eq'  # 比较运算符：eq, gt, lt, ge, le, contains
    type: Optional[str] = None  # JSON对象类型

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

# 性能监控中间件
@app.middleware("http")
async def performance_middleware(request: Request, call_next):
    # 检查系统资源
    can_process, error_message = resource_manager.check_resources()
    if not can_process:
        return JSONResponse(
            status_code=503,
            content={"detail": error_message}
        )
    
    # 并发限制
    if len(asyncio.all_tasks()) > MAX_CONCURRENT_REQUESTS:
        return JSONResponse(
            status_code=503,
            content={"detail": "服务器繁忙，请稍后重试"}
        )
    
    # 记录处理时间
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    # 添加性能指标到响应头
    response.headers["X-Process-Time"] = str(process_time)
    response.headers["X-Cache-Stats"] = str(cache.get_stats())
    
    # 记录慢请求
    if process_time > 1.0:
        logger.warning(f"Slow request: {request.url} took {process_time:.2f}s")
    
    return response

# 全局错误处理
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global error: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

# 健康检查接口
@api_router.get("/health")
async def health_check():
    """系统健康检查"""
    resources = resource_manager.get_system_resources()
    cache_stats = cache.get_stats()
    
    return {
        "status": "healthy",
        "system_resources": resources,
        "cache_stats": cache_stats,
        "worker_count": thread_pool._max_workers,
        "current_tasks": len(asyncio.all_tasks())
    }

# 上传对象接口
@api_router.post("/upload", response_model=ObjectMetadata)
async def upload_object(
    file: UploadFile = File(...), 
    db: Session = Depends(get_db)
):
    """上传对象到存储服务"""
    try:
        # 文件大小限制检查
        MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
        content = await file.read(MAX_FILE_SIZE + 1)
        if len(content) > MAX_FILE_SIZE:
            raise HTTPException(status_code=413, detail="File too large")
            
        # 创建新的存储对象
        db_object = ObjectStorage(
            id=str(uuid.uuid4()),
            name=file.filename,
            content=content,
            content_type=file.content_type,
            size=len(content)
        )
        
        # 使用线程池处理数据库操作
        def db_operation():
            try:
                db.add(db_object)
                db.commit()
                db.refresh(db_object)
                return db_object
            except Exception as e:
                db.rollback()
                raise e

        db_object = await asyncio.get_event_loop().run_in_executor(
            thread_pool, 
            db_operation
        )
        
        # 创建响应对象
        metadata = ObjectMetadata(
            id=db_object.id,
            name=db_object.name,
            content_type=db_object.content_type,
            size=db_object.size,
            created_at=str(db_object.created_at)
        )
        
        # 缓存元数据
        cache.set(f"metadata:{db_object.id}", metadata.dict())
        
        return metadata
    
    except Exception as e:
        logger.error(f"Failed to upload file {file.filename}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"上传失败: {str(e)}")

# 下载对象接口
@api_router.get("/download/{object_id}")
async def download_object(
    object_id: str, 
    db: Session = Depends(get_db)
):
    """下载指定对象"""
    try:
        # 先从缓存获取
        cached_data = cache.get(f"content:{object_id}")
        if cached_data:
            logger.debug(f"Cache hit for object_id: {object_id}")
            return cached_data
            
        # 缓存未命中，从数据库获取
        def db_operation():
            return db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
            
        db_object = await asyncio.get_event_loop().run_in_executor(
            thread_pool, 
            db_operation
        )
        
        if not db_object:
            raise HTTPException(status_code=404, detail="对象未找到")
        
        response_data = {
            "file_name": db_object.name,
            "content_type": db_object.content_type,
            "content": db_object.content,
            "type": db_object.type
        }
        
        # 缓存响应数据
        cache.set(f"content:{object_id}", response_data)
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading object {object_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"下载失败: {str(e)}")

# 列出所有对象接口
@api_router.get("/list", response_model=List[ObjectMetadata])
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
@api_router.post("/upload/json", response_model=ObjectMetadata)
async def upload_json_object(
    json_data: JSONObjectModel, 
    db: Session = Depends(get_db)
):
    """上传JSON对象"""
    try:
        content_str = json.dumps(json_data.content)
        if len(content_str.encode('utf-8')) > 10 * 1024 * 1024:  # 10MB限制
            raise HTTPException(status_code=413, detail="JSON content too large")
            
        object_id = str(uuid.uuid4())
        current_time = datetime.now().isoformat()
        
        def db_operation():
            try:
                obj = ObjectStorage(
                    id=object_id,
                    name=json_data.name or object_id,
                    content=content_str,
                    content_type='application/json',
                    type=json_data.type,
                    size=len(content_str),
                    created_at=current_time
                )
                db.add(obj)
                db.commit()
                db.refresh(obj)
                return obj
            except Exception as e:
                db.rollback()
                raise e
                
        db_object = await asyncio.get_event_loop().run_in_executor(
            thread_pool, 
            db_operation
        )
        
        metadata = ObjectMetadata(
            id=db_object.id,
            name=db_object.name,
            content_type=db_object.content_type,
            size=db_object.size,
            created_at=str(db_object.created_at)
        )
        
        # 缓存元数据和内容
        cache.set(f"metadata:{db_object.id}", metadata.dict())
        cache.set(f"content:{db_object.id}", json_data.content)
        
        return metadata
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to upload JSON object: {str(e)}")
        raise HTTPException(status_code=422, detail=str(e))

# JSON对象批量上传接口
@api_router.post("/upload/json/batch", response_model=List[ObjectMetadata])
async def upload_json_objects_batch(
    json_objects: List[JSONObjectModel], 
    db: Session = Depends(get_db)
):
    """批量上传JSON对象"""
    try:
        # 存储所有对象并收集元数据
        metadata_list = []
        
        db_objects = []
        for json_data in json_objects:
            # 序列化JSON数据
            content = json.dumps(json_data.content, ensure_ascii=False).encode('utf-8')
            
            # 创建新的存储对象
            db_object = ObjectStorage(
                id=json_data.id or str(uuid.uuid4()),
                name=json_data.name or f"json_object_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                content=content,
                content_type="application/json",
                type=json_data.type,
                size=len(content)
            )
            db_objects.append(db_object)
        
        # 保存到数据库
        def db_operation():
            try:
                db.add_all(db_objects)
                db.commit()
                return db_objects
            except Exception as e:
                db.rollback()
                raise e

        db_objects = await asyncio.get_event_loop().run_in_executor(
            thread_pool, 
            db_operation
        )
        
        for db_object in db_objects:
            metadata_list.append(ObjectMetadata(
                id=db_object.id,
                name=db_object.name,
                content_type=db_object.content_type,
                size=db_object.size,
                created_at=str(db_object.created_at)
            ))
        
        # 缓存元数据
        for metadata in metadata_list:
            cache.set(f"metadata:{metadata.id}", metadata.dict())
        
        return metadata_list
    
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

# JSON对象更新接口
@api_router.put("/update/json/{object_id}", response_model=ObjectMetadata)
async def update_json_object(
    object_id: str,
    json_data: JSONObjectModel, 
    db: Session = Depends(get_db)
):
    """更新指定ID的JSON对象"""
    try:
        # 查找现有对象
        def db_operation():
            return db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
            
        db_object = await asyncio.get_event_loop().run_in_executor(
            thread_pool, 
            db_operation
        )
        
        if not db_object:
            raise HTTPException(status_code=404, detail="对象未找到")
        
        # 序列化新的JSON数据
        content = json.dumps(json_data.content).encode('utf-8')
        
        # 更新对象
        db_object.content = content
        db_object.name = json_data.name or db_object.name
        db_object.size = len(content)
        
        # 提交更改
        def db_operation():
            try:
                db.commit()
                db.refresh(db_object)
                return db_object
            except Exception as e:
                db.rollback()
                raise e

        db_object = await asyncio.get_event_loop().run_in_executor(
            thread_pool, 
            db_operation
        )
        
        # 缓存元数据和内容
        cache.set(f"metadata:{db_object.id}", ObjectMetadata(
            id=db_object.id,
            name=db_object.name,
            content_type=db_object.content_type,
            size=db_object.size,
            created_at=str(db_object.created_at)
        ).dict())
        cache.set(f"content:{db_object.id}", json_data.content)
        
        return ObjectMetadata(
            id=db_object.id,
            name=db_object.name,
            content_type=db_object.content_type,
            size=db_object.size,
            created_at=str(db_object.created_at)
        )
    
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

# JSON对象查询接口
@api_router.post("/query/json", response_model=List[JSONObjectResponse])
async def query_json_objects(
    query: JSONQueryModel, 
    db: Session = Depends(get_db),
    limit: Optional[int] = 100,
    offset: Optional[int] = 0
):
    """根据 JSONPath 查询和过滤 JSON 对象"""
    try:
        logger.debug(f"Querying JSON objects with path: {query.jsonpath}, value: {query.value}, operator: {query.operator}")
        
        # 验证 JSONPath 表达式
        if not validate_jsonpath(query.jsonpath):
            raise ValueError(f"Invalid JSONPath expression: {query.jsonpath}. JSONPath must start with '$'")
        
        # 解析 JSONPath 表达式中的条件
        def extract_condition_from_jsonpath(path: str) -> tuple:
            """从 JSONPath 表达式中提取条件
            例如: $[?(@.type=="idea")] -> ("type", "idea")
            """
            import re
            # 匹配 JSONPath 条件表达式
            pattern = r'\$\[\?\(@\.(\w+)==["\'](.+)["\']\)\]'
            match = re.match(pattern, path)
            if match:
                return match.groups()
            return None, None

        # 构建基础查询
        base_query = db.query(ObjectStorage).filter(
            ObjectStorage.content_type == "application/json"
        )
        
        # 从 JSONPath 中提取查询条件
        field, value = extract_condition_from_jsonpath(query.jsonpath)
        if field and value:
            # 将 JSONPath 条件转换为 SQL 查询
            base_query = base_query.filter(getattr(ObjectStorage, field) == value)
        elif query.type:  # 如果没有 JSONPath 条件但有 type 参数
            base_query = base_query.filter(ObjectStorage.type == query.type)
            
        # 打印 SQL 查询语句
        logger.debug(f"SQL Query: {base_query.statement}")
            
        # 执行分页查询    
        def db_operation():
            return base_query.offset(offset).limit(limit).all()
            
        objects = await asyncio.get_event_loop().run_in_executor(
            thread_pool, 
            db_operation
        )
        
        # 打印查询到的对象
        logger.debug("Query results:")
        for obj in objects:
            logger.debug(f"Object: {obj.__dict__}")
            
        logger.debug(f"Found {len(objects)} JSON objects before filtering")
        logger.debug(f"Query JSONPath: {query.jsonpath}")
        
        # 打印每个对象的关键信息
        for obj in objects:
            try:
                content = json.loads(obj.content)
                logger.debug(f"Object[{obj.id}] - Type: {obj.type}, Content：field: {content}")
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON content for object ID: {obj.id}")
                continue
        
        # 使用 JSONPath 和条件过滤
        matched_objects = []
        
        for obj in objects:
            try:
                # 解析JSON内容
                content = json.loads(obj.content)
                matched_objects.append((obj, content))
                logger.debug("Matched objects:")
                for obj2, content2 in matched_objects:
                    logger.debug(f"Object[{obj2.id}] - Type: {obj2.type}, Content: {content2}")

                
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON content for object ID: {obj.id}")
                continue
            except Exception as e:
                logger.error(f"Error processing object {obj.id}: {str(e)}")
                continue
        
        logger.info(f"Query returned {len(matched_objects)} matches")
        
        # 转换为响应模型
        return [
            JSONObjectResponse(
                id=obj.id,
                name=obj.name,
                content_type=obj.content_type,
                size=obj.size,
                created_at=str(obj.created_at),
                type=obj.type,
                content=content
            ) for obj, content in matched_objects
        ]
    
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Error during JSON query: {str(e)}")
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")

def validate_jsonpath(path: str) -> bool:
    """验证 JSONPath 表达式的基本格式"""
    if not path:
        return False
    # 检查基本格式
    if not path.startswith('$'):
        return False
    # 允许 JSONPath 过滤表达式中的特殊字符
    # 例如: $.data[?(@.type=="idea")] 是合法的
    return True

def _apply_filter(value, compare_value, operator):
    """
    根据指定运算符比较值
    """
    try:
        if operator == 'eq':
            return value == compare_value
        elif operator == 'gt':
            return value > compare_value
        elif operator == 'lt':
            return value < compare_value
        elif operator == 'ge':
            return value >= compare_value
        elif operator == 'le':
            return value <= compare_value
        elif operator == 'contains':
            if isinstance(value, (list, str, dict)):
                return compare_value in value
            return False
        else:
            raise ValueError(f"不支持的运算符: {operator}")
    except TypeError:
        return False

# 启动事件处理
@app.on_event("startup")
async def startup_event():
    logger.info("Starting up server...")
    logger.info(f"Available workers: {thread_pool._max_workers}")
    logger.info(f"Memory limit: {cache._max_memory / (1024*1024):.2f}MB")
    logger.info(f"Max concurrent requests: {MAX_CONCURRENT_REQUESTS}")

# 关闭事件处理
@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down server...")
    thread_pool.shutdown(wait=True)
    cache.clear()

# 将路由添加到主应用
app.include_router(api_router)

# 启动服务器配置
if __name__ == "__main__":
    import uvicorn
    
    # 获取CPU核心数
    workers = min(multiprocessing.cpu_count(), 4)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        workers=workers,
        limit_concurrency=1000,
        limit_max_requests=10000,
        timeout_keep_alive=5,
        log_level="warning"
    )

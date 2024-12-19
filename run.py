"""ObSave 启动脚本"""

import uvicorn
from fastapi import FastAPI, File, UploadFile, Request, APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict
from typing import Dict, Any, Optional, List
from obsave.core.storage import ObjectStorage
from obsave.core.exceptions import ObjectNotFoundError, StorageError
from obsave.monitoring.middleware import PrometheusMiddleware
from obsave.monitoring.metrics import metrics_endpoint, metrics_collector
import logging
import os
import json
import uuid
from datetime import datetime
import asyncio
import shutil
from fastapi import Path, Body, Query

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# 创建应用实例
app = FastAPI(
    title="ObSave API",
    description="""ObSave 是一个高性能对象存储服务。
    
    ## 功能特点
    * 对象存储：支持存储、获取、删除对象
    * JSON 存储：支持 JSON 对象的存储和查询
    * 文件上传：支持文件上传和管理
    * 备份管理：支持创建和恢复备份
    * 监控指标：集成 Prometheus 监控
    """,
    version="1.0.0",
    docs_url="/objsave/docs",
    redoc_url="/objsave/redoc",
    openapi_url="/objsave/openapi.json"
)

# 创建路由器
router = APIRouter(
    prefix="/objsave",
    tags=["object-storage"],
    responses={404: {"description": "Not found"}},
)

# 添加中间件
app.add_middleware(PrometheusMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 创建存储实例
storage = ObjectStorage(
    base_path=os.path.join(os.path.dirname(__file__), "data", "storage")
)

# 数据模型
class JSONObject(BaseModel):
    content: Dict[str, Any]
    type: Optional[str] = None
    name: Optional[str] = None

class JSONQuery(BaseModel):
    query: str
    value: Any

class BackupInfo(BaseModel):
    name: str
    size: int
    created_at: str
    status: str

# 健康检查端点
@router.get("/health", 
    summary="健康检查",
    description="检查服务是否正常运行",
    response_description="返回服务状态信息")
async def health_check():
    """
    健康检查端点，用于监控服务状态
    
    Returns:
        dict: 包含服务状态、时间戳和版本信息
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

# 对象存储端点
@router.post("/objects/{key}",
    summary="存储对象",
    description="将数据存储到指定的键下",
    response_description="返回存储操作的结果")
async def store_object(
    key: str = Path(..., description="对象的唯一标识符"),
    data: bytes = Body(..., description="要存储的数据")
):
    """
    存储对象数据
    
    Args:
        key: 对象的唯一标识符
        data: 要存储的二进制数据
        
    Returns:
        dict: 包含操作是否成功的信息
    """
    success = await storage.store(key, data)
    return {"success": success}

@router.get("/objects/{key}",
    summary="获取对象",
    description="获取指定键的对象数据",
    response_description="返回对象数据或错误信息",
    responses={
        404: {"description": "对象未找到"},
        200: {"description": "成功返回对象数据"}
    })
async def get_object(
    key: str = Path(..., description="要获取的对象的唯一标识符")
):
    """
    获取对象数据
    
    Args:
        key: 对象的唯一标识符
        
    Returns:
        dict: 包含对象数据或错误信息
        
    Raises:
        ObjectNotFoundError: 如果对象不存在
    """
    try:
        data = await storage.get(key)
        return {"data": data}
    except ObjectNotFoundError:
        return {"error": "Object not found"}

@router.delete("/objects/{key}",
    summary="删除对象",
    description="删除指定键的对象",
    response_description="返回删除操作的结果")
async def delete_object(
    key: str = Path(..., description="要删除的对象的唯一标识符")
):
    """
    删除对象
    
    Args:
        key: 对象的唯一标识符
        
    Returns:
        dict: 包含操作是否成功的信息
    """
    success = await storage.delete(key)
    return {"success": success}

# 文件上传端点
@router.post("/upload/file",
    summary="上传文件",
    description="上传文件并存储",
    response_description="返回上传结果，包含文件ID和元数据")
async def upload_file(
    file: UploadFile = File(..., description="要上传的文件")
):
    """
    上传文件接口
    
    Args:
        file: 要上传的文件对象
        
    Returns:
        dict: 包含文件ID、文件名、内容类型、大小等信息
    """
    try:
        # 生成唯一文件名
        file_id = str(uuid.uuid4())
        content = await file.read()
        
        # 存储文件
        success = await storage.store(file_id, content)
        
        if success:
            return {
                "success": True,
                "file_id": file_id,
                "filename": file.filename,
                "content_type": file.content_type,
                "size": len(content)
            }
        else:
            return {"success": False, "error": "Failed to store file"}
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        return {"success": False, "error": str(e)}

# JSON 对象端点
@router.post("/upload/json",
    summary="上传 JSON 对象",
    description="上传并存储 JSON 对象",
    response_description="返回上传结果，包含对象ID和元数据")
async def upload_json(
    json_obj: JSONObject = Body(..., description="要上传的 JSON 对象")
):
    """
    上传 JSON 对象
    
    Args:
        json_obj: JSON 对象数据
        
    Returns:
        dict: 包含对象ID、类型、名称、大小等信息
    """
    try:
        # 生成唯一ID
        obj_id = str(uuid.uuid4())
        data = json.dumps(json_obj.dict()).encode('utf-8')
        
        success = await storage.store(obj_id, data)
        if success:
            return {
                "success": True,
                "id": obj_id,
                "type": json_obj.type,
                "name": json_obj.name,
                "size": len(data)
            }
        else:
            return {"success": False, "error": "Failed to store JSON object"}
    except Exception as e:
        logger.error(f"Error uploading JSON: {str(e)}")
        return {"success": False, "error": str(e)}

@router.post("/upload/json/batch",
    summary="批量上传 JSON 对象",
    description="批量上传并存储多个 JSON 对象",
    response_description="返回每个对象的上传结果")
async def upload_json_batch(
    objects: List[JSONObject] = Body(..., description="要上传的 JSON 对象列表")
):
    """
    批量上传 JSON 对象
    
    Args:
        objects: JSON 对象列表
        
    Returns:
        dict: 包含每个对象的上传结果
    """
    results = []
    for obj in objects:
        try:
            obj_id = str(uuid.uuid4())
            data = json.dumps(obj.dict()).encode('utf-8')
            success = await storage.store(obj_id, data)
            results.append({
                "success": success,
                "id": obj_id,
                "type": obj.type,
                "name": obj.name,
                "size": len(data)
            })
        except Exception as e:
            results.append({
                "success": False,
                "error": str(e)
            })
    return {"results": results}

# 列出对象
@router.get("/list",
    summary="列出对象",
    description="列出存储的所有对象，支持分页",
    response_description="返回对象列表和分页信息")
async def list_objects(
    limit: int = Query(10, description="每页数量", ge=1, le=100),
    offset: int = Query(0, description="起始位置", ge=0)
):
    """
    列出存储的对象
    
    Args:
        limit: 每页返回的对象数量，默认10，最大100
        offset: 分页偏移量，默认0
        
    Returns:
        dict: 包含对象列表和分页信息
    """
    try:
        # 获取存储目录
        storage_path = storage.base_path
        objects = []
        
        # 遍历存储目录
        for root, _, files in os.walk(storage_path):
            for file in files:
                # 获取完整路径
                file_path = os.path.join(root, file)
                # 获取相对路径（作为key）
                rel_path = os.path.relpath(file_path, storage_path)
                
                try:
                    # 获取文件信息
                    stat = os.stat(file_path)
                    objects.append({
                        "key": rel_path,
                        "size": stat.st_size,
                        "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                        "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
                except OSError:
                    continue
        
        # 排序、分页
        objects.sort(key=lambda x: x["modified_at"], reverse=True)
        total = len(objects)
        objects = objects[offset:offset + limit]
        
        return {
            "total": total,
            "offset": offset,
            "limit": limit,
            "objects": objects
        }
    except Exception as e:
        logger.error(f"Error listing objects: {str(e)}")
        return {"error": "Failed to list objects"}

# 统计信息
@router.get("/stats",
    summary="获取统计信息",
    description="获取存储系统的统计信息",
    response_description="返回存储统计数据")
async def get_stats():
    """
    获取存储统计信息
    
    Returns:
        dict: 包含存储使用情况的统计信息
    """
    return storage.get_stats()

# 监控指标端点
@router.get("/metrics",
    summary="获取监控指标",
    description="获取 Prometheus 格式的监控指标",
    response_description="返回 Prometheus 格式的指标数据")
async def metrics():
    """
    获取 Prometheus 监控指标
    
    Returns:
        Response: Prometheus 格式的指标数据
    """
    return await metrics_endpoint()

# 备份相关端点
@router.post("/backup/create",
    summary="创建备份",
    description="创建系统数据的备份",
    response_description="返回备份创建的结果")
async def create_backup(
    background_tasks: BackgroundTasks,
    name: Optional[str] = Query(None, description="备份名称，如果不指定将自动生成")
):
    """
    创建系统备份
    
    Args:
        background_tasks: 后台任务对象
        name: 可选的备份名称
        
    Returns:
        dict: 包含备份创建的状态信息
    """
    try:
        # 生成备份名称
        backup_name = name or f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        backup_dir = os.path.join(os.path.dirname(__file__), "data", "backups", backup_name)
        
        # 创建备份目录
        os.makedirs(backup_dir, exist_ok=True)
        
        # 在后台执行备份
        def do_backup():
            try:
                shutil.copytree(storage.base_path, os.path.join(backup_dir, "storage"))
                logger.info(f"Backup created: {backup_name}")
            except Exception as e:
                logger.error(f"Backup failed: {str(e)}")
        
        background_tasks.add_task(do_backup)
        
        return {
            "success": True,
            "backup_name": backup_name,
            "status": "started"
        }
    except Exception as e:
        logger.error(f"Error creating backup: {str(e)}")
        return {"success": False, "error": str(e)}

@router.get("/backup/list",
    summary="列出备份",
    description="列出所有可用的备份",
    response_description="返回备份列表")
async def list_backups():
    """
    列出所有备份
    
    Returns:
        dict: 包含备份列表，每个备份包含名称、大小、创建时间等信息
    """
    try:
        backup_dir = os.path.join(os.path.dirname(__file__), "data", "backups")
        if not os.path.exists(backup_dir):
            return {"backups": []}
            
        backups = []
        for name in os.listdir(backup_dir):
            path = os.path.join(backup_dir, name)
            if os.path.isdir(path):
                try:
                    size = sum(
                        os.path.getsize(os.path.join(dirpath, filename))
                        for dirpath, _, filenames in os.walk(path)
                        for filename in filenames
                    )
                    stat = os.stat(path)
                    backups.append({
                        "name": name,
                        "size": size,
                        "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                        "status": "completed"
                    })
                except OSError:
                    continue
                    
        return {"backups": backups}
    except Exception as e:
        logger.error(f"Error listing backups: {str(e)}")
        return {"error": "Failed to list backups"}

@router.post("/backup/restore/{backup_name}",
    summary="恢复备份",
    description="从指定的备份恢复系统数据",
    response_description="返回恢复操作的结果")
async def restore_backup(
    backup_name: str = Path(..., description="要恢复的备份名称")
):
    """
    恢复系统备份
    
    Args:
        backup_name: 要恢复的备份名称
        
    Returns:
        dict: 包含恢复操作的结果
    """
    try:
        backup_path = os.path.join(os.path.dirname(__file__), "data", "backups", backup_name, "storage")
        if not os.path.exists(backup_path):
            return {"success": False, "error": "Backup not found"}
            
        # 清空当前存储
        shutil.rmtree(storage.base_path)
        # 恢复备份
        shutil.copytree(backup_path, storage.base_path)
        
        return {"success": True}
    except Exception as e:
        logger.error(f"Error restoring backup: {str(e)}")
        return {"success": False, "error": str(e)}

# 注册路由
app.include_router(router)

if __name__ == "__main__":
    # 创建必要的目录
    os.makedirs(os.path.join(os.path.dirname(__file__), "data", "storage"), exist_ok=True)
    os.makedirs(os.path.join(os.path.dirname(__file__), "data", "backups"), exist_ok=True)
    
    # 启动服务
    uvicorn.run(
        "run:app",  # 使用模块路径
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

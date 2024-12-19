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

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# 创建应用实例
app = FastAPI(
    title="ObSave API",
    description="High-performance object storage service",
    version="1.0.0"
)

# 创建路由器
router = APIRouter(
    prefix="/objsave",
    tags=["object-storage"]
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
@router.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

# 对象存储端点
@router.post("/objects/{key}")
async def store_object(key: str, data: bytes):
    success = await storage.store(key, data)
    return {"success": success}

@router.get("/objects/{key}")
async def get_object(key: str):
    try:
        data = await storage.get(key)
        return {"data": data}
    except ObjectNotFoundError:
        return {"error": "Object not found"}

@router.delete("/objects/{key}")
async def delete_object(key: str):
    success = await storage.delete(key)
    return {"success": success}

# 文件上传端点
@router.post("/upload/file")
async def upload_file(file: UploadFile = File(...)):
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
@router.post("/upload/json")
async def upload_json(json_obj: JSONObject):
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

@router.post("/upload/json/batch")
async def upload_json_batch(objects: List[JSONObject]):
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
@router.get("/list")
async def list_objects(limit: int = 10, offset: int = 0):
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
@router.get("/stats")
async def get_stats():
    return storage.get_stats()

# 监控指标端点
@router.get("/metrics")
async def metrics():
    return await metrics_endpoint()

# 备份相关端点
@router.post("/backup/create")
async def create_backup(background_tasks: BackgroundTasks, name: Optional[str] = None):
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

@router.get("/backup/list")
async def list_backups():
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

@router.post("/backup/restore/{backup_name}")
async def restore_backup(backup_name: str):
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

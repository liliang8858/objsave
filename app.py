import os
import uuid
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel, ConfigDict
import json
from jsonpath_ng import parse
import logging
from datetime import datetime

from db import init_db, get_db, ObjectStorage

# 配置日志记录
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 创建FastAPI应用
app = FastAPI(title="对象存储服务", description="轻量级本地对象存储HTTP服务")

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有源
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有HTTP方法
    allow_headers=["*"],  # 允许所有请求头
)

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
    data: Dict[str, Any]
    name: Optional[str] = None
    content_type: str = "application/json"

# JSON 查询模型
class JSONQueryModel(BaseModel):
    jsonpath: str  # JSONPath查询表达式
    value: Optional[Any] = None  # 可选的精确匹配值
    operator: Optional[str] = 'eq'  # 比较运算符：eq, gt, lt, ge, le, contains

# JSON对象响应模型
class JSONObjectResponse(BaseModel):
    id: str
    name: str
    content_type: str
    size: int
    created_at: str
    data: Dict[str, Any]

    model_config = ConfigDict(from_attributes=True)

# 上传对象接口
@app.post("/upload", response_model=ObjectMetadata)
async def upload_object(
    file: UploadFile = File(...), 
    db: Session = Depends(get_db)
):
    """
    上传对象到存储服务
    
    - 接受任意类型和大小的文件
    - 使用UUID生成唯一标识符
    - 存储文件内容和元数据
    """
    try:
        # 读取文件内容
        content = await file.read()
        
        # 创建新的存储对象
        db_object = ObjectStorage(
            id=str(uuid.uuid4()),
            name=file.filename,
            content=content,
            content_type=file.content_type,
            size=len(content)
        )
        
        logger.debug(f"Uploading file: {file.filename} (size: {len(content)} bytes, type: {file.content_type})")
        
        # 保存到数据库
        db.add(db_object)
        db.commit()
        db.refresh(db_object)
        
        logger.info(f"Successfully uploaded file: {file.filename} with ID: {db_object.id}")
        
        return ObjectMetadata(
            id=db_object.id,
            name=db_object.name,
            content_type=db_object.content_type,
            size=db_object.size,
            created_at=str(db_object.created_at)
        )
    
    except Exception as e:
        logger.error(f"Failed to upload file {file.filename}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"上传失败: {str(e)}")

# 下载对象接口
@app.get("/download/{object_id}")
async def download_object(
    object_id: str, 
    db: Session = Depends(get_db)
):
    """
    根据对象ID下载文件
    
    - 返回完整的文件内容
    - 如果对象不存在，返回404错误
    """
    logger.debug(f"Attempting to download object with ID: {object_id}")
    
    db_object = db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
    
    if not db_object:
        logger.warning(f"Object not found with ID: {object_id}")
        raise HTTPException(status_code=404, detail="对象未找到")
    
    logger.info(f"Successfully retrieved object: {db_object.name} (ID: {object_id})")
    
    return {
        "file_name": db_object.name,
        "content_type": db_object.content_type,
        "content": db_object.content
    }

# 列出所有对象接口
@app.get("/list", response_model=List[ObjectMetadata])
async def list_objects(
    db: Session = Depends(get_db),
    limit: Optional[int] = 100,
    offset: Optional[int] = 0
):
    """
    列出存储的对象
    
    - 支持分页
    - 默认返回前100个对象
    """
    objects = db.query(ObjectStorage).offset(offset).limit(limit).all()
    
    return [
        ObjectMetadata(
            id=obj.id,
            name=obj.name,
            content_type=obj.content_type,
            size=obj.size,
            created_at=str(obj.created_at)
        ) for obj in objects
    ]

# 删除对象接口
@app.delete("/delete/{object_id}")
async def delete_object(
    object_id: str, 
    db: Session = Depends(get_db)
):
    """
    根据对象ID删除文件
    
    - 如果对象不存在，返回404错误
    """
    logger.debug(f"Attempting to delete object with ID: {object_id}")
    
    db_object = db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
    
    if not db_object:
        logger.warning(f"Object not found with ID: {object_id}")
        raise HTTPException(status_code=404, detail="对象未找到")
    
    object_name = db_object.name
    db.delete(db_object)
    db.commit()
    
    logger.info(f"Successfully deleted object: {object_name} (ID: {object_id})")
    
    return {"message": "对象删除成功"}

# JSON对象上传接口
@app.post("/upload/json", response_model=ObjectMetadata)
async def upload_json_object(
    json_data: JSONObjectModel, 
    db: Session = Depends(get_db)
):
    """
    上传JSON对象到存储服务
    
    - 使用UUID生成唯一标识符
    - 存储JSON内容和元数据
    """
    try:
        # 序列化JSON数据
        content = json.dumps(json_data.data).encode('utf-8')
        
        logger.debug(f"Uploading JSON object: {json_data.name or 'unnamed'} (size: {len(content)} bytes)")
        logger.debug(f"JSON content: {json_data.data}")
        
        # 创建新的存储对象
        db_object = ObjectStorage(
            id=str(uuid.uuid4()),
            name=json_data.name or "unnamed_json_object",
            content=content,
            content_type="application/json",
            size=len(content)
        )
        
        # 保存到数据库
        db.add(db_object)
        db.commit()
        db.refresh(db_object)
        
        logger.info(f"Successfully uploaded JSON object: {db_object.name} with ID: {db_object.id}")
        
        return ObjectMetadata(
            id=db_object.id,
            name=db_object.name,
            content_type=db_object.content_type,
            size=db_object.size,
            created_at=str(db_object.created_at)
        )
    except Exception as e:
        logger.error(f"Failed to upload JSON object: {str(e)}")
        raise HTTPException(status_code=422, detail=str(e))

# JSON对象批量上传接口
@app.post("/upload/json/batch", response_model=List[ObjectMetadata])
async def upload_json_objects_batch(
    json_objects: List[JSONObjectModel], 
    db: Session = Depends(get_db)
):
    """
    批量上传JSON对象到存储服务
    
    - 支持一次性上传多个JSON对象
    - 每个对象使用UUID生成唯一标识符
    """
    try:
        # 存储所有对象并收集元数据
        metadata_list = []
        
        for json_data in json_objects:
            # 序列化JSON数据
            content = json.dumps(json_data.data).encode('utf-8')
            
            # 创建新的存储对象
            db_object = ObjectStorage(
                id=str(uuid.uuid4()),
                name=json_data.name or "unnamed_json_object",
                content=content,
                content_type="application/json",
                size=len(content)
            )
            
            # 保存到数据库
            db.add(db_object)
            
            # 收集元数据
            metadata_list.append(ObjectMetadata(
                id=db_object.id,
                name=db_object.name,
                content_type=db_object.content_type,
                size=db_object.size,
                created_at=str(db_object.created_at)
            ))
        
        # 提交数据库事务
        db.commit()
        
        return metadata_list
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))

# JSON对象更新接口
@app.put("/update/json/{object_id}", response_model=ObjectMetadata)
async def update_json_object(
    object_id: str,
    json_data: JSONObjectModel, 
    db: Session = Depends(get_db)
):
    """
    更新指定ID的JSON对象
    
    - 根据对象ID更新JSON内容
    - 如果对象不存在，返回404错误
    """
    try:
        # 查找现有对象
        db_object = db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
        
        if not db_object:
            raise HTTPException(status_code=404, detail="对象未找到")
        
        # 序列化新的JSON数据
        content = json.dumps(json_data.data).encode('utf-8')
        
        # 更新对象
        db_object.content = content
        db_object.name = json_data.name or db_object.name
        db_object.size = len(content)
        
        # 提交更改
        db.commit()
        db.refresh(db_object)
        
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
@app.post("/query/json", response_model=List[JSONObjectResponse])
async def query_json_objects(
    query: JSONQueryModel, 
    db: Session = Depends(get_db),
    limit: Optional[int] = 100,
    offset: Optional[int] = 0
):
    """
    根据 JSONPath 查询和过滤 JSON 对象
    
    支持的操作:
    - 精确匹配 (eq)
    - 大于 (gt)
    - 小于 (lt)
    - 大于等于 (ge)
    - 小于等于 (le)
    - 包含 (contains)
    """
    try:
        logger.debug(f"Querying JSON objects with path: {query.jsonpath}, value: {query.value}, operator: {query.operator}")
        
        # 查询所有 JSON 对象
        json_objects = db.query(ObjectStorage).filter(
            ObjectStorage.content_type == "application/json"
        ).offset(offset).limit(limit).all()
        
        logger.debug(f"Found {len(json_objects)} JSON objects before filtering")
        
        # 使用 JSONPath 和条件过滤
        matched_objects = []
        jsonpath_expr = parse(query.jsonpath)
        
        for db_object in json_objects:
            # 解析 JSON 内容
            json_data = json.loads(db_object.content.decode('utf-8'))
            
            # 使用 JSONPath 查找匹配的值
            matches = [match.value for match in jsonpath_expr.find(json_data)]
            
            # 根据不同操作符进行过滤
            if matches:
                for match in matches:
                    if query.value is None or _apply_filter(match, query.value, query.operator):
                        matched_objects.append((db_object, json_data))
                        logger.debug(f"Matched object ID: {db_object.id}, name: {db_object.name}, match value: {match}")
                        break
        
        logger.info(f"Query returned {len(matched_objects)} matches")
        
        # 转换为响应模型
        return [
            JSONObjectResponse(
                id=obj.id,
                name=obj.name,
                content_type=obj.content_type,
                size=obj.size,
                created_at=str(obj.created_at),
                data=data
            ) for obj, data in matched_objects
        ]
    
    except Exception as e:
        logger.error(f"Error during JSON query: {str(e)}")
        raise HTTPException(status_code=422, detail=str(e))

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

# 启动服务器配置
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

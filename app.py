import os
import uuid
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pydantic import BaseModel, ConfigDict
import json
import jsonpath
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
    type: str
    content: Dict[str, Any]
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
    type: Optional[str] = None
    content: Dict[str, Any]

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
    
    response_data = {
        "file_name": db_object.name,
        "content_type": db_object.content_type,
        "content": db_object.content,
        "type": db_object.type
    }
    
    # 如果是JSON对象，添加type字段
    if db_object.content_type == "application/json":
        try:
            json_content = json.loads(db_object.content)
            if "type" in json_content:
                response_data["type"] = json_content["type"]
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse JSON content for object ID: {object_id}")
    
    return response_data

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
        content = json.dumps(json_data.content, ensure_ascii=False).encode('utf-8')
        
        logger.debug(f"Uploading JSON object: {json_data.name or 'unnamed'} (size: {len(content)} bytes)")
        logger.debug(f"JSON content: {json_data.content}")
        logger.debug(f"JSON type: {json_data.type}")
        
        # 创建新的存储对象
        db_object = ObjectStorage(
            id=json_data.id or str(uuid.uuid4()),
            name=json_data.name or f"json_object_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            content=content,
            content_type="application/json",
            type=json_data.type,
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
        db.add_all(db_objects)
        db.commit()
        
        for db_object in db_objects:
            metadata_list.append(ObjectMetadata(
                id=db_object.id,
                name=db_object.name,
                content_type=db_object.content_type,
                size=db_object.size,
                created_at=str(db_object.created_at)
            ))
        
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
        content = json.dumps(json_data.content).encode('utf-8')
        
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
    
    JSONPath 示例:
    - $.name                     (查询根级别的 name 字段)
    - $.address.city            (查询嵌套的 city 字段)
    - $.items[*]               (查询数组中的所有项)
    - $.*.name                 (查询任意层级下的 name 字段)
    - $.data[?(@.type=="idea")] (查询 data 数组中 type 为 "idea" 的项)
    """
    try:
        logger.debug(f"Querying JSON objects with path: {query.jsonpath}, value: {query.value}, operator: {query.operator}")
        
        # 验证 JSONPath 表达式
        if not validate_jsonpath(query.jsonpath):
            raise ValueError(f"Invalid JSONPath expression: {query.jsonpath}. JSONPath must start with '$'")
        
        # 查询所有 JSON 对象
        objects = db.query(ObjectStorage).filter(
            ObjectStorage.content_type == "application/json"
        ).offset(offset).limit(limit).all()
        
        logger.debug(f"Found {len(objects)} JSON objects before filtering")
        
        # 使用 JSONPath 和条件过滤
        matched_objects = []
        
        for obj in objects:
            try:
                # 解析JSON内容
                content = json.loads(obj.content)
                
                # 使用 JSONPath 查找匹配的值
                found_values = jsonpath.jsonpath(content, query.jsonpath)
                
                if found_values:
                    # 如果找到匹配项，应用过滤器
                    for value in found_values:
                        if query.value is None or _apply_filter(value, query.value, query.operator):
                            matched_objects.append((obj, content))
                            break
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
                type=content.get('type'),
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

# 启动服务器配置
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

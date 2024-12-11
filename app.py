import os
import uuid
from typing import List, Optional
from fastapi import FastAPI, File, UploadFile, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel, ConfigDict

from db import init_db, get_db, ObjectStorage

# 创建FastAPI应用
app = FastAPI(title="对象存储服务", description="轻量级本地对象存储HTTP服务")

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
        
        # 保存到数据库
        db.add(db_object)
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
    db_object = db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
    
    if not db_object:
        raise HTTPException(status_code=404, detail="对象未找到")
    
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
    db_object = db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
    
    if not db_object:
        raise HTTPException(status_code=404, detail="对象未找到")
    
    db.delete(db_object)
    db.commit()
    
    return {"message": "对象删除成功"}

# 启动服务器配置
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

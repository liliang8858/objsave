import sqlite3
import json
import os
import uuid
from sqlalchemy import Column, String, Integer, LargeBinary, DateTime, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timezone

DATABASE = 'object_storage.db'

# 确保数据库目录存在
os.makedirs('data', exist_ok=True)

# SQLite数据库路径
DATABASE_URL = 'sqlite:///./data/objects.db'

# 创建基础模型类
Base = declarative_base()

class ObjectStorage(Base):
    """对象存储模型"""
    __tablename__ = 'objects'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, index=True)
    content = Column(LargeBinary)
    content_type = Column(String)
    type = Column(String, nullable=True)
    size = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

# 创建数据库引擎
engine = create_engine(DATABASE_URL, connect_args={'check_same_thread': False})

# 创建会话工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建数据库表
def init_db():
    """初始化数据库"""
    Base.metadata.create_all(bind=engine)

def add_object(data):
    """存储 JSON 对象到数据库"""
    db = SessionLocal()
    obj = ObjectStorage(content=data)
    db.add(obj)
    db.commit()
    db.refresh(obj)
    db.close()
    return obj.id

def get_object(object_id):
    """根据 ID 查询 JSON 对象"""
    db = SessionLocal()
    obj = db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
    db.close()
    if obj:
        return obj.content
    return None

def get_all_objects():
    """获取所有存储的 JSON 对象"""
    db = SessionLocal()
    rows = db.query(ObjectStorage).all()
    db.close()
    return [{"id": row.id, "data": row.content, "created_at": row.created_at} for row in rows]

def get_objects_by_field(field, value):
    """根据 JSON 字段进行过滤查询"""
    db = SessionLocal()
    rows = db.query(ObjectStorage).filter(getattr(ObjectStorage, field) == value).all()
    db.close()
    return [{"id": row.id, "data": row.content, "created_at": row.created_at} for row in rows]

def update_object(object_id, new_data):
    """更新指定 ID 的 JSON 对象"""
    db = SessionLocal()
    obj = db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
    if obj:
        obj.content = new_data
        db.commit()
    db.close()

def delete_object(object_id):
    """删除指定 ID 的 JSON 对象"""
    db = SessionLocal()
    obj = db.query(ObjectStorage).filter(ObjectStorage.id == object_id).first()
    if obj:
        db.delete(obj)
        db.commit()
    db.close()

# 获取数据库会话
def get_db():
    """获取数据库会话"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

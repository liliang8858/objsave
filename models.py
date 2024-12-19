from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, Text
from sqlalchemy.orm import relationship
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, Any

from database import Base

class ObjectStorage(Base):
    """数据库存储对象模型"""
    __tablename__ = "objects"
    
    id = Column(String, primary_key=True)
    name = Column(String)
    content_type = Column(String)
    content = Column(Text)  # 使用Text类型存储JSON内容
    type = Column(String)   # 添加type字段
    size = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    owner = Column(String, nullable=True)
    
class ObjectMetadata(BaseModel):
    """对象元数据模型"""
    id: str
    name: str
    content_type: str
    size: int
    created_at: str
    type: Optional[str] = None
    owner: Optional[str] = None
    
class JSONObjectModel(BaseModel):
    """JSON对象模型"""
    id: Optional[str] = None
    name: Optional[str] = None
    content: Dict[str, Any]
    type: Optional[str] = None
    content_type: str = "application/json"

class User(Base):
    """用户模型"""
    __tablename__ = "users"
    
    id = Column(String, primary_key=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    role = Column(String, default="user")  # user, admin
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime, nullable=True)
    
class UserCreate(BaseModel):
    """用户创建模型"""
    username: str
    email: str
    password: str
    role: Optional[str] = "user"
    
class UserResponse(BaseModel):
    """用户响应模型"""
    id: str
    username: str
    email: str
    role: str
    created_at: datetime
    last_login: Optional[datetime]

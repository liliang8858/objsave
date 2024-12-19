from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, Text, JSON
from sqlalchemy.orm import relationship
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, Any
import uuid
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

from obsave.core.database import Base

class ObjectStorage(Base):
    """对象存储数据模型"""
    __tablename__ = 'object_storage'

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(255), nullable=False)
    content_type = Column(String(100), nullable=False)
    type = Column(String(50), nullable=True)
    size = Column(Integer, nullable=False)
    content = Column(Text, nullable=True)
    obj_metadata = Column(JSON, nullable=True)
    owner = Column(String(100), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<ObjectStorage(id={self.id}, name={self.name}, type={self.type})>"

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

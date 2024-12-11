import os
import io
import pytest
import logging
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from pydantic import BaseModel

# Configure logging
def pytest_configure(config):
    """Configure logging for pytest"""
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Log to console
            logging.FileHandler('test_app.log', encoding='utf-8')  # Log to file
        ]
    )

logger = logging.getLogger(__name__)

from app import app, get_db
from db import Base, ObjectStorage

# 对象元数据模型
class ObjectMetadata(BaseModel):
    id: str
    name: str
    content_type: str
    size: int
    created_at: str

    class Config:
        from_attributes = True  # 替换 orm_mode = True

# 创建测试数据库引擎
TEST_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    TEST_DATABASE_URL, 
    connect_args={"check_same_thread": False},
    poolclass=StaticPool
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 重写数据库依赖
def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

# 替换应用程序的数据库依赖
app.dependency_overrides[get_db] = override_get_db

# 创建测试客户端
client = TestClient(app)

def setup_module(module):
    """在所有测试之前创建数据库表"""
    logger.info("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created.")

def teardown_module(module):
    """在所有测试之后删除数据库表"""
    logger.info("Dropping database tables...")
    Base.metadata.drop_all(bind=engine)
    logger.info("Database tables dropped.")

def test_upload_file():
    """测试文件上传"""
    logger.info("Testing file upload...")
    test_file = io.BytesIO(b"test file content")
    test_file.name = "test.txt"
    
    response = client.post(
        "/upload", 
        files={"file": ("test.txt", test_file, "text/plain")}
    )
    
    logger.info("File uploaded. Checking response...")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "test.txt"
    assert data["content_type"] == "text/plain"
    assert data["size"] == len(b"test file content")
    
    # 保存对象ID以供后续测试使用
    global test_object_id
    test_object_id = data["id"]
    logger.info("File uploaded successfully.")

def test_download_file():
    """测试文件下载"""
    logger.info("Testing file download...")
    response = client.get(f"/download/{test_object_id}")
    
    logger.info("File downloaded. Checking response...")
    assert response.status_code == 200
    assert response.json()["file_name"] == "test.txt"
    assert response.json()["content_type"] == "text/plain"
    assert response.json()["content"] == "test file content"
    logger.info("File downloaded successfully.")

def test_list_objects():
    """测试列出对象"""
    logger.info("Testing list objects...")
    response = client.get("/list")
    
    logger.info("Objects listed. Checking response...")
    assert response.status_code == 200
    objects = response.json()
    assert len(objects) > 0
    assert objects[0]["name"] == "test.txt"
    logger.info("Objects listed successfully.")

def test_delete_object():
    """测试删除对象"""
    logger.info("Testing delete object...")
    response = client.delete(f"/delete/{test_object_id}")
    
    logger.info("Object deleted. Checking response...")
    assert response.status_code == 200
    assert response.json()["message"] == "对象删除成功"
    logger.info("Object deleted successfully.")

def test_download_nonexistent_object():
    """测试下载不存在的对象"""
    logger.info("Testing download nonexistent object...")
    response = client.get("/download/nonexistent_id")
    
    logger.info("Nonexistent object downloaded. Checking response...")
    assert response.status_code == 404
    assert "对象未找到" in response.json()["detail"]
    logger.info("Nonexistent object downloaded successfully.")

def test_upload_multiple_files():
    """测试上传多个文件"""
    logger.info("Testing upload multiple files...")
    files = [
        ("file", ("test1.txt", io.BytesIO(b"content1"), "text/plain")),
        ("file", ("test2.txt", io.BytesIO(b"content2"), "text/plain"))
    ]
    
    response = client.post("/upload", files=files)
    
    logger.info("Multiple files uploaded. Checking response...")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "test2.txt"  # 最后一个文件
    logger.info("Multiple files uploaded successfully.")

def test_list_paginated_objects():
    """测试分页列出对象"""
    logger.info("Testing list paginated objects...")
    # 上传多个对象
    for i in range(5):
        client.post(
            "/upload", 
            files={"file": (f"test{i}.txt", io.BytesIO(f"content{i}".encode()), "text/plain")}
        )
    
    # 测试分页
    response = client.get("/list?limit=3&offset=2")
    
    logger.info("Paginated objects listed. Checking response...")
    assert response.status_code == 200
    objects = response.json()
    assert len(objects) == 3
    logger.info("Paginated objects listed successfully.")

def test_upload_large_file():
    """测试上传大文件"""
    logger.info("Testing upload large file...")
    large_content = b"x" * (1024 * 1024)  # 1MB文件
    test_file = io.BytesIO(large_content)
    test_file.name = "large_file.bin"
    
    response = client.post(
        "/upload", 
        files={"file": ("large_file.bin", test_file, "application/octet-stream")}
    )
    
    logger.info("Large file uploaded. Checking response...")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "large_file.bin"
    assert data["size"] == len(large_content)
    logger.info("Large file uploaded successfully.")

# 可以添加更多测试用例...

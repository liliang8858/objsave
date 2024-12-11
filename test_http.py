import os
import io
import json
import pytest
import logging
import httpx
from typing import Dict, Any

# 配置日志
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # 控制台输出
        logging.FileHandler('test_http.log', encoding='utf-8')  # 文件日志
    ]
)
logger = logging.getLogger(__name__)

# 测试服务器配置
BASE_URL = "http://localhost:8000"

@pytest.fixture
def http_client():
    """创建 HTTP 客户端"""
    return httpx.Client(base_url=BASE_URL)

@pytest.fixture(scope="module")
def uploaded_file_id():
    return None

def test_file_upload(http_client, uploaded_file_id):
    """测试文件上传接口"""
    logger.info("开始测试文件上传接口...")
    
    # 准备测试文件
    test_file = {
        "file": ("test_upload.txt", "这是一个测试文件内容".encode('utf-8'), "text/plain")
    }
    
    # 发送上传请求
    response = http_client.post("/upload", files=test_file)
    
    logger.info(f"上传响应状态码: {response.status_code}")
    logger.info(f"上传响应内容: {response.json()}")
    
    assert response.status_code == 200
    upload_data = response.json()
    
    assert "id" in upload_data
    assert upload_data["name"] == "test_upload.txt"
    assert upload_data["content_type"] == "text/plain"
    
    # Update the uploaded_file_id fixture
    uploaded_file_id = upload_data["id"]
    
    return uploaded_file_id

def test_file_download(http_client, uploaded_file_id):
    """测试文件下载接口"""
    # 先上传文件，再下载
    file_id = test_file_upload(http_client, uploaded_file_id)

    logger.info(f"开始下载文件，文件ID: {file_id}")

    response = http_client.get(f"/download/{file_id}")

    logger.info(f"下载响应状态码: {response.status_code}")
    logger.info(f"下载响应内容: {response.json()}")

    assert response.status_code == 200
    download_data = response.json()
    
    assert download_data["file_name"] == "test_upload.txt"
    assert download_data["content"] == "这是一个测试文件内容"

def test_list_objects(http_client):
    """测试对象列表接口"""
    # 先上传几个文件
    test_file_upload(http_client, None)
    test_file_upload(http_client, None)
    
    logger.info("开始测试对象列表接口...")
    
    response = http_client.get("/list")
    
    logger.info(f"列表响应状态码: {response.status_code}")
    logger.info(f"对象列表: {response.json()}")
    
    assert response.status_code == 200
    objects = response.json()
    
    assert len(objects) >= 2
    for obj in objects:
        assert "id" in obj
        assert "name" in obj
        assert "content_type" in obj

def test_delete_object(http_client, uploaded_file_id):
    """测试对象删除接口"""
    # 先上传文件
    file_id = test_file_upload(http_client, uploaded_file_id)
    
    logger.info(f"开始删除文件，文件ID: {file_id}")
    
    response = http_client.delete(f"/delete/{file_id}")
    
    logger.info(f"删除响应状态码: {response.status_code}")
    logger.info(f"删除响应内容: {response.json()}")
    
    assert response.status_code == 200
    assert response.json()["message"] == "对象删除成功"
    
    # 验证文件是否真的被删除
    download_response = http_client.get(f"/download/{file_id}")
    assert download_response.status_code == 404

def test_large_file_upload(http_client):
    """测试大文件上传"""
    logger.info("开始测试大文件上传...")
    
    # 生成大文件（约 1MB）
    large_content = b"A" * (1024 * 1024)
    test_file = {
        "file": ("large_test_file.bin", large_content, "application/octet-stream")
    }
    
    response = http_client.post("/upload", files=test_file)
    
    logger.info(f"大文件上传响应状态码: {response.status_code}")
    logger.info(f"大文件上传响应内容: {response.json()}")
    
    assert response.status_code == 200
    upload_data = response.json()
    
    assert upload_data["size"] == len(large_content)
    assert upload_data["name"] == "large_test_file.bin"

def test_json_object_operations(http_client):
    """测试 JSON 对象的完整操作流程"""
    logger.info("开始测试 JSON 对象操作流程...")
    
    # 1. 上传 JSON 对象
    json_data = {
        "data": {
            "user": {
                "name": "Test User",
                "age": 30,
                "email": "test@example.com"
            },
            "tags": ["test", "example"]
        },
        "name": "test_user_profile"
    }
    
    upload_response = http_client.post("/upload/json", json=json_data)
    
    logger.info(f"JSON 对象上传响应状态码: {upload_response.status_code}")
    logger.info(f"JSON 对象上传响应内容: {upload_response.json()}")
    
    assert upload_response.status_code == 200
    upload_data = upload_response.json()
    object_id = upload_data["id"]
    
    # 2. 更新 JSON 对象
    update_data = {
        "data": {
            "user": {
                "name": "Updated User",
                "age": 31
            }
        },
        "name": "updated_user_profile"
    }
    
    update_response = http_client.put(f"/update/json/{object_id}", json=update_data)
    
    logger.info(f"JSON 对象更新响应状态码: {update_response.status_code}")
    logger.info(f"JSON 对象更新响应内容: {update_response.json()}")
    
    assert update_response.status_code == 200
    
    # 3. 查询 JSON 对象
    query_data = {
        "jsonpath": "$.user.age",
        "value": 31,
        "operator": "eq"
    }
    
    query_response = http_client.post("/query/json", json=query_data)
    
    logger.info(f"JSON 对象查询响应状态码: {query_response.status_code}")
    logger.info(f"JSON 对象查询响应内容: {query_response.json()}")
    
    assert query_response.status_code == 200
    query_results = query_response.json()
    assert len(query_results) > 0

def test_error_handling(http_client):
    """测试错误处理"""
    logger.info("开始测试错误处理...")
    
    # 1. 下载不存在的对象
    download_response = http_client.get("/download/nonexistent_id")
    
    logger.info(f"下载不存在对象响应状态码: {download_response.status_code}")
    logger.info(f"下载不存在对象响应内容: {download_response.json()}")
    
    assert download_response.status_code == 404
    
    # 2. 删除不存在的对象
    delete_response = http_client.delete("/delete/nonexistent_id")
    
    logger.info(f"删除不存在对象响应状态码: {delete_response.status_code}")
    logger.info(f"删除不存在对象响应内容: {delete_response.json()}")
    
    assert delete_response.status_code == 404

def test_pagination(http_client):
    """测试分页功能"""
    logger.info("开始测试分页功能...")
    
    # 上传多个对象
    for i in range(10):
        test_file = {
            "file": (f"test_file_{i}.txt", f"Content {i}".encode(), "text/plain")
        }
        http_client.post("/upload", files=test_file)
    
    # 测试限制返回数量
    response_limit = http_client.get("/list?limit=5")
    
    logger.info(f"分页限制响应状态码: {response_limit.status_code}")
    logger.info(f"分页限制响应内容: {response_limit.json()}")
    
    assert response_limit.status_code == 200
    assert len(response_limit.json()) == 5
    
    # 测试偏移量
    response_offset = http_client.get("/list?limit=5&offset=5")
    
    logger.info(f"分页偏移响应状态码: {response_offset.status_code}")
    logger.info(f"分页偏移响应内容: {response_offset.json()}")
    
    assert response_offset.status_code == 200
    assert len(response_offset.json()) == 5

# 可以添加更多测试用例...

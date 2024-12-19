"""ObSave 启动脚本"""

import uvicorn
from fastapi import FastAPI
from obsave.core.storage import ObjectStorage
from obsave.monitoring.middleware import PrometheusMiddleware
import logging
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 创建应用实例
app = FastAPI(
    title="ObSave API",
    description="High-performance object storage service",
    version="1.0.0"
)

# 添加 Prometheus 中间件
app.add_middleware(PrometheusMiddleware)

# 创建存储实例
storage = ObjectStorage(
    base_path=os.path.join(os.path.dirname(__file__), "data", "storage")
)

# 健康检查端点
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

# 对象存储端点
@app.post("/objects/{key}")
async def store_object(key: str, data: bytes):
    success = await storage.store(key, data)
    return {"success": success}

@app.get("/objects/{key}")
async def get_object(key: str):
    try:
        data = await storage.get(key)
        return {"data": data}
    except ObjectNotFoundError:
        return {"error": "Object not found"}

@app.delete("/objects/{key}")
async def delete_object(key: str):
    success = await storage.delete(key)
    return {"success": success}

@app.get("/stats")
async def get_stats():
    return storage.get_stats()

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

"""Example of using ObSave with Prometheus monitoring."""

from fastapi import FastAPI
from obsave.monitoring.middleware import PrometheusMiddleware
from obsave.monitoring.metrics import metrics_endpoint
from obsave.storage.manager import StorageManager
import uvicorn
import asyncio
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建 FastAPI 应用
app = FastAPI(title="ObSave Example")

# 添加 Prometheus 中间件
app.add_middleware(PrometheusMiddleware)

# 创建存储管理器
storage = StorageManager(base_path="./data")

# 添加 Prometheus 指标端点
@app.get("/metrics")
async def metrics():
    return await metrics_endpoint()

# 示例端点
@app.post("/objects/{key}")
async def store_object(key: str, data: bytes):
    success = await storage.set(key, data)
    return {"success": success}

@app.get("/objects/{key}")
async def get_object(key: str):
    data = await storage.get(key)
    if data is None:
        return {"error": "Object not found"}
    return {"data": data}

@app.delete("/objects/{key}")
async def delete_object(key: str):
    success = await storage.delete(key)
    return {"success": success}

@app.get("/stats")
async def get_stats():
    return storage.get_stats()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

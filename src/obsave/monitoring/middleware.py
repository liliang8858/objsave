"""Monitoring middleware for ObSave."""

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
import time
from .metrics import metrics_collector
import logging

logger = logging.getLogger(__name__)

class PrometheusMiddleware(BaseHTTPMiddleware):
    """Prometheus 监控中间件"""
    
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        
    async def dispatch(self, request: Request, call_next):
        # 排除 metrics 端点，避免重复计数
        if request.url.path == "/metrics":
            return await call_next(request)
            
        method = request.method
        path = request.url.path
        
        # 开始计时
        start_time = metrics_collector.track_request(method, path)
        
        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            status_code = 500
            raise
        finally:
            # 记录请求指标
            metrics_collector.track_request_end(start_time, method, path, status_code)
            
        return response

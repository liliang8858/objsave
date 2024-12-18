import time
from collections import defaultdict
from datetime import datetime
import logging
from typing import Dict, Optional
import asyncio
from threading import Lock

logger = logging.getLogger(__name__)

class TokenBucket:
    """令牌桶算法实现"""
    def __init__(self, capacity: int, fill_rate: float):
        self.capacity = capacity  # 桶的容量
        self.fill_rate = fill_rate  # 令牌填充速率
        self.tokens = capacity  # 当前令牌数量
        self.last_time = time.time()  # 上次更新时间
        self.lock = Lock()
        
    def _refill(self) -> None:
        """重新填充令牌"""
        now = time.time()
        delta = now - self.last_time
        new_tokens = delta * self.fill_rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_time = now
        
    def consume(self, tokens: int = 1) -> bool:
        """
        消耗令牌
        返回是否允许通过
        """
        with self.lock:
            self._refill()
            if tokens <= self.tokens:
                self.tokens -= tokens
                return True
            return False

class RateLimiter:
    """
    高性能限流器
    - 支持多种限流策略
    - 支持IP限流
    - 支持接口限流
    - 支持用户限流
    """
    def __init__(self):
        # IP限流
        self.ip_buckets: Dict[str, TokenBucket] = defaultdict(
            lambda: TokenBucket(capacity=100, fill_rate=10)  # 默认每IP每秒10个请求，最大突发100
        )
        
        # 接口限流
        self.endpoint_buckets: Dict[str, TokenBucket] = defaultdict(
            lambda: TokenBucket(capacity=1000, fill_rate=100)  # 默认每接口每秒100个请求，最大突发1000
        )
        
        # 全局限流
        self.global_bucket = TokenBucket(
            capacity=10000,  # 最大突发10000请求
            fill_rate=1000  # 每秒1000个请求
        )
        
        # 黑名单
        self.blacklist: Dict[str, float] = {}
        
        # 统计信息
        self.stats = {
            "total_requests": 0,
            "rejected_requests": 0,
            "blacklisted_ips": 0
        }
        self.stats_lock = Lock()
        
    async def is_allowed(self, ip: str, endpoint: str) -> bool:
        """检查请求是否允许通过"""
        try:
            # 更新统计信息
            with self.stats_lock:
                self.stats["total_requests"] += 1
            
            # 检查黑名单
            if ip in self.blacklist:
                if time.time() < self.blacklist[ip]:
                    logger.warning(f"Blocked request from blacklisted IP: {ip}")
                    with self.stats_lock:
                        self.stats["rejected_requests"] += 1
                    return False
                else:
                    # 黑名单过期，移除
                    del self.blacklist[ip]
            
            # 检查全局限流
            if not self.global_bucket.consume():
                logger.warning("Global rate limit exceeded")
                with self.stats_lock:
                    self.stats["rejected_requests"] += 1
                return False
            
            # 检查IP限流
            if not self.ip_buckets[ip].consume():
                logger.warning(f"IP rate limit exceeded for {ip}")
                self._maybe_blacklist(ip)
                with self.stats_lock:
                    self.stats["rejected_requests"] += 1
                return False
            
            # 检查接口限流
            if not self.endpoint_buckets[endpoint].consume():
                logger.warning(f"Endpoint rate limit exceeded for {endpoint}")
                with self.stats_lock:
                    self.stats["rejected_requests"] += 1
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Rate limiting error: {str(e)}")
            # 发生错误时，为安全起见拒绝请求
            return False
    
    def _maybe_blacklist(self, ip: str) -> None:
        """根据IP的请求模式决定是否加入黑名单"""
        bucket = self.ip_buckets[ip]
        if bucket.tokens < bucket.capacity * 0.1:  # 令牌数量低于10%
            # 暂时加入黑名单15分钟
            self.blacklist[ip] = time.time() + 900  # 15 * 60
            with self.stats_lock:
                self.stats["blacklisted_ips"] += 1
            logger.warning(f"IP {ip} has been blacklisted for 15 minutes due to excessive requests")
    
    def get_stats(self) -> Dict[str, int]:
        """获取限流统计信息"""
        with self.stats_lock:
            return {
                "total_requests": self.stats["total_requests"],
                "rejected_requests": self.stats["rejected_requests"],
                "blacklisted_ips": len(self.blacklist),
                "rejection_rate": f"{(self.stats['rejected_requests'] / self.stats['total_requests'] * 100):.2f}%" if self.stats['total_requests'] > 0 else "0%"
            }

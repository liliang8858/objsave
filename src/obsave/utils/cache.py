import time
from threading import Lock
from typing import Any, Dict, Optional
import logging
import sys

logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self, max_items: int = 10000, shards: int = 16, ttl: int = 3600):
        self.max_items = max_items
        self.shards = shards
        self.ttl = ttl
        self.items_per_shard = max_items // shards
        
        # 使用分片减少锁竞争
        self.cache_shards = [
            {
                "data": {},  # 实际数据
                "access_times": {},  # 访问时间
                "expire_times": {},  # 过期时间
                "lock": Lock()  # 分片锁
            }
            for _ in range(shards)
        ]
        
        # 统计信息
        self.stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0
        }
        self.stats_lock = Lock()
        
    def _get_shard(self, key: str) -> dict:
        """获取键对应的分片"""
        shard_index = hash(key) % self.shards
        return self.cache_shards[shard_index]
        
    def get(self, key: str) -> Any:
        """获取缓存项"""
        shard = self._get_shard(key)
        with shard["lock"]:
            if key in shard["data"]:
                # 检查是否过期
                if time.time() > shard["expire_times"].get(key, float("inf")):
                    self._remove_from_shard(shard, key)
                    with self.stats_lock:
                        self.stats["misses"] += 1
                    return None
                    
                # 更新访问时间
                shard["access_times"][key] = time.time()
                with self.stats_lock:
                    self.stats["hits"] += 1
                return shard["data"][key]
            
        with self.stats_lock:
            self.stats["misses"] += 1
        return None
        
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """设置缓存项"""
        shard = self._get_shard(key)
        with shard["lock"]:
            # 如果分片已满，执行LRU淘汰
            if len(shard["data"]) >= self.items_per_shard and key not in shard["data"]:
                self._evict_from_shard(shard)
                
            current_time = time.time()
            shard["data"][key] = value
            shard["access_times"][key] = current_time
            shard["expire_times"][key] = current_time + (ttl or self.ttl)
            
            # 检查分片使用率
            usage = len(shard["data"]) / self.items_per_shard
            if usage >= 0.9:  # 90%警告阈值
                logger.warning(f"Cache shard at {usage*100:.1f}% capacity ({len(shard['data'])}/{self.items_per_shard})")
                
        return True
        
    def _evict_from_shard(self, shard: dict) -> None:
        """从分片中淘汰最久未使用的项"""
        if not shard["data"]:
            return
            
        # 找出最久未访问的键
        oldest_key = min(
            shard["data"].keys(),
            key=lambda k: shard["access_times"].get(k, 0)
        )
        
        self._remove_from_shard(shard, oldest_key)
        with self.stats_lock:
            self.stats["evictions"] += 1
            
    def _remove_from_shard(self, shard: dict, key: str) -> None:
        """从分片中移除指定键"""
        shard["data"].pop(key, None)
        shard["access_times"].pop(key, None)
        shard["expire_times"].pop(key, None)
        
    def cleanup_expired(self) -> int:
        """清理所有过期的缓存项"""
        cleaned = 0
        current_time = time.time()
        
        for shard in self.cache_shards:
            with shard["lock"]:
                # 找出所有过期的键
                expired_keys = [
                    key for key, expire_time in shard["expire_times"].items()
                    if current_time > expire_time
                ]
                
                # 移除过期项
                for key in expired_keys:
                    self._remove_from_shard(shard, key)
                    cleaned += 1
                    
        return cleaned
        
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total_items = sum(len(shard["data"]) for shard in self.cache_shards)
        capacity_usage = (total_items / self.max_items) * 100
        
        with self.stats_lock:
            total_requests = self.stats["hits"] + self.stats["misses"]
            hit_ratio = self.stats["hits"] / total_requests if total_requests > 0 else 0
            
            return {
                "total_items": total_items,
                "capacity_usage": f"{capacity_usage:.1f}%",
                "max_items": self.max_items,
                "hits": self.stats["hits"],
                "misses": self.stats["misses"],
                "hit_ratio": hit_ratio,
                "evictions": self.stats["evictions"]
            }

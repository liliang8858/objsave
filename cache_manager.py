import time
from threading import Lock
from typing import Any, Dict, Optional
from datetime import datetime, timedelta
import logging
import sys

logger = logging.getLogger(__name__)

class CacheManager:
    """
    线程安全的内存缓存管理器
    支持TTL和LRU淘汰策略，内存限制
    """
    def __init__(self, max_size: int = 10000, default_ttl: int = 3600, max_memory_mb: int = 2048):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._lock = Lock()
        self._memory_usage = 0
        self._max_memory = max_memory_mb * 1024 * 1024  # 转换为字节
        self._hits = 0
        self._misses = 0
        
    def get(self, key: str) -> Optional[Any]:
        """获取缓存值，如果过期则删除并返回None"""
        with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
            
            cache_data = self._cache[key]
            if self._is_expired(cache_data['expire_at']):
                del self._cache[key]
                self._memory_usage -= cache_data.get('size', 0)
                self._misses += 1
                return None
                
            # 更新访问时间（LRU）
            cache_data['last_access'] = time.time()
            self._hits += 1
            return cache_data['value']
            
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存值"""
        with self._lock:
            # 估算对象大小
            try:
                value_size = sys.getsizeof(value)
                if isinstance(value, (str, bytes)):
                    value_size = len(value.encode('utf-8'))
            except:
                value_size = 1024  # 默认1KB
                
            # 检查内存限制
            if self._memory_usage + value_size > self._max_memory:
                self._evict_until_fit(value_size)
                
            # 检查缓存大小限制
            if len(self._cache) >= self._max_size:
                self._evict_cache()
                
            expire_at = datetime.now() + timedelta(seconds=ttl or self._default_ttl)
            
            # 如果键已存在，先减去旧值的大小
            if key in self._cache:
                self._memory_usage -= self._cache[key].get('size', 0)
                
            self._cache[key] = {
                'value': value,
                'expire_at': expire_at,
                'last_access': time.time(),
                'size': value_size
            }
            self._memory_usage += value_size
            
    def delete(self, key: str) -> None:
        """删除缓存值"""
        with self._lock:
            if key in self._cache:
                self._memory_usage -= self._cache[key].get('size', 0)
                del self._cache[key]
            
    def clear(self) -> None:
        """清空所有缓存"""
        with self._lock:
            self._cache.clear()
            self._memory_usage = 0
            
    def _is_expired(self, expire_at: datetime) -> bool:
        """检查是否过期"""
        return datetime.now() > expire_at
        
    def _evict_until_fit(self, required_size: int) -> None:
        """清理缓存直到有足够空间"""
        if required_size > self._max_memory:
            raise ValueError("单个值大小超过最大内存限制")
            
        items = sorted(
            self._cache.items(),
            key=lambda x: (self._is_expired(x[1]['expire_at']), x[1]['last_access'])
        )
        
        for key, item in items:
            if self._memory_usage + required_size <= self._max_memory:
                break
            self._memory_usage -= item.get('size', 0)
            del self._cache[key]
        
    def _evict_cache(self) -> None:
        """淘汰策略：优先清理过期键，其次使用LRU策略"""
        # 先清理过期的键
        current_time = datetime.now()
        expired_keys = [
            k for k, v in self._cache.items() 
            if self._is_expired(v['expire_at'])
        ]
        for k in expired_keys:
            self._memory_usage -= self._cache[k].get('size', 0)
            del self._cache[k]
            
        # 如果还是太多，使用LRU策略清理最旧的20%
        if len(self._cache) >= self._max_size:
            items = sorted(
                self._cache.items(), 
                key=lambda x: x[1]['last_access']
            )
            to_remove = int(len(items) * 0.2)
            for k, v in items[:to_remove]:
                self._memory_usage -= v.get('size', 0)
                del self._cache[k]
                
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        with self._lock:
            total = len(self._cache)
            expired = sum(1 for v in self._cache.values() if self._is_expired(v['expire_at']))
            return {
                'total_items': total,
                'expired_items': expired,
                'active_items': total - expired,
                'memory_usage_mb': self._memory_usage / (1024 * 1024),
                'max_memory_mb': self._max_memory / (1024 * 1024),
                'memory_usage_percent': (self._memory_usage / self._max_memory) * 100,
                'hits': self._hits,
                'misses': self._misses,
                'hit_ratio': self._hits / (self._hits + self._misses) if (self._hits + self._misses) > 0 else 0
            }

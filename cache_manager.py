import time
from threading import Lock
from typing import Any, Dict, Optional, List
from datetime import datetime, timedelta
import logging
import sys
from collections import OrderedDict

logger = logging.getLogger(__name__)

class LRUCache:
    """LRU缓存实现"""
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity
        
    def get(self, key: str) -> Any:
        if key not in self.cache:
            return None
        # 移动到末尾（最近使用）
        self.cache.move_to_end(key)
        return self.cache[key]
        
    def put(self, key: str, value: Any) -> None:
        if key in self.cache:
            # 如果已存在，移动到末尾
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            # 删除最久未使用的项
            self.cache.popitem(last=False)

class CacheManager:
    """
    高性能内存缓存管理器
    - 使用LRU算法
    - 支持TTL
    - 内存限制
    - 分片存储
    """
    def __init__(self, max_items: int = 10000, shards: int = 8):
        self._shards = [
            {
                'cache': LRUCache(max_items // shards),
                'lock': Lock(),
                'metadata': {}  # 存储TTL等元数据
            }
            for _ in range(shards)
        ]
        self._stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0
        }
        self._stats_lock = Lock()
        
    def _get_shard(self, key: str) -> dict:
        """获取key对应的分片"""
        shard_index = hash(key) % len(self._shards)
        return self._shards[shard_index]
        
    def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        shard = self._get_shard(key)
        with shard['lock']:
            value = shard['cache'].get(key)
            if value is None:
                with self._stats_lock:
                    self._stats['misses'] += 1
                return None
                
            metadata = shard['metadata'].get(key)
            if metadata and metadata['expire_at'] < datetime.now():
                # 已过期
                shard['cache'].cache.pop(key)
                shard['metadata'].pop(key)
                with self._stats_lock:
                    self._stats['evictions'] += 1
                    self._stats['misses'] += 1
                return None
                
            with self._stats_lock:
                self._stats['hits'] += 1
            return value
            
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存值"""
        shard = self._get_shard(key)
        with shard['lock']:
            shard['cache'].put(key, value)
            if ttl:
                shard['metadata'][key] = {
                    'expire_at': datetime.now() + timedelta(seconds=ttl)
                }
                
    def delete(self, key: str) -> None:
        """删除缓存值"""
        shard = self._get_shard(key)
        with shard['lock']:
            if key in shard['cache'].cache:
                shard['cache'].cache.pop(key)
                shard['metadata'].pop(key, None)
                
    def clear(self) -> None:
        """清空所有缓存"""
        for shard in self._shards:
            with shard['lock']:
                shard['cache'].cache.clear()
                shard['metadata'].clear()
                
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        with self._stats_lock:
            total_items = sum(len(shard['cache'].cache) for shard in self._shards)
            return {
                'total_items': total_items,
                'hits': self._stats['hits'],
                'misses': self._stats['misses'],
                'hit_ratio': self._stats['hits'] / (self._stats['hits'] + self._stats['misses']) if (self._stats['hits'] + self._stats['misses']) > 0 else 0,
                'evictions': self._stats['evictions']
            }

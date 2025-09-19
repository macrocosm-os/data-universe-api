"""
Redis utility functions with memory leak fixes
"""
import os
import redis
import time
from typing import Dict, Any, Optional
from collections import OrderedDict


class RedisClient:
    """Client for Redis operations with fallback to in-memory cache"""

    def __init__(self, redis_url: str = None, max_cache_size: int = 10000):
        """
        Initialize Redis client with fallback

        Args:
            redis_url: Redis connection URL
            max_cache_size: Maximum number of items in in-memory cache
        """
        self.redis_url = redis_url or os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        self.connected = False
        self.client = None
        self.max_cache_size = max_cache_size

        # In-memory fallback with LRU eviction and TTL
        self.cache = OrderedDict()  # For LRU eviction
        self.cache_ttl = {}  # Track expiry times
        self.counters = OrderedDict()  # For LRU eviction
        self.counter_ttl = {}  # Track expiry times

        # Try to connect
        self._connect()

    def _connect(self):
        """Attempt to connect to Redis"""
        try:
            self.client = redis.from_url(self.redis_url)
            self.client.ping()  # Test connection
            self.connected = True
            print("Connected to Redis successfully")
        except Exception as e:
            print(f"Redis connection failed: {str(e)}. Using in-memory fallback.")
            self.connected = False

    def _cleanup_expired(self, cache_dict: OrderedDict, ttl_dict: Dict):
        """Remove expired items from cache"""
        current_time = time.time()
        expired_keys = [k for k, expiry in ttl_dict.items() if expiry < current_time]
        for key in expired_keys:
            cache_dict.pop(key, None)
            ttl_dict.pop(key, None)

    def _evict_lru(self, cache_dict: OrderedDict, ttl_dict: Dict):
        """Evict oldest items if cache is full"""
        while len(cache_dict) >= self.max_cache_size:
            oldest_key = next(iter(cache_dict))
            cache_dict.pop(oldest_key, None)
            ttl_dict.pop(oldest_key, None)

    def get(self, key: str) -> Optional[str]:
        """Get a value from Redis with in-memory fallback"""
        if self.connected:
            try:
                return self.client.get(key)
            except Exception:
                print("Redis get failed, using in-memory fallback")

        # In-memory fallback with TTL check
        self._cleanup_expired(self.cache, self.cache_ttl)
        
        if key in self.cache:
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            return self.cache[key]
        
        return None

    def set(self, key: str, value: str, expire: int = None):
        """Set a value in Redis with in-memory fallback"""
        if self.connected:
            try:
                if expire:
                    self.client.setex(key, expire, value)
                else:
                    self.client.set(key, value)
                return
            except Exception:
                print("Redis set failed, using in-memory fallback")

        # In-memory fallback with size limits
        self._cleanup_expired(self.cache, self.cache_ttl)
        self._evict_lru(self.cache, self.cache_ttl)
        
        self.cache[key] = value
        if expire:
            self.cache_ttl[key] = time.time() + expire

    def delete(self, key: str):
        """Delete a key from Redis with in-memory fallback"""
        if self.connected:
            try:
                self.client.delete(key)
                return
            except Exception:
                print("Redis delete failed, using in-memory fallback")

        # In-memory fallback
        self.cache.pop(key, None)
        self.cache_ttl.pop(key, None)

    def get_counter(self, key: str) -> int:
        """Get a counter value with in-memory fallback"""
        if self.connected:
            try:
                value = self.client.get(key)
                return int(value) if value else 0
            except Exception:
                print("Redis get_counter failed, using in-memory fallback")

        # In-memory fallback with TTL check
        self._cleanup_expired(self.counters, self.counter_ttl)
        
        if key in self.counters:
            # Move to end (most recently used)
            self.counters.move_to_end(key)
            return self.counters[key]
        
        return 0

    def increment_counter(self, key: str, expire: int = 86400) -> int:
        """Increment a counter with in-memory fallback"""
        if self.connected:
            try:
                value = self.client.incr(key)
                # Set expiry if not already set
                if expire and self.client.ttl(key) == -1:
                    self.client.expire(key, expire)
                return value
            except Exception:
                print("Redis increment_counter failed, using in-memory fallback")

        # In-memory fallback with size limits
        self._cleanup_expired(self.counters, self.counter_ttl)
        self._evict_lru(self.counters, self.counter_ttl)
        
        current_value = self.counters.get(key, 0)
        self.counters[key] = current_value + 1
        
        # Set TTL for counter
        if expire:
            self.counter_ttl[key] = time.time() + expire
            
        return self.counters[key]

    def cleanup(self):
        """Manual cleanup method to remove expired items"""
        self._cleanup_expired(self.cache, self.cache_ttl)
        self._cleanup_expired(self.counters, self.counter_ttl)

    def get_cache_stats(self):
        """Get cache statistics for monitoring"""
        return {
            'cache_size': len(self.cache),
            'cache_max_size': self.max_cache_size,
            'counters_size': len(self.counters),
            'connected_to_redis': self.connected
        }
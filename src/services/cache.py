"""Cache Service using Redis"""

import json
from typing import Optional, Any, Dict
import structlog
import redis.asyncio as redis

from src.utils.config import settings

logger = structlog.get_logger()


class CacheService:
    """
    Redis-based caching service

    Features:
    - Async operations
    - Automatic JSON serialization
    - TTL support
    - Connection pooling
    """

    def __init__(self):
        self.redis = redis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True
        )
        self.default_ttl = 300  # 5 minutes

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get a value from cache

        Args:
            key: Cache key

        Returns:
            Cached value or None
        """
        try:
            value = await self.redis.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.warning("cache_get_error", key=key, error=str(e))
            return None

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set a value in cache

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time to live in seconds

        Returns:
            True if successful
        """
        try:
            serialized = json.dumps(value, default=str)
            await self.redis.set(
                key,
                serialized,
                ex=ttl or self.default_ttl
            )
            return True
        except Exception as e:
            logger.warning("cache_set_error", key=key, error=str(e))
            return False

    async def delete(self, key: str) -> bool:
        """Delete a key from cache"""
        try:
            await self.redis.delete(key)
            return True
        except Exception as e:
            logger.warning("cache_delete_error", key=key, error=str(e))
            return False

    async def increment(self, key: str, amount: int = 1) -> int:
        """Increment a counter"""
        try:
            return await self.redis.incrby(key, amount)
        except Exception as e:
            logger.warning("cache_increment_error", key=key, error=str(e))
            return 0

    async def get_many(self, keys: list) -> Dict[str, Any]:
        """Get multiple values at once"""
        try:
            values = await self.redis.mget(keys)
            return {
                key: json.loads(value) if value else None
                for key, value in zip(keys, values)
            }
        except Exception as e:
            logger.warning("cache_mget_error", error=str(e))
            return {}

    async def set_hash(self, name: str, mapping: Dict[str, Any]) -> bool:
        """Set a hash"""
        try:
            serialized = {k: json.dumps(v, default=str) for k, v in mapping.items()}
            await self.redis.hset(name, mapping=serialized)
            return True
        except Exception as e:
            logger.warning("cache_hset_error", name=name, error=str(e))
            return False

    async def get_hash(self, name: str) -> Dict[str, Any]:
        """Get all fields of a hash"""
        try:
            values = await self.redis.hgetall(name)
            return {k: json.loads(v) for k, v in values.items()}
        except Exception as e:
            logger.warning("cache_hgetall_error", name=name, error=str(e))
            return {}

    async def close(self):
        """Close Redis connection"""
        await self.redis.close()

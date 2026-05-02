from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis

_client: aioredis.Redis | None = None  # type: ignore[type-arg]


def init_redis(redis_url: str) -> None:
    global _client
    _client = aioredis.from_url(redis_url, decode_responses=True)


def get_redis() -> aioredis.Redis:  # type: ignore[type-arg]
    if _client is None:
        raise RuntimeError("Redis client not initialized. Call init_redis() first.")
    return _client


async def cache_get(key: str) -> Any | None:
    value = await get_redis().get(key)
    if value is None:
        return None
    return json.loads(value)


async def cache_set(key: str, value: Any, ttl: int = 60) -> None:
    await get_redis().setex(key, ttl, json.dumps(value, default=str))


async def cache_delete(key: str) -> None:
    await get_redis().delete(key)


async def acquire_lock(resource: str, ttl_seconds: int = 30) -> bool:
    """Try to acquire a distributed lock. Returns True if acquired."""
    result = await get_redis().set(f"lock:{resource}", "1", nx=True, ex=ttl_seconds)
    return result is True


async def release_lock(resource: str) -> None:
    await get_redis().delete(f"lock:{resource}")


async def close() -> None:
    if _client is not None:
        await _client.aclose()

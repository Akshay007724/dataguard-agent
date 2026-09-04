from __future__ import annotations

import json
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as aioredis

_client: aioredis.Redis | None = None


class LockAcquisitionError(Exception):
    """Raised when a distributed lock cannot be acquired."""


def init_redis(redis_url: str) -> None:
    global _client
    _client = aioredis.from_url(redis_url, decode_responses=True)


def get_redis() -> aioredis.Redis:
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


async def acquire_lock(resource: str, ttl_seconds: int = 30) -> str | None:
    """Try to acquire a distributed lock with a unique token.
    Returns the token string if acquired, None otherwise.
    """
    token = str(uuid.uuid4())
    result = await get_redis().set(f"lock:{resource}", token, nx=True, ex=ttl_seconds)
    return token if result is True else None


async def release_lock(resource: str, token: str | None = None) -> bool:
    """Release a distributed lock. If token is provided, atomically release only if token matches."""
    if token is None:
        await get_redis().delete(f"lock:{resource}")
        return True

    lua_script = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """
    res = await get_redis().eval(lua_script, 1, f"lock:{resource}", token)  # type: ignore[misc]
    return bool(res == 1)


@asynccontextmanager
async def distributed_lock(resource: str, ttl_seconds: int = 30) -> AsyncGenerator[str, None]:
    token = await acquire_lock(resource, ttl_seconds=ttl_seconds)
    if token is None:
        raise LockAcquisitionError(f"Could not acquire lock for resource '{resource}'")
    try:
        yield token
    finally:
        await release_lock(resource, token)


async def close() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None

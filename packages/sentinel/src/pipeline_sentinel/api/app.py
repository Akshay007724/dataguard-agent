from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from dataguard_core.logging import get_logger
from dataguard_core.store import postgres, redis
from pipeline_sentinel.config import settings
from pipeline_sentinel.mcp_server.server import _build_dependencies, _dispatch

log = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    postgres.init_engine(settings.database_url)
    redis.init_redis(settings.redis_url)
    await postgres.create_tables()

    adapters, tracer, detectors, llm = _build_dependencies()
    app.state.adapters = adapters
    app.state.tracer = tracer
    app.state.detectors = detectors
    app.state.llm = llm

    log.info("sentinel_api_started", host=settings.server_host, port=settings.server_port)
    yield

    for adapter in getattr(app.state, "adapters", []):
        with contextlib.suppress(Exception):
            await adapter.aclose()
    with contextlib.suppress(Exception):
        await app.state.tracer._client.aclose()
    await postgres.dispose_engine()
    await redis.close()
    log.info("sentinel_api_stopped")


def create_app() -> FastAPI:
    application = FastAPI(
        title="Pipeline Sentinel",
        description="MCP-native agentic triage for data pipeline failures",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )

    @application.get("/")
    async def root() -> dict[str, str]:
        return {
            "name": "DataGuard Agent — Pipeline Sentinel",
            "version": "0.1.0",
            "status": "healthy",
            "documentation": "/docs",
            "health_check": "/health",
            "metrics": "/metrics",
            "mcp_endpoint": "/mcp/call",
        }

    @application.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": "0.1.0"}

    @application.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> PlainTextResponse:
        return PlainTextResponse(
            content=generate_latest().decode("utf-8"),
            media_type=CONTENT_TYPE_LATEST,
        )

    @application.post("/mcp/call")
    @application.post("/api/mcp/call")
    async def mcp_call(request: Request) -> JSONResponse:
        """HTTP wrapper over MCP tool calls.

        Body: {"tool": "<name>", "arguments": {...}}
        """
        body: dict[str, Any] = await request.json()
        tool_name = body.get("tool", "")
        arguments = body.get("arguments", {})

        if not tool_name:
            raise HTTPException(status_code=400, detail="'tool' field is required")

        result_str = await _dispatch(
            name=tool_name,
            arguments=arguments,
            adapters=request.app.state.adapters,
            tracer=request.app.state.tracer,
            detectors=request.app.state.detectors,
            llm=request.app.state.llm,
        )

        return JSONResponse(content=json.loads(result_str))

    return application

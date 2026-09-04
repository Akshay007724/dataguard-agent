from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient
from typer.testing import CliRunner

from pipeline_sentinel.api.app import create_app
from pipeline_sentinel.cli.main import app as cli_app

runner = CliRunner()


@pytest.mark.asyncio
async def test_health_endpoint() -> None:
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_metrics_endpoint() -> None:
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        assert "sentinel" in resp.text


@pytest.mark.asyncio
async def test_mcp_call_endpoint() -> None:
    app = create_app()
    app.state.adapters = []
    app.state.tracer = None
    app.state.detectors = []
    app.state.llm = None

    with patch("pipeline_sentinel.api.app._dispatch", new=AsyncMock(return_value=json.dumps({"result": "ok"}))):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post("/mcp/call", json={"tool": "list_pipelines", "arguments": {}})
            assert resp.status_code == 200
            assert resp.json() == {"result": "ok"}


def test_cli_version() -> None:
    result = runner.invoke(cli_app, ["version"])
    assert result.exit_code == 0
    assert "pipeline-sentinel" in result.output

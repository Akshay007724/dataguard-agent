from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline_sentinel.detectors.schema_drift import SchemaDriftDetector
from pipeline_sentinel.mcp_server.server import _build_dependencies, _dispatch


class TestServerDispatch:
    @pytest.mark.asyncio
    async def test_dispatch_unknown_tool(self) -> None:
        res = await _dispatch("non_existent_tool", {}, [], MagicMock(), [], MagicMock())
        assert "Unknown tool" in res

    def test_build_dependencies(self) -> None:
        adapters, tracer, detectors, llm = _build_dependencies()
        assert len(adapters) == 2
        assert tracer is not None
        assert len(detectors) == 3
        assert llm is not None


class TestSchemaDriftDetectorFetch:
    @pytest.mark.asyncio
    async def test_schema_drift_with_marquez_client(self) -> None:
        mock_marquez = AsyncMock()
        mock_marquez.get_dataset.return_value = {
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "email", "type": "string"},
            ]
        }
        detector = SchemaDriftDetector("http://marquez:5000", marquez_client=mock_marquez)
        assert detector.name == "schema_drift"

        with (
            patch("pipeline_sentinel.detectors.schema_drift.redis.cache_get", return_value=None),
            patch("pipeline_sentinel.detectors.schema_drift.redis.cache_set", new=AsyncMock()),
        ):
            result = await detector.run("customer_data")
            assert result.passed is True
            assert len(result.checks) == 1
            assert result.checks[0].name == "schema_baseline"

    @pytest.mark.asyncio
    async def test_schema_drift_detected(self) -> None:
        mock_marquez = AsyncMock()
        mock_marquez.get_dataset.return_value = {
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "new_col", "type": "string"},
            ]
        }
        detector = SchemaDriftDetector("http://marquez:5000", marquez_client=mock_marquez)
        baseline = {"id": "int", "old_col": "string"}

        with patch("pipeline_sentinel.detectors.schema_drift.redis.cache_get", return_value=baseline):
            result = await detector.run("customer_data")
            assert result.passed is False
            failed_names = [c.name for c in result.failed_checks]
            assert "columns_removed" in failed_names
            assert "columns_added" in failed_names

    @pytest.mark.asyncio
    async def test_dispatch_all_routes(self) -> None:
        with (
            patch("pipeline_sentinel.mcp_server.server.handle_list_pipelines", new=AsyncMock(return_value="[]")),
            patch("pipeline_sentinel.mcp_server.server.handle_get_pipeline_status", new=AsyncMock(return_value="{}")),
            patch("pipeline_sentinel.mcp_server.server.handle_get_failure_details", new=AsyncMock(return_value="{}")),
            patch("pipeline_sentinel.mcp_server.server.handle_trace_lineage", new=AsyncMock(return_value="{}")),
            patch("pipeline_sentinel.mcp_server.server.handle_diagnose_failure", new=AsyncMock(return_value="{}")),
            patch("pipeline_sentinel.mcp_server.server.handle_propose_remediation", new=AsyncMock(return_value="{}")),
            patch("pipeline_sentinel.mcp_server.server.handle_check_data_quality", new=AsyncMock(return_value="{}")),
            patch("pipeline_sentinel.mcp_server.server.handle_get_recent_incidents", new=AsyncMock(return_value="{}")),
            patch("pipeline_sentinel.mcp_server.server.handle_file_incident", new=AsyncMock(return_value="{}")),
            patch("pipeline_sentinel.mcp_server.server.handle_execute_remediation", new=AsyncMock(return_value="{}")),
        ):
            for tool in [
                "list_pipelines",
                "get_pipeline_status",
                "get_failure_details",
                "trace_lineage",
                "diagnose_failure",
                "propose_remediation",
                "check_data_quality",
                "get_recent_incidents",
                "file_incident",
                "execute_remediation",
            ]:
                res = await _dispatch(tool, {}, [], MagicMock(), [], MagicMock())
                assert "error" not in res or res == "{}"

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from pipeline_sentinel.detectors.custom_sql import CustomSQLDetector
from pipeline_sentinel.detectors.freshness import FreshnessDetector
from pipeline_sentinel.lineage.openlineage import MarquezClient
from pipeline_sentinel.lineage.tracer import LineageTracer


class TestMarquezAndTracer:
    @pytest.mark.asyncio
    async def test_marquez_client_methods(self) -> None:
        mock_client = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"updatedAt": "2024-01-01T00:00:00Z"}
        mock_client.get.return_value = mock_resp

        marquez = MarquezClient("http://marquez:5000", namespace="default")
        marquez._client = mock_client

        data = await marquez.get_dataset("users")
        assert data == {"updatedAt": "2024-01-01T00:00:00Z"}

        tracer = LineageTracer(marquez)
        mock_resp.json.return_value = {"graph": []}
        graph = await tracer.trace(dataset_id="users")
        assert graph.root_id == "dataset:users"

        await marquez.aclose()
        mock_client.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_tracer_requires_args(self) -> None:
        marquez = MarquezClient("http://marquez:5000")
        tracer = LineageTracer(marquez)
        with pytest.raises(ValueError):
            await tracer.trace()


class TestDetectors:
    @pytest.mark.asyncio
    async def test_freshness_detector_pass(self) -> None:
        mock_marquez = AsyncMock()
        mock_marquez.get_dataset.return_value = {"updatedAt": datetime.now(UTC).isoformat()}

        detector = FreshnessDetector("http://marquez:5000", marquez_client=mock_marquez)
        assert detector.name == "freshness"

        result = await detector.run("my_dataset")
        assert result.passed is True
        assert len(result.checks) == 1

    @pytest.mark.asyncio
    async def test_custom_sql_detector(self) -> None:
        detector = CustomSQLDetector("SELECT 1")
        assert detector.name == "custom_sql"
        res = await detector.run("ds")
        assert res.passed is True

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataguard_agents.watchdog import WatchdogAgent, _TRIAGE_DEBOUNCE_KEY, _TRIAGE_DEBOUNCE_TTL


def _make_ctx(adapters: list[Any] | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.adapters = adapters or []
    return ctx


def _make_pipeline(pid: str) -> MagicMock:
    p = MagicMock()
    p.id = pid
    return p


def _make_adapter(pipeline_ids: list[str]) -> MagicMock:
    from dataguard_adapters.base import RunStatus  # noqa: PLC0415

    adapter = MagicMock()
    adapter.orchestrator_name = "airflow"
    adapter.list_pipelines = AsyncMock(return_value=[_make_pipeline(pid) for pid in pipeline_ids])
    return adapter


def _make_report(incidents: list[str] | None = None) -> MagicMock:
    report = MagicMock()
    report.incidents_filed = incidents or []
    return report


class TestWatchdogDebounce:
    @pytest.mark.asyncio
    async def test_debounced_pipeline_skipped(self) -> None:
        adapter = _make_adapter(["pipe-a"])
        ctx = _make_ctx([adapter])
        watchdog = WatchdogAgent(ctx)

        with (
            patch("dataguard_agents.watchdog.redis.cache_get", new=AsyncMock(return_value={"triaged_at": "2024-01-01"})),
            patch("dataguard_agents.watchdog.redis.cache_set", new=AsyncMock()) as mock_set,
            patch("dataguard_agents.watchdog.TriageAgent") as MockAgent,
        ):
            reports = await watchdog.run_once()

        assert reports == []
        MockAgent.return_value.run.assert_not_called()
        mock_set.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_debounced_pipeline_triaged(self) -> None:
        adapter = _make_adapter(["pipe-b"])
        ctx = _make_ctx([adapter])
        watchdog = WatchdogAgent(ctx)
        expected_report = _make_report(["INC-1"])

        with (
            patch("dataguard_agents.watchdog.redis.cache_get", new=AsyncMock(return_value=None)),
            patch("dataguard_agents.watchdog.redis.cache_set", new=AsyncMock()) as mock_set,
            patch("dataguard_agents.watchdog.TriageAgent") as MockAgent,
        ):
            MockAgent.return_value.run = AsyncMock(return_value=expected_report)
            reports = await watchdog.run_once()

        assert len(reports) == 1
        assert reports[0] is expected_report
        MockAgent.return_value.run.assert_awaited_once_with(scope="pipe-b")
        mock_set.assert_awaited_once_with(
            f"{_TRIAGE_DEBOUNCE_KEY}pipe-b",
            pytest.approx({"triaged_at": mock_set.call_args[0][1]["triaged_at"]}),
            ttl=_TRIAGE_DEBOUNCE_TTL,
        )

    @pytest.mark.asyncio
    async def test_mixed_debounced_and_new(self) -> None:
        adapter = _make_adapter(["pipe-a", "pipe-b"])
        ctx = _make_ctx([adapter])
        watchdog = WatchdogAgent(ctx)

        async def fake_cache_get(key: str) -> Any:
            return {"triaged_at": "x"} if "pipe-a" in key else None

        with (
            patch("dataguard_agents.watchdog.redis.cache_get", new=fake_cache_get),
            patch("dataguard_agents.watchdog.redis.cache_set", new=AsyncMock()),
            patch("dataguard_agents.watchdog.TriageAgent") as MockAgent,
        ):
            MockAgent.return_value.run = AsyncMock(return_value=_make_report())
            reports = await watchdog.run_once()

        assert len(reports) == 1
        MockAgent.return_value.run.assert_awaited_once_with(scope="pipe-b")

    @pytest.mark.asyncio
    async def test_no_failing_pipelines_returns_empty(self) -> None:
        adapter = _make_adapter([])
        ctx = _make_ctx([adapter])
        watchdog = WatchdogAgent(ctx)

        with patch("dataguard_agents.watchdog.TriageAgent") as MockAgent:
            reports = await watchdog.run_once()

        assert reports == []
        MockAgent.return_value.run.assert_not_called()

    @pytest.mark.asyncio
    async def test_adapter_error_continues(self) -> None:
        bad_adapter = MagicMock()
        bad_adapter.orchestrator_name = "broken"
        bad_adapter.list_pipelines = AsyncMock(side_effect=RuntimeError("timeout"))

        good_adapter = _make_adapter(["pipe-c"])
        ctx = _make_ctx([bad_adapter, good_adapter])
        watchdog = WatchdogAgent(ctx)

        with (
            patch("dataguard_agents.watchdog.redis.cache_get", new=AsyncMock(return_value=None)),
            patch("dataguard_agents.watchdog.redis.cache_set", new=AsyncMock()),
            patch("dataguard_agents.watchdog.TriageAgent") as MockAgent,
        ):
            MockAgent.return_value.run = AsyncMock(return_value=_make_report())
            reports = await watchdog.run_once()

        assert len(reports) == 1

    @pytest.mark.asyncio
    async def test_triage_error_continues_to_next(self) -> None:
        adapter = _make_adapter(["pipe-x", "pipe-y"])
        ctx = _make_ctx([adapter])
        watchdog = WatchdogAgent(ctx)
        good_report = _make_report(["INC-99"])

        async def run_side_effect(scope: str) -> Any:
            if scope == "pipe-x":
                raise RuntimeError("triage exploded")
            return good_report

        with (
            patch("dataguard_agents.watchdog.redis.cache_get", new=AsyncMock(return_value=None)),
            patch("dataguard_agents.watchdog.redis.cache_set", new=AsyncMock()),
            patch("dataguard_agents.watchdog.TriageAgent") as MockAgent,
        ):
            MockAgent.return_value.run = AsyncMock(side_effect=run_side_effect)
            reports = await watchdog.run_once()

        assert len(reports) == 1
        assert reports[0] is good_report

    @pytest.mark.asyncio
    async def test_mark_debounced_uses_correct_ttl(self) -> None:
        adapter = _make_adapter(["pipe-d"])
        ctx = _make_ctx([adapter])
        custom_ttl = 7200
        watchdog = WatchdogAgent(ctx, debounce_ttl=custom_ttl)

        with (
            patch("dataguard_agents.watchdog.redis.cache_get", new=AsyncMock(return_value=None)),
            patch("dataguard_agents.watchdog.redis.cache_set", new=AsyncMock()) as mock_set,
            patch("dataguard_agents.watchdog.TriageAgent") as MockAgent,
        ):
            MockAgent.return_value.run = AsyncMock(return_value=_make_report())
            await watchdog.run_once()

        _, kwargs = mock_set.call_args
        assert kwargs["ttl"] == custom_ttl

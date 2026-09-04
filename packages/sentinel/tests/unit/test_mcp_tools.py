from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataguard_adapters.base import PipelineSummary, RunDetails, RunStatus
from dataguard_core.store.postgres import IncidentRow, RemediationPlanRow
from pipeline_sentinel.config import settings
from pipeline_sentinel.mcp_server.tools.diagnosis import handle_diagnose_failure
from pipeline_sentinel.mcp_server.tools.incidents import handle_file_incident, handle_get_recent_incidents
from pipeline_sentinel.mcp_server.tools.pipelines import (
    handle_get_failure_details,
    handle_get_pipeline_status,
    handle_list_pipelines,
)
from pipeline_sentinel.mcp_server.tools.quality import (
    handle_check_data_quality,
    handle_trace_lineage,
)
from pipeline_sentinel.mcp_server.tools.remediation import (
    handle_execute_remediation,
    handle_propose_remediation,
)


class TestPipelineTools:
    @pytest.mark.asyncio
    async def test_handle_list_pipelines(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.orchestrator_name = "airflow"
        mock_adapter.list_pipelines = AsyncMock(return_value=[])

        with (
            patch("pipeline_sentinel.mcp_server.tools.pipelines.redis.cache_get", return_value=None),
            patch("pipeline_sentinel.mcp_server.tools.pipelines.redis.cache_set", new=AsyncMock()),
        ):
            result_str = await handle_list_pipelines([mock_adapter], {})
            data = json.loads(result_str)
            assert isinstance(data, list)

    @pytest.mark.asyncio
    async def test_handle_get_pipeline_status(self) -> None:
        mock_adapter = MagicMock()
        mock_summary = PipelineSummary(
            id="dag-1",
            name="dag-1",
            orchestrator="airflow",
            owner=None,
            tags=[],
            last_run_status=None,
            last_run_at=None,
            schedule=None,
            is_paused=False,
        )
        mock_adapter.get_pipeline = AsyncMock(return_value=mock_summary)
        mock_adapter.get_run_history = AsyncMock(return_value=[])

        with (
            patch("pipeline_sentinel.mcp_server.tools.pipelines.redis.cache_get", return_value=None),
            patch("pipeline_sentinel.mcp_server.tools.pipelines.redis.cache_set", new=AsyncMock()),
        ):
            result_str = await handle_get_pipeline_status([mock_adapter], {"pipeline_id": "dag-1"})
            data = json.loads(result_str)
            assert data["id"] == "dag-1"

    @pytest.mark.asyncio
    async def test_handle_get_failure_details(self) -> None:
        mock_adapter = MagicMock()
        mock_run = RunDetails(
            run_id="r1",
            pipeline_id="dag-1",
            status=RunStatus.FAILED,
            started_at=None,
            ended_at=None,
            duration_seconds=10.0,
            error_message="OOM killed",
            failing_task="task_a",
            retry_number=0,
        )
        mock_adapter.get_latest_run = AsyncMock(return_value=mock_run)
        mock_adapter.get_run_logs = AsyncMock(return_value="Killed python process")
        mock_adapter.get_run_history = AsyncMock(return_value=[mock_run])

        result_str = await handle_get_failure_details([mock_adapter], {"pipeline_id": "dag-1"})
        data = json.loads(result_str)
        assert data["pipeline_id"] == "dag-1"
        assert data["failing_task"] == "task_a"


class TestIncidentTools:
    @pytest.mark.asyncio
    async def test_file_and_get_incidents(self) -> None:
        row = IncidentRow(
            id="INC-123",
            title="Pipeline broke",
            pipeline_id="pipe-1",
            severity="high",
            status="open",
            description="desc",
        )
        with (
            patch("pipeline_sentinel.mcp_server.tools.incidents._incident_repo.file_incident", return_value=row),
            patch(
                "pipeline_sentinel.mcp_server.tools.incidents._incident_repo.get_recent_incidents", return_value=[row]
            ),
        ):
            res_file = await handle_file_incident(
                {
                    "title": "Pipeline broke",
                    "pipeline_id": "pipe-1",
                    "severity": "high",
                    "description": "desc",
                }
            )
            assert "INC-123" in res_file

            res_get = await handle_get_recent_incidents({"time_window": "24h"})
            data = json.loads(res_get)
            assert data["total"] == 1


class TestRemediationTools:
    @pytest.mark.asyncio
    async def test_propose_and_execute_remediation(self) -> None:
        with (
            patch("pipeline_sentinel.mcp_server.tools.remediation._remediation_repo.save_plan", new=AsyncMock()),
            patch("pipeline_sentinel.mcp_server.tools.remediation._remediation_repo.get_plan") as mock_get_plan,
            patch("pipeline_sentinel.mcp_server.tools.remediation._audit_repo.record_audit", new=AsyncMock()),
        ):
            res = await handle_propose_remediation(
                {
                    "pipeline_id": "pipe-1",
                    "diagnosis_id": "diag-1",
                    "root_cause_category": "oom",
                }
            )
            data = json.loads(res)
            rem_id = data["remediation_id"]

            plan = RemediationPlanRow(
                id=rem_id,
                diagnosis_id="diag-1",
                pipeline_id="pipe-1",
                steps_json="[]",
                risk_level="low",
                rollback_plan="rollback",
            )
            mock_get_plan.return_value = plan

            # Test gate: confirm missing
            fail_confirm = await handle_execute_remediation({"remediation_id": rem_id})
            assert "confirm must be true" in fail_confirm

            # Test gate: approver missing
            fail_approver = await handle_execute_remediation({"remediation_id": rem_id, "confirm": True})
            assert "approver_id is required" in fail_approver

            # Test successful execution when enabled
            with patch.object(settings, "auto_remediation_enabled", True):
                success = await handle_execute_remediation(
                    {
                        "remediation_id": rem_id,
                        "confirm": True,
                        "approver_id": "eng@corp.com",
                    }
                )
                data_exec = json.loads(success)
                assert data_exec["status"] == "queued"


class TestDiagnosisTool:
    @pytest.mark.asyncio
    async def test_diagnose_lock_busy(self) -> None:
        with patch("pipeline_sentinel.mcp_server.tools.diagnosis.redis.acquire_lock", return_value=None):
            res = await handle_diagnose_failure([], MagicMock(), [], MagicMock(), {"pipeline_id": "pipe-busy"})
            assert "concurrent_diagnosis_in_progress" in res

    @pytest.mark.asyncio
    async def test_diagnose_deterministic_hit(self) -> None:
        mock_adapter = MagicMock()
        mock_run = RunDetails(
            run_id="r1",
            pipeline_id="pipe-oom",
            status=RunStatus.FAILED,
            started_at=None,
            ended_at=None,
            duration_seconds=5.0,
            error_message="Process OOMKilled",
            failing_task="task1",
            retry_number=0,
        )
        mock_adapter.get_latest_run = AsyncMock(return_value=mock_run)
        mock_adapter.get_run_logs = AsyncMock(return_value="out of memory")

        with (
            patch("pipeline_sentinel.mcp_server.tools.diagnosis.redis.acquire_lock", return_value="token-123"),
            patch("pipeline_sentinel.mcp_server.tools.diagnosis.redis.release_lock", new=AsyncMock()),
        ):
            res = await handle_diagnose_failure(
                [mock_adapter], MagicMock(), [], MagicMock(), {"pipeline_id": "pipe-oom"}
            )
            data = json.loads(res)
            assert data["root_cause_category"] == "oom"
            assert data["llm_used"] is False


class TestQualityAndLineageTools:
    @pytest.mark.asyncio
    async def test_check_data_quality(self) -> None:
        mock_detector = MagicMock()
        mock_detector.name = "schema_drift"
        mock_result = MagicMock()
        mock_result.detector = "schema_drift"
        mock_result.dataset_id = "users_table"
        mock_result.passed = True
        mock_result.highest_severity = None
        mock_result.checks = []
        mock_detector.run = AsyncMock(return_value=mock_result)

        res = await handle_check_data_quality([mock_detector], {"dataset_id": "users_table"})
        data = json.loads(res)
        assert data["dataset_id"] == "users_table"
        assert data["overall_passed"] is True

    @pytest.mark.asyncio
    async def test_trace_lineage(self) -> None:
        mock_tracer = MagicMock()
        mock_graph = MagicMock()
        mock_graph.root_id = "dataset:users"
        mock_graph.nodes = []
        mock_graph.edges = []
        mock_tracer.trace = AsyncMock(return_value=mock_graph)

        res = await handle_trace_lineage(mock_tracer, {"dataset_id": "users"})
        data = json.loads(res)
        assert data["root_id"] == "dataset:users"

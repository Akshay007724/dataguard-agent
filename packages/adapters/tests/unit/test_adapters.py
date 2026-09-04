from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dataguard_adapters.airflow import AirflowAdapter
from dataguard_adapters.argo import ArgoAdapter
from dataguard_adapters.base import PipelineNotFoundError, RunStatus
from dataguard_adapters.dagster import DagsterAdapter
from dataguard_adapters.prefect import PrefectAdapter
from dataguard_adapters.registry import AdapterRegistry


class TestAirflowAdapter:
    @pytest.mark.asyncio
    async def test_list_pipelines(self) -> None:
        mock_client = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "dags": [
                {
                    "dag_id": "dag_1",
                    "owners": ["data-eng"],
                    "tags": [{"name": "core"}],
                    "last_dag_run_state": "success",
                    "last_run": "2024-01-01T00:00:00Z",
                    "schedule_interval": "@daily",
                    "is_paused": False,
                }
            ]
        }
        mock_client.get.return_value = mock_resp

        adapter = AirflowAdapter("http://airflow:8080", "admin", "admin", client=mock_client)
        assert adapter.orchestrator_name == "airflow"

        pipelines = await adapter.list_pipelines()
        assert len(pipelines) == 1
        assert pipelines[0].id == "dag_1"
        assert pipelines[0].last_run_status == RunStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_get_pipeline_not_found(self) -> None:
        mock_client = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_client.get.return_value = mock_resp

        adapter = AirflowAdapter("http://airflow:8080", "admin", "admin", client=mock_client)
        with pytest.raises(PipelineNotFoundError):
            await adapter.get_pipeline("missing_dag")

    @pytest.mark.asyncio
    async def test_get_run_and_logs(self) -> None:
        mock_client = AsyncMock()
        mock_run_resp = MagicMock()
        mock_run_resp.status_code = 200
        mock_run_resp.json.return_value = {
            "dag_run_id": "run_1",
            "state": "failed",
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": "2024-01-01T00:05:00Z",
        }
        mock_task_resp = MagicMock()
        mock_task_resp.status_code = 200
        mock_task_resp.json.return_value = {"task_instances": [{"task_id": "load_task"}]}

        mock_log_resp = MagicMock()
        mock_log_resp.status_code = 200
        mock_log_resp.text = "Error: Out of memory"

        mock_client.get.side_effect = [mock_run_resp, mock_task_resp, mock_log_resp]

        adapter = AirflowAdapter("http://airflow:8080", "admin", "admin", client=mock_client)
        run = await adapter.get_run("dag_1", "run_1")
        assert run.run_id == "run_1"
        assert run.status == RunStatus.FAILED
        assert run.failing_task == "load_task"

        logs = await adapter.get_run_logs("dag_1", "run_1", task_id="load_task")
        assert "Error: Out of memory" in logs

        await adapter.aclose()
        mock_client.aclose.assert_called_once()


class TestArgoAdapter:
    @pytest.mark.asyncio
    async def test_list_pipelines(self) -> None:
        mock_client = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "items": [
                {
                    "metadata": {"name": "wf-1", "labels": {"owner": "platform", "tag_env": "prod"}},
                    "status": {"phase": "Succeeded", "startedAt": "2024-01-01T00:00:00Z"},
                }
            ]
        }
        mock_client.get.return_value = mock_resp

        adapter = ArgoAdapter("https://argo.corp", client=mock_client)
        assert adapter.orchestrator_name == "argo"

        pipelines = await adapter.list_pipelines()
        assert len(pipelines) == 1
        assert pipelines[0].id == "wf-1"
        assert pipelines[0].last_run_status == RunStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_get_pipeline_not_found(self) -> None:
        mock_client = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_client.get.return_value = mock_resp

        adapter = ArgoAdapter("https://argo.corp", client=mock_client)
        with pytest.raises(PipelineNotFoundError):
            await adapter.get_pipeline("missing_wf")

    @pytest.mark.asyncio
    async def test_get_run_and_logs(self) -> None:
        mock_client = AsyncMock()
        mock_run_resp = MagicMock()
        mock_run_resp.status_code = 200
        mock_run_resp.json.return_value = {
            "metadata": {"name": "wf-fail-1"},
            "status": {
                "phase": "Failed",
                "startedAt": "2024-01-01T00:00:00Z",
                "finishedAt": "2024-01-01T00:02:00Z",
                "message": "Pod OOMKilled",
                "nodes": {"node-1": {"name": "step-calc", "phase": "Failed", "type": "Pod"}},
            },
        }
        mock_log_resp = MagicMock()
        mock_log_resp.status_code = 200
        mock_log_resp.text = "Traceback: Killed"

        mock_client.get.side_effect = [mock_run_resp, mock_log_resp]

        adapter = ArgoAdapter("https://argo.corp", client=mock_client)
        run = await adapter.get_run("wf-fail-1", "wf-fail-1")
        assert run.status == RunStatus.FAILED
        assert run.failing_task == "step-calc"

        logs = await adapter.get_run_logs("wf-fail-1", "wf-fail-1")
        assert "Traceback: Killed" in logs

        await adapter.aclose()
        mock_client.aclose.assert_called_once()


class TestAdapterRegistry:
    @pytest.mark.asyncio
    async def test_registry_operations(self) -> None:
        mock_airflow = MagicMock()
        mock_airflow.orchestrator_name = "airflow"
        mock_airflow.list_pipelines = AsyncMock(return_value=[])
        mock_airflow.aclose = AsyncMock()

        registry = AdapterRegistry([mock_airflow])
        assert registry.get("airflow") == mock_airflow
        assert registry.get("unknown") is None
        assert len(registry.list_adapters()) == 1

        await registry.list_all_pipelines()
        mock_airflow.list_pipelines.assert_called_once()

        await registry.aclose()
        mock_airflow.aclose.assert_called_once()


def test_stubs_raise_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        DagsterAdapter()

    with pytest.raises(NotImplementedError):
        PrefectAdapter()


class TestMoreAdapterCoverage:
    @pytest.mark.asyncio
    async def test_airflow_get_latest_run(self) -> None:
        mock_client = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "dag_runs": [{"dag_run_id": "r_latest", "state": "success", "start_date": None, "end_date": None}]
        }
        mock_client.get.return_value = mock_resp
        adapter = AirflowAdapter("http://airflow:8080", "admin", "admin", client=mock_client)
        latest = await adapter.get_latest_run("dag_1")
        assert latest.run_id == "r_latest"

    @pytest.mark.asyncio
    async def test_argo_get_latest_run(self) -> None:
        mock_client = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"items": [{"metadata": {"name": "wf_latest"}, "status": {"phase": "Succeeded"}}]}
        mock_client.get.return_value = mock_resp
        adapter = ArgoAdapter("https://argo.corp", client=mock_client)
        latest = await adapter.get_latest_run("wf_1")
        assert latest.run_id == "wf_latest"

    @pytest.mark.asyncio
    async def test_find_adapter_for_pipeline(self) -> None:
        mock_airflow = MagicMock()
        mock_airflow.orchestrator_name = "airflow"
        mock_airflow.get_pipeline = AsyncMock(side_effect=PipelineNotFoundError("not found"))

        mock_argo = MagicMock()
        mock_argo.orchestrator_name = "argo"
        mock_argo.get_pipeline = AsyncMock(return_value=MagicMock())

        registry = AdapterRegistry([mock_airflow, mock_argo])
        found = await registry.find_adapter_for_pipeline("argo-pipe")
        assert found == mock_argo

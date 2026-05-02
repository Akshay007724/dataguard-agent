from __future__ import annotations

from datetime import datetime

import httpx

from dataguard_adapters.base import OrchestratorAdapter, PipelineSummary, RunDetails, RunStatus
from dataguard_core.logging import get_logger
from dataguard_core.metrics import adapter_request_duration

log = get_logger(__name__)

_STATUS_MAP: dict[str, RunStatus] = {
    "success": RunStatus.SUCCESS,
    "failed": RunStatus.FAILED,
    "running": RunStatus.RUNNING,
    "queued": RunStatus.QUEUED,
    "skipped": RunStatus.SKIPPED,
    "upstream_failed": RunStatus.UPSTREAM_FAILED,
}


class AirflowAdapter(OrchestratorAdapter):
    """Airflow REST API v2 adapter.

    Requires a service account with Viewer role. Never requests write permissions.

    Args:
        base_url: Airflow webserver URL, e.g. http://airflow:8080.
        username: Basic auth username.
        password: Basic auth password.
        timeout: Per-request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = 30,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=f"{base_url.rstrip('/')}/api/v1",
            auth=(username, password),
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

    @property
    def orchestrator_name(self) -> str:
        return "airflow"

    async def list_pipelines(
        self,
        tag: str | None = None,
        status: RunStatus | None = None,
    ) -> list[PipelineSummary]:
        with adapter_request_duration.labels(adapter="airflow", operation="list_dags").time():
            params: dict[str, str | int] = {"limit": 200}
            if tag:
                params["tags"] = tag
            resp = await self._client.get("/dags", params=params)
            resp.raise_for_status()

        dags = resp.json().get("dags", [])
        summaries = [self._dag_to_summary(d) for d in dags]

        if status is not None:
            summaries = [s for s in summaries if s.last_run_status == status]

        return summaries

    async def get_pipeline(self, pipeline_id: str) -> PipelineSummary:
        with adapter_request_duration.labels(adapter="airflow", operation="get_dag").time():
            resp = await self._client.get(f"/dags/{pipeline_id}")
            resp.raise_for_status()
        return self._dag_to_summary(resp.json())

    async def get_run(self, pipeline_id: str, run_id: str) -> RunDetails:
        with adapter_request_duration.labels(adapter="airflow", operation="get_dag_run").time():
            resp = await self._client.get(f"/dags/{pipeline_id}/dagRuns/{run_id}")
            resp.raise_for_status()

        run = resp.json()
        failing_task = await self._get_failing_task(pipeline_id, run_id)
        return self._run_to_details(run, pipeline_id, failing_task)

    async def get_latest_run(self, pipeline_id: str) -> RunDetails:
        history = await self.get_run_history(pipeline_id, limit=1)
        if not history:
            raise ValueError(f"No runs found for pipeline {pipeline_id!r}")
        return history[0]

    async def get_run_history(self, pipeline_id: str, limit: int = 10) -> list[RunDetails]:
        with adapter_request_duration.labels(adapter="airflow", operation="list_dag_runs").time():
            resp = await self._client.get(
                f"/dags/{pipeline_id}/dagRuns",
                params={"limit": limit, "order_by": "-start_date"},
            )
            resp.raise_for_status()

        runs = resp.json().get("dag_runs", [])
        return [self._run_to_details(r, pipeline_id) for r in runs]

    async def get_run_logs(
        self,
        pipeline_id: str,
        run_id: str,
        task_id: str | None = None,
        head_lines: int = 50,
        tail_lines: int = 100,
    ) -> str:
        if task_id is None:
            task_id = await self._get_failing_task(pipeline_id, run_id)
            if task_id is None:
                return "(no failed task instances found)"

        with adapter_request_duration.labels(adapter="airflow", operation="get_task_log").time():
            resp = await self._client.get(
                f"/dags/{pipeline_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1",
                headers={"Accept": "text/plain"},
            )
            resp.raise_for_status()

        return self._trim_log(resp.text, head_lines, tail_lines)

    async def _get_failing_task(self, pipeline_id: str, run_id: str) -> str | None:
        resp = await self._client.get(
            f"/dags/{pipeline_id}/dagRuns/{run_id}/taskInstances",
            params={"state": "failed"},
        )
        if resp.status_code != 200:
            return None
        instances = resp.json().get("task_instances", [])
        return instances[0]["task_id"] if instances else None

    @staticmethod
    def _dag_to_summary(dag: dict) -> PipelineSummary:  # type: ignore[type-arg]
        return PipelineSummary(
            id=dag.get("dag_id", ""),
            name=dag.get("dag_id", ""),
            orchestrator="airflow",
            owner=", ".join(dag.get("owners", []) or []) or None,
            tags=[t["name"] for t in dag.get("tags", []) or []],
            last_run_status=_STATUS_MAP.get(
                (dag.get("last_dag_run_state") or ""), RunStatus.UNKNOWN
            ) if dag.get("last_dag_run_state") else None,
            last_run_at=_parse_dt(dag.get("last_run")),
            schedule=dag.get("schedule_interval") or dag.get("timetable_summary"),
            is_paused=dag.get("is_paused", False),
        )

    @staticmethod
    def _run_to_details(
        run: dict,  # type: ignore[type-arg]
        pipeline_id: str,
        failing_task: str | None = None,
    ) -> RunDetails:
        started = _parse_dt(run.get("start_date"))
        ended = _parse_dt(run.get("end_date"))
        duration: float | None = None
        if started and ended:
            duration = (ended - started).total_seconds()

        return RunDetails(
            run_id=run.get("dag_run_id", ""),
            pipeline_id=pipeline_id,
            status=_STATUS_MAP.get(run.get("state", ""), RunStatus.UNKNOWN),
            started_at=started,
            ended_at=ended,
            duration_seconds=duration,
            error_message=run.get("note"),
            failing_task=failing_task,
            retry_number=0,
        )

    async def close(self) -> None:
        await self._client.aclose()


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None

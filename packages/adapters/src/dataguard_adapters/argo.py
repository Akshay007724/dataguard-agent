from __future__ import annotations

from datetime import datetime

import httpx

from dataguard_adapters.base import (
    OrchestratorAdapter,
    OrchestratorConnectionError,
    PipelineNotFoundError,
    PipelineSummary,
    RunDetails,
    RunNotFoundError,
    RunStatus,
)
from dataguard_core.logging import get_logger
from dataguard_core.metrics import adapter_request_duration

log = get_logger(__name__)

_PHASE_MAP: dict[str, RunStatus] = {
    "Succeeded": RunStatus.SUCCESS,
    "Failed": RunStatus.FAILED,
    "Error": RunStatus.FAILED,
    "Running": RunStatus.RUNNING,
    "Pending": RunStatus.QUEUED,
    "Skipped": RunStatus.SKIPPED,
}


class ArgoAdapter(OrchestratorAdapter):
    """Argo Workflows REST API adapter."""

    def __init__(
        self,
        host: str,
        namespace: str = "argo",
        token: str | None = None,
        verify_ssl: bool = True,
        timeout: int = 30,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        import os

        resolved_token = token or os.environ.get("ARGO_TOKEN")
        headers = {"Authorization": f"Bearer {resolved_token}"} if resolved_token else {}

        self._client = client or httpx.AsyncClient(
            base_url=f"{host.rstrip('/')}/api/v1",
            headers=headers,
            verify=verify_ssl,
            timeout=timeout,
        )
        self._namespace = namespace

    @property
    def orchestrator_name(self) -> str:
        return "argo"

    async def list_pipelines(
        self,
        tag: str | None = None,
        status: RunStatus | None = None,
    ) -> list[PipelineSummary]:
        with adapter_request_duration.labels(adapter="argo", operation="list_workflows").time():
            params: dict[str, str] = {}
            if tag:
                params["listOptions.labelSelector"] = f"tag={tag}"
            try:
                resp = await self._client.get(
                    f"/workflows/{self._namespace}",
                    params=params,
                )
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                raise OrchestratorConnectionError(f"Argo connection failed: {exc}") from exc

        items = resp.json().get("items") or []
        summaries = [self._workflow_to_summary(w) for w in items]

        if status is not None:
            summaries = [s for s in summaries if s.last_run_status == status]

        return summaries

    async def get_pipeline(self, pipeline_id: str) -> PipelineSummary:
        with adapter_request_duration.labels(adapter="argo", operation="get_workflow").time():
            try:
                resp = await self._client.get(f"/workflows/{self._namespace}/{pipeline_id}")
                if resp.status_code == 404:
                    raise PipelineNotFoundError(f"Workflow {pipeline_id!r} not found in Argo")
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                raise OrchestratorConnectionError(f"Argo connection failed: {exc}") from exc
        return self._workflow_to_summary(resp.json())

    async def get_run(self, pipeline_id: str, run_id: str) -> RunDetails:
        wf_id = run_id or pipeline_id
        with adapter_request_duration.labels(adapter="argo", operation="get_workflow_run").time():
            try:
                resp = await self._client.get(f"/workflows/{self._namespace}/{wf_id}")
                if resp.status_code == 404:
                    raise RunNotFoundError(f"Run {wf_id!r} not found in Argo")
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                raise OrchestratorConnectionError(f"Argo connection failed: {exc}") from exc
        return self._workflow_to_details(resp.json(), pipeline_id)

    async def get_latest_run(self, pipeline_id: str) -> RunDetails:
        history = await self.get_run_history(pipeline_id, limit=1)
        if not history:
            raise RunNotFoundError(f"No runs found for workflow {pipeline_id!r}")
        return history[0]

    async def get_run_history(self, pipeline_id: str, limit: int = 10) -> list[RunDetails]:
        with adapter_request_duration.labels(adapter="argo", operation="list_workflow_history").time():
            try:
                resp = await self._client.get(
                    f"/workflows/{self._namespace}",
                    params={
                        "listOptions.labelSelector": f"workflows.argoproj.io/workflow-template={pipeline_id}",
                        "listOptions.limit": str(limit),
                    },
                )
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                raise OrchestratorConnectionError(f"Argo connection failed: {exc}") from exc

        items = resp.json().get("items") or []
        if not items:
            try:
                single = await self.get_run(pipeline_id, pipeline_id)
                return [single]
            except Exception:
                return []

        return [self._workflow_to_details(w, pipeline_id) for w in items[:limit]]

    async def get_run_logs(
        self,
        pipeline_id: str,
        run_id: str,
        task_id: str | None = None,
        head_lines: int = 50,
        tail_lines: int = 100,
    ) -> str:
        wf_id = run_id or pipeline_id
        params: dict[str, str] = {"logOptions.container": "main"}
        if task_id:
            params["podName"] = task_id

        with adapter_request_duration.labels(adapter="argo", operation="get_workflow_logs").time():
            try:
                resp = await self._client.get(
                    f"/workflows/{self._namespace}/{wf_id}/log",
                    params=params,
                )
                if resp.status_code == 404:
                    return f"(no logs found for workflow {wf_id})"
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                raise OrchestratorConnectionError(f"Argo connection failed: {exc}") from exc

        return self._trim_log(resp.text, head_lines, tail_lines)

    @staticmethod
    def _workflow_to_summary(wf: dict) -> PipelineSummary:  # type: ignore[type-arg]
        metadata = wf.get("metadata", {})
        status = wf.get("status", {})
        labels = metadata.get("labels", {})

        phase = status.get("phase", "Unknown")
        run_status = _PHASE_MAP.get(phase, RunStatus.UNKNOWN)

        return PipelineSummary(
            id=metadata.get("name", ""),
            name=metadata.get("name", ""),
            orchestrator="argo",
            owner=labels.get("owner"),
            tags=[v for k, v in labels.items() if k.startswith("tag") or k == "tier"],
            last_run_status=run_status,
            last_run_at=_parse_dt(status.get("startedAt")),
            schedule=metadata.get("annotations", {}).get("cron"),
            is_paused=False,
        )

    @staticmethod
    def _workflow_to_details(wf: dict, pipeline_id: str) -> RunDetails:  # type: ignore[type-arg]
        metadata = wf.get("metadata", {})
        status = wf.get("status", {})

        started = _parse_dt(status.get("startedAt"))
        ended = _parse_dt(status.get("finishedAt"))
        duration: float | None = None
        if started and ended:
            duration = (ended - started).total_seconds()

        failing_task: str | None = None
        for node in (status.get("nodes") or {}).values():
            if node.get("phase") in ("Failed", "Error") and node.get("type") == "Pod":
                failing_task = node.get("name")
                break

        return RunDetails(
            run_id=metadata.get("name", ""),
            pipeline_id=pipeline_id,
            status=_PHASE_MAP.get(status.get("phase", ""), RunStatus.UNKNOWN),
            started_at=started,
            ended_at=ended,
            duration_seconds=duration,
            error_message=status.get("message"),
            failing_task=failing_task,
            retry_number=0,
        )

    async def aclose(self) -> None:
        await self._client.aclose()


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None

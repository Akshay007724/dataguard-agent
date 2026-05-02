from __future__ import annotations

from datetime import datetime

import httpx

from dataguard_adapters.base import OrchestratorAdapter, PipelineSummary, RunDetails, RunStatus
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
    """Argo Workflows REST API adapter.

    Uses httpx directly against the Argo Workflows API server.
    Requires a ServiceAccount token with get/list/watch on workflows.

    Args:
        host: Argo API server URL, e.g. https://argo.example.com.
        namespace: Kubernetes namespace to query.
        token: Bearer token. If None, tries ARGO_TOKEN env var.
        verify_ssl: Set False only for local dev with self-signed certs.
    """

    def __init__(
        self,
        host: str,
        namespace: str = "argo",
        token: str | None = None,
        verify_ssl: bool = True,
        timeout: int = 30,
    ) -> None:
        import os

        resolved_token = token or os.environ.get("ARGO_TOKEN")
        headers = {"Authorization": f"Bearer {resolved_token}"} if resolved_token else {}

        self._client = httpx.AsyncClient(
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
        # Argo WorkflowTemplates represent "pipelines"; individual Workflows are runs
        with adapter_request_duration.labels(adapter="argo", operation="list_workflow_templates").time():
            resp = await self._client.get(f"/workflow-templates/{self._namespace}")
            resp.raise_for_status()

        templates = resp.json().get("items") or []
        summaries = [self._template_to_summary(t) for t in templates]

        if tag:
            summaries = [
                s for s in summaries
                if tag in s.tags
            ]
        return summaries

    async def get_pipeline(self, pipeline_id: str) -> PipelineSummary:
        with adapter_request_duration.labels(adapter="argo", operation="get_workflow_template").time():
            resp = await self._client.get(
                f"/workflow-templates/{self._namespace}/{pipeline_id}"
            )
            resp.raise_for_status()
        return self._template_to_summary(resp.json())

    async def get_run(self, pipeline_id: str, run_id: str) -> RunDetails:
        with adapter_request_duration.labels(adapter="argo", operation="get_workflow").time():
            resp = await self._client.get(f"/workflows/{self._namespace}/{run_id}")
            resp.raise_for_status()
        return self._workflow_to_details(resp.json(), pipeline_id)

    async def get_latest_run(self, pipeline_id: str) -> RunDetails:
        history = await self.get_run_history(pipeline_id, limit=1)
        if not history:
            raise ValueError(f"No runs found for pipeline {pipeline_id!r}")
        return history[0]

    async def get_run_history(self, pipeline_id: str, limit: int = 10) -> list[RunDetails]:
        with adapter_request_duration.labels(adapter="argo", operation="list_workflows").time():
            resp = await self._client.get(
                f"/workflows/{self._namespace}",
                params={
                    "listOptions.labelSelector": f"workflows.argoproj.io/workflow-template={pipeline_id}",
                    "listOptions.limit": limit,
                },
            )
            resp.raise_for_status()

        workflows = resp.json().get("items") or []
        details = [self._workflow_to_details(w, pipeline_id) for w in workflows]
        return sorted(details, key=lambda d: d.started_at or datetime.min, reverse=True)

    async def get_run_logs(
        self,
        pipeline_id: str,
        run_id: str,
        task_id: str | None = None,
        head_lines: int = 50,
        tail_lines: int = 100,
    ) -> str:
        params: dict[str, str | int] = {"logOptions.follow": "false"}
        if task_id:
            params["podName"] = task_id

        with adapter_request_duration.labels(adapter="argo", operation="get_workflow_log").time():
            resp = await self._client.get(
                f"/workflows/{self._namespace}/{run_id}/log",
                params=params,
            )
            if resp.status_code == 404:
                return f"(logs not available for workflow {run_id})"
            resp.raise_for_status()

        # Argo log endpoint returns newline-delimited JSON; extract content field
        lines = []
        for line in resp.text.splitlines():
            try:
                import json
                entry = json.loads(line)
                content = entry.get("result", {}).get("content", "")
                if content:
                    lines.append(content)
            except (ValueError, KeyError):
                lines.append(line)

        raw = "\n".join(lines)
        return self._trim_log(raw, head_lines, tail_lines)

    @staticmethod
    def _template_to_summary(template: dict) -> PipelineSummary:  # type: ignore[type-arg]
        meta = template.get("metadata", {})
        labels = meta.get("labels", {}) or {}
        return PipelineSummary(
            id=meta.get("name", ""),
            name=meta.get("name", ""),
            orchestrator="argo",
            owner=labels.get("owner") or meta.get("namespace"),
            tags=list(labels.keys()),
            last_run_status=None,
            last_run_at=None,
            schedule=template.get("spec", {}).get("schedules", [None])[0],
            is_paused=labels.get("workflows.argoproj.io/paused") == "true",
        )

    @staticmethod
    def _workflow_to_details(workflow: dict, pipeline_id: str) -> RunDetails:  # type: ignore[type-arg]
        meta = workflow.get("metadata", {})
        status = workflow.get("status", {})
        phase = status.get("phase", "Unknown")

        started = _parse_dt(status.get("startedAt"))
        finished = _parse_dt(status.get("finishedAt"))
        duration: float | None = None
        if started and finished:
            duration = (finished - started).total_seconds()

        # Find the first failed node
        failing_task: str | None = None
        for node in (status.get("nodes") or {}).values():
            if node.get("phase") in ("Failed", "Error") and node.get("type") == "Pod":
                failing_task = node.get("displayName") or node.get("id")
                break

        return RunDetails(
            run_id=meta.get("name", ""),
            pipeline_id=pipeline_id,
            status=_PHASE_MAP.get(phase, RunStatus.UNKNOWN),
            started_at=started,
            ended_at=finished,
            duration_seconds=duration,
            error_message=status.get("message"),
            failing_task=failing_task,
            retry_number=0,
            metadata={"namespace": meta.get("namespace", "")},
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

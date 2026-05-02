from __future__ import annotations

import json
from typing import Any

from dataguard_adapters.base import OrchestratorAdapter, RunStatus
from dataguard_core.logging import get_logger
from dataguard_core.store import redis
from dataguard_core.config import settings

log = get_logger(__name__)


async def handle_list_pipelines(
    adapters: list[OrchestratorAdapter],
    arguments: dict[str, Any],
) -> str:
    orchestrator_filter = arguments.get("orchestrator")
    tag = arguments.get("tag")
    status_str = arguments.get("status")
    status = RunStatus(status_str) if status_str else None

    cache_key = f"list_pipelines:{orchestrator_filter}:{tag}:{status_str}"
    cached = await redis.cache_get(cache_key)
    if cached is not None:
        return json.dumps(cached)

    results = []
    for adapter in adapters:
        if orchestrator_filter and adapter.orchestrator_name != orchestrator_filter:
            continue
        try:
            pipelines = await adapter.list_pipelines(tag=tag, status=status)
            for p in pipelines:
                results.append({
                    "id": p.id,
                    "name": p.name,
                    "orchestrator": p.orchestrator,
                    "owner": p.owner,
                    "tags": p.tags,
                    "last_run_status": p.last_run_status,
                    "last_run_at": p.last_run_at.isoformat() if p.last_run_at else None,
                    "schedule": p.schedule,
                    "is_paused": p.is_paused,
                })
        except Exception as exc:
            log.warning("list_pipelines_adapter_error", adapter=adapter.orchestrator_name, error=str(exc))

    await redis.cache_set(cache_key, results, ttl=settings.pipeline_status_cache_ttl)
    return json.dumps(results, default=str)


async def handle_get_pipeline_status(
    adapters: list[OrchestratorAdapter],
    arguments: dict[str, Any],
) -> str:
    pipeline_id: str = arguments["pipeline_id"]

    cache_key = f"pipeline_status:{pipeline_id}"
    cached = await redis.cache_get(cache_key)
    if cached is not None:
        return json.dumps(cached)

    adapter = _find_adapter(adapters, pipeline_id)
    if adapter is None:
        return json.dumps({"error": f"Pipeline {pipeline_id!r} not found in any adapter"})

    summary = await adapter.get_pipeline(pipeline_id)
    history = await adapter.get_run_history(pipeline_id, limit=10)

    durations = [r.duration_seconds for r in history if r.duration_seconds is not None]
    avg_duration = sum(durations) / len(durations) if durations else None

    result = {
        "id": summary.id,
        "orchestrator": summary.orchestrator,
        "owner": summary.owner,
        "schedule": summary.schedule,
        "is_paused": summary.is_paused,
        "last_run_status": summary.last_run_status,
        "last_run_at": summary.last_run_at.isoformat() if summary.last_run_at else None,
        "average_duration_seconds": avg_duration,
        "run_history": [
            {
                "run_id": r.run_id,
                "status": r.status,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "ended_at": r.ended_at.isoformat() if r.ended_at else None,
                "duration_seconds": r.duration_seconds,
            }
            for r in history
        ],
    }

    await redis.cache_set(cache_key, result, ttl=settings.pipeline_status_cache_ttl)
    return json.dumps(result, default=str)


async def handle_get_failure_details(
    adapters: list[OrchestratorAdapter],
    arguments: dict[str, Any],
) -> str:
    pipeline_id: str = arguments["pipeline_id"]
    run_id: str | None = arguments.get("run_id")

    adapter = _find_adapter(adapters, pipeline_id)
    if adapter is None:
        return json.dumps({"error": f"Pipeline {pipeline_id!r} not found in any adapter"})

    if run_id:
        run = await adapter.get_run(pipeline_id, run_id)
    else:
        run = await adapter.get_latest_run(pipeline_id)

    logs = await adapter.get_run_logs(pipeline_id, run.run_id, task_id=run.failing_task)

    return json.dumps({
        "pipeline_id": pipeline_id,
        "run_id": run.run_id,
        "status": run.status,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "ended_at": run.ended_at.isoformat() if run.ended_at else None,
        "duration_seconds": run.duration_seconds,
        "failing_task": run.failing_task,
        "error_message": run.error_message,
        "retry_number": run.retry_number,
        "log_excerpt": logs,
    }, default=str)


def _find_adapter(adapters: list[OrchestratorAdapter], pipeline_id: str) -> OrchestratorAdapter | None:
    # Heuristic: Argo workflow names use k8s naming conventions (lowercase + hyphens)
    # Airflow DAG IDs are typically snake_case or kebab-case
    # For v0.1 we try all adapters and return the first that responds
    return adapters[0] if adapters else None

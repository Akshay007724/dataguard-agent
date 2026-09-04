from __future__ import annotations

import asyncio
import json
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from dataguard_adapters.base import OrchestratorAdapter
from dataguard_core.llm.client import LLMClient
from dataguard_core.logging import get_logger
from dataguard_core.metrics import llm_deterministic_hits
from dataguard_core.store import redis
from pipeline_sentinel.detectors.base import BaseDetector
from pipeline_sentinel.lineage.tracer import LineageTracer
from pipeline_sentinel.mcp_server.prompts.diagnosis import (
    DIAGNOSIS_SYSTEM_PROMPT,
    build_diagnosis_prompt,
)

log = get_logger(__name__)

_LOCK_TTL = 60  # seconds — prevent concurrent diagnoses of same pipeline


# ── Domain models ────────────────────────────────────────────────────────────


class IncidentRef(BaseModel):
    id: str
    title: str
    root_cause_category: str
    resolution: str | None = None


class DiagnosisResult(BaseModel):
    diagnosis_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: str
    run_id: str | None
    root_cause_category: str
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[str]
    similar_incidents: list[IncidentRef]
    recommended_action: str
    llm_used: bool = False


# ── Deterministic pattern matchers ───────────────────────────────────────────

_PATTERNS: list[tuple[str, str, str, float]] = [
    # (pattern_name, root_cause, regex, confidence)
    (
        "oom_killed",
        "oom",
        r"(?i)(out of memory|OOMKilled|memory limit exceeded|Killed\s+python)",
        0.95,
    ),
    (
        "connection_timeout",
        "source_unavailable",
        r"(?i)(connection (timed out|refused|reset)|could not connect|ECONNREFUSED|socket timeout)",
        0.90,
    ),
    (
        "http_503",
        "source_unavailable",
        r"(?i)(503 Service Unavailable|upstream connect error|503 error)",
        0.88,
    ),
    ("key_error", "code_error", r"KeyError:\s*['\"](\w+)['\"]", 0.85),
    (
        "column_not_found",
        "schema_drift",
        r"(?i)(column ['\"]?\w+['\"]? (does not exist|not found|unknown)|AnalysisException.*cannot resolve)",
        0.92,
    ),
    (
        "schema_mismatch",
        "schema_drift",
        r"(?i)(schema mismatch|incompatible schema|field .* not in schema)",
        0.90,
    ),
    (
        "upstream_failed",
        "dependency_failure",
        r"(?i)(upstream task.*failed|depends on.*which failed|Task.*upstream_failed)",
        0.85,
    ),
]


def _run_pattern_matchers(log_text: str) -> tuple[str, float, str] | None:
    """Try each deterministic pattern against the log. Return (category, confidence, pattern_name) or None."""
    for pattern_name, root_cause, regex, confidence in _PATTERNS:
        if re.search(regex, log_text):
            return root_cause, confidence, pattern_name
    return None


# ── Main handler ─────────────────────────────────────────────────────────────


async def handle_diagnose_failure(
    adapters: list[OrchestratorAdapter],
    tracer: LineageTracer,
    detectors: list[BaseDetector],
    llm: LLMClient,
    arguments: dict[str, Any],
) -> str:
    pipeline_id: str = arguments["pipeline_id"]
    run_id: str | None = arguments.get("run_id")

    # Distributed lock: prevent race on concurrent calls for the same pipeline
    lock_key = f"diagnose:{pipeline_id}"
    token = await redis.acquire_lock(lock_key, ttl_seconds=_LOCK_TTL)
    if not token:
        log.warning("diagnose_lock_busy", pipeline_id=pipeline_id)
        return json.dumps(
            {
                "error": "concurrent_diagnosis_in_progress",
                "message": f"Diagnosis is already in progress for pipeline {pipeline_id!r}",
                "pipeline_id": pipeline_id,
            }
        )

    try:
        return await _diagnose(adapters, tracer, detectors, llm, pipeline_id, run_id)
    finally:
        await redis.release_lock(lock_key, token)


async def _diagnose(
    adapters: list[OrchestratorAdapter],
    tracer: LineageTracer,
    detectors: list[BaseDetector],
    llm: LLMClient,
    pipeline_id: str,
    run_id: str | None,
) -> str:
    # 1. Select adapter
    target_adapter = None
    for ad in adapters:
        try:
            await ad.get_pipeline(pipeline_id)
            target_adapter = ad
            break
        except Exception:
            continue
    adapter = target_adapter or (adapters[0] if adapters else None)
    if adapter is None:
        return json.dumps({"error": "No orchestrator adapters configured"})

    if run_id:
        run = await adapter.get_run(pipeline_id, run_id)
    else:
        run = await adapter.get_latest_run(pipeline_id)

    actual_run_id = run.run_id

    # 2. Fetch logs
    logs = await adapter.get_run_logs(pipeline_id, actual_run_id, task_id=run.failing_task)

    # 3. Try deterministic matchers first
    match = _run_pattern_matchers(logs + (run.error_message or ""))
    if match is not None:
        root_cause, confidence, pattern_name = match
        llm_deterministic_hits.labels(pattern=pattern_name).inc()
        log.info(
            "diagnose_deterministic_hit",
            pipeline_id=pipeline_id,
            pattern=pattern_name,
            confidence=confidence,
        )
        result = DiagnosisResult(
            pipeline_id=pipeline_id,
            run_id=actual_run_id,
            root_cause_category=root_cause,
            confidence=confidence,
            evidence=[f"Log pattern matched: {pattern_name}"],
            similar_incidents=[],
            recommended_action=_recommend(root_cause),
            llm_used=False,
        )
        return result.model_dump_json()

    # 4. Gather context for LLM concurrently
    lineage_task = asyncio.create_task(_build_lineage_summary(tracer, pipeline_id))
    quality_task = asyncio.create_task(_build_quality_summary(detectors, pipeline_id))
    lineage_summary, quality_summary = await asyncio.gather(lineage_task, quality_task)

    # 5. LLM diagnosis
    log.info("diagnose_llm_call", pipeline_id=pipeline_id)
    prompt = build_diagnosis_prompt(
        pipeline_id=pipeline_id,
        run_id=actual_run_id,
        status=str(run.status),
        failing_task=run.failing_task,
        error_message=run.error_message,
        log_excerpt=logs,
        lineage_summary=lineage_summary,
        quality_summary=quality_summary,
        historical_incidents="(historical incident search not yet implemented — v0.2)",
    )

    llm_result = await llm.complete_structured(
        prompt=prompt,
        schema=DiagnosisResult,
        system=DIAGNOSIS_SYSTEM_PROMPT,
    )
    result = llm_result.model_copy(update={"pipeline_id": pipeline_id, "run_id": actual_run_id, "llm_used": True})
    return result.model_dump_json()


async def _build_lineage_summary(tracer: LineageTracer, pipeline_id: str) -> str:
    try:
        graph = await tracer.trace(pipeline_id=pipeline_id, direction="upstream", depth=3)
        if not graph.nodes:
            return "(no lineage data available)"
        lines = [f"Upstream of {pipeline_id}:"]
        for node in graph.nodes:
            status_str = f" [{node.last_run_status}]" if node.last_run_status else ""
            age_str = ""
            if node.last_run_at:
                node_dt = node.last_run_at.replace(tzinfo=UTC) if node.last_run_at.tzinfo is None else node.last_run_at
                age_h = (datetime.now(UTC) - node_dt).total_seconds() / 3600
                age_str = f" (last run {age_h:.1f}h ago)"
            lines.append(f"  {node.type}: {node.name}{status_str}{age_str}")
        return "\n".join(lines)
    except Exception as exc:
        log.warning("lineage_summary_failed", error=str(exc))
        return "(lineage query failed)"


async def _build_quality_summary(detectors: list[BaseDetector], pipeline_id: str) -> str:
    if not detectors:
        return "(no quality detectors configured)"

    async def _eval_detector(detector: BaseDetector) -> str:
        try:
            result = await detector.run(pipeline_id)
            if result.passed:
                return f"  {detector.name}: PASS"
            failures = ", ".join(f"{c.name} ({c.severity})" for c in result.failed_checks)
            return f"  {detector.name}: FAIL — {failures}"
        except Exception as exc:
            return f"  {detector.name}: ERROR ({exc})"

    lines = await asyncio.gather(*[_eval_detector(d) for d in detectors])
    return "\n".join(lines)


def _recommend(root_cause: str) -> str:
    recommendations = {
        "oom": "Scale executor memory limit to 2x or optimize batch/partition size in the transformation step.",
        "source_unavailable": "Check upstream network connectivity, service health dashboard, and verify credentials.",
        "code_error": "Inspect recent commits to pipeline logic; verify column names and schema assumptions.",
        "schema_drift": "Trigger schema comparison tool, verify upstream contract changes, and update pipeline DDL.",
        "dependency_failure": "Trace upstream pipeline runs to identify the failing root task and triage upstream.",
    }
    return recommendations.get(root_cause, "Review logs, inspect upstream dependencies, and file an incident.")

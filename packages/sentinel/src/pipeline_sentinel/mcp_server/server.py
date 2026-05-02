from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from dataguard_adapters.airflow import AirflowAdapter
from dataguard_adapters.argo import ArgoAdapter
from dataguard_adapters.base import OrchestratorAdapter
from dataguard_core.llm.client import LLMClient
from dataguard_core.logging import get_logger
from dataguard_core.metrics import mcp_tool_duration, mcp_tool_errors
from dataguard_core.store import postgres, redis

from pipeline_sentinel.config import settings
from pipeline_sentinel.detectors.freshness import FreshnessDetector
from pipeline_sentinel.detectors.schema_drift import SchemaDriftDetector
from pipeline_sentinel.detectors.volume_anomaly import VolumeAnomalyDetector
from pipeline_sentinel.lineage.openlineage import MarquezClient
from pipeline_sentinel.lineage.tracer import LineageTracer
from pipeline_sentinel.mcp_server.tools.diagnosis import handle_diagnose_failure
from pipeline_sentinel.mcp_server.tools.incidents import handle_file_incident, handle_get_recent_incidents
from pipeline_sentinel.mcp_server.tools.pipelines import (
    handle_get_failure_details,
    handle_get_pipeline_status,
    handle_list_pipelines,
)
from pipeline_sentinel.mcp_server.tools.quality import handle_check_data_quality, handle_trace_lineage
from pipeline_sentinel.mcp_server.tools.remediation import handle_execute_remediation, handle_propose_remediation

log = get_logger(__name__)


def _build_dependencies() -> tuple[
    list[OrchestratorAdapter],
    LineageTracer,
    list[Any],
    LLMClient,
]:
    adapters: list[OrchestratorAdapter] = [
        AirflowAdapter(
            base_url=settings.airflow_base_url,
            username=settings.airflow_username,
            password=settings.airflow_password,
        ),
        ArgoAdapter(
            host=settings.argo_host,
            namespace=settings.argo_namespace,
            token=settings.argo_token,
            verify_ssl=settings.argo_verify_ssl,
        ),
    ]

    marquez = MarquezClient(
        base_url=settings.openlineage_url,
        namespace=settings.openlineage_namespace,
    )
    tracer = LineageTracer(marquez)

    detectors = [
        SchemaDriftDetector(
            marquez_url=settings.openlineage_url,
            namespace=settings.openlineage_namespace,
        ),
        VolumeAnomalyDetector(),
        FreshnessDetector(
            marquez_url=settings.openlineage_url,
            namespace=settings.openlineage_namespace,
        ),
    ]

    api_key: str | None = None
    if settings.anthropic_api_key:
        api_key = settings.anthropic_api_key.get_secret_value()
    elif settings.openai_api_key:
        api_key = settings.openai_api_key.get_secret_value()

    llm = LLMClient(model=settings.llm_model, api_key=api_key)

    return adapters, tracer, detectors, llm


async def run_stdio_server() -> None:
    """Start the MCP server over stdio (used by Claude Desktop, Cursor, mcp CLI)."""
    postgres.init_engine(settings.database_url)
    redis.init_redis(settings.redis_url)
    await postgres.create_tables()

    adapters, tracer, detectors, llm = _build_dependencies()

    server = Server("pipeline-sentinel")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return _tool_definitions()

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        log.info("mcp_tool_called", tool=name)
        with mcp_tool_duration.labels(tool=name).time():
            try:
                result = await _dispatch(name, arguments, adapters, tracer, detectors, llm)
                return [TextContent(type="text", text=result)]
            except Exception as exc:
                error_type = type(exc).__name__
                mcp_tool_errors.labels(tool=name, error_type=error_type).inc()
                log.error("mcp_tool_error", tool=name, error=str(exc), exc_info=True)
                return [TextContent(type="text", text=json.dumps({"error": str(exc), "tool": name}))]

    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


async def _dispatch(
    name: str,
    arguments: dict[str, Any],
    adapters: list[OrchestratorAdapter],
    tracer: LineageTracer,
    detectors: list[Any],
    llm: LLMClient,
) -> str:
    match name:
        case "list_pipelines":
            return await handle_list_pipelines(adapters, arguments)
        case "get_pipeline_status":
            return await handle_get_pipeline_status(adapters, arguments)
        case "get_failure_details":
            return await handle_get_failure_details(adapters, arguments)
        case "trace_lineage":
            return await handle_trace_lineage(tracer, arguments)
        case "diagnose_failure":
            return await handle_diagnose_failure(adapters, tracer, detectors, llm, arguments)
        case "propose_remediation":
            return await handle_propose_remediation(arguments)
        case "check_data_quality":
            return await handle_check_data_quality(detectors, arguments)
        case "get_recent_incidents":
            return await handle_get_recent_incidents(arguments)
        case "file_incident":
            return await handle_file_incident(arguments)
        case "execute_remediation":
            return await handle_execute_remediation(arguments)
        case _:
            return json.dumps({"error": f"Unknown tool: {name!r}"})


def _tool_definitions() -> list[Tool]:
    return [
        Tool(
            name="list_pipelines",
            description="List all pipelines with current status and SLA compliance.",
            inputSchema={
                "type": "object",
                "properties": {
                    "orchestrator": {"type": "string", "enum": ["airflow", "argo"], "description": "Filter to a specific orchestrator"},
                    "tag": {"type": "string", "description": "Filter to pipelines with this tag"},
                    "status": {"type": "string", "enum": ["healthy", "degraded", "failed"], "description": "Filter by last run status"},
                },
            },
        ),
        Tool(
            name="get_pipeline_status",
            description="Detailed status for a single pipeline: run history, duration trends, upstream/downstream graph.",
            inputSchema={
                "type": "object",
                "required": ["pipeline_id"],
                "properties": {
                    "pipeline_id": {"type": "string"},
                },
            },
        ),
        Tool(
            name="get_failure_details",
            description="Full error context for a failed run: stack trace, log excerpt (smart truncation), retry history.",
            inputSchema={
                "type": "object",
                "required": ["pipeline_id"],
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "run_id": {"type": "string", "description": "Specific run ID. Defaults to most recent failure."},
                },
            },
        ),
        Tool(
            name="trace_lineage",
            description="Traverse the OpenLineage graph upstream or downstream from a dataset or pipeline.",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_id": {"type": "string"},
                    "pipeline_id": {"type": "string"},
                    "direction": {"type": "string", "enum": ["upstream", "downstream", "both"], "default": "both"},
                    "depth": {"type": "integer", "default": 3, "minimum": 1, "maximum": 10},
                },
            },
        ),
        Tool(
            name="diagnose_failure",
            description="Root cause analysis: deterministic pattern matching + LLM reasoning over logs, lineage, and quality signals.",
            inputSchema={
                "type": "object",
                "required": ["pipeline_id"],
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "run_id": {"type": "string", "description": "Defaults to most recent failure"},
                },
            },
        ),
        Tool(
            name="propose_remediation",
            description="Generate a structured remediation plan from a diagnosis result.",
            inputSchema={
                "type": "object",
                "required": ["pipeline_id", "diagnosis_id"],
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "diagnosis_id": {"type": "string"},
                    "root_cause_category": {"type": "string"},
                },
            },
        ),
        Tool(
            name="check_data_quality",
            description="Run quality checks against a dataset: schema drift, volume anomaly, freshness.",
            inputSchema={
                "type": "object",
                "required": ["dataset_id"],
                "properties": {
                    "dataset_id": {"type": "string"},
                    "checks": {
                        "type": "array",
                        "items": {"type": "string", "enum": ["schema_drift", "volume_anomaly", "freshness"]},
                        "description": "Checks to run. Defaults to all.",
                    },
                },
            },
        ),
        Tool(
            name="get_recent_incidents",
            description="Query incident history filtered by time window, severity, and status.",
            inputSchema={
                "type": "object",
                "properties": {
                    "time_window": {"type": "string", "default": "24h", "description": "e.g. '24h', '7d', '30m'"},
                    "severity": {"type": "string", "enum": ["critical", "high", "medium", "low"]},
                    "status": {"type": "string", "enum": ["open", "in_progress", "resolved"]},
                },
            },
        ),
        Tool(
            name="file_incident",
            description="Create an incident record in the state store.",
            inputSchema={
                "type": "object",
                "required": ["title", "pipeline_id", "severity", "description"],
                "properties": {
                    "title": {"type": "string"},
                    "pipeline_id": {"type": "string"},
                    "severity": {"type": "string", "enum": ["critical", "high", "medium", "low"]},
                    "description": {"type": "string"},
                    "diagnosis_id": {"type": "string"},
                },
            },
        ),
        Tool(
            name="execute_remediation",
            description="Execute an approved remediation plan. Requires AUTO_REMEDIATION_ENABLED=true, confirm=true, and an approver_id.",
            inputSchema={
                "type": "object",
                "required": ["remediation_id", "confirm", "approver_id"],
                "properties": {
                    "remediation_id": {"type": "string"},
                    "confirm": {"type": "boolean", "description": "Must be true to proceed"},
                    "approver_id": {"type": "string", "description": "Identity of the human approving this action"},
                },
            },
        ),
    ]

from __future__ import annotations

import json
from typing import Any

from dataguard_adapters.base import OrchestratorAdapter
from dataguard_core.llm.client import LLMClient
from dataguard_core.logging import get_logger

from pipeline_sentinel.detectors.base import BaseDetector
from pipeline_sentinel.lineage.tracer import LineageTracer
from pipeline_sentinel.mcp_server.server import _dispatch

log = get_logger(__name__)

# Maximum tool-use turns before the agent is forced to stop
_MAX_TURNS = 30

# Anthropic prompt caching: mark system prompt as cacheable (ephemeral TTL = 5 min)
_CACHE_CONTROL = {"type": "ephemeral"}


class AgentContext:
    """Shared runtime context threaded through every agent run."""

    def __init__(
        self,
        adapters: list[OrchestratorAdapter],
        tracer: LineageTracer,
        detectors: list[BaseDetector],
        llm: LLMClient,
    ) -> None:
        self.adapters = adapters
        self.tracer = tracer
        self.detectors = detectors
        self.llm = llm


class ToolRegistry:
    """Executes MCP tool calls on behalf of the agentic loop."""

    def __init__(self, ctx: AgentContext) -> None:
        self._ctx = ctx

    async def execute(self, tool_name: str, arguments: dict[str, Any]) -> str:
        log.debug("agent_tool_call", tool=tool_name, args=arguments)
        return await _dispatch(
            name=tool_name,
            arguments=arguments,
            adapters=self._ctx.adapters,
            tracer=self._ctx.tracer,
            detectors=self._ctx.detectors,
            llm=self._ctx.llm,
        )


# Tool definitions for litellm/Anthropic tool_use — mirrors mcp_server/server.py
# Last definition carries cache_control so the full tool list is cached.
AGENT_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "list_pipelines",
            "description": "List all pipelines with current status.",
            "parameters": {
                "type": "object",
                "properties": {
                    "orchestrator": {"type": "string", "enum": ["airflow", "argo"]},
                    "status": {"type": "string", "enum": ["healthy", "degraded", "failed"]},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_failure_details",
            "description": "Full error context for a failed pipeline run.",
            "parameters": {
                "type": "object",
                "required": ["pipeline_id"],
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "run_id": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "trace_lineage",
            "description": "Traverse the OpenLineage graph from a dataset or pipeline.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset_id": {"type": "string"},
                    "pipeline_id": {"type": "string"},
                    "direction": {"type": "string", "enum": ["upstream", "downstream", "both"], "default": "upstream"},
                    "depth": {"type": "integer", "default": 3},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "diagnose_failure",
            "description": "Root cause analysis combining pattern matching and LLM reasoning.",
            "parameters": {
                "type": "object",
                "required": ["pipeline_id"],
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "run_id": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "propose_remediation",
            "description": "Generate a structured remediation plan from a diagnosis.",
            "parameters": {
                "type": "object",
                "required": ["pipeline_id", "diagnosis_id"],
                "properties": {
                    "pipeline_id": {"type": "string"},
                    "diagnosis_id": {"type": "string"},
                    "root_cause_category": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_data_quality",
            "description": "Run quality checks (schema drift, volume, freshness) on a dataset.",
            "parameters": {
                "type": "object",
                "required": ["dataset_id"],
                "properties": {
                    "dataset_id": {"type": "string"},
                    "checks": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "file_incident",
            "description": "Create an incident record in the state store.",
            "parameters": {
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
        },
    },
]

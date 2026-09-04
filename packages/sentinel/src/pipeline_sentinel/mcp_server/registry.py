from __future__ import annotations

from typing import Any

from mcp.types import Tool

TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "list_pipelines",
        "description": "List all pipelines with current status and SLA compliance.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "orchestrator": {
                    "type": "string",
                    "enum": ["airflow", "argo"],
                    "description": "Filter to a specific orchestrator",
                },
                "tag": {"type": "string", "description": "Filter to pipelines with this tag"},
                "status": {
                    "type": "string",
                    "enum": ["healthy", "degraded", "failed"],
                    "description": "Filter by last run status",
                },
            },
        },
    },
    {
        "name": "get_pipeline_status",
        "description": "Detailed status for a single pipeline: run history, duration trends, upstream/downstream graph.",
        "inputSchema": {
            "type": "object",
            "required": ["pipeline_id"],
            "properties": {
                "pipeline_id": {"type": "string"},
            },
        },
    },
    {
        "name": "get_failure_details",
        "description": "Full error context for a failed run: stack trace, log excerpt (smart truncation), retry history.",
        "inputSchema": {
            "type": "object",
            "required": ["pipeline_id"],
            "properties": {
                "pipeline_id": {"type": "string"},
                "run_id": {"type": "string", "description": "Specific run ID. Defaults to most recent failure."},
            },
        },
    },
    {
        "name": "trace_lineage",
        "description": "Traverse the OpenLineage graph upstream or downstream from a dataset or pipeline.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "pipeline_id": {"type": "string"},
                "direction": {"type": "string", "enum": ["upstream", "downstream", "both"], "default": "both"},
                "depth": {"type": "integer", "default": 3, "minimum": 1, "maximum": 10},
            },
        },
    },
    {
        "name": "diagnose_failure",
        "description": "Root cause analysis: deterministic pattern matching + LLM reasoning over logs, lineage, and quality signals.",
        "inputSchema": {
            "type": "object",
            "required": ["pipeline_id"],
            "properties": {
                "pipeline_id": {"type": "string"},
                "run_id": {"type": "string", "description": "Defaults to most recent failure"},
            },
        },
    },
    {
        "name": "propose_remediation",
        "description": "Generate a structured remediation plan from a diagnosis result.",
        "inputSchema": {
            "type": "object",
            "required": ["pipeline_id", "diagnosis_id"],
            "properties": {
                "pipeline_id": {"type": "string"},
                "diagnosis_id": {"type": "string"},
                "root_cause_category": {"type": "string"},
            },
        },
    },
    {
        "name": "check_data_quality",
        "description": "Run quality checks against a dataset: schema drift, volume anomaly, freshness.",
        "inputSchema": {
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
    },
    {
        "name": "get_recent_incidents",
        "description": "Query incident history filtered by time window, severity, and status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "time_window": {"type": "string", "default": "24h", "description": "e.g. '24h', '7d', '30m'"},
                "severity": {"type": "string", "enum": ["critical", "high", "medium", "low"]},
                "status": {"type": "string", "enum": ["open", "in_progress", "resolved"]},
            },
        },
    },
    {
        "name": "file_incident",
        "description": "Create an incident record in the state store.",
        "inputSchema": {
            "type": "object",
            "required": ["title", "pipeline_id", "severity", "description"],
            "properties": {
                "title": {"type": "string"},
                "pipeline_id": {"type": "string"},
                "severity": {"type": "string", "enum": ["critical", "high", "medium", "low"]},
                "description": {"type": "string"},
                "diagnosis_id": {"type": "string"},
                "root_cause_category": {"type": "string"},
            },
        },
    },
    {
        "name": "execute_remediation",
        "description": "Execute an approved remediation plan. Requires explicit confirmation and auto-remediation enabled.",
        "inputSchema": {
            "type": "object",
            "required": ["remediation_id", "confirm", "approver_id"],
            "properties": {
                "remediation_id": {"type": "string"},
                "confirm": {"type": "boolean", "description": "Must be true to execute"},
                "approver_id": {"type": "string", "description": "ID or email of the approving engineer"},
            },
        },
    },
]


def get_mcp_tools() -> list[Tool]:
    """Generates MCP Tool definitions from canonical metadata."""
    return [
        Tool(
            name=defn["name"],
            description=defn["description"],
            inputSchema=defn["inputSchema"],
        )
        for defn in TOOL_DEFINITIONS
    ]


def get_agent_tools() -> list[dict[str, Any]]:
    """Generates LiteLLM/Anthropic function tool definitions from canonical metadata."""
    tools: list[dict[str, Any]] = [
        {
            "type": "function",
            "function": {
                "name": defn["name"],
                "description": defn["description"],
                "parameters": defn["inputSchema"],
            },
        }
        for defn in TOOL_DEFINITIONS
    ]
    if tools:
        # Anthropic prompt caching on last tool definition
        tools[-1]["cache_control"] = {"type": "ephemeral"}
    return tools

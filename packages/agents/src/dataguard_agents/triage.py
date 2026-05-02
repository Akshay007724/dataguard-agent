"""TriageAgent — autonomous multi-pipeline triage using an agentic tool-use loop.

The agent discovers failing pipelines, diagnoses each one, proposes remediations,
and files incidents for high/critical failures — all without human intervention.

Usage:
    ctx = AgentContext(adapters, tracer, detectors, llm)
    agent = TriageAgent(ctx)
    report = await agent.run()
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import litellm

from dataguard_core.logging import get_logger
from dataguard_core.metrics import mcp_tool_duration

from dataguard_agents.base import AGENT_TOOLS, AgentContext, ToolRegistry, _MAX_TURNS
from dataguard_agents.report import TriageReport, build_report_from_conversation

log = get_logger(__name__)

_SYSTEM_PROMPT = """\
You are the DataGuard Triage Agent — an autonomous data pipeline incident responder.

## Mission
Systematically triage all failing or degraded data pipelines in the environment.

## Protocol (follow in order)
1. Call list_pipelines to discover all pipelines. Filter for failed/degraded status.
2. For each failing pipeline (prioritize: FAILED before DEGRADED):
   a. Call get_failure_details to get the error context and logs
   b. Call trace_lineage(direction="upstream") to understand data dependencies
   c. Call diagnose_failure to determine root cause and confidence
   d. Call propose_remediation with the diagnosis_id
   e. If severity is "critical" or "high" (confidence >= 0.7): call file_incident
3. When all failing pipelines are processed, return a JSON triage report.

## Rules
- Never re-diagnose a pipeline you already processed in this session
- If a pipeline has no recent failures, skip it
- Do not call execute_remediation — that requires human approval
- Be efficient: one pipeline at a time, in order of severity

## Output
When done, output a JSON object:
{
  "triage_completed_at": "<ISO datetime>",
  "pipelines_checked": <int>,
  "failures_found": <int>,
  "incidents_filed": [<incident_id>, ...],
  "diagnoses": [
    {
      "pipeline_id": "...",
      "root_cause": "...",
      "confidence": 0.0,
      "severity": "...",
      "remediation_id": "...",
      "incident_id": "..."
    }
  ],
  "summary": "<one paragraph human-readable summary>"
}
"""


class TriageAgent:
    """Autonomous pipeline triage agent.

    Runs an agentic loop using the configured LLM with tool use.
    Calls MCP tools directly (not via MCP protocol) for efficiency.

    Args:
        ctx: Shared agent context with adapters, detectors, tracer, LLM.
        max_turns: Safety cap on tool-use turns (default 30).
    """

    def __init__(self, ctx: AgentContext, max_turns: int = _MAX_TURNS) -> None:
        self._ctx = ctx
        self._registry = ToolRegistry(ctx)
        self._max_turns = max_turns

    async def run(self, scope: str | None = None) -> TriageReport:
        """Run a full triage pass.

        Args:
            scope: Optional pipeline ID to triage a single pipeline.
                   If None, discovers and triages all failing pipelines.

        Returns:
            TriageReport with diagnoses, incidents filed, and summary.
        """
        started_at = datetime.now(timezone.utc)

        if scope:
            initial = f"Triage the pipeline '{scope}'. Diagnose its failure, propose remediation, and file an incident if severity is high or critical."
        else:
            initial = "Triage all failing or degraded data pipelines. Follow the protocol."

        messages: list[dict[str, Any]] = [
            {
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": _SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
            },
            {"role": "user", "content": initial},
        ]

        turns = 0
        final_content = ""

        log.info("triage_agent_start", scope=scope or "all")

        while turns < self._max_turns:
            turns += 1

            response = await litellm.acompletion(
                model=self._ctx.llm._model,
                messages=messages,
                tools=AGENT_TOOLS,
                tool_choice="auto",
                temperature=0.0,
            )

            choice = response.choices[0]
            message = choice.message
            finish_reason = choice.finish_reason

            # Append assistant message to history
            messages.append(message.model_dump(exclude_none=True))

            if finish_reason == "stop" or finish_reason == "end_turn":
                final_content = message.content or ""
                log.info("triage_agent_done", turns=turns)
                break

            # Execute tool calls
            tool_calls = message.tool_calls or []
            if not tool_calls:
                final_content = message.content or ""
                break

            for tc in tool_calls:
                with mcp_tool_duration.labels(tool=tc.function.name).time():
                    try:
                        args = json.loads(tc.function.arguments or "{}")
                        result = await self._registry.execute(tc.function.name, args)
                    except Exception as exc:
                        result = json.dumps({"error": str(exc)})
                        log.warning("agent_tool_error", tool=tc.function.name, error=str(exc))

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })
        else:
            log.warning("triage_agent_max_turns", max_turns=self._max_turns)
            final_content = json.dumps({
                "error": f"Agent reached max_turns={self._max_turns}",
                "partial_conversation_turns": turns,
            })

        return build_report_from_conversation(
            final_content=final_content,
            messages=messages,
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
        )

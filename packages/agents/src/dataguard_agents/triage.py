"""TriageAgent — autonomous multi-pipeline triage using an agentic tool-use loop."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import litellm

from dataguard_agents.base import _MAX_TURNS, AGENT_TOOLS, AgentContext, ToolRegistry
from dataguard_agents.report import TriageReport, build_report_from_conversation
from dataguard_core.logging import get_logger
from dataguard_core.metrics import llm_tokens, mcp_tool_duration

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
    """Autonomous pipeline triage agent."""

    def __init__(self, ctx: AgentContext, max_turns: int = _MAX_TURNS) -> None:
        self._ctx = ctx
        self._registry = ToolRegistry(ctx)
        self._max_turns = max_turns

    async def run(self, scope: str | None = None) -> TriageReport:
        started_at = datetime.now(UTC)

        if scope:
            initial = (
                f"Triage the pipeline '{scope}'. Diagnose its failure, propose remediation, "
                "and file an incident if severity is high or critical."
            )
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

            model_name = getattr(self._ctx.llm, "model", getattr(self._ctx.llm, "_model", "unknown"))

            response = await litellm.acompletion(
                model=model_name,
                messages=messages,
                tools=AGENT_TOOLS,
                tool_choice="auto",
                temperature=0.0,
            )

            # Track tokens
            usage = getattr(response, "usage", None)
            if usage:
                in_tok = getattr(usage, "prompt_tokens", 0)
                out_tok = getattr(usage, "completion_tokens", 0)
                if isinstance(in_tok, int | float) and isinstance(out_tok, int | float):
                    provider = model_name.split("/")[0] if "/" in model_name else "unknown"
                    llm_tokens.labels(provider=provider, model=model_name, direction="input").inc(in_tok)
                    llm_tokens.labels(provider=provider, model=model_name, direction="output").inc(out_tok)

            choices = getattr(response, "choices", None)
            if not choices:
                break
            choice = choices[0]
            message = choice.message
            finish_reason = getattr(choice, "finish_reason", "stop")

            # Append assistant message to history
            messages.append(message.model_dump(exclude_none=True))

            if finish_reason in ("stop", "end_turn"):
                final_content = message.content or ""
                log.info("triage_agent_done", turns=turns)
                break

            # Execute tool calls concurrently
            tool_calls = message.tool_calls or []
            if not tool_calls:
                final_content = message.content or ""
                break

            async def _run_tool(tc: Any) -> dict[str, Any]:
                fn = getattr(tc, "function", None)
                fn_name = (getattr(fn, "name", "") or "") if fn else ""
                raw_args = (getattr(fn, "arguments", "{}") or "{}") if fn else "{}"
                tc_id = getattr(tc, "id", "tc") or "tc"
                with mcp_tool_duration.labels(tool=fn_name).time():
                    try:
                        args = json.loads(raw_args)
                        res = await self._registry.execute(fn_name, args)
                    except Exception as exc:
                        res = json.dumps({"error": str(exc)})
                        log.warning("agent_tool_error", tool=fn_name, error=str(exc))
                return {
                    "role": "tool",
                    "tool_call_id": tc_id,
                    "content": res,
                }

            tool_results = await asyncio.gather(*[_run_tool(tc) for tc in tool_calls])
            messages.extend(tool_results)
        else:
            log.warning("triage_agent_max_turns", max_turns=self._max_turns)
            final_content = json.dumps(
                {
                    "error": f"Agent reached max_turns={self._max_turns}",
                    "partial_conversation_turns": turns,
                }
            )

        return build_report_from_conversation(
            final_content=final_content,
            messages=messages,
            started_at=started_at,
            completed_at=datetime.now(UTC),
        )

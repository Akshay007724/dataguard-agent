from __future__ import annotations

from typing import Any

from dataguard_adapters.base import OrchestratorAdapter
from dataguard_core.llm.client import LLMClient
from dataguard_core.logging import get_logger
from pipeline_sentinel.detectors.base import BaseDetector
from pipeline_sentinel.lineage.tracer import LineageTracer
from pipeline_sentinel.mcp_server.registry import get_agent_tools
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


# Canonical tool definitions imported from Sentinel single-source-of-truth registry
AGENT_TOOLS: list[dict[str, Any]] = get_agent_tools()

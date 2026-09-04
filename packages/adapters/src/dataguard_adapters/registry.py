from __future__ import annotations

import asyncio
from typing import Any

from dataguard_adapters.base import (
    OrchestratorAdapter,
    PipelineNotFoundError,
    PipelineSummary,
    RunStatus,
)
from dataguard_core.logging import get_logger

log = get_logger(__name__)


class AdapterRegistry:
    """Manages multi-orchestrator adapters and dispatches pipeline queries."""

    def __init__(self, adapters: list[OrchestratorAdapter] | None = None) -> None:
        self._adapters: dict[str, OrchestratorAdapter] = {}
        if adapters:
            for adapter in adapters:
                self.register(adapter)

    def register(self, adapter: OrchestratorAdapter) -> None:
        self._adapters[adapter.orchestrator_name] = adapter

    def get(self, name: str) -> OrchestratorAdapter | None:
        return self._adapters.get(name)

    def list_adapters(self) -> list[OrchestratorAdapter]:
        return list(self._adapters.values())

    async def find_adapter_for_pipeline(self, pipeline_id: str) -> OrchestratorAdapter | None:
        """Finds which adapter owns this pipeline_id by querying adapters concurrently."""
        for adapter in self._adapters.values():
            try:
                await adapter.get_pipeline(pipeline_id)
                return adapter
            except (PipelineNotFoundError, ValueError):
                continue
            except Exception as exc:
                log.debug("adapter_probe_error", adapter=adapter.orchestrator_name, error=str(exc))
                continue
        return None

    async def list_all_pipelines(
        self,
        tag: str | None = None,
        status: RunStatus | None = None,
        orchestrator: str | None = None,
    ) -> list[PipelineSummary]:
        """Queries registered adapters concurrently to collect all pipelines."""
        target_adapters = (
            [self._adapters[orchestrator]]
            if orchestrator and orchestrator in self._adapters
            else list(self._adapters.values())
        )

        async def _query_adapter(adapter: OrchestratorAdapter) -> list[PipelineSummary]:
            try:
                return await adapter.list_pipelines(tag=tag, status=status)
            except Exception as exc:
                log.warning("list_pipelines_adapter_error", adapter=adapter.orchestrator_name, error=str(exc))
                return []

        results = await asyncio.gather(*[_query_adapter(a) for a in target_adapters])
        combined: list[PipelineSummary] = []
        for subset in results:
            combined.extend(subset)
        return combined

    async def aclose(self) -> None:
        """Gracefully closes all registered adapters."""
        await asyncio.gather(*[adapter.aclose() for adapter in self._adapters.values()], return_exceptions=True)

    async def __aenter__(self) -> AdapterRegistry:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.aclose()

"""WatchdogAgent — periodic pipeline monitoring that auto-triggers triage.

Polls orchestrators on a configurable interval, detects new failures,
and runs TriageAgent only for pipelines with newly detected issues.
Already-triaged pipelines are debounced via Redis to avoid duplicate incidents.

Usage:
    ctx = AgentContext(...)
    watchdog = WatchdogAgent(ctx, poll_interval=300)
    await watchdog.run_forever()  # blocks; cancel with KeyboardInterrupt or SIGTERM
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

from dataguard_core.logging import get_logger
from dataguard_core.store import redis

from dataguard_agents.base import AgentContext
from dataguard_agents.triage import TriageAgent
from dataguard_agents.report import TriageReport

log = get_logger(__name__)

_TRIAGE_DEBOUNCE_KEY = "watchdog:triaged:"
_TRIAGE_DEBOUNCE_TTL = 3600  # 1 hour — don't re-triage same pipeline within this window


class WatchdogAgent:
    """Periodic pipeline health monitor.

    On each poll cycle:
    1. Queries all adapters for pipeline status
    2. Identifies pipelines that are newly failed/degraded
    3. Skips pipelines debounced from a recent triage pass
    4. Runs TriageAgent for each new failure
    5. Logs the triage report

    Args:
        ctx: Shared agent context.
        poll_interval: Seconds between polling cycles. Default 300 (5 min).
        debounce_ttl: Seconds before a triaged pipeline is eligible for re-triage.
    """

    def __init__(
        self,
        ctx: AgentContext,
        poll_interval: int = 300,
        debounce_ttl: int = _TRIAGE_DEBOUNCE_TTL,
    ) -> None:
        self._ctx = ctx
        self._poll_interval = poll_interval
        self._debounce_ttl = debounce_ttl
        self._running = False

    async def run_forever(self) -> None:
        """Block forever, polling on each interval. Cancel to stop."""
        self._running = True
        log.info("watchdog_start", poll_interval=self._poll_interval)

        while self._running:
            try:
                await self._poll_cycle()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error("watchdog_cycle_error", error=str(exc), exc_info=True)

            await asyncio.sleep(self._poll_interval)

        log.info("watchdog_stopped")

    async def run_once(self) -> list[TriageReport]:
        """Run a single poll cycle. Useful for testing and one-shot invocations."""
        return await self._poll_cycle()

    def stop(self) -> None:
        self._running = False

    async def _poll_cycle(self) -> list[TriageReport]:
        cycle_start = datetime.now(timezone.utc)
        log.info("watchdog_cycle_start", at=cycle_start.isoformat())

        failing: list[str] = []
        for adapter in self._ctx.adapters:
            try:
                from dataguard_adapters.base import RunStatus
                pipelines = await adapter.list_pipelines(status=RunStatus.FAILED)
                failing.extend(p.id for p in pipelines)
            except Exception as exc:
                log.warning("watchdog_adapter_error", adapter=adapter.orchestrator_name, error=str(exc))

        if not failing:
            log.info("watchdog_no_failures")
            return []

        # Debounce: skip pipelines triaged recently
        new_failures = [p for p in failing if not await self._is_debounced(p)]

        if not new_failures:
            log.info("watchdog_all_debounced", count=len(failing))
            return []

        log.info("watchdog_new_failures", count=len(new_failures), pipelines=new_failures)

        reports: list[TriageReport] = []
        agent = TriageAgent(self._ctx)

        for pipeline_id in new_failures:
            try:
                report = await agent.run(scope=pipeline_id)
                reports.append(report)
                await self._mark_debounced(pipeline_id)
                log.info(
                    "watchdog_triage_done",
                    pipeline_id=pipeline_id,
                    incidents=len(report.incidents_filed),
                )
            except Exception as exc:
                log.error("watchdog_triage_error", pipeline_id=pipeline_id, error=str(exc), exc_info=True)

        return reports

    async def _is_debounced(self, pipeline_id: str) -> bool:
        value = await redis.cache_get(f"{_TRIAGE_DEBOUNCE_KEY}{pipeline_id}")
        return value is not None

    async def _mark_debounced(self, pipeline_id: str) -> None:
        await redis.cache_set(
            f"{_TRIAGE_DEBOUNCE_KEY}{pipeline_id}",
            {"triaged_at": datetime.now(timezone.utc).isoformat()},
            ttl=self._debounce_ttl,
        )

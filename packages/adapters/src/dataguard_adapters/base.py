from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Self


class AdapterError(Exception):
    """Base exception for orchestrator adapter operations."""


class PipelineNotFoundError(AdapterError):
    """Raised when a requested pipeline/DAG does not exist."""


class RunNotFoundError(AdapterError):
    """Raised when a requested run does not exist."""


class OrchestratorConnectionError(AdapterError):
    """Raised when communication with an orchestrator fails."""


class RunStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    QUEUED = "queued"
    SKIPPED = "skipped"
    UPSTREAM_FAILED = "upstream_failed"
    UNKNOWN = "unknown"


@dataclass
class RunDetails:
    run_id: str
    pipeline_id: str
    status: RunStatus
    started_at: datetime | None
    ended_at: datetime | None
    duration_seconds: float | None
    error_message: str | None
    failing_task: str | None
    retry_number: int
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass
class PipelineSummary:
    id: str
    name: str
    orchestrator: str
    owner: str | None
    tags: list[str]
    last_run_status: RunStatus | None
    last_run_at: datetime | None
    schedule: str | None
    is_paused: bool


class OrchestratorAdapter(ABC):
    """Read-only interface to a pipeline orchestrator.

    All methods are async. Implementations must not write to or trigger
    anything in the orchestrator — read access only.
    """

    @property
    @abstractmethod
    def orchestrator_name(self) -> str:
        """Stable identifier, e.g. 'airflow', 'argo'."""

    @abstractmethod
    async def list_pipelines(
        self,
        tag: str | None = None,
        status: RunStatus | None = None,
    ) -> list[PipelineSummary]:
        """Return all pipelines visible to this adapter."""

    @abstractmethod
    async def get_pipeline(self, pipeline_id: str) -> PipelineSummary:
        """Return metadata for a single pipeline.

        Raises:
            PipelineNotFoundError: If the pipeline does not exist.
        """

    @abstractmethod
    async def get_run(self, pipeline_id: str, run_id: str) -> RunDetails:
        """Return details for a specific run.

        Raises:
            RunNotFoundError: If the run does not exist.
        """

    @abstractmethod
    async def get_latest_run(self, pipeline_id: str) -> RunDetails:
        """Return details for the most recent run.

        Raises:
            RunNotFoundError: If the pipeline has no runs.
        """

    @abstractmethod
    async def get_run_history(self, pipeline_id: str, limit: int = 10) -> list[RunDetails]:
        """Return the N most recent runs, newest first."""

    @abstractmethod
    async def get_run_logs(
        self,
        pipeline_id: str,
        run_id: str,
        task_id: str | None = None,
        head_lines: int = 50,
        tail_lines: int = 100,
    ) -> str:
        """Return a log excerpt for a run."""

    async def aclose(self) -> None:  # noqa: B027
        """Release any network connections and client sessions."""

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.aclose()

    @staticmethod
    def _trim_log(raw: str, head: int, tail: int) -> str:
        lines = raw.splitlines()
        if len(lines) <= head + tail:
            return raw
        omitted = len(lines) - head - tail
        return "\n".join([*lines[:head], f"... [{omitted} lines omitted] ...", *lines[-tail:]])

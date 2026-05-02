from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum


class DetectorSeverity(StrEnum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class CheckResult:
    name: str
    passed: bool
    severity: DetectorSeverity
    message: str
    actual: str | float | None = None
    expected: str | float | None = None
    sample_rows: list[dict] = field(default_factory=list)  # type: ignore[type-arg]


@dataclass
class DetectorResult:
    detector: str
    dataset_id: str
    checks: list[CheckResult]

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def failed_checks(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed]

    @property
    def highest_severity(self) -> DetectorSeverity | None:
        order = [DetectorSeverity.CRITICAL, DetectorSeverity.HIGH, DetectorSeverity.MEDIUM, DetectorSeverity.LOW, DetectorSeverity.INFO]
        for sev in order:
            if any(c.severity == sev and not c.passed for c in self.checks):
                return sev
        return None


class BaseDetector(ABC):
    """Base class for all data quality detectors.

    Subclasses implement run() and return a DetectorResult. Detectors
    are stateless — all config comes via __init__.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable detector identifier, e.g. 'schema_drift'."""

    @abstractmethod
    async def run(self, dataset_id: str) -> DetectorResult:
        """Execute quality checks against the given dataset.

        Args:
            dataset_id: Logical dataset identifier (namespace/name format for OpenLineage).

        Returns:
            DetectorResult with one CheckResult per check performed.
        """

from __future__ import annotations

from pipeline_sentinel.detectors.base import BaseDetector, DetectorResult


class CustomSQLDetector(BaseDetector):
    """Custom SQL quality check detector."""

    def __init__(self, query: str = "", name: str = "custom_sql") -> None:
        self._query = query
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def run(self, dataset_id: str) -> DetectorResult:
        return DetectorResult(detector=self.name, dataset_id=dataset_id, checks=[])

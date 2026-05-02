from __future__ import annotations

from datetime import datetime, timezone

import httpx

from dataguard_core.logging import get_logger
from dataguard_core.metrics import detector_duration

from pipeline_sentinel.detectors.base import BaseDetector, CheckResult, DetectorResult, DetectorSeverity

log = get_logger(__name__)


class FreshnessDetector(BaseDetector):
    """Checks that a dataset was updated within an expected time window.

    Queries Marquez for the dataset's last modified time and compares
    against a configured SLA in hours.

    Args:
        marquez_url: Marquez API base URL.
        namespace: OpenLineage namespace.
        freshness_sla_hours: Maximum acceptable age in hours before flagging.
    """

    def __init__(
        self,
        marquez_url: str,
        namespace: str = "default",
        freshness_sla_hours: float = 24.0,
    ) -> None:
        self._marquez_url = marquez_url.rstrip("/")
        self._namespace = namespace
        self._sla_hours = freshness_sla_hours

    @property
    def name(self) -> str:
        return "freshness"

    async def run(self, dataset_id: str) -> DetectorResult:
        with detector_duration.labels(detector=self.name).time():
            return await self._run(dataset_id)

    async def _run(self, dataset_id: str) -> DetectorResult:
        last_modified = await self._fetch_last_modified(dataset_id)

        if last_modified is None:
            return DetectorResult(
                detector=self.name,
                dataset_id=dataset_id,
                checks=[
                    CheckResult(
                        name="freshness_available",
                        passed=False,
                        severity=DetectorSeverity.HIGH,
                        message=f"Cannot determine freshness for {dataset_id} — not registered in Marquez or no runs recorded",
                    )
                ],
            )

        now = datetime.now(timezone.utc)
        if last_modified.tzinfo is None:
            last_modified = last_modified.replace(tzinfo=timezone.utc)

        age_hours = (now - last_modified).total_seconds() / 3600
        sla_met = age_hours <= self._sla_hours

        return DetectorResult(
            detector=self.name,
            dataset_id=dataset_id,
            checks=[
                CheckResult(
                    name="freshness_sla",
                    passed=sla_met,
                    severity=self._severity(age_hours, self._sla_hours),
                    message=(
                        f"Dataset is fresh: last updated {age_hours:.1f}h ago (SLA: {self._sla_hours}h)"
                        if sla_met
                        else f"Freshness SLA breached: last updated {age_hours:.1f}h ago (SLA: {self._sla_hours}h)"
                    ),
                    actual=f"{age_hours:.1f}h",
                    expected=f"≤ {self._sla_hours}h",
                )
            ],
        )

    async def _fetch_last_modified(self, dataset_id: str) -> datetime | None:
        url = f"{self._marquez_url}/api/v1/namespaces/{self._namespace}/datasets/{dataset_id}"
        async with httpx.AsyncClient(timeout=10) as client:
            try:
                resp = await client.get(url)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                data = resp.json()
                updated_at = data.get("updatedAt")
                if updated_at:
                    return datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                return None
            except httpx.HTTPError as exc:
                log.warning("marquez_freshness_fetch_failed", dataset_id=dataset_id, error=str(exc))
                return None

    @staticmethod
    def _severity(age_hours: float, sla_hours: float) -> DetectorSeverity:
        ratio = age_hours / sla_hours
        if ratio >= 3:
            return DetectorSeverity.CRITICAL
        if ratio >= 2:
            return DetectorSeverity.HIGH
        if ratio >= 1:
            return DetectorSeverity.MEDIUM
        return DetectorSeverity.INFO

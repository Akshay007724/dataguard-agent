from __future__ import annotations

import json

import httpx

from dataguard_core.logging import get_logger
from dataguard_core.metrics import detector_duration
from dataguard_core.store import redis

from pipeline_sentinel.detectors.base import BaseDetector, CheckResult, DetectorResult, DetectorSeverity

log = get_logger(__name__)

_SCHEMA_CACHE_PREFIX = "schema:"
_SCHEMA_CACHE_TTL = 3600  # 1 hour — we want to detect drift, not mask it


class SchemaDriftDetector(BaseDetector):
    """Detects column additions, removals, and type changes against a cached baseline.

    The first time a dataset is seen, its schema is recorded as the baseline.
    Subsequent runs compare against that baseline. Drift is flagged if columns
    are added, removed, or change type.

    Args:
        marquez_url: Marquez API base URL for fetching dataset schemas.
        namespace: OpenLineage namespace to query.
    """

    def __init__(self, marquez_url: str, namespace: str = "default") -> None:
        self._marquez_url = marquez_url.rstrip("/")
        self._namespace = namespace

    @property
    def name(self) -> str:
        return "schema_drift"

    async def run(self, dataset_id: str) -> DetectorResult:
        with detector_duration.labels(detector=self.name).time():
            return await self._run(dataset_id)

    async def _run(self, dataset_id: str) -> DetectorResult:
        current_schema = await self._fetch_current_schema(dataset_id)
        if current_schema is None:
            return DetectorResult(
                detector=self.name,
                dataset_id=dataset_id,
                checks=[
                    CheckResult(
                        name="schema_available",
                        passed=False,
                        severity=DetectorSeverity.HIGH,
                        message=f"Schema not available for {dataset_id} — not registered in Marquez",
                    )
                ],
            )

        cache_key = f"{_SCHEMA_CACHE_PREFIX}{dataset_id}"
        baseline = await redis.cache_get(cache_key)

        if baseline is None:
            # First observation — store as baseline, pass
            await redis.cache_set(cache_key, current_schema, ttl=_SCHEMA_CACHE_TTL)
            log.info("schema_baseline_recorded", dataset_id=dataset_id, columns=len(current_schema))
            return DetectorResult(
                detector=self.name,
                dataset_id=dataset_id,
                checks=[
                    CheckResult(
                        name="schema_baseline",
                        passed=True,
                        severity=DetectorSeverity.INFO,
                        message="Schema baseline recorded. Will detect drift on subsequent runs.",
                        actual=str(len(current_schema)),
                        expected=None,
                    )
                ],
            )

        checks = self._compare_schemas(dataset_id, baseline, current_schema)
        return DetectorResult(detector=self.name, dataset_id=dataset_id, checks=checks)

    async def _fetch_current_schema(self, dataset_id: str) -> dict[str, str] | None:
        """Returns {column_name: type} dict from Marquez."""
        url = f"{self._marquez_url}/api/v1/namespaces/{self._namespace}/datasets/{dataset_id}"
        async with httpx.AsyncClient(timeout=10) as client:
            try:
                resp = await client.get(url)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                data = resp.json()
                fields = data.get("fields") or []
                return {f["name"]: f.get("type", "unknown") for f in fields}
            except httpx.HTTPError as exc:
                log.warning("marquez_schema_fetch_failed", dataset_id=dataset_id, error=str(exc))
                return None

    @staticmethod
    def _compare_schemas(
        dataset_id: str,
        baseline: dict[str, str],
        current: dict[str, str],
    ) -> list[CheckResult]:
        checks: list[CheckResult] = []

        removed = set(baseline) - set(current)
        added = set(current) - set(baseline)
        type_changed = {
            col for col in (set(baseline) & set(current))
            if baseline[col] != current[col]
        }

        if not removed and not added and not type_changed:
            checks.append(CheckResult(
                name="schema_unchanged",
                passed=True,
                severity=DetectorSeverity.INFO,
                message=f"Schema matches baseline ({len(current)} columns)",
            ))
            return checks

        if removed:
            checks.append(CheckResult(
                name="columns_removed",
                passed=False,
                severity=DetectorSeverity.CRITICAL,
                message=f"Columns removed from {dataset_id}: {sorted(removed)}",
                actual=str(sorted(set(current.keys()))),
                expected=str(sorted(set(baseline.keys()))),
            ))

        if added:
            checks.append(CheckResult(
                name="columns_added",
                passed=False,
                severity=DetectorSeverity.MEDIUM,
                message=f"New columns in {dataset_id}: {sorted(added)}",
                actual=str(sorted(added)),
                expected="(no new columns)",
            ))

        for col in sorted(type_changed):
            checks.append(CheckResult(
                name=f"type_changed_{col}",
                passed=False,
                severity=DetectorSeverity.HIGH,
                message=f"Column {col!r} type changed: {baseline[col]!r} → {current[col]!r}",
                actual=current[col],
                expected=baseline[col],
            ))

        return checks

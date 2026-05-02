from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from dataguard_core.logging import get_logger
from dataguard_core.metrics import detector_duration
from dataguard_core.store import redis

from pipeline_sentinel.detectors.base import BaseDetector, CheckResult, DetectorResult, DetectorSeverity

log = get_logger(__name__)

_VOLUME_HISTORY_KEY = "volume_history:"
_VOLUME_HISTORY_TTL = 86400 * 30  # keep 30 days of history
_MIN_HISTORY_POINTS = 5  # require at least this many points before flagging


class VolumeAnomalyDetector(BaseDetector):
    """Detects row-count anomalies using a rolling z-score against historical baselines.

    Row counts are pushed to the detector via record_volume() (called from the
    OpenLineage event handler). The detector then compares the latest count to the
    rolling mean ± (threshold * stddev).

    Args:
        z_score_threshold: Number of standard deviations before flagging. Default 3.0.
    """

    def __init__(self, z_score_threshold: float = 3.0) -> None:
        self._threshold = z_score_threshold

    @property
    def name(self) -> str:
        return "volume_anomaly"

    async def run(self, dataset_id: str) -> DetectorResult:
        with detector_duration.labels(detector=self.name).time():
            return await self._run(dataset_id)

    async def _run(self, dataset_id: str) -> DetectorResult:
        history = await self._load_history(dataset_id)

        if len(history) < _MIN_HISTORY_POINTS:
            return DetectorResult(
                detector=self.name,
                dataset_id=dataset_id,
                checks=[
                    CheckResult(
                        name="volume_history_insufficient",
                        passed=True,
                        severity=DetectorSeverity.INFO,
                        message=f"Only {len(history)} volume observations — need {_MIN_HISTORY_POINTS} before anomaly detection activates",
                    )
                ],
            )

        counts = [h["count"] for h in history]
        latest = counts[-1]
        baseline = counts[:-1]

        mean = sum(baseline) / len(baseline)
        variance = sum((x - mean) ** 2 for x in baseline) / len(baseline)
        stddev = variance ** 0.5

        if stddev == 0:
            # All historical values identical — any deviation is anomalous
            if latest != mean:
                z = float("inf")
            else:
                return DetectorResult(
                    detector=self.name,
                    dataset_id=dataset_id,
                    checks=[
                        CheckResult(
                            name="volume_stable",
                            passed=True,
                            severity=DetectorSeverity.INFO,
                            message=f"Row count stable at {latest:,}",
                            actual=str(latest),
                            expected=str(int(mean)),
                        )
                    ],
                )
        else:
            z = abs(latest - mean) / stddev

        passed = z <= self._threshold
        pct_change = ((latest - mean) / mean * 100) if mean > 0 else 0.0

        return DetectorResult(
            detector=self.name,
            dataset_id=dataset_id,
            checks=[
                CheckResult(
                    name="volume_z_score",
                    passed=passed,
                    severity=self._severity(z),
                    message=(
                        f"Row count {latest:,} is within normal range (z={z:.2f})"
                        if passed
                        else f"Row count anomaly: {latest:,} rows (z={z:.2f}, {pct_change:+.1f}% vs mean {mean:,.0f})"
                    ),
                    actual=str(latest),
                    expected=f"{mean:.0f} ± {stddev:.0f}",
                )
            ],
        )

    async def record_volume(self, dataset_id: str, row_count: int) -> None:
        """Record a row count observation. Called from OpenLineage event handler."""
        history = await self._load_history(dataset_id)
        history.append({"count": row_count, "ts": datetime.now(timezone.utc).isoformat()})
        # Keep last 90 observations
        history = history[-90:]
        key = f"{_VOLUME_HISTORY_KEY}{dataset_id}"
        await redis.cache_set(key, history, ttl=_VOLUME_HISTORY_TTL)

    @staticmethod
    async def _load_history(dataset_id: str) -> list[dict]:  # type: ignore[type-arg]
        key = f"{_VOLUME_HISTORY_KEY}{dataset_id}"
        data = await redis.cache_get(key)
        return data if isinstance(data, list) else []

    @staticmethod
    def _severity(z: float) -> DetectorSeverity:
        if z > 6:
            return DetectorSeverity.CRITICAL
        if z > 4:
            return DetectorSeverity.HIGH
        if z > 3:
            return DetectorSeverity.MEDIUM
        return DetectorSeverity.LOW

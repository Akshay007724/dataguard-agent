from __future__ import annotations

import pytest

from pipeline_sentinel.detectors.base import DetectorSeverity
from pipeline_sentinel.detectors.schema_drift import SchemaDriftDetector
from pipeline_sentinel.detectors.volume_anomaly import VolumeAnomalyDetector


class TestSchemaDriftCompare:
    def test_no_drift_returns_single_pass_check(self) -> None:
        checks = SchemaDriftDetector._compare_schemas(
            "ds1",
            {"id": "INTEGER", "name": "VARCHAR"},
            {"id": "INTEGER", "name": "VARCHAR"},
        )
        assert len(checks) == 1
        assert checks[0].passed
        assert checks[0].name == "schema_unchanged"

    def test_column_removed_is_critical(self) -> None:
        checks = SchemaDriftDetector._compare_schemas(
            "ds1",
            {"id": "INTEGER", "revenue": "DECIMAL"},
            {"id": "INTEGER"},
        )
        failed = [c for c in checks if not c.passed]
        assert any(c.name == "columns_removed" for c in failed)
        assert any(c.severity == DetectorSeverity.CRITICAL for c in failed)

    def test_column_added_is_medium(self) -> None:
        checks = SchemaDriftDetector._compare_schemas(
            "ds1",
            {"id": "INTEGER"},
            {"id": "INTEGER", "new_col": "TEXT"},
        )
        failed = [c for c in checks if not c.passed]
        assert any(c.name == "columns_added" for c in failed)
        assert any(c.severity == DetectorSeverity.MEDIUM for c in failed)

    def test_type_changed_is_high(self) -> None:
        checks = SchemaDriftDetector._compare_schemas(
            "ds1",
            {"id": "INTEGER"},
            {"id": "VARCHAR"},
        )
        failed = [c for c in checks if not c.passed]
        assert any("type_changed_id" in c.name for c in failed)
        assert any(c.severity == DetectorSeverity.HIGH for c in failed)

    def test_multiple_drift_types_all_reported(self) -> None:
        checks = SchemaDriftDetector._compare_schemas(
            "ds1",
            {"id": "INTEGER", "old_col": "TEXT"},
            {"id": "BIGINT", "new_col": "TEXT"},
        )
        names = {c.name for c in checks}
        assert "columns_removed" in names
        assert "columns_added" in names
        assert "type_changed_id" in names

    def test_empty_baseline_all_added(self) -> None:
        checks = SchemaDriftDetector._compare_schemas("ds1", {}, {"a": "INT", "b": "TEXT"})
        assert any(c.name == "columns_added" for c in checks)

    def test_empty_current_all_removed(self) -> None:
        checks = SchemaDriftDetector._compare_schemas("ds1", {"a": "INT"}, {})
        assert any(c.name == "columns_removed" for c in checks)
        assert any(c.severity == DetectorSeverity.CRITICAL for c in checks)

    def test_check_message_contains_dataset_id(self) -> None:
        checks = SchemaDriftDetector._compare_schemas("my_dataset", {"x": "INT"}, {})
        assert any("my_dataset" in c.message for c in checks)


class TestVolumeAnomalySeverity:
    def test_low_below_3(self) -> None:
        assert VolumeAnomalyDetector._severity(2.9) == DetectorSeverity.LOW

    def test_low_at_3(self) -> None:
        assert VolumeAnomalyDetector._severity(3.0) == DetectorSeverity.LOW

    def test_medium_above_3(self) -> None:
        assert VolumeAnomalyDetector._severity(3.1) == DetectorSeverity.MEDIUM

    def test_medium_at_4(self) -> None:
        assert VolumeAnomalyDetector._severity(4.0) == DetectorSeverity.MEDIUM

    def test_high_above_4(self) -> None:
        assert VolumeAnomalyDetector._severity(4.5) == DetectorSeverity.HIGH

    def test_high_at_6(self) -> None:
        assert VolumeAnomalyDetector._severity(6.0) == DetectorSeverity.HIGH

    def test_critical_above_6(self) -> None:
        assert VolumeAnomalyDetector._severity(6.1) == DetectorSeverity.CRITICAL

    def test_critical_infinity(self) -> None:
        assert VolumeAnomalyDetector._severity(float("inf")) == DetectorSeverity.CRITICAL

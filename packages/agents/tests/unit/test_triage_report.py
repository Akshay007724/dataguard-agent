from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from dataguard_agents.report import TriageReport, _extract_incident_ids, build_report_from_conversation

_START = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
_END = datetime(2024, 6, 1, 10, 5, 0, tzinfo=timezone.utc)


def _build(content: str, messages: list[dict] | None = None) -> TriageReport:
    return build_report_from_conversation(
        final_content=content,
        messages=messages or [],
        started_at=_START,
        completed_at=_END,
    )


class TestBuildReportFromConversation:
    def test_valid_json_parsed_correctly(self) -> None:
        payload = {
            "pipelines_checked": 3,
            "failures_found": 2,
            "incidents_filed": ["INC-001", "INC-002"],
            "diagnoses": [
                {
                    "pipeline_id": "pipe-a",
                    "root_cause": "oom",
                    "confidence": 0.95,
                    "severity": "critical",
                    "remediation_id": "REM-1",
                    "incident_id": "INC-001",
                }
            ],
            "summary": "Two pipelines failed due to OOM.",
        }
        report = _build(json.dumps(payload))
        assert report.pipelines_checked == 3
        assert report.failures_found == 2
        assert report.incidents_filed == ["INC-001", "INC-002"]
        assert len(report.diagnoses) == 1
        d = report.diagnoses[0]
        assert d.pipeline_id == "pipe-a"
        assert d.root_cause == "oom"
        assert d.confidence == 0.95
        assert d.severity == "critical"
        assert d.remediation_id == "REM-1"
        assert d.incident_id == "INC-001"
        assert report.summary == "Two pipelines failed due to OOM."

    def test_json_in_markdown_json_block(self) -> None:
        payload = {"pipelines_checked": 1, "failures_found": 0, "incidents_filed": [], "diagnoses": [], "summary": "ok"}
        report = _build(f"```json\n{json.dumps(payload)}\n```")
        assert report.pipelines_checked == 1

    def test_json_in_plain_code_block(self) -> None:
        payload = {"pipelines_checked": 2, "failures_found": 0, "incidents_filed": [], "diagnoses": [], "summary": "ok"}
        report = _build(f"```\n{json.dumps(payload)}\n```")
        assert report.pipelines_checked == 2

    def test_invalid_json_fallback_to_defaults(self) -> None:
        report = _build("This pipeline is misbehaving.")
        assert report.pipelines_checked == 0
        assert report.failures_found == 0
        assert report.diagnoses == []

    def test_invalid_json_summary_uses_content(self) -> None:
        content = "Just a plain text message from the agent."
        report = _build(content)
        assert report.summary == content[:500]

    def test_duration_seconds_computed(self) -> None:
        assert _build("{}").duration_seconds == 300.0

    def test_failures_found_defaults_to_diagnoses_count(self) -> None:
        payload = {
            "pipelines_checked": 2,
            "incidents_filed": [],
            "diagnoses": [
                {"pipeline_id": "a", "root_cause": "oom", "confidence": 0.9, "severity": "high"},
                {"pipeline_id": "b", "root_cause": "schema_drift", "confidence": 0.8, "severity": "medium"},
            ],
            "summary": "two failures",
        }
        report = _build(json.dumps(payload))
        assert report.failures_found == 2

    def test_incidents_extracted_from_messages_when_json_empty(self) -> None:
        messages = [{"role": "tool", "content": json.dumps({"incident_id": "INC-999"})}]
        report = _build("not json", messages=messages)
        assert "INC-999" in report.incidents_filed

    def test_raw_agent_output_preserved(self) -> None:
        report = _build("raw output string")
        assert report.raw_agent_output == "raw output string"

    def test_timestamps_stored(self) -> None:
        report = _build("{}")
        assert report.triage_started_at == _START
        assert report.triage_completed_at == _END

    def test_empty_content_uses_no_summary_placeholder(self) -> None:
        report = _build("")
        assert report.summary == "(no summary)"


class TestExtractIncidentIds:
    def test_extracts_from_tool_messages(self) -> None:
        messages = [
            {"role": "tool", "content": json.dumps({"incident_id": "INC-1"})},
            {"role": "tool", "content": json.dumps({"incident_id": "INC-2"})},
        ]
        assert _extract_incident_ids(messages) == ["INC-1", "INC-2"]

    def test_ignores_non_tool_roles(self) -> None:
        messages = [
            {"role": "assistant", "content": json.dumps({"incident_id": "INC-X"})},
            {"role": "user", "content": json.dumps({"incident_id": "INC-Y"})},
        ]
        assert _extract_incident_ids(messages) == []

    def test_ignores_tool_messages_without_incident_id(self) -> None:
        messages = [{"role": "tool", "content": json.dumps({"diagnosis_id": "DIAG-1", "status": "ok"})}]
        assert _extract_incident_ids(messages) == []

    def test_handles_invalid_json_gracefully(self) -> None:
        messages = [{"role": "tool", "content": "this is not json"}]
        assert _extract_incident_ids(messages) == []

    def test_empty_messages(self) -> None:
        assert _extract_incident_ids([]) == []

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class DiagnosisSummary:
    pipeline_id: str
    root_cause: str
    confidence: float
    severity: str
    remediation_id: str | None = None
    incident_id: str | None = None


@dataclass
class TriageReport:
    triage_completed_at: datetime
    triage_started_at: datetime
    pipelines_checked: int
    failures_found: int
    incidents_filed: list[str]
    diagnoses: list[DiagnosisSummary]
    summary: str
    raw_agent_output: str = ""

    @property
    def duration_seconds(self) -> float:
        return (self.triage_completed_at - self.triage_started_at).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        return {
            "triage_completed_at": self.triage_completed_at.isoformat(),
            "triage_started_at": self.triage_started_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "pipelines_checked": self.pipelines_checked,
            "failures_found": self.failures_found,
            "incidents_filed": self.incidents_filed,
            "diagnoses": [
                {
                    "pipeline_id": d.pipeline_id,
                    "root_cause": d.root_cause,
                    "confidence": d.confidence,
                    "severity": d.severity,
                    "remediation_id": d.remediation_id,
                    "incident_id": d.incident_id,
                }
                for d in self.diagnoses
            ],
            "summary": self.summary,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


def build_report_from_conversation(
    final_content: str,
    messages: list[dict[str, Any]],
    started_at: datetime,
    completed_at: datetime,
) -> TriageReport:
    """Parse the agent's final JSON output into a TriageReport.

    Falls back to extracting data from the conversation history
    if the agent's final message is not valid JSON.
    """
    # Try to parse the agent's structured JSON output
    parsed: dict[str, Any] = {}
    try:
        # Agent may embed JSON in markdown code blocks
        content = final_content.strip()
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        parsed = json.loads(content)
    except (json.JSONDecodeError, IndexError):
        pass

    diagnoses = [
        DiagnosisSummary(
            pipeline_id=d.get("pipeline_id", ""),
            root_cause=d.get("root_cause", "unknown"),
            confidence=float(d.get("confidence", 0.0)),
            severity=d.get("severity", "unknown"),
            remediation_id=d.get("remediation_id"),
            incident_id=d.get("incident_id"),
        )
        for d in parsed.get("diagnoses", [])
    ]

    # If agent JSON is empty, extract incidents from tool call results
    incidents_filed = parsed.get("incidents_filed", [])
    if not incidents_filed:
        incidents_filed = _extract_incident_ids(messages)

    return TriageReport(
        triage_completed_at=completed_at,
        triage_started_at=started_at,
        pipelines_checked=parsed.get("pipelines_checked", 0),
        failures_found=parsed.get("failures_found", len(diagnoses)),
        incidents_filed=incidents_filed,
        diagnoses=diagnoses,
        summary=parsed.get("summary", final_content[:500] if final_content else "(no summary)"),
        raw_agent_output=final_content,
    )


def _extract_incident_ids(messages: list[dict[str, Any]]) -> list[str]:
    """Scan tool results for filed incident IDs when agent JSON is unavailable."""
    ids: list[str] = []
    for msg in messages:
        if msg.get("role") == "tool":
            try:
                data = json.loads(msg.get("content", "{}"))
                if "incident_id" in data:
                    ids.append(data["incident_id"])
            except (json.JSONDecodeError, TypeError):
                pass
    return ids

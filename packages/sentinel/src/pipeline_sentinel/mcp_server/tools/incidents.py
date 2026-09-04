from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from dataguard_core.logging import get_logger
from dataguard_core.store.repositories import IncidentRepository

log = get_logger(__name__)
_incident_repo = IncidentRepository()


async def handle_get_recent_incidents(arguments: dict[str, Any]) -> str:
    time_window_str: str = arguments.get("time_window", "24h")
    severity_filter: str | None = arguments.get("severity")
    status_filter: str | None = arguments.get("status")

    window_hours = _parse_time_window(time_window_str)
    since = datetime.now(UTC) - timedelta(hours=window_hours)

    rows = await _incident_repo.get_recent_incidents(
        since=since,
        severity=severity_filter,
        status=status_filter,
        limit=50,
    )

    incidents = [
        {
            "id": row.id,
            "title": row.title,
            "pipeline_id": row.pipeline_id,
            "severity": row.severity,
            "status": row.status,
            "root_cause_category": row.root_cause_category,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None,
            "resolution": row.resolution,
        }
        for row in rows
    ]

    return json.dumps(
        {
            "time_window": time_window_str,
            "total": len(incidents),
            "incidents": incidents,
        }
    )


async def handle_file_incident(arguments: dict[str, Any]) -> str:
    title: str = arguments["title"]
    pipeline_id: str = arguments["pipeline_id"]
    severity: str = arguments["severity"]
    description: str = arguments["description"]
    diagnosis_id: str | None = arguments.get("diagnosis_id")
    root_cause_category: str | None = arguments.get("root_cause_category")

    row = await _incident_repo.file_incident(
        title=title,
        pipeline_id=pipeline_id,
        severity=severity,
        description=description,
        diagnosis_id=diagnosis_id,
        root_cause_category=root_cause_category,
    )

    log.info("incident_filed", incident_id=row.id, pipeline_id=pipeline_id, severity=severity)

    return json.dumps(
        {
            "incident_id": row.id,
            "title": title,
            "pipeline_id": pipeline_id,
            "severity": severity,
            "status": row.status,
            "message": f"Incident {row.id} created successfully",
            "integrations": {
                "jira": None,
                "pagerduty": None,
                "slack": None,
                "note": "External integrations available in v0.2",
            },
        }
    )


def _parse_time_window(window: str) -> float:
    """Parse '24h', '7d', '30m' into hours."""
    window = window.strip().lower()
    if window.endswith("h"):
        return float(window[:-1])
    if window.endswith("d"):
        return float(window[:-1]) * 24
    if window.endswith("m"):
        return float(window[:-1]) / 60
    try:
        return float(window)
    except ValueError:
        return 24.0

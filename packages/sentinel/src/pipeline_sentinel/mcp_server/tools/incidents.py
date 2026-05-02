from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select

from dataguard_core.logging import get_logger
from dataguard_core.store.postgres import IncidentRow, get_session

log = get_logger(__name__)


async def handle_get_recent_incidents(arguments: dict[str, Any]) -> str:
    time_window_str: str = arguments.get("time_window", "24h")
    severity_filter: str | None = arguments.get("severity")
    status_filter: str | None = arguments.get("status")

    window_hours = _parse_time_window(time_window_str)
    since = datetime.now(timezone.utc) - timedelta(hours=window_hours)

    async with get_session() as session:
        stmt = select(IncidentRow).where(IncidentRow.created_at >= since)
        if severity_filter:
            stmt = stmt.where(IncidentRow.severity == severity_filter)
        if status_filter:
            stmt = stmt.where(IncidentRow.status == status_filter)
        stmt = stmt.order_by(IncidentRow.created_at.desc()).limit(50)

        result = await session.execute(stmt)
        rows = result.scalars().all()

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

    return json.dumps({
        "time_window": time_window_str,
        "total": len(incidents),
        "incidents": incidents,
    })


async def handle_file_incident(arguments: dict[str, Any]) -> str:
    title: str = arguments["title"]
    pipeline_id: str = arguments["pipeline_id"]
    severity: str = arguments["severity"]
    description: str = arguments["description"]
    diagnosis_id: str | None = arguments.get("diagnosis_id")
    root_cause_category: str | None = arguments.get("root_cause_category")

    incident_id = f"INC-{uuid.uuid4().hex[:8].upper()}"

    async with get_session() as session:
        row = IncidentRow(
            id=incident_id,
            title=title,
            pipeline_id=pipeline_id,
            severity=severity,
            status="open",
            description=description,
            diagnosis_id=diagnosis_id,
            root_cause_category=root_cause_category,
        )
        session.add(row)

    log.info("incident_filed", incident_id=incident_id, pipeline_id=pipeline_id, severity=severity)

    return json.dumps({
        "incident_id": incident_id,
        "title": title,
        "pipeline_id": pipeline_id,
        "severity": severity,
        "status": "open",
        "message": f"Incident {incident_id} created successfully",
        "integrations": {
            "jira": None,
            "pagerduty": None,
            "slack": None,
            "note": "External integrations available in v0.2",
        },
    })


def _parse_time_window(window: str) -> float:
    """Parse '24h', '7d', '30m' into hours."""
    window = window.strip().lower()
    if window.endswith("h"):
        return float(window[:-1])
    if window.endswith("d"):
        return float(window[:-1]) * 24
    if window.endswith("m"):
        return float(window[:-1]) / 60
    return 24.0  # default

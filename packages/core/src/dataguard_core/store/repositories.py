from __future__ import annotations

from datetime import datetime

from sqlalchemy import select

from dataguard_core.store.postgres import (
    IncidentRow,
    RemediationAuditRow,
    RemediationPlanRow,
    get_session,
)


class IncidentRepository:
    """Encapsulates all database operations for incidents."""

    async def get_recent_incidents(
        self,
        since: datetime,
        severity: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[IncidentRow]:
        async with get_session() as session:
            stmt = select(IncidentRow).where(IncidentRow.created_at >= since)
            if severity:
                stmt = stmt.where(IncidentRow.severity == severity)
            if status:
                stmt = stmt.where(IncidentRow.status == status)
            stmt = stmt.order_by(IncidentRow.created_at.desc()).limit(limit)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def file_incident(
        self,
        title: str,
        pipeline_id: str,
        severity: str,
        description: str,
        diagnosis_id: str | None = None,
        root_cause_category: str | None = None,
    ) -> IncidentRow:
        async with get_session() as session:
            incident = IncidentRow(
                title=title,
                pipeline_id=pipeline_id,
                severity=severity,
                description=description,
                diagnosis_id=diagnosis_id,
                root_cause_category=root_cause_category,
            )
            session.add(incident)
            await session.flush()
            # session commit is handled by get_session()
            return incident

    async def get_incident(self, incident_id: str) -> IncidentRow | None:
        async with get_session() as session:
            stmt = select(IncidentRow).where(IncidentRow.id == incident_id)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()


class RemediationRepository:
    """Encapsulates all database operations for remediation plans."""

    async def get_plan(self, plan_id: str) -> RemediationPlanRow | None:
        async with get_session() as session:
            stmt = select(RemediationPlanRow).where(RemediationPlanRow.id == plan_id)
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def save_plan(self, plan: RemediationPlanRow) -> None:
        async with get_session() as session:
            session.add(plan)


class AuditRepository:
    """Encapsulates immutable audit records for remediations."""

    async def record_audit(self, audit: RemediationAuditRow) -> None:
        async with get_session() as session:
            session.add(audit)

    async def get_audits_for_pipeline(self, pipeline_id: str, limit: int = 50) -> list[RemediationAuditRow]:
        async with get_session() as session:
            stmt = select(RemediationAuditRow).where(RemediationAuditRow.pipeline_id == pipeline_id).limit(limit)
            result = await session.execute(stmt)
            return list(result.scalars().all())

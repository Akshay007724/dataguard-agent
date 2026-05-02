from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator

from sqlalchemy import DateTime, Float, String, Text, func
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ── ORM base ────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class IncidentRow(Base):
    __tablename__ = "incidents"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: f"INC-{uuid.uuid4().hex[:8].upper()}")
    title: Mapped[str] = mapped_column(String(512))
    pipeline_id: Mapped[str] = mapped_column(String(256), index=True)
    severity: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(32), default="open", index=True)
    description: Mapped[str] = mapped_column(Text)
    diagnosis_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    root_cause_category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolution: Mapped[str | None] = mapped_column(Text, nullable=True)


class IncidentPatternRow(Base):
    __tablename__ = "incident_patterns"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: str(uuid.uuid4()))
    pattern_name: Mapped[str] = mapped_column(String(256), unique=True, index=True)
    root_cause_category: Mapped[str] = mapped_column(String(64))
    log_pattern: Mapped[str] = mapped_column(Text)  # regex to match against logs
    confidence: Mapped[float] = mapped_column(Float, default=0.9)
    resolution_template: Mapped[str | None] = mapped_column(Text, nullable=True)


class RemediationPlanRow(Base):
    __tablename__ = "remediation_plans"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: str(uuid.uuid4()))
    diagnosis_id: Mapped[str] = mapped_column(String(64), index=True)
    pipeline_id: Mapped[str] = mapped_column(String(256))
    steps_json: Mapped[str] = mapped_column(Text)  # JSON-serialized list[RemediationStep]
    risk_level: Mapped[str] = mapped_column(String(16))
    estimated_resolution_minutes: Mapped[int | None] = mapped_column(nullable=True)
    rollback_plan: Mapped[str] = mapped_column(Text)
    requires_human_approval: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RemediationAuditRow(Base):
    """Immutable audit log — never update, only insert."""

    __tablename__ = "remediation_audit"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, default=lambda: str(uuid.uuid4()))
    remediation_id: Mapped[str] = mapped_column(String(64), index=True)
    pipeline_id: Mapped[str] = mapped_column(String(256))
    approver_id: Mapped[str] = mapped_column(String(256))
    risk_level: Mapped[str] = mapped_column(String(16))
    actions_taken_json: Mapped[str] = mapped_column(Text)
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    outcome: Mapped[str] = mapped_column(String(32))  # success | failed | partial


# ── Engine management ────────────────────────────────────────────────────────

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_engine(database_url: str) -> None:
    global _engine, _session_factory
    _engine = create_async_engine(
        database_url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        echo=False,
    )
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("Database engine not initialized. Call init_engine() first.")
    return _engine


async def create_tables() -> None:
    async with get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    if _session_factory is None:
        raise RuntimeError("Database session factory not initialized. Call init_engine() first.")
    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise

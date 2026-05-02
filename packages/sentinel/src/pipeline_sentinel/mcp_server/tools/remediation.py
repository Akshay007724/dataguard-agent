from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from dataguard_core.logging import get_logger
from dataguard_core.store.postgres import RemediationAuditRow, RemediationPlanRow, get_session
from pipeline_sentinel.config import settings

log = get_logger(__name__)

_RISK_ORDER = {"low": 0, "medium": 1, "high": 2}


async def handle_propose_remediation(arguments: dict[str, Any]) -> str:
    pipeline_id: str = arguments["pipeline_id"]
    diagnosis_id: str = arguments["diagnosis_id"]
    root_cause: str = arguments.get("root_cause_category", "unknown")

    steps = _playbook(root_cause)
    risk_level = _risk_for_cause(root_cause)
    requires_approval = risk_level in ("medium", "high")

    remediation_id = str(uuid.uuid4())
    rollback = "Re-trigger the pipeline from its last successful run."

    async with get_session() as session:
        row = RemediationPlanRow(
            id=remediation_id,
            diagnosis_id=diagnosis_id,
            pipeline_id=pipeline_id,
            steps_json=json.dumps(steps),
            risk_level=risk_level,
            estimated_resolution_minutes=_estimate_minutes(root_cause),
            rollback_plan=rollback,
            requires_human_approval=requires_approval,
        )
        session.add(row)

    return json.dumps({
        "remediation_id": remediation_id,
        "diagnosis_id": diagnosis_id,
        "pipeline_id": pipeline_id,
        "steps": steps,
        "risk_level": risk_level,
        "estimated_resolution_minutes": _estimate_minutes(root_cause),
        "rollback_plan": rollback,
        "requires_human_approval": requires_approval,
        "auto_remediation_eligible": _is_auto_eligible(risk_level),
    })


async def handle_execute_remediation(arguments: dict[str, Any]) -> str:
    remediation_id: str = arguments["remediation_id"]
    confirm: bool = arguments.get("confirm", False)
    approver_id: str = arguments.get("approver_id", "")

    # Gate 1: explicit confirmation
    if not confirm:
        return json.dumps({
            "error": "confirm must be true to execute remediation",
            "hint": "Pass confirm=true and approver_id to acknowledge you understand the action.",
        })

    # Gate 2: approver present
    if not approver_id:
        return json.dumps({"error": "approver_id is required for remediation execution"})

    # Gate 3: load the plan
    async with get_session() as session:
        result = await session.execute(
            select(RemediationPlanRow).where(RemediationPlanRow.id == remediation_id)
        )
        plan = result.scalar_one_or_none()

    if plan is None:
        return json.dumps({"error": f"Remediation plan {remediation_id!r} not found"})

    # Gate 4: pipeline opt-in
    if not settings.auto_remediation_enabled:
        return json.dumps({
            "error": "Auto-remediation is not enabled",
            "hint": "Set AUTO_REMEDIATION_ENABLED=true in your environment to enable. Read ARCHITECTURE.md for the full security model.",
        })

    # Gate 5: risk level check
    plan_risk = _RISK_ORDER.get(plan.risk_level, 99)
    max_risk = _RISK_ORDER.get(settings.auto_remediation_max_risk, 0)
    if plan_risk > max_risk:
        return json.dumps({
            "error": f"Remediation risk level '{plan.risk_level}' exceeds AUTO_REMEDIATION_MAX_RISK='{settings.auto_remediation_max_risk}'",
            "hint": "Raise AUTO_REMEDIATION_MAX_RISK or execute this remediation manually.",
        })

    # All gates passed — record audit trail before acting
    audit_id = str(uuid.uuid4())
    async with get_session() as session:
        audit = RemediationAuditRow(
            id=audit_id,
            remediation_id=remediation_id,
            pipeline_id=plan.pipeline_id,
            approver_id=approver_id,
            risk_level=plan.risk_level,
            actions_taken_json=plan.steps_json,
            outcome="initiated",
        )
        session.add(audit)

    log.info(
        "remediation_executing",
        remediation_id=remediation_id,
        pipeline_id=plan.pipeline_id,
        approver_id=approver_id,
        risk_level=plan.risk_level,
    )

    # v0.1: execution stubs — actual remediation actions implemented in v0.2
    return json.dumps({
        "audit_id": audit_id,
        "remediation_id": remediation_id,
        "pipeline_id": plan.pipeline_id,
        "status": "queued",
        "message": "Remediation execution queued. Full automated execution is available in v0.2.",
        "steps": json.loads(plan.steps_json),
        "executed_at": datetime.now(timezone.utc).isoformat(),
    })


def _playbook(root_cause: str) -> list[dict[str, Any]]:  # type: ignore[type-arg]
    playbooks: dict[str, list[dict[str, Any]]] = {  # type: ignore[type-arg]
        "oom": [
            {"order": 1, "type": "manual", "description": "Identify the task consuming excessive memory via profiling or logs"},
            {"order": 2, "type": "scale", "description": "Increase executor memory limit to 2x current value"},
            {"order": 3, "type": "retry", "description": "Re-trigger the failed run after resource adjustment"},
        ],
        "source_unavailable": [
            {"order": 1, "type": "manual", "description": "Verify source system health and connectivity"},
            {"order": 2, "type": "retry", "description": "Retry the pipeline — source may have recovered"},
            {"order": 3, "type": "manual", "description": "If source remains down, notify the data owner and file an incident"},
        ],
        "schema_drift": [
            {"order": 1, "type": "manual", "description": "Identify which upstream table changed (check lineage graph)"},
            {"order": 2, "type": "code_change", "description": "Update downstream SELECT list to match new schema, or add schema backfill"},
            {"order": 3, "type": "retry", "description": "Re-trigger after code change is deployed"},
        ],
        "code_error": [
            {"order": 1, "type": "manual", "description": "Review the failing task code and the log KeyError or TypeError"},
            {"order": 2, "type": "code_change", "description": "Fix the bug and deploy the updated pipeline code"},
            {"order": 3, "type": "retry", "description": "Re-trigger after deployment"},
        ],
        "dependency_failure": [
            {"order": 1, "type": "manual", "description": "Diagnose and resolve the upstream pipeline failure first"},
            {"order": 2, "type": "retry", "description": "Re-trigger this pipeline after upstream completes successfully"},
        ],
    }
    return playbooks.get(root_cause, [
        {"order": 1, "type": "manual", "description": "Inspect full logs and contact the pipeline owner"},
        {"order": 2, "type": "manual", "description": "File an incident for tracking"},
    ])


def _risk_for_cause(root_cause: str) -> str:
    low_risk = {"source_unavailable", "dependency_failure", "sla_violation"}
    high_risk = {"code_error", "schema_drift"}
    if root_cause in low_risk:
        return "low"
    if root_cause in high_risk:
        return "high"
    return "medium"


def _estimate_minutes(root_cause: str) -> int | None:
    estimates = {
        "oom": 30,
        "source_unavailable": 15,
        "schema_drift": 60,
        "code_error": 120,
        "dependency_failure": 45,
    }
    return estimates.get(root_cause)


def _is_auto_eligible(risk_level: str) -> bool:
    max_risk = settings.auto_remediation_max_risk
    return settings.auto_remediation_enabled and _RISK_ORDER.get(risk_level, 99) <= _RISK_ORDER.get(max_risk, 0)

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from dataguard_core.logging import get_logger
from dataguard_core.store.postgres import RemediationAuditRow, RemediationPlanRow
from dataguard_core.store.repositories import AuditRepository, RemediationRepository
from pipeline_sentinel.config import settings

log = get_logger(__name__)

_remediation_repo = RemediationRepository()
_audit_repo = AuditRepository()

_RISK_ORDER = {"low": 1, "medium": 2, "high": 3, "critical": 4}


async def handle_propose_remediation(arguments: dict[str, Any]) -> str:
    pipeline_id: str = arguments["pipeline_id"]
    diagnosis_id: str = arguments["diagnosis_id"]
    root_cause: str = arguments.get("root_cause_category", "unknown")

    steps = _playbook(root_cause)
    risk_level = _assess_risk(steps)
    requires_approval = risk_level in ("medium", "high", "critical")
    rollback = _rollback_plan(root_cause)

    remediation_id = f"REM-{uuid.uuid4().hex[:8].upper()}"

    plan = RemediationPlanRow(
        id=remediation_id,
        diagnosis_id=diagnosis_id,
        pipeline_id=pipeline_id,
        steps_json=json.dumps(steps),
        risk_level=risk_level,
        estimated_resolution_minutes=_estimate_minutes(root_cause),
        rollback_plan=rollback,
        requires_human_approval=requires_approval,
    )
    await _remediation_repo.save_plan(plan)

    return json.dumps(
        {
            "remediation_id": remediation_id,
            "diagnosis_id": diagnosis_id,
            "pipeline_id": pipeline_id,
            "steps": steps,
            "risk_level": risk_level,
            "estimated_resolution_minutes": _estimate_minutes(root_cause),
            "rollback_plan": rollback,
            "requires_human_approval": requires_approval,
            "auto_remediation_eligible": _is_auto_eligible(risk_level),
        }
    )


async def handle_execute_remediation(arguments: dict[str, Any]) -> str:
    remediation_id: str = arguments["remediation_id"]
    confirm: bool = arguments.get("confirm", False)
    approver_id: str = arguments.get("approver_id", "")

    # Gate 1: explicit confirmation
    if not confirm:
        return json.dumps(
            {
                "error": "confirm must be true to execute remediation",
                "hint": "Pass confirm=true and approver_id to acknowledge you understand the action.",
            }
        )

    # Gate 2: approver present
    if not approver_id:
        return json.dumps({"error": "approver_id is required for remediation execution"})

    # Gate 3: load the plan
    plan = await _remediation_repo.get_plan(remediation_id)
    if plan is None:
        return json.dumps({"error": f"Remediation plan {remediation_id!r} not found"})

    # Gate 4: pipeline opt-in
    if not settings.auto_remediation_enabled:
        return json.dumps(
            {
                "error": "Auto-remediation is not enabled",
                "hint": "Set AUTO_REMEDIATION_ENABLED=true in your environment to enable. Read ARCHITECTURE.md for the full security model.",
            }
        )

    # Gate 5: risk level check
    plan_risk = _RISK_ORDER.get(plan.risk_level, 99)
    max_risk = _RISK_ORDER.get(settings.auto_remediation_max_risk, 0)
    if plan_risk > max_risk:
        return json.dumps(
            {
                "error": f"Remediation risk level '{plan.risk_level}' exceeds AUTO_REMEDIATION_MAX_RISK='{settings.auto_remediation_max_risk}'",
                "hint": "Raise AUTO_REMEDIATION_MAX_RISK or execute this remediation manually.",
            }
        )

    # All gates passed — record audit trail before acting
    audit_id = str(uuid.uuid4())
    audit = RemediationAuditRow(
        id=audit_id,
        remediation_id=remediation_id,
        pipeline_id=plan.pipeline_id,
        approver_id=approver_id,
        risk_level=plan.risk_level,
        actions_taken_json=plan.steps_json,
        outcome="initiated",
    )
    await _audit_repo.record_audit(audit)

    log.info(
        "remediation_executing",
        remediation_id=remediation_id,
        pipeline_id=plan.pipeline_id,
        approver_id=approver_id,
        risk_level=plan.risk_level,
    )

    # v0.1: execution stubs — actual remediation actions implemented in v0.2
    return json.dumps(
        {
            "audit_id": audit_id,
            "remediation_id": remediation_id,
            "pipeline_id": plan.pipeline_id,
            "status": "queued",
            "message": "Remediation execution queued. Full automated execution is available in v0.2.",
            "steps": json.loads(plan.steps_json),
            "executed_at": datetime.now(UTC).isoformat(),
        }
    )


def _playbook(root_cause: str) -> list[dict[str, Any]]:
    playbooks: dict[str, list[dict[str, Any]]] = {
        "oom": [
            {
                "order": 1,
                "type": "manual",
                "description": "Identify the task consuming excessive memory via profiling or logs",
            },
            {"order": 2, "type": "scale", "description": "Increase executor memory limit to 2x current value"},
            {"order": 3, "type": "retry", "description": "Re-trigger the failed run after resource adjustment"},
        ],
        "source_unavailable": [
            {"order": 1, "type": "wait", "description": "Wait 5 minutes for transient source recovery"},
            {"order": 2, "type": "ping", "description": "Verify source endpoint health check responds 200"},
            {"order": 3, "type": "retry", "description": "Re-trigger failed tasks only"},
        ],
        "schema_drift": [
            {
                "order": 1,
                "type": "inspect",
                "description": "Run check_data_quality to identify added/removed/modified columns",
            },
            {
                "order": 2,
                "type": "manual",
                "description": "Update downstream table DDL or transformation logic to match new schema",
            },
            {"order": 3, "type": "backfill", "description": "Backfill data from time of schema change if needed"},
        ],
        "code_error": [
            {
                "order": 1,
                "type": "inspect",
                "description": "Review stack trace and recent git commits to pipeline code",
            },
            {"order": 2, "type": "manual", "description": "Deploy fix to pipeline repository"},
            {"order": 3, "type": "retry", "description": "Clear failed task state and resume pipeline"},
        ],
        "dependency_failure": [
            {"order": 1, "type": "trace", "description": "Trace upstream lineage to identify failed root DAG/workflow"},
            {"order": 2, "type": "triage_upstream", "description": "Diagnose root upstream failure first"},
            {"order": 3, "type": "retry", "description": "Once upstream succeeds, re-run this downstream pipeline"},
        ],
    }
    return playbooks.get(
        root_cause,
        [
            {"order": 1, "type": "inspect", "description": "Review run logs and error context"},
            {"order": 2, "type": "manual", "description": "Apply fix manually"},
        ],
    )


def _assess_risk(steps: list[dict[str, Any]]) -> str:
    types = {s["type"] for s in steps}
    if "scale" in types:
        return "medium"
    if "backfill" in types:
        return "high"
    return "low"


def _estimate_minutes(root_cause: str) -> int:
    estimates = {
        "oom": 15,
        "source_unavailable": 20,
        "schema_drift": 45,
        "code_error": 60,
        "dependency_failure": 30,
    }
    return estimates.get(root_cause, 30)


def _rollback_plan(root_cause: str) -> str:
    rollbacks = {
        "oom": "Revert executor memory limit to previous configuration in Helm/task definition.",
        "source_unavailable": "No state changed; cancel retries if upstream remains unresponsive.",
        "schema_drift": "Revert DDL migration if applied; restore previous schema from backup.",
        "code_error": "Roll back deployment to previous image tag via helm rollback or git revert.",
        "dependency_failure": "No rollback needed; upstream fix is self-contained.",
    }
    return rollbacks.get(root_cause, "Review manual changes and revert to previous configuration.")


def _is_auto_eligible(risk_level: str) -> bool:
    if not settings.auto_remediation_enabled:
        return False
    plan_risk = _RISK_ORDER.get(risk_level, 99)
    max_risk = _RISK_ORDER.get(settings.auto_remediation_max_risk, 0)
    return plan_risk <= max_risk

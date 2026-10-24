"""Human-in-the-loop gate for every deployment.

Nothing reaches a platform on its own. The agent (or a user) can only PROPOSE
a deployment; a named human must approve it. Approval re-checks that the exact
spec that passed the sandbox is the one being deployed (fails closed), then
calls the platform connector. Every proposal and decision is audited.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from .models import AuditRow, PendingActionRow

ACTION_TYPES = ("DEPLOY_PIPELINE",)


class ActionError(ValueError):
    pass


def audit(s: Session, actor: str, event: str, detail: dict) -> None:
    s.add(AuditRow(actor=actor or "anonymous", event=event, detail=detail))


def action_dict(r: PendingActionRow) -> dict:
    return {"id": r.id, "action_type": r.action_type, "pipeline_id": r.pipeline_id,
            "payload": r.payload, "status": r.status, "requested_by": r.requested_by,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "decided_by": r.decided_by,
            "decided_at": r.decided_at.isoformat() if r.decided_at else None,
            "result": r.result}


def propose(s: Session, action_type: str, payload: dict, requested_by: str,
            pipeline_id: int | None = None) -> dict:
    if action_type not in ACTION_TYPES:
        raise ActionError(f"action_type must be one of {ACTION_TYPES}")
    existing = (s.query(PendingActionRow)
                .filter(PendingActionRow.pipeline_id == pipeline_id, PendingActionRow.status == "PENDING")
                .first())
    if existing:
        return action_dict(existing)
    row = PendingActionRow(action_type=action_type, payload=payload,
                           requested_by=requested_by or "anonymous", pipeline_id=pipeline_id)
    s.add(row)
    s.flush()
    audit(s, requested_by, "DEPLOY_PROPOSED", {"action_id": row.id, "pipeline_id": pipeline_id,
                                               "spec_hash": payload.get("spec_hash")})
    s.commit()
    return action_dict(row)


def list_actions(s: Session, status: str | None = None) -> list[dict]:
    q = s.query(PendingActionRow)
    if status:
        q = q.filter(PendingActionRow.status == status)
    return [action_dict(r) for r in q.order_by(PendingActionRow.id.desc())]


def decide(s: Session, action_id: int, approve: bool, decided_by: str, reason: str = "") -> dict:
    from . import orchestration
    from .governance import is_ai_identity  # local import: governance imports this module
    decided_by = (decided_by or "").strip()
    if len(decided_by) < 2:
        raise ActionError("A named human approver is required.")
    if is_ai_identity(decided_by):
        raise ActionError("A deployment cannot be approved by the agent, the AI or an automated system.")
    if not approve and len((reason or "").strip()) < 5:
        raise ActionError("A reason (at least 5 characters) is required to reject.")
    row = s.get(PendingActionRow, action_id)
    if row is None:
        raise LookupError(f"No action {action_id}")
    if row.status != "PENDING":
        raise ActionError(f"Action {action_id} is already {row.status}; decisions are final.")
    if approve and row.requested_by.strip().lower() == decided_by.lower() and row.requested_by != "agent":
        raise ActionError("Separation of duties: the person who requested a deployment can't approve it.")

    row.decided_by = decided_by
    row.decided_at = datetime.now(timezone.utc)
    if not approve:
        row.status = "REJECTED"
        row.result = f"Rejected; nothing was deployed. Reason: {reason.strip()}"
        orchestration.mark_rejected(s, row.pipeline_id)
    else:
        try:
            run = orchestration.deploy(s, row.pipeline_id, row.payload.get("spec_hash"), decided_by)
        except orchestration.WorkflowError as exc:
            s.rollback()
            raise ActionError(str(exc)) from exc
        row.status = "EXECUTED"
        row.result = (f"Deployed to {run['platform']} as {row.payload.get('target')}: "
                      f"{run['rows_written']} row(s), job {run['job_id']} (simulated).")
    audit(s, decided_by, "DEPLOY_APPROVED" if approve else "DEPLOY_REJECTED",
          {"action_id": row.id, "pipeline_id": row.pipeline_id, "result": row.result})
    s.commit()
    return action_dict(row)

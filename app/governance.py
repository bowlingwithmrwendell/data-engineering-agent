"""AI governance controls: audit of every AI call, the human review gate,
the kill switch, and the limiter.

Design rule: every control FAILS CLOSED. If the kill-switch state cannot be
read, the AI is treated as switched off. If a usage count cannot be read, the
call is refused. If a review is in any state other than an approved one, its
text is never published.
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, update
from sqlalchemy.orm import Session

from . import config
from .actions import audit
from .grounding import numbers as _numbers
from .models import AICallRow, AIReviewRow, KillSwitchEventRow

# --- statuses ------------------------------------------------------------------
PENDING_REVIEW = "PENDING_REVIEW"
APPROVED = "APPROVED"
APPROVED_WITH_EDITS = "APPROVED_WITH_EDITS"
REJECTED = "REJECTED"
PUBLISHABLE = frozenset({APPROVED, APPROVED_WITH_EDITS})
REVIEW_STATUSES = (PENDING_REVIEW, APPROVED, APPROVED_WITH_EDITS, REJECTED)

# Outcomes that mean the model was actually called (these count toward limits).
CONSUMING = ("OK", "ERROR")


class GovernanceError(Exception):
    status_code = 400

    def __init__(self, message: str, status_code: int | None = None, retry_after: int | None = None):
        super().__init__(message)
        if status_code:
            self.status_code = status_code
        self.retry_after = retry_after


class AIBlocked(GovernanceError):
    """The call was refused before reaching the model (kill switch or limiter)."""


class ReviewError(GovernanceError):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --- who is calling --------------------------------------------------------------

def client_address(headers, peer: str | None) -> str:
    """Client address, honouring X-Forwarded-For only for trusted proxy hops."""
    hops = config.TRUSTED_PROXY_HOPS
    if hops > 0:
        xff = [p.strip() for p in (headers.get("x-forwarded-for") or "").split(",") if p.strip()]
        if len(xff) >= hops:
            return xff[-hops]
    return peer or "unknown"


def visitor_id(address: str) -> str:
    """Salted hash: rate limiting per visitor without storing raw IP addresses."""
    return "v-" + hashlib.sha256(f"{config.VISITOR_SALT}|{address}".encode()).hexdigest()[:16]


# --- kill switch -----------------------------------------------------------------

def kill_switch_state(s: Session) -> dict:
    if not config.AI_ENABLED:
        return {"engaged": True, "source": "environment (AI_ENABLED=false)", "since": None,
                "actor": None, "reason": "Disabled by deployment configuration"}
    try:
        last = s.query(KillSwitchEventRow).order_by(KillSwitchEventRow.id.desc()).first()
    except Exception as exc:  # fail closed
        s.rollback()
        return {"engaged": True, "source": "fail-closed", "since": None, "actor": None,
                "reason": f"Kill-switch state unreadable ({type(exc).__name__}); AI disabled"}
    if last is None:
        return {"engaged": False, "source": "default", "since": None, "actor": None, "reason": ""}
    return {"engaged": bool(last.engaged), "source": "runtime",
            "since": last.at.isoformat() if last.at else None, "actor": last.actor,
            "reason": last.reason}


def set_kill_switch(s: Session, engaged: bool, actor: str, reason: str) -> dict:
    s.add(KillSwitchEventRow(engaged=engaged, actor=actor, reason=reason))
    audit(s, actor, "KILL_SWITCH_ENGAGED" if engaged else "KILL_SWITCH_RELEASED", {"reason": reason})
    s.commit()
    return kill_switch_state(s)


# --- limiter ---------------------------------------------------------------------

def _day_start(now: datetime) -> datetime:
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def usage(s: Session, visitor: str | None = None, now: datetime | None = None) -> dict:
    now = now or _utcnow()
    day = _day_start(now)
    consuming = AICallRow.outcome.in_(CONSUMING)
    q_day = s.query(func.count(AICallRow.id)).filter(consuming, AICallRow.at >= day)
    out = {
        "calls_today_global": q_day.scalar() or 0,
        "tokens_today_global": int(s.query(func.coalesce(
            func.sum(AICallRow.input_tokens + AICallRow.output_tokens), 0))
            .filter(AICallRow.at >= day).scalar() or 0),
    }
    if visitor:
        out["calls_today_visitor"] = q_day.filter(AICallRow.visitor == visitor).scalar() or 0
        out["calls_last_minute_visitor"] = (s.query(func.count(AICallRow.id))
                                            .filter(consuming, AICallRow.visitor == visitor,
                                                    AICallRow.at >= now - timedelta(seconds=60))
                                            .scalar() or 0)
    return out


def guard(s: Session, visitor: str, kind: str, requested_by: str = "") -> None:
    """Refuse the call (and log the refusal) unless the switch is off and limits allow."""
    ks = kill_switch_state(s)
    if ks["engaged"]:
        _refused(s, visitor, kind, requested_by, "KILL_SWITCH", ks["reason"] or ks["source"])
        raise AIBlocked("AI is switched off by the kill switch. The builder, sandbox and catalog still work.",
                        503)
    try:
        u = usage(s, visitor)
    except Exception as exc:  # fail closed
        s.rollback()
        raise AIBlocked(f"Usage could not be verified ({type(exc).__name__}); call refused.", 503) from exc

    if u["calls_last_minute_visitor"] >= config.RATE_LIMIT_PER_MINUTE:
        _refused(s, visitor, kind, requested_by, "RATE_LIMITED",
                 f"{config.RATE_LIMIT_PER_MINUTE}/minute per visitor")
        raise AIBlocked(f"Rate limit: {config.RATE_LIMIT_PER_MINUTE} AI requests per minute. "
                        "Try again shortly.", 429, retry_after=60)
    if u["calls_today_visitor"] >= config.DAILY_CAP_PER_VISITOR:
        _refused(s, visitor, kind, requested_by, "CAP_REACHED",
                 f"{config.DAILY_CAP_PER_VISITOR}/day per visitor")
        raise AIBlocked(f"Daily cap reached: {config.DAILY_CAP_PER_VISITOR} AI requests per visitor "
                        "per day (resets 00:00 UTC).", 429, retry_after=_seconds_to_midnight())
    if u["calls_today_global"] >= config.DAILY_CAP_GLOBAL:
        _refused(s, visitor, kind, requested_by, "CAP_REACHED", f"{config.DAILY_CAP_GLOBAL}/day global")
        raise AIBlocked("The sandbox's daily AI allowance is used up (resets 00:00 UTC). "
                        "The builder, sandbox and catalog still work.", 429, retry_after=_seconds_to_midnight())
    if u["tokens_today_global"] >= config.DAILY_TOKEN_BUDGET:
        _refused(s, visitor, kind, requested_by, "CAP_REACHED",
                 f"{config.DAILY_TOKEN_BUDGET} tokens/day budget")
        raise AIBlocked("The sandbox's daily token budget is used up (resets 00:00 UTC).", 429,
                        retry_after=_seconds_to_midnight())


def _seconds_to_midnight() -> int:
    now = _utcnow()
    return int((_day_start(now) + timedelta(days=1) - now).total_seconds()) + 1


def _refused(s: Session, visitor, kind, requested_by, outcome, detail):
    log_ai_call(s, visitor=visitor, requested_by=requested_by, kind=kind, model=config.ANTHROPIC_MODEL,
                tools=[], model_calls=0, input_tokens=0, output_tokens=0, outcome=outcome, detail=detail)
    s.commit()


# --- audit of every AI call ---------------------------------------------------------

def log_ai_call(s: Session, *, visitor, requested_by, kind, model, tools, model_calls,
                input_tokens, output_tokens, outcome, detail="", review_id=None) -> AICallRow:
    row = AICallRow(visitor=visitor, requested_by=requested_by or "", kind=kind, model=model,
                    tools=list(tools), model_calls=model_calls, input_tokens=input_tokens,
                    output_tokens=output_tokens, outcome=outcome, detail=str(detail)[:2000],
                    review_id=review_id)
    s.add(row)
    audit(s, "agent", "AI_CALL", {"kind": kind, "visitor": visitor, "model": model, "tools": list(tools),
                                  "input_tokens": input_tokens, "output_tokens": output_tokens,
                                  "outcome": outcome, "review_id": review_id})
    return row


def ai_call_dict(r: AICallRow) -> dict:
    return {"id": r.id, "at": r.at.isoformat() if r.at else None, "visitor": r.visitor,
            "requested_by": r.requested_by, "kind": r.kind, "model": r.model, "tools": r.tools,
            "model_calls": r.model_calls, "input_tokens": r.input_tokens,
            "output_tokens": r.output_tokens, "outcome": r.outcome, "detail": r.detail,
            "review_id": r.review_id}


# --- human review gate --------------------------------------------------------------

def create_review(s: Session, *, kind, prompt, ai_output, model, tools_used, grounded,
                  unverified_numbers, requested_by="", visitor="", pipeline_ids=None) -> AIReviewRow:
    """Every AI output is stored PENDING_REVIEW. The status argument is not accepted."""
    row = AIReviewRow(kind=kind, prompt=prompt, ai_output=ai_output, model=model,
                      tools_used=list(tools_used), grounded=bool(grounded),
                      unverified_numbers=list(unverified_numbers), requested_by=requested_by or "",
                      visitor=visitor, pipeline_ids=list(pipeline_ids or []), status=PENDING_REVIEW)
    s.add(row)
    s.flush()
    audit(s, "agent", "AI_OUTPUT_PENDING_REVIEW", {"review_id": row.id, "kind": kind,
                                                   "grounded": bool(grounded)})
    return row


# Names that identify an AI or an automated system. A reviewer must be a person.
_AI_WORDS = {"agent", "ai", "claude", "anthropic", "assistant", "bot", "chatbot", "system", "model",
             "llm", "gpt", "openai", "gemini", "copilot", "automation", "automated", "auto", "api",
             "sonnet", "opus", "haiku", "mythos", "fable", "service", "script", "cron", "pipeline",
             "machine", "robot", "autoapprove", "autoapprover", "selfapprove"}


def is_ai_identity(name: str) -> bool:
    n = name.strip().lower()
    if n == config.ANTHROPIC_MODEL.lower():
        return True
    squashed = re.sub(r"[^a-z]", "", n)
    if any(w in squashed for w in ("claude", "anthropic", "chatbot", "autoapprov", "selfapprov")):
        return True
    words = set(re.findall(r"[a-z]+", n))
    return bool(words & _AI_WORDS)


def validate_reviewer(name: str | None) -> str:
    name = (name or "").strip()
    if len(name) < 2 or not re.search(r"[A-Za-z]{2}", name):
        raise ReviewError("A named human reviewer is required.", 422)
    if is_ai_identity(name):
        raise ReviewError("AI output cannot be approved by the AI, an agent or an automated system. "
                          "A named human must review it.", 403)
    return name


def check_reviewer_token(token: str | None) -> None:
    if config.REVIEWER_TOKEN and token != config.REVIEWER_TOKEN:
        raise ReviewError("Reviewer token required for review decisions.", 401)


def decide_review(s: Session, review_id: int, decision: str, reviewer: str,
                  edited_output: str | None = None, reason: str | None = None) -> dict:
    """approve | edit | reject. One decision per output; anything unexpected leaves it pending."""
    reviewer = validate_reviewer(reviewer)
    row = s.get(AIReviewRow, review_id)
    if row is None:
        raise ReviewError(f"No AI output {review_id}", 404)
    if row.status != PENDING_REVIEW:
        raise ReviewError(f"AI output {review_id} is already {row.status}; decisions are final.", 409)

    reason = (reason or "").strip()
    if decision == "approve":
        if not row.grounded:
            raise ReviewError("This output contains numbers that no engine produced "
                              f"({', '.join(row.unverified_numbers)}). Edit them out or reject it.", 422)
        new_status, final = APPROVED, row.ai_output
    elif decision == "edit":
        final = (edited_output or "").strip()
        if not final:
            raise ReviewError("edited_output is required for edit-then-approve.", 422)
        if final == row.ai_output.strip():
            raise ReviewError("The edit is identical to the AI output; use approve instead.", 422)
        leftover = sorted(_numbers(final) & set(row.unverified_numbers))
        if leftover:
            raise ReviewError(f"The edit still contains unverified numbers: {', '.join(leftover)}.", 422)
        new_status = APPROVED_WITH_EDITS
    elif decision == "reject":
        if len(reason) < 5:
            raise ReviewError("A reason (at least 5 characters) is required to reject.", 422)
        new_status, final = REJECTED, None
    else:
        raise ReviewError("decision must be approve, edit or reject", 422)

    # Compare-and-set: only a row still PENDING_REVIEW can change. Two reviewers
    # racing cannot both win, and a failure leaves the output pending.
    res = s.execute(update(AIReviewRow)
                    .where(AIReviewRow.id == review_id, AIReviewRow.status == PENDING_REVIEW)
                    .values(status=new_status, final_output=final, reviewer=reviewer,
                            decided_at=_utcnow(), reason=reason or None)
                    .execution_options(synchronize_session=False))
    if res.rowcount != 1:
        s.rollback()
        raise ReviewError(f"AI output {review_id} was decided by someone else.", 409)
    audit(s, reviewer, f"AI_OUTPUT_{new_status}", {"review_id": review_id, "kind": row.kind,
                                                   "reason": reason or None,
                                                   "edited": new_status == APPROVED_WITH_EDITS})
    s.commit()
    s.refresh(row)
    return review_dict(row)


def published_text(row: AIReviewRow | None) -> str:
    """The only way AI text leaves the review queue. Anything not approved is refused."""
    if row is None:
        raise ReviewError("No such AI output.", 404)
    if row.status not in PUBLISHABLE or not row.final_output or not row.reviewer:
        raise ReviewError(f"AI output {row.id} is {row.status}; only human-approved output is "
                          "published.", 409)
    return row.final_output


def review_dict(r: AIReviewRow) -> dict:
    return {"id": r.id, "kind": r.kind, "pipeline_ids": r.pipeline_ids, "status": r.status,
            "validated": r.status in PUBLISHABLE,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "prompt": r.prompt, "ai_output": r.ai_output, "final_output": r.final_output,
            "model": r.model, "tools_used": r.tools_used, "grounded": r.grounded,
            "unverified_numbers": r.unverified_numbers, "requested_by": r.requested_by,
            "reviewer": r.reviewer, "decided_at": r.decided_at.isoformat() if r.decided_at else None,
            "reason": r.reason}


def list_reviews(s: Session, status: str | None = None, limit: int = 50) -> list[dict]:
    q = s.query(AIReviewRow)
    if status:
        q = q.filter(AIReviewRow.status == status)
    return [review_dict(r) for r in q.order_by(AIReviewRow.id.desc()).limit(limit)]


def review_stats(s: Session) -> dict:
    counts = {st: 0 for st in REVIEW_STATUSES}
    for st, n in s.query(AIReviewRow.status, func.count(AIReviewRow.id)).group_by(AIReviewRow.status):
        counts[st] = n
    total = sum(counts.values())
    validated = counts[APPROVED] + counts[APPROVED_WITH_EDITS]
    decided = validated + counts[REJECTED]
    published_unvalidated = (s.query(func.count(AIReviewRow.id))
                             .filter(AIReviewRow.status.in_(PUBLISHABLE),
                                     AIReviewRow.reviewer.is_(None)).scalar() or 0)
    return {
        "total_ai_outputs": total,
        "by_status": counts,
        # A human has made the decision (approve / edit / reject) on this share of AI outputs.
        "human_validated": decided,
        "pct_human_validated": None if not total else round(decided / total * 100, 1),
        "pending_review": counts[PENDING_REVIEW],
        # Of everything published, the share a human validated. By design this is 100%.
        "published": validated,
        "pct_published_human_validated": None if not validated else
        round((validated - published_unvalidated) / validated * 100, 1),
        "pct_approved_unchanged": None if not decided else round(counts[APPROVED] / decided * 100, 1),
        "pct_edited": None if not decided else round(counts[APPROVED_WITH_EDITS] / decided * 100, 1),
        "pct_rejected": None if not decided else round(counts[REJECTED] / decided * 100, 1),
    }

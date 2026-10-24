"""Database schema (PostgreSQL in Docker / Render, SQLite for local runs and tests).

Information Layer tables: datasets (the metadata catalog), lineage edges,
pipelines, sandbox validations and run history.
Governance tables: pending actions, audit log, AI-call log, AI review queue,
kill-switch events. The three log tables are append-only.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import JSON, Boolean, Date, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --- Information Layer -------------------------------------------------------------

class DatasetRow(Base):
    """One entry in the metadata catalog: a table on one of the platforms."""
    __tablename__ = "datasets"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)        # e.g. "orders"
    friendly_name: Mapped[str] = mapped_column(String(120))
    description: Mapped[str] = mapped_column(Text, default="")
    layer: Mapped[str] = mapped_column(String(16))                      # bronze | silver | gold
    platform: Mapped[str] = mapped_column(String(16))                   # databricks | snowflake | fabric
    owner: Mapped[str] = mapped_column(String(120), default="")
    tags: Mapped[list] = mapped_column(JSON, default=list)              # governance / compliance tags
    columns: Mapped[list] = mapped_column(JSON, default=list)           # [{name,type,description,classification,...}]
    relationships: Mapped[list] = mapped_column(JSON, default=list)     # [{column, references}]
    curated_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_loaded: Mapped[date | None] = mapped_column(Date, nullable=True)
    refresh: Mapped[str] = mapped_column(String(16), default="daily")
    origin: Mapped[str] = mapped_column(String(16), default="seed")     # seed | pipeline
    pipeline_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rows: Mapped[list | None] = mapped_column(JSON, nullable=True)      # sandbox copy of the data


class LineageRow(Base):
    """Dataset-level lineage edge plus the column mappings that produced it."""
    __tablename__ = "lineage"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(64), index=True)
    target: Mapped[str] = mapped_column(String(64), index=True)
    pipeline_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    column_map: Mapped[list] = mapped_column(JSON, default=list)        # [{output, from, transform}]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class PipelineRow(Base):
    """A pipeline is a validated spec. Code is always compiled from the spec."""
    __tablename__ = "pipelines"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200))
    spec: Mapped[dict] = mapped_column(JSON)
    spec_hash: Mapped[str] = mapped_column(String(16))
    version: Mapped[int] = mapped_column(Integer, default=1)
    # DRAFT | VALIDATED | FAILED_VALIDATION | PENDING_APPROVAL | DEPLOYED | REJECTED
    status: Mapped[str] = mapped_column(String(24), default="DRAFT", index=True)
    created_by: Mapped[str] = mapped_column(String(120), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    deployed_hash: Mapped[str | None] = mapped_column(String(16), nullable=True)


class ValidationRow(Base):
    """One sandbox test run of one exact version of a pipeline spec."""
    __tablename__ = "validations"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_id: Mapped[int] = mapped_column(Integer, index=True)
    spec_hash: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(24))      # PASSED | PASSED_WITH_WARNINGS | FAILED
    result: Mapped[dict] = mapped_column(JSON)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class RunRow(Base):
    """Historical run log: one row per (simulated) deployment or job run."""
    __tablename__ = "runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_id: Mapped[int] = mapped_column(Integer, index=True)
    platform: Mapped[str] = mapped_column(String(16))
    job_id: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16))      # SUCCEEDED | FAILED
    rows_written: Mapped[int] = mapped_column(Integer, default=0)
    duration_ms: Mapped[int] = mapped_column(Integer, default=0)
    log: Mapped[list] = mapped_column(JSON, default=list)
    approved_by: Mapped[str] = mapped_column(String(120), default="")
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class LegacyCompareRow(Base):
    """Legacy SQL vs pipeline output on the same sample data, for one exact spec version."""
    __tablename__ = "legacy_comparisons"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_id: Mapped[int] = mapped_column(Integer, index=True)
    spec_hash: Mapped[str] = mapped_column(String(16))
    legacy_sql: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(24))      # EQUIVALENT | DIFFERENT | COULD_NOT_RUN
    result: Mapped[dict] = mapped_column(JSON)
    by: Mapped[str] = mapped_column(String(120), default="")
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


# --- Governance ----------------------------------------------------------------------

class PendingActionRow(Base):
    """Every deployment waits here for a named human."""
    __tablename__ = "pending_actions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    action_type: Mapped[str] = mapped_column(String(40))
    pipeline_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    payload: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(16), default="PENDING")
    requested_by: Mapped[str] = mapped_column(String(120))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    decided_by: Mapped[str | None] = mapped_column(String(120), nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    result: Mapped[str | None] = mapped_column(Text, nullable=True)


class AuditRow(Base):
    """Append-only trail of every tool call, change and approval decision."""
    __tablename__ = "audit_log"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    actor: Mapped[str] = mapped_column(String(120))
    event: Mapped[str] = mapped_column(String(60))
    detail: Mapped[dict] = mapped_column(JSON)


class AICallRow(Base):
    """One row per model call attempt: who, when, model, tools, tokens, outcome."""
    __tablename__ = "ai_call_log"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)
    visitor: Mapped[str] = mapped_column(String(64), index=True)   # salted hash of the client address
    requested_by: Mapped[str] = mapped_column(String(120), default="")
    kind: Mapped[str] = mapped_column(String(32))
    model: Mapped[str] = mapped_column(String(80))
    tools: Mapped[list] = mapped_column(JSON, default=list)
    model_calls: Mapped[int] = mapped_column(Integer, default=0)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    # OK | ERROR | RATE_LIMITED | CAP_REACHED | KILL_SWITCH | UNAVAILABLE
    outcome: Mapped[str] = mapped_column(String(32))
    detail: Mapped[str] = mapped_column(Text, default="")
    review_id: Mapped[int | None] = mapped_column(Integer, nullable=True)


class AIReviewRow(Base):
    """Every AI answer waits here, PENDING_REVIEW, until a named human decides."""
    __tablename__ = "ai_reviews"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    kind: Mapped[str] = mapped_column(String(32))
    prompt: Mapped[str] = mapped_column(Text)
    ai_output: Mapped[str] = mapped_column(Text)
    model: Mapped[str] = mapped_column(String(80))
    tools_used: Mapped[list] = mapped_column(JSON, default=list)
    pipeline_ids: Mapped[list] = mapped_column(JSON, default=list)
    grounded: Mapped[bool] = mapped_column(Boolean, default=False)
    unverified_numbers: Mapped[list] = mapped_column(JSON, default=list)
    requested_by: Mapped[str] = mapped_column(String(120), default="")
    visitor: Mapped[str] = mapped_column(String(64), default="")
    status: Mapped[str] = mapped_column(String(24), default="PENDING_REVIEW", index=True)
    final_output: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewer: Mapped[str | None] = mapped_column(String(120), nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class KillSwitchEventRow(Base):
    """Append-only history of runtime kill-switch changes. Latest row wins."""
    __tablename__ = "kill_switch_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    engaged: Mapped[bool] = mapped_column(Boolean)
    actor: Mapped[str] = mapped_column(String(120))
    reason: Mapped[str] = mapped_column(Text, default="")


# --- append-only enforcement ---------------------------------------------------
from sqlalchemy import event  # noqa: E402


class AppendOnlyViolation(RuntimeError):
    pass


def _refuse(kind):
    def handler(mapper, connection, target):
        raise AppendOnlyViolation(f"{type(target).__tablename__} is append-only; {kind} refused")
    return handler


for _cls in (AuditRow, AICallRow, KillSwitchEventRow, RunRow):
    event.listen(_cls, "before_update", _refuse("UPDATE"))
    event.listen(_cls, "before_delete", _refuse("DELETE"))

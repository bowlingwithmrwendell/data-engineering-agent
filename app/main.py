"""Data Engineering Agent -- FastAPI service (Interaction Layer: console, REST API, chat)."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import Session

from . import actions, agent, config, governance, information, orchestration
from .connectors import REGISTRY
from .db import Base, SessionLocal, engine, get_session
from .models import AICallRow, AIReviewRow, AuditRow
from .seed import EXAMPLES, seed

STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(engine)
    with SessionLocal() as s:
        seed(s)
    yield


app = FastAPI(
    title="Data Engineering Agent",
    version="1.0.0",
    description=("Build governed data pipelines on Databricks, Snowflake and Microsoft Fabric in plain "
                 "English. Claude drafts a spec; a deterministic compiler writes the SQL, PySpark and dbt; "
                 "an isolated sandbox tests it; a named human approves deployment. Fictional demo data."),
    lifespan=lifespan,
)
app.add_middleware(CORSMiddleware, allow_origins=config.CORS_ORIGINS,
                   allow_methods=["GET", "POST", "PUT"], allow_headers=["Content-Type"])


class RequestSizeLimit:
    """Reject any request body over MAX_REQUEST_BYTES with 413, whether or not
    the client sends Content-Length (streamed bodies are counted as they arrive)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        limit = config.MAX_REQUEST_BYTES
        headers = dict(scope.get("headers") or [])
        try:
            declared = int(headers.get(b"content-length", b"0"))
        except ValueError:
            declared = limit + 1
        if declared > limit:
            return await self._reject(send, limit)

        seen = 0

        async def counted():
            nonlocal seen
            msg = await receive()
            if msg["type"] == "http.request":
                seen += len(msg.get("body", b""))
                if seen > limit:
                    raise _TooLarge()
            return msg

        started = False

        async def tracking_send(msg):
            nonlocal started
            if msg["type"] == "http.response.start":
                started = True
            await send(msg)

        try:
            await self.app(scope, counted, tracking_send)
        except _TooLarge:
            if not started:
                await self._reject(send, limit)

    @staticmethod
    async def _reject(send, limit):
        resp = JSONResponse({"detail": f"Request body exceeds the {limit}-byte limit."}, status_code=413)
        await send({"type": "http.response.start", "status": 413, "headers": resp.raw_headers})
        await send({"type": "http.response.body", "body": resp.body})


class _TooLarge(Exception):
    pass


app.add_middleware(RequestSizeLimit)


def visitor(request: Request) -> str:
    return governance.visitor_id(
        governance.client_address(request.headers, request.client.host if request.client else None))


def _gov_http(exc: governance.GovernanceError) -> HTTPException:
    headers = {"Retry-After": str(exc.retry_after)} if exc.retry_after else None
    return HTTPException(exc.status_code, str(exc), headers=headers)


def _wf(fn, *a, **kw):
    try:
        return fn(*a, **kw)
    except orchestration.WorkflowError as exc:
        raise HTTPException(exc.status_code, str(exc)) from exc
    except information.NotFound as exc:
        raise HTTPException(404, str(exc)) from exc


# --- request models ---------------------------------------------------------

class SpecIn(BaseModel):
    spec: dict
    by: str = Field(default="", max_length=120)
    pipeline_id: int | None = Field(default=None, description="For /api/compile: the pipeline being edited")


class FixIn(BaseModel):
    patch: dict
    by: str = Field(default="", max_length=120)


class ByIn(BaseModel):
    by: str = Field(default="", max_length=120)


class AskIn(BaseModel):
    question: str = Field(min_length=3, max_length=config.MAX_QUESTION_CHARS)
    requested_by: str = Field(default="", max_length=120)


class DecisionIn(BaseModel):
    approve: bool
    decided_by: str = Field(min_length=1, max_length=120)
    reason: str = Field(default="", max_length=2000)


class ReviewDecisionIn(BaseModel):
    decision: Literal["approve", "edit", "reject"]
    reviewer: str = Field(min_length=1, max_length=120)
    edited_output: str | None = Field(default=None, max_length=20000)
    reason: str | None = Field(default=None, max_length=2000)


class KillSwitchIn(BaseModel):
    engaged: bool
    actor: str = Field(min_length=2, max_length=120)
    reason: str = Field(min_length=5, max_length=500)


# --- pages ---------------------------------------------------------------------

@app.get("/", include_in_schema=False)
def console():
    return FileResponse(STATIC / "index.html")


@app.get("/governance", include_in_schema=False)
def governance_page():
    return FileResponse(STATIC / "governance.html")


@app.get("/health")
def health(s: Session = Depends(get_session)):
    from .models import DatasetRow
    n = s.query(func.count(DatasetRow.id)).scalar()
    return {"status": "ok", "datasets": n, "as_of": config.AS_OF_DATE.isoformat(),
            "ai_configured": bool(config.ANTHROPIC_API_KEY)}


# --- Information Layer -------------------------------------------------------------

@app.get("/api/platforms", tags=["Platforms"])
def get_platforms():
    return [c.describe() for c in REGISTRY.values()]


@app.get("/api/catalog", tags=["Catalog & lineage"])
def get_catalog(q: str | None = None, s: Session = Depends(get_session)):
    """Every dataset, or a keyword search (business words work: 'revenue', 'satisfaction')."""
    return information.search(s, q, limit=20) if q else information.list_datasets(s)


@app.get("/api/catalog/{dataset_id}", tags=["Catalog & lineage"])
def get_dataset(dataset_id: str, s: Session = Depends(get_session)):
    return _wf(information.get_dataset, s, dataset_id)


@app.get("/api/lineage/{dataset_id}", tags=["Catalog & lineage"])
def get_lineage(dataset_id: str, s: Session = Depends(get_session)):
    _wf(information.get_dataset, s, dataset_id, 0)
    return information.lineage(s, dataset_id)


@app.get("/api/runs", tags=["Catalog & lineage"])
def get_runs(pipeline_id: int | None = None, s: Session = Depends(get_session)):
    return information.runs(s, pipeline_id)


@app.get("/api/examples", tags=["Pipelines"])
def get_examples():
    return EXAMPLES


# --- Orchestration: pipelines --------------------------------------------------------

@app.post("/api/compile", tags=["Pipelines"])
def compile_spec(body: SpecIn, s: Session = Depends(get_session)):
    """Check and compile a spec without saving it: plain-English summary, issues, generated code."""
    return _wf(orchestration.compile_preview, s, body.spec, body.pipeline_id)


@app.get("/api/pipelines", tags=["Pipelines"])
def list_pipelines(s: Session = Depends(get_session)):
    return orchestration.list_pipelines(s)


@app.post("/api/pipelines", status_code=201, tags=["Pipelines"])
def create_pipeline(body: SpecIn, s: Session = Depends(get_session)):
    return _wf(orchestration.create, s, body.spec, body.by)


@app.get("/api/pipelines/{pipeline_id}", tags=["Pipelines"])
def get_pipeline(pipeline_id: int, s: Session = Depends(get_session)):
    row = _wf(orchestration._get, s, pipeline_id)
    return orchestration.pipeline_dict(s, row, with_code=True)


@app.put("/api/pipelines/{pipeline_id}", tags=["Pipelines"])
def update_pipeline(pipeline_id: int, body: SpecIn, s: Session = Depends(get_session)):
    return _wf(orchestration.update, s, pipeline_id, body.spec, body.by)


@app.post("/api/pipelines/{pipeline_id}/test", tags=["Pipelines"])
def test_pipeline(pipeline_id: int, body: ByIn | None = None, s: Session = Depends(get_session)):
    """Run the pipeline in the isolated code sandbox and record the verdict."""
    return _wf(orchestration.validate, s, pipeline_id, body.by if body else "")


@app.post("/api/pipelines/{pipeline_id}/fix", tags=["Pipelines"])
def fix_pipeline(pipeline_id: int, body: FixIn, s: Session = Depends(get_session)):
    """Apply a suggested fix (a check's `patch`) and re-test."""
    return _wf(orchestration.fix, s, pipeline_id, body.patch, body.by)


@app.post("/api/pipelines/{pipeline_id}/request-deploy", tags=["Approvals"])
def request_deploy(pipeline_id: int, body: ByIn | None = None, s: Session = Depends(get_session)):
    """Queue a tested pipeline for deployment. A named human must approve it."""
    return _wf(orchestration.propose_deploy, s, pipeline_id, body.by if body else "")


# --- Approvals ------------------------------------------------------------------------

@app.get("/api/actions", tags=["Approvals"])
def get_actions(status: str | None = None, s: Session = Depends(get_session)):
    return actions.list_actions(s, status)


@app.post("/api/actions/{action_id}/decision", tags=["Approvals"])
def decide_action(action_id: int, body: DecisionIn, s: Session = Depends(get_session),
                  x_reviewer_token: str | None = Header(default=None)):
    try:
        governance.check_reviewer_token(x_reviewer_token)
        return actions.decide(s, action_id, body.approve, body.decided_by, body.reason)
    except governance.ReviewError as exc:
        raise _gov_http(exc) from exc
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except actions.ActionError as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/api/audit", tags=["Governance"])
def get_audit(limit: int = Query(50, ge=1, le=500), s: Session = Depends(get_session)):
    rows = s.query(AuditRow).order_by(AuditRow.id.desc()).limit(limit)
    return [{"id": r.id, "at": r.at.isoformat(), "actor": r.actor, "event": r.event,
             "detail": r.detail} for r in rows]


# --- Agent (plain English) -----------------------------------------------------------------

@app.post("/api/agent/ask", tags=["Agent"])
def ask_agent(body: AskIn, s: Session = Depends(get_session), who: str = Depends(visitor)):
    """Returns a DRAFT answer stored PENDING_REVIEW, plus any pipelines it drafted."""
    try:
        return agent.ask(s, body.question, visitor=who, requested_by=body.requested_by)
    except governance.AIBlocked as exc:
        raise _gov_http(exc) from exc
    except agent.AgentUnavailable as exc:
        raise HTTPException(503, str(exc)) from exc
    except agent.AgentError as exc:
        raise HTTPException(502, str(exc)) from exc


# --- human review gate for AI output ---------------------------------------------

@app.get("/api/reviews", tags=["Human review"])
def get_reviews(status: str | None = None, limit: int = Query(50, ge=1, le=200),
                s: Session = Depends(get_session)):
    return governance.list_reviews(s, status, limit)


@app.get("/api/reviews/{review_id}", tags=["Human review"])
def get_review(review_id: int, s: Session = Depends(get_session)):
    row = s.get(AIReviewRow, review_id)
    if row is None:
        raise HTTPException(404, f"No AI output {review_id}")
    return governance.review_dict(row)


@app.post("/api/reviews/{review_id}/decision", tags=["Human review"])
def decide_review(review_id: int, body: ReviewDecisionIn, s: Session = Depends(get_session),
                  x_reviewer_token: str | None = Header(default=None)):
    """approve | edit (edit-then-approve) | reject (reason required). Decisions are final."""
    try:
        governance.check_reviewer_token(x_reviewer_token)
        return governance.decide_review(s, review_id, body.decision, body.reviewer,
                                        body.edited_output, body.reason)
    except governance.ReviewError as exc:
        raise _gov_http(exc) from exc


@app.get("/api/reviews/{review_id}/published", tags=["Human review"])
def get_published(review_id: int, s: Session = Depends(get_session)):
    """The human-approved text. 409 for anything pending or rejected (fails closed)."""
    row = s.get(AIReviewRow, review_id)
    try:
        text = governance.published_text(row)
    except governance.ReviewError as exc:
        raise _gov_http(exc) from exc
    return {"id": row.id, "status": row.status, "text": text, "reviewer": row.reviewer,
            "decided_at": row.decided_at.isoformat() if row.decided_at else None}


# --- AI call log and governance scorecard ------------------------------------------

@app.get("/api/ai-calls", tags=["Governance"])
def get_ai_calls(limit: int = Query(50, ge=1, le=500), s: Session = Depends(get_session)):
    rows = s.query(AICallRow).order_by(AICallRow.id.desc()).limit(limit)
    return [governance.ai_call_dict(r) for r in rows]


@app.get("/api/governance", tags=["Governance"])
def get_governance(s: Session = Depends(get_session), who: str = Depends(visitor)):
    from .models import PendingActionRow, PipelineRow
    ks = governance.kill_switch_state(s)
    u = governance.usage(s, who)
    outcomes = dict(s.query(AICallRow.outcome, func.count(AICallRow.id)).group_by(AICallRow.outcome).all())
    deploys = dict(s.query(PendingActionRow.status, func.count(PendingActionRow.id))
                   .group_by(PendingActionRow.status).all())
    return {
        "model_card": {
            "provider": "Anthropic",
            "model": config.ANTHROPIC_MODEL,
            "role": "Translator and explainer. Turns a plain-English request into a pipeline spec, runs "
                    "the sandbox test, and explains the result.",
            "not_used_for": ["Writing SQL, PySpark or dbt (a deterministic compiler does)",
                             "Calculating any figure (the sandbox executes the pipeline)",
                             "Deploying, or approving its own answer or any deployment",
                             "Changing the catalog or running arbitrary queries"],
            "tools": [t["name"] for t in agent.TOOLS],
            "tool_permissions": "Read the catalog; create and test DRAFT pipelines in the sandbox; queue a "
                                "deployment request that only a named human can approve.",
            "safeguards": ["Code compiled from a typed spec, never written freehand",
                           "Isolated sandbox test with data-quality and PII checks before any request",
                           "Only the exact spec version that passed can deploy; re-tested at approval",
                           "Separation of duties: requester can't approve their own deployment",
                           "Numeric grounding check on every AI answer",
                           "Every AI answer PENDING_REVIEW until a named human decides",
                           "Every AI call logged: who, when, model, tools, tokens, outcome",
                           "Kill switch and per-visitor limits, all fail closed"],
            "known_limits": ["Specs cover select / join / filter / group / total / mask; window functions, "
                             "incremental merges and streaming are out of scope in v1",
                             "Platform connectors are simulated; real workspaces need a connector subclass",
                             "Grounding checks numbers, not wording: a reviewer must still read the answer",
                             "Demo data estate only; not validated on real company data"],
        },
        "data_policy": {
            "data": "Fictional 'Lakeside Outfitters' data estate. No real people, companies or figures.",
            "sent_to_model": "The request, the system prompt, catalog metadata and up to 8 preview rows of "
                             "sandbox results. PII columns reach the model only as masked values or counts.",
            "visitor_identity": "Client address is salted and hashed for rate limiting; never stored raw.",
            "retention": "Sandbox data is reseeded each time the free instance restarts. Audit, AI-call "
                         "and run records are append-only while it runs.",
            "secrets": "ANTHROPIC_API_KEY is read from the environment only; never logged or returned.",
        },
        "limits": {
            "rate_limit_per_minute_per_visitor": config.RATE_LIMIT_PER_MINUTE,
            "daily_cap_per_visitor": config.DAILY_CAP_PER_VISITOR,
            "daily_cap_global": config.DAILY_CAP_GLOBAL,
            "daily_token_budget": config.DAILY_TOKEN_BUDGET,
            "max_tokens_per_call": config.MAX_TOKENS,
            "max_agent_turns": config.MAX_AGENT_TURNS,
            "max_request_bytes": config.MAX_REQUEST_BYTES,
            "max_question_chars": config.MAX_QUESTION_CHARS,
            "sandbox_timeout_seconds": config.SANDBOX_TIMEOUT_SECONDS,
            "reset": "Daily caps reset 00:00 UTC",
        },
        "kill_switch": {**ks, "runtime_control": "enabled (admin token)" if config.ADMIN_TOKEN
                        else "locked (set ADMIN_TOKEN to enable; AI_ENABLED=false always works)"},
        "human_validation": governance.review_stats(s),
        "deployments": {"by_status": deploys,
                        "pipelines": dict(s.query(PipelineRow.status, func.count(PipelineRow.id))
                                          .group_by(PipelineRow.status).all())},
        "usage_today": {**u, "calls_by_outcome_all_time": outcomes},
        "review_gate": {"reviewer_token_required": bool(config.REVIEWER_TOKEN)},
    }


@app.post("/api/governance/kill-switch", tags=["Governance"])
def post_kill_switch(body: KillSwitchIn, s: Session = Depends(get_session),
                     x_admin_token: str | None = Header(default=None)):
    if not config.ADMIN_TOKEN:
        raise HTTPException(403, "Runtime kill switch is locked: set ADMIN_TOKEN, or set "
                                 "AI_ENABLED=false in the environment.")
    if x_admin_token != config.ADMIN_TOKEN:
        raise HTTPException(401, "Admin token required.")
    if governance.is_ai_identity(body.actor):
        raise HTTPException(403, "The kill switch must be operated by a named human.")
    return governance.set_kill_switch(s, body.engaged, body.actor.strip(), body.reason.strip())

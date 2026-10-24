"""Interaction Layer (natural language) -> Orchestration Engine.

Claude turns a plain-English request into a pipeline SPEC, never into
freehand SQL. It searches the catalog, drafts a spec, runs the sandbox test,
reads the results and explains them. The compiler writes the code; the
sandbox proves it; a named human approves the deployment.

What Claude can do (tools):     search the catalog, read dataset details and
                                lineage, draft or revise a pipeline, run the
                                sandbox test, apply a suggested fix, request
                                deployment (which only queues it for a human).
What Claude cannot do:          deploy, approve anything, change the catalog,
                                run arbitrary SQL, or publish its own answer.

Guards on every AI call (same pattern as the PMO Delivery Agent):
  1. Kill switch and limiter run first and fail closed (governance.guard).
  2. Every tool call is audited; every AI call is logged with who, when,
     model, tools, tokens and outcome.
  3. Numbers in the answer are checked against tool results (grounding).
  4. The answer is a DRAFT stored PENDING_REVIEW until a named human decides.
"""
from __future__ import annotations

import json

from sqlalchemy.orm import Session

from . import actions, config, governance, information, orchestration
from .connectors import REGISTRY
from .grounding import is_grounded
from .migration import ConformSpec
from .spec import PipelineSpec

SYSTEM_PROMPT = """\
IDENTITY & ROLE:
You are the Data Engineering Agent for Agile by Killette: a patient senior data engineer who
helps people who are NOT developers get trustworthy data pipelines built on Databricks,
Snowflake and Microsoft Fabric, while giving developers full access to the generated code.

HOW YOU WORK (always in this order):
1. Understand the request. If something essential is missing (which numbers, broken down by
   what, which platform), make a sensible choice and say what you assumed. Ask only if you
   truly cannot proceed.
2. Find the data with search_catalog / describe_dataset. Prefer Certified (silver) tables over
   raw ones. Use relationships listed in the catalog for join keys.
3. Draft a pipeline spec with draft_pipeline (or revise_pipeline). You never write SQL, PySpark
   or dbt yourself: the compiler generates code from the spec.
4. Always run test_pipeline. If checks fail and a fix has a patch, apply it with apply_fix and
   test again (at most twice). Never hide a warning.
5. Explain the result in plain English first: what the table contains, what the checks found,
   what the person should know. Then one short line for developers (the pipeline id and that
   code is on its Code tab).
6. Only call request_deployment if the person asked to deploy/publish/ship it. Tell them a
   named human must approve it; you cannot deploy.

MIGRATIONS (an acquired company, or moving platforms):
- To bring another company's data onto ours, use draft_conform_pipeline: define OUR standard model
  (column names, types, key, required, allowed values), then map each source onto it: rename
  columns, convert types (cast), translate codes (value_map), and swap their IDs for ours through a
  crosswalk table (lookup). Acquired datasets are tagged "Acquired" in the catalog.
- test_pipeline then reconciles: every source row arrives once, totals are preserved, unreadable
  values and crosswalk misses are counted, keys don't collide between companies. Explain each
  finding in business terms (e.g. "8 customers already shop with us").
- If the person pastes legacy SQL, draft the equivalent pipeline and call compare_with_legacy to
  prove both return the same rows. Never claim equivalence without that result.
- For a platform move use replatform_plan, and reconcile_tables to compare an old and a new table.

GOVERNANCE RULES (non-negotiable):
- Never state a number that is not in a tool result. Never estimate or calculate figures.
- Personal information (PII) must be masked or left out; never work around a PII failure.
- You cannot deploy, approve, change the catalog or run arbitrary queries.
- The data is a fictional demo estate for "Lakeside Outfitters". Never present it as real.

STYLE: warm, plain, short. No jargon without a one-line explanation (e.g. "join = matching rows
from two tables"). Use small Markdown tables for previews. Lead with the answer.
"""


def _inline(schema: dict) -> dict:
    """Inline $ref/$defs so the tool schema is self-contained."""
    defs = schema.get("$defs", {})

    def walk(node):
        if isinstance(node, dict):
            if "$ref" in node:
                return walk(defs[node["$ref"].split("/")[-1]])
            return {k: walk(v) for k, v in node.items() if k not in ("$defs", "title")}
        if isinstance(node, list):
            return [walk(x) for x in node]
        return node
    return walk(schema)


SPEC_SCHEMA = _inline(PipelineSpec.model_json_schema())
CONFORM_SCHEMA = _inline(ConformSpec.model_json_schema())

TOOLS = [
    {"name": "search_catalog",
     "description": "Search the data catalog by keywords (business words work: 'revenue', 'customers', "
                    "'satisfaction'). Returns datasets with friendly names, platform, tags and freshness.",
     "input_schema": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}},
    {"name": "describe_dataset",
     "description": "Full details of one dataset: columns (with PII/Confidential tags and accepted values), "
                    "relationships (join keys), owner, freshness, 5 sample rows and lineage.",
     "input_schema": {"type": "object", "properties": {"dataset_id": {"type": "string"}},
                      "required": ["dataset_id"]}},
    {"name": "list_platforms",
     "description": "The connected platforms (Databricks, Snowflake, Microsoft Fabric) and how each names tables.",
     "input_schema": {"type": "object", "properties": {}}},
    {"name": "draft_pipeline",
     "description": "Create a DRAFT pipeline from a spec. Column references use 'dataset.column'. "
                    "Returns the pipeline id, a plain-English summary and any design issues.",
     "input_schema": {"type": "object", "properties": {"spec": SPEC_SCHEMA}, "required": ["spec"]}},
    {"name": "revise_pipeline",
     "description": "Replace the spec of an existing pipeline (returns it to DRAFT).",
     "input_schema": {"type": "object", "properties": {"pipeline_id": {"type": "integer"}, "spec": SPEC_SCHEMA},
                      "required": ["pipeline_id", "spec"]}},
    {"name": "test_pipeline",
     "description": "Run the pipeline in the isolated sandbox on sample data: compiles, executes and runs "
                    "data-quality and compliance checks. Returns the verdict, each check and a preview.",
     "input_schema": {"type": "object", "properties": {"pipeline_id": {"type": "integer"}},
                      "required": ["pipeline_id"]}},
    {"name": "apply_fix",
     "description": "Apply a suggested fix (the 'patch' object from a failed or warning check) and re-test.",
     "input_schema": {"type": "object", "properties": {"pipeline_id": {"type": "integer"},
                                                       "patch": {"type": "object"}},
                      "required": ["pipeline_id", "patch"]}},
    {"name": "draft_conform_pipeline",
     "description": "Create a DRAFT migration pipeline that maps one or more sources (e.g. ours and an acquired "
                    "company's) onto one standard model and stacks them with a source_system column. Mappings "
                    "use bare column names of each source.",
     "input_schema": {"type": "object", "properties": {"spec": CONFORM_SCHEMA}, "required": ["spec"]}},
    {"name": "revise_conform_pipeline",
     "description": "Replace the spec of an existing migration (conform) pipeline.",
     "input_schema": {"type": "object", "properties": {"pipeline_id": {"type": "integer"}, "spec": CONFORM_SCHEMA},
                      "required": ["pipeline_id", "spec"]}},
    {"name": "compare_with_legacy",
     "description": "Run a legacy SELECT query and a pipeline on the same sample data and report whether the "
                    "results are equivalent (row by row).",
     "input_schema": {"type": "object", "properties": {"pipeline_id": {"type": "integer"}, "sql": {"type": "string"}},
                      "required": ["pipeline_id", "sql"]}},
    {"name": "reconcile_tables",
     "description": "Compare two catalog tables: row counts, keys only on one side, and totals per key.",
     "input_schema": {"type": "object", "properties": {
         "left": {"type": "string"}, "right": {"type": "string"},
         "key": {"type": "string", "description": "Column in both, or 'left_col=right_col'"},
         "measures": {"type": "array", "items": {"type": "string"}}}, "required": ["left", "right", "key"]}},
    {"name": "replatform_plan",
     "description": "What changes if every pipeline moves to another platform (new table names, cross-platform reads).",
     "input_schema": {"type": "object", "properties": {"platform": {"type": "string",
                                                                    "enum": ["databricks", "snowflake", "fabric"]}},
                      "required": ["platform"]}},
    {"name": "request_deployment",
     "description": "Queue a tested pipeline for deployment. Does NOT deploy: a named human must approve.",
     "input_schema": {"type": "object", "properties": {"pipeline_id": {"type": "integer"}},
                      "required": ["pipeline_id"]}},
]


PIPELINE_TOOLS = {"draft_pipeline", "revise_pipeline", "test_pipeline", "apply_fix", "request_deployment",
                  "draft_conform_pipeline", "revise_conform_pipeline"}


def _compact_pipeline(p: dict) -> dict:
    out = {k: p[k] for k in ("id", "name", "status", "version", "summary", "platform_name", "target")}
    out["design_issues"] = p["issues"]
    v = p.get("validation")
    if v:
        out["test"] = {"status": v["status"], "headline": v["headline"], "row_count": v["row_count"],
                       "checks": [{k: c[k] for k in ("status", "title", "message", "fix", "patch") if k in c}
                                  for c in v["checks"] if c["status"] != "PASS"],
                       "passed_checks": [c["title"] for c in v["checks"] if c["status"] == "PASS"],
                       "schema": v["schema"], "preview": v["preview"][:8]}
    return out


def run_tool(s: Session, name: str, args: dict, by: str = "agent") -> dict:
    if name == "search_catalog":
        return {"results": [{k: d[k] for k in ("id", "friendly_name", "description", "layer", "platform_name",
                                                "tags", "freshness_days", "curated_version")}
                            for d in information.search(s, args.get("query", ""))]}
    if name == "describe_dataset":
        d = information.get_dataset(s, args["dataset_id"], sample=5)
        d.pop("rows", None)
        return d
    if name == "list_platforms":
        return {"platforms": [c.describe() for c in REGISTRY.values()]}
    if name == "draft_pipeline":
        return _compact_pipeline(orchestration.create(s, args["spec"], by))
    if name == "revise_pipeline":
        return _compact_pipeline(orchestration.update(s, int(args["pipeline_id"]), args["spec"], by))
    if name == "test_pipeline":
        return _compact_pipeline(orchestration.validate(s, int(args["pipeline_id"]), by))
    if name == "apply_fix":
        return _compact_pipeline(orchestration.fix(s, int(args["pipeline_id"]), args["patch"], by))
    if name == "draft_conform_pipeline":
        return _compact_pipeline(orchestration.create(s, {**args["spec"], "kind": "conform"}, by))
    if name == "revise_conform_pipeline":
        return _compact_pipeline(orchestration.update(s, int(args["pipeline_id"]),
                                                      {**args["spec"], "kind": "conform"}, by))
    if name == "compare_with_legacy":
        r = orchestration.compare_legacy(s, int(args["pipeline_id"]), args["sql"], by)
        return {k: r[k] for k in r if k not in ("explanation",)}
    if name == "reconcile_tables":
        return orchestration.reconcile_tables(s, args["left"], args["right"], args["key"],
                                              list(args.get("measures") or []), by)
    if name == "replatform_plan":
        return orchestration.replatform_plan(s, args["platform"])
    if name == "request_deployment":
        out = orchestration.propose_deploy(s, int(args["pipeline_id"]), by)
        return {"pipeline": _compact_pipeline(out["pipeline"]), "approval_request": out["action"]["id"],
                "note": "Queued. A named human must approve it on the Approvals tab."}
    raise ValueError(f"Unknown tool {name}")


class AgentUnavailable(RuntimeError):
    pass


class AgentError(RuntimeError):
    pass


def _client():
    if not config.ANTHROPIC_API_KEY:
        raise AgentUnavailable("Plain-English requests need ANTHROPIC_API_KEY. Everything else works: "
                               "use the step-by-step builder.")
    from anthropic import Anthropic
    return Anthropic(api_key=config.ANTHROPIC_API_KEY)


def _usage(resp) -> tuple[int, int]:
    u = getattr(resp, "usage", None)
    return (int(getattr(u, "input_tokens", 0) or 0), int(getattr(u, "output_tokens", 0) or 0))


def ask(s: Session, question: str, max_turns: int | None = None, client=None,
        visitor: str = "local", requested_by: str = "") -> dict:
    """Answer a request. The answer is a DRAFT, stored PENDING_REVIEW."""
    kind = "AGENT_ASK"
    question = question[:config.MAX_QUESTION_CHARS]
    governance.guard(s, visitor, kind, requested_by)          # kill switch + limiter, fail closed
    try:
        client = client or _client()
    except AgentUnavailable as exc:
        governance.log_ai_call(s, visitor=visitor, requested_by=requested_by, kind=kind,
                               model=config.ANTHROPIC_MODEL, tools=[], model_calls=0,
                               input_tokens=0, output_tokens=0, outcome="UNAVAILABLE", detail=str(exc))
        s.commit()
        raise

    max_turns = max_turns or config.MAX_AGENT_TURNS
    messages = [{"role": "user", "content": question}]
    trace: list[dict] = []
    facts: list = []
    pipeline_ids: list[int] = []
    tin = tout = calls = 0
    actor = f"agent (for {requested_by})" if requested_by else "agent"

    def log(outcome, detail="", review_id=None):
        governance.log_ai_call(s, visitor=visitor, requested_by=requested_by, kind=kind,
                               model=config.ANTHROPIC_MODEL, tools=[t["tool"] for t in trace],
                               model_calls=calls, input_tokens=tin, output_tokens=tout,
                               outcome=outcome, detail=detail, review_id=review_id)

    try:
        answer = None
        for _ in range(max_turns):
            resp = client.messages.create(model=config.ANTHROPIC_MODEL, max_tokens=config.MAX_TOKENS,
                                          system=SYSTEM_PROMPT, tools=TOOLS, messages=messages)
            calls += 1
            i, o = _usage(resp)
            tin, tout = tin + i, tout + o
            if resp.stop_reason != "tool_use":
                answer = "".join(b.text for b in resp.content if b.type == "text").strip()
                break
            messages.append({"role": "assistant", "content": resp.content})
            results = []
            for block in resp.content:
                if block.type != "tool_use":
                    continue
                args = dict(block.input)
                try:
                    out = run_tool(s, block.name, args, by="agent")
                    is_error = False
                except (orchestration.WorkflowError, information.NotFound, ValueError, KeyError) as exc:
                    s.rollback()
                    out = {"error": str(exc), "issues": getattr(exc, "issues", [])}
                    is_error = True
                pid = None
                if not is_error and block.name in PIPELINE_TOOLS:
                    pid = out.get("id") or (out.get("pipeline") or {}).get("id")
                if pid and pid not in pipeline_ids:
                    pipeline_ids.append(pid)
                facts.append(out)
                trace.append({"tool": block.name, "input": args, "error": is_error})
                actions.audit(s, actor, "TOOL_CALL", {"tool": block.name, "input": args, "error": is_error})
                s.commit()
                results.append({"type": "tool_result", "tool_use_id": block.id,
                                "content": json.dumps(out, default=str), "is_error": is_error})
            messages.append({"role": "user", "content": results})
    except Exception as exc:
        s.rollback()
        log("ERROR", f"{type(exc).__name__}: {exc}")
        s.commit()
        raise AgentError("The model call failed; nothing was published.") from exc

    if answer is None:
        answer = ("I ran out of steps before finishing. Your draft pipeline (if any) is saved on the "
                  "Pipelines tab, where you can test and finish it.")
        grounded, unknown = True, []
    else:
        grounded, unknown = is_grounded(answer, {"question": question, "facts": facts})

    review = governance.create_review(
        s, kind=kind, prompt=question, ai_output=answer, model=config.ANTHROPIC_MODEL,
        tools_used=[t["tool"] for t in trace], grounded=grounded, unverified_numbers=unknown,
        requested_by=requested_by, visitor=visitor, pipeline_ids=pipeline_ids)
    log("OK", "answer stored PENDING_REVIEW", review.id)
    s.commit()
    return {"review_id": review.id, "status": review.status, "validated": False,
            "draft_answer": answer, "tools_used": trace, "pipeline_ids": pipeline_ids,
            "grounded": grounded, "unverified_numbers": unknown, "model": config.ANTHROPIC_MODEL,
            "usage": {"model_calls": calls, "input_tokens": tin, "output_tokens": tout},
            "notice": "Draft only. This AI answer is PENDING_REVIEW until a named human approves, edits "
                      "or rejects it. Pipelines it drafted still need a sandbox pass and human approval "
                      "to deploy."}

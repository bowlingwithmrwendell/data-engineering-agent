"""Orchestration Engine: intent -> tasks -> state, with error recovery.

A pipeline moves through one explicit state machine:

    DRAFT ──test──► VALIDATED ──request──► PENDING_APPROVAL ──human approves──► DEPLOYED
      ▲   └──────► FAILED_VALIDATION              │                              │
      │                 │  (apply a suggested fix) └──human rejects──► REJECTED   │
      └──── any change to the spec returns it to DRAFT (and cancels a pending request)

Rules that fail closed:
  * Only the exact spec version (hash) that passed the sandbox can be proposed.
  * Approval re-runs the sandbox on that spec; if anything fails now, nothing deploys.
  * Changing the spec after a request supersedes the request.

Error recovery: every failed check carries a plain-English fix, and most carry
a machine-applicable patch, so "Apply fix" is one click for a non-developer.
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone

from pydantic import ValidationError
from sqlalchemy.orm import Session

from . import actions, compiler, config, connectors, information, sandbox
from .models import DatasetRow, LineageRow, PendingActionRow, PipelineRow, RunRow, ValidationRow
from .spec import PLATFORM_NAMES, Issue, PipelineSpec, resolve, spec_hash, summarize

STATES = ("DRAFT", "VALIDATED", "FAILED_VALIDATION", "PENDING_APPROVAL", "DEPLOYED", "REJECTED")


class WorkflowError(Exception):
    def __init__(self, message: str, status_code: int = 409, issues: list | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.issues = issues or []


def _now():
    return datetime.now(timezone.utc)


# --- spec handling --------------------------------------------------------------------

def parse_spec(data: dict) -> PipelineSpec:
    try:
        return PipelineSpec.model_validate(data)
    except ValidationError as exc:
        msgs = []
        for e in exc.errors()[:6]:
            where = ".".join(str(p) for p in e["loc"]) or "spec"
            msgs.append(f"{where}: {e['msg']}")
        raise WorkflowError("The pipeline description is incomplete: " + "; ".join(msgs), 422) from exc


def check(s: Session, spec: PipelineSpec, pipeline_id: int | None = None):
    """Resolve the spec against the catalog. Returns (resolved|None, issues, catalog)."""
    catalog = information.catalog_map(s)
    resolved, issues = resolve(spec, catalog)
    t = spec.target.name
    existing = catalog.get(t)
    if existing and existing["origin"] == "pipeline" and existing["pipeline_id"] != pipeline_id:
        issues.append(Issue("error", "TARGET_TAKEN",
                            f"'{t}' is already published by pipeline #{existing['pipeline_id']}.",
                            "Choose another name.", {"op": "rename_target", "to": f"{t}_v2"}))
    used = [spec.source] + [j.dataset for j in spec.joins]
    if t in used:
        issues.append(Issue("error", "TARGET_IS_INPUT", f"'{t}' can't read from itself.",
                            "Choose another name for the result.", {"op": "rename_target", "to": f"{t}_v2"}))
    return resolved, issues, catalog


def compile_preview(s: Session, data: dict, pipeline_id: int | None = None) -> dict:
    """Stateless: check and compile a spec without saving it (live preview in the builder)."""
    spec = parse_spec(data)
    r, issues, catalog = check(s, spec, pipeline_id)
    errors = [i for i in issues if i.level == "error"]
    return {"summary": summarize(spec, catalog), "spec_hash": spec_hash(spec),
            "issues": [i.as_dict() for i in issues], "ready_to_test": not errors,
            "artifacts": compiler.artifacts(r, catalog) if r and not errors else []}


def apply_patch(data: dict, patch: dict) -> dict:
    """Apply one suggested fix to a spec dict. Unknown ops are refused."""
    d = copy.deepcopy(data)
    op = patch.get("op")

    def swap_ref(ref, old, new):
        return new if ref == old else ref

    if op == "replace_dataset":
        old, new = patch["from"], patch["to"]
        if d.get("source") == old:
            d["source"] = new
        for j in d.get("joins", []):
            if j["dataset"] == old:
                j["dataset"] = new

        def ren(ref):
            if isinstance(ref, str) and ref.startswith(old + "."):
                return new + ref[len(old):]
            return ref
        for j in d.get("joins", []):
            j["left"], j["right"] = ren(j["left"]), ren(j["right"])
        for f in d.get("filters", []):
            f["column"] = ren(f["column"])
        d["columns"] = [ren(c) for c in d.get("columns", [])]
        for g in d.get("group_by", []):
            g["column"] = ren(g["column"])
        for m in d.get("measures", []):
            m["column"] = ren(m.get("column"))
        for m in d.get("masks", []):
            m["column"] = ren(m["column"])
    elif op == "replace_column":
        old, new = patch["from"], patch["to"]
        for j in d.get("joins", []):
            j["left"], j["right"] = swap_ref(j["left"], old, new), swap_ref(j["right"], old, new)
        for f in d.get("filters", []):
            f["column"] = swap_ref(f["column"], old, new)
        d["columns"] = [swap_ref(c, old, new) for c in d.get("columns", [])]
        for g in d.get("group_by", []):
            g["column"] = swap_ref(g["column"], old, new)
        for m in d.get("measures", []):
            m["column"] = swap_ref(m.get("column"), old, new)
        for m in d.get("masks", []):
            m["column"] = swap_ref(m["column"], old, new)
    elif op == "add_mask":
        masks = [m for m in d.get("masks", []) if m["column"] != patch["column"]]
        masks.append({"column": patch["column"], "method": patch.get("method", "hash")})
        d["masks"] = masks
    elif op == "rename_target":
        d["target"]["name"] = patch["to"]
    elif op == "rename_output":
        for g in d.get("group_by", []):
            if g.get("name") == patch["from"]:
                g["name"] = patch["to"]
        for m in d.get("measures", []):
            if m.get("name") == patch["from"]:
                m["name"] = patch["to"]
    elif op == "set_measure_agg":
        for m in d.get("measures", []):
            if m.get("name") == patch["name"]:
                m["agg"] = patch["agg"]
    elif op == "set_join_how":
        for j in d.get("joins", []):
            if j["dataset"] == patch["dataset"]:
                j["how"] = patch["how"]
    elif op == "replace_filter_value":
        for f in d.get("filters", []):
            if f["column"] == patch["column"]:
                v = f.get("value")
                f["value"] = ([patch["to"] if x == patch["from"] else x for x in v] if isinstance(v, list)
                              else (patch["to"] if v == patch["from"] else v))
    elif op == "columns_to_group_by":
        d["group_by"] = d.get("group_by", []) + [{"column": c} for c in d.get("columns", [])]
        d["columns"] = []
    else:
        raise WorkflowError(f"Unknown fix '{op}'.", 422)
    return d


# --- pipelines ----------------------------------------------------------------------------

def _get(s: Session, pipeline_id: int) -> PipelineRow:
    row = s.get(PipelineRow, pipeline_id)
    if row is None:
        raise WorkflowError(f"No pipeline #{pipeline_id}", 404)
    return row


def create(s: Session, data: dict, created_by: str = "") -> dict:
    spec = parse_spec(data)
    clean = spec.model_dump(mode="json")
    row = PipelineRow(name=spec.name, spec=clean, spec_hash=spec_hash(clean), created_by=created_by or "")
    s.add(row)
    s.flush()
    actions.audit(s, created_by, "PIPELINE_DRAFTED", {"pipeline_id": row.id, "spec_hash": row.spec_hash})
    s.commit()
    return pipeline_dict(s, row)


def update(s: Session, pipeline_id: int, data: dict, by: str = "") -> dict:
    row = _get(s, pipeline_id)
    spec = parse_spec(data)
    clean = spec.model_dump(mode="json")
    h = spec_hash(clean)
    if h == row.spec_hash:
        return pipeline_dict(s, row)
    _supersede_pending(s, row.id, by)
    row.spec, row.spec_hash, row.name = clean, h, spec.name
    row.version += 1
    row.status = "DRAFT"
    row.updated_at = _now()
    actions.audit(s, by, "PIPELINE_CHANGED", {"pipeline_id": row.id, "version": row.version, "spec_hash": h})
    s.commit()
    return pipeline_dict(s, row)


def _supersede_pending(s: Session, pipeline_id: int, by: str) -> None:
    for a in s.query(PendingActionRow).filter(PendingActionRow.pipeline_id == pipeline_id,
                                              PendingActionRow.status == "PENDING"):
        a.status = "SUPERSEDED"
        a.result = "The pipeline changed after this request; request a new deployment."
        actions.audit(s, by, "DEPLOY_SUPERSEDED", {"action_id": a.id, "pipeline_id": pipeline_id})


def validate(s: Session, pipeline_id: int, by: str = "") -> dict:
    row = _get(s, pipeline_id)
    spec = parse_spec(row.spec)
    result = _run_sandbox(s, spec, row.id)
    stored = {k: v for k, v in result.items() if k != "rows"}
    s.add(ValidationRow(pipeline_id=row.id, spec_hash=row.spec_hash, status=result["status"], result=stored))
    if row.status != "DEPLOYED" or row.deployed_hash != row.spec_hash:
        row.status = "FAILED_VALIDATION" if result["status"] == "FAILED" else "VALIDATED"
    actions.audit(s, by, "SANDBOX_TEST", {"pipeline_id": row.id, "spec_hash": row.spec_hash,
                                         "status": result["status"], "counts": result["counts"]})
    s.commit()
    return pipeline_dict(s, row)


def _run_sandbox(s: Session, spec: PipelineSpec, pipeline_id: int | None) -> dict:
    r, issues, catalog = check(s, spec, pipeline_id)
    if r is None:
        checks = [{"id": f"rule:{i.code}", "title": "Design check", "status": "FAIL", "message": i.message,
                   "fix": i.fix, **({"patch": i.patch} if i.patch else {}),
                   "explanation": {"formula": "spec rule " + i.code, "inputs": {},
                                   "reasoning": "Checked against the catalog before running."}}
                  for i in issues]
        return {"status": "FAILED", "headline": f"Not ready: {len(checks)} problem(s) to fix",
                "platform": PLATFORM_NAMES[spec.platform],
                "counts": {"fail": len(checks), "warn": 0, "pass": 0}, "checks": checks, "row_count": 0,
                "schema": [], "preview": [], "rows": [], "elapsed_ms": 0, "sandbox_sql": None}
    return sandbox.run(r, issues)


def fix(s: Session, pipeline_id: int, patch: dict, by: str = "") -> dict:
    row = _get(s, pipeline_id)
    new = apply_patch(row.spec, patch)
    actions.audit(s, by, "FIX_APPLIED", {"pipeline_id": row.id, "patch": patch})
    update(s, pipeline_id, new, by)
    return validate(s, pipeline_id, by)


def latest_validation(s: Session, row: PipelineRow) -> ValidationRow | None:
    return (s.query(ValidationRow).filter(ValidationRow.pipeline_id == row.id)
            .order_by(ValidationRow.id.desc()).first())


def propose_deploy(s: Session, pipeline_id: int, requested_by: str = "") -> dict:
    row = _get(s, pipeline_id)
    v = latest_validation(s, row)
    if v is None or v.spec_hash != row.spec_hash:
        raise WorkflowError("Test this version in the sandbox before requesting deployment.")
    if v.status == "FAILED":
        raise WorkflowError("The latest sandbox test failed. Fix the problems, test again, then request "
                            "deployment.")
    if row.status == "DEPLOYED" and row.deployed_hash == row.spec_hash:
        raise WorkflowError("This exact version is already deployed.")
    spec = parse_spec(row.spec)
    conn = connectors.get(spec.platform)
    r, _, catalog = check(s, spec, row.id)
    files = [f["path"] for f in compiler.artifacts(r, catalog)]
    payload = {"pipeline": row.name, "spec_hash": row.spec_hash, "version": row.version,
               "platform": PLATFORM_NAMES[spec.platform],
               "target": conn.target_name(spec.target.layer, spec.target.name),
               "summary": summarize(spec, catalog), "validation": {"status": v.status, **v.result["counts"]},
               "warnings": [c["message"] for c in v.result["checks"] if c["status"] == "WARN"],
               "files": files}
    action = actions.propose(s, "DEPLOY_PIPELINE", payload, requested_by or "anonymous", row.id)
    row.status = "PENDING_APPROVAL"
    s.commit()
    return {"pipeline": pipeline_dict(s, row), "action": action}


def mark_rejected(s: Session, pipeline_id: int | None) -> None:
    if pipeline_id is None:
        return
    row = s.get(PipelineRow, pipeline_id)
    if row and row.status == "PENDING_APPROVAL":
        row.status = "REJECTED"


def deploy(s: Session, pipeline_id: int, approved_hash: str | None, approver: str) -> dict:
    """Called only from an approved action. Re-tests, then deploys through the connector."""
    row = _get(s, pipeline_id)
    if not approved_hash or approved_hash != row.spec_hash:
        raise WorkflowError("The pipeline changed after deployment was requested; nothing was deployed.")
    spec = parse_spec(row.spec)
    result = _run_sandbox(s, spec, row.id)              # CI gate: re-run checks on the exact spec
    if result["status"] == "FAILED":
        raise WorkflowError("The sandbox re-test at deployment failed; nothing was deployed.")
    r, _, catalog = check(s, spec, row.id)
    conn = connectors.get(spec.platform)
    target = conn.target_name(spec.target.layer, spec.target.name)
    files = [f["path"] for f in compiler.artifacts(r, catalog)]
    out = conn.deploy(pipeline_id=row.id, target=target, rows_written=len(result["rows"]), files=files)
    s.add(RunRow(pipeline_id=row.id, platform=spec.platform, job_id=out["job_id"], status=out["status"],
                 rows_written=out["rows_written"], duration_ms=out["duration_ms"], log=out["log"],
                 approved_by=approver))
    _publish_dataset(s, row, spec, r, catalog, result, approver)
    row.status, row.deployed_hash, row.updated_at = "DEPLOYED", row.spec_hash, _now()
    actions.audit(s, approver, "PIPELINE_DEPLOYED", {"pipeline_id": row.id, "spec_hash": row.spec_hash,
                                                     "target": target, "job_id": out["job_id"]})
    return {**out, "platform": PLATFORM_NAMES[spec.platform], "target": target}


def _publish_dataset(s, row, spec, r, catalog, result, approver) -> None:
    """Information Layer update: register the new table, its tags and its lineage."""
    tags = set()
    for d in r.datasets.values():
        tags |= {t for t in d["tags"] if t in ("Confidential", "Finance", "Internal")}
    if spec.masks:
        tags.add("PII-masked")
    cmap = compiler.column_map(r)
    descr = {c["output"]: (c["transform"].capitalize() if c["from"] is None else
                           f"{c['transform'].capitalize()} from {c['from']}" if c["transform"].startswith(
                               ("copied", "truncated")) else
                           f"{c['transform'].capitalize()} {c['from']}") for c in cmap}
    cols = [{"name": c["name"], "type": c["type"], "description": descr.get(c["name"], ""),
             **({"classification": c["classification"]} if c.get("classification") else {})}
            for c in result["schema"]]
    ds = s.get(DatasetRow, spec.target.name)
    if ds is None:
        ds = DatasetRow(id=spec.target.name)
        s.add(ds)
    ds.friendly_name = spec.name
    ds.description = summarize(spec, catalog)
    ds.layer, ds.platform = spec.target.layer, spec.platform
    ds.owner = f"Published by the Data Engineering Agent · approved by {approver}"
    ds.tags = sorted(tags) or ["Internal"]
    ds.columns, ds.relationships, ds.curated_version = cols, [], None
    ds.last_loaded, ds.refresh = config.AS_OF_DATE, spec.schedule
    ds.origin, ds.pipeline_id, ds.rows = "pipeline", row.id, result["rows"]
    s.query(LineageRow).filter(LineageRow.pipeline_id == row.id).delete()
    for d in r.datasets:
        s.add(LineageRow(source=d, target=spec.target.name, pipeline_id=row.id,
                         column_map=[c for c in cmap if c["from"] is None or c["from"].startswith(d + ".")]))


# --- views ---------------------------------------------------------------------------------

STEP_TITLES = [
    ("describe", "Describe what you need"),
    ("find", "Find the right data"),
    ("compile", "Generate code for the platform"),
    ("test", "Test in the sandbox"),
    ("approve", "Human approval"),
    ("deploy", "Deploy and record lineage"),
]


def workflow(row: PipelineRow, v: ValidationRow | None, pending: bool) -> list[dict]:
    tested = v is not None and v.spec_hash == row.spec_hash
    failed = tested and v.status == "FAILED"
    st = row.status
    state = {"describe": "done", "find": "done", "compile": "done"}
    state["test"] = "failed" if failed else ("done" if tested else "current")
    if st == "DEPLOYED" and row.deployed_hash == row.spec_hash:
        state["approve"], state["deploy"] = "done", "done"
    elif st == "REJECTED":
        state["approve"], state["deploy"] = "failed", "todo"
    elif pending:
        state["approve"], state["deploy"] = "current", "todo"
    else:
        state["approve"] = "current" if tested and not failed else "todo"
        state["deploy"] = "todo"
    return [{"id": k, "title": t, "state": state[k]} for k, t in STEP_TITLES]


def pipeline_dict(s: Session, row: PipelineRow, with_code: bool = False) -> dict:
    spec = parse_spec(row.spec)
    r, issues, catalog = check(s, spec, row.id)
    v = latest_validation(s, row)
    pending = (s.query(PendingActionRow).filter(PendingActionRow.pipeline_id == row.id,
                                                PendingActionRow.status == "PENDING").first())
    conn = connectors.get(spec.platform)
    out = {"id": row.id, "name": row.name, "status": row.status, "version": row.version,
           "spec": row.spec, "spec_hash": row.spec_hash, "summary": summarize(spec, catalog),
           "platform": spec.platform, "platform_name": PLATFORM_NAMES[spec.platform],
           "target": conn.target_name(spec.target.layer, spec.target.name),
           "created_by": row.created_by, "created_at": row.created_at.isoformat() if row.created_at else None,
           "updated_at": row.updated_at.isoformat() if row.updated_at else None,
           "deployed_version_is_current": row.deployed_hash == row.spec_hash if row.deployed_hash else None,
           "issues": [i.as_dict() for i in issues],
           "validation": None if v is None else {"id": v.id, "status": v.status, "spec_hash": v.spec_hash,
                                                 "current": v.spec_hash == row.spec_hash,
                                                 "at": v.at.isoformat() if v.at else None, **v.result},
           "pending_action": actions.action_dict(pending) if pending else None,
           "workflow": workflow(row, v, pending is not None),
           "runs": information.runs(s, row.id, limit=5)}
    if with_code:
        errors = [i for i in issues if i.level == "error"]
        out["artifacts"] = compiler.artifacts(r, catalog) if r and not errors else []
    return out


def list_pipelines(s: Session) -> list[dict]:
    out = []
    for row in s.query(PipelineRow).order_by(PipelineRow.id.desc()):
        v = latest_validation(s, row)
        spec = row.spec
        out.append({"id": row.id, "name": row.name, "status": row.status, "version": row.version,
                    "platform": spec["platform"], "platform_name": PLATFORM_NAMES[spec["platform"]],
                    "target": spec["target"]["name"],
                    "last_test": None if v is None else {"status": v.status,
                                                         "current": v.spec_hash == row.spec_hash},
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None})
    return out

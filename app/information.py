"""Information Layer: metadata catalog, lineage, run history and governance tags.

The catalog is what makes the agent usable by non-developers: every dataset
has a friendly name, a plain-English description, column descriptions, an
owner, a home platform, freshness, and classification tags (PII,
Confidential...). Lineage records where every published table came from,
down to the column (schema mappings).
"""
from __future__ import annotations

import re

from sqlalchemy.orm import Session

from . import config
from .models import DatasetRow, LineageRow, RunRow
from .spec import PLATFORM_NAMES


class NotFound(LookupError):
    pass


def dataset_dict(d: DatasetRow, with_rows: bool = False) -> dict:
    out = {"id": d.id, "friendly_name": d.friendly_name, "description": d.description, "layer": d.layer,
           "platform": d.platform, "platform_name": PLATFORM_NAMES.get(d.platform, d.platform),
           "owner": d.owner, "tags": d.tags, "columns": d.columns, "relationships": d.relationships,
           "curated_version": d.curated_version, "refresh": d.refresh,
           "last_loaded": d.last_loaded.isoformat() if d.last_loaded else None,
           "freshness_days": (config.AS_OF_DATE - d.last_loaded).days if d.last_loaded else None,
           "origin": d.origin, "pipeline_id": d.pipeline_id, "row_count": len(d.rows or [])}
    if with_rows:
        out["rows"] = d.rows or []
    return out


def catalog_map(s: Session) -> dict[str, dict]:
    """id -> catalog entry WITH sample rows (used by the spec checker and the sandbox)."""
    return {d.id: dataset_dict(d, with_rows=True) for d in s.query(DatasetRow).order_by(DatasetRow.id)}


def list_datasets(s: Session) -> list[dict]:
    rows = s.query(DatasetRow).all()
    layer_order = {"bronze": 0, "silver": 1, "gold": 2}
    rows.sort(key=lambda d: (layer_order.get(d.layer, 9), d.friendly_name))
    return [dataset_dict(d) for d in rows]


def get_dataset(s: Session, dataset_id: str, sample: int = 10) -> dict:
    d = s.get(DatasetRow, dataset_id)
    if d is None:
        raise NotFound(f"No dataset '{dataset_id}' in the catalog")
    out = dataset_dict(d)
    # PII is never shown in previews (to people or to the model), even for fictional data.
    pii = {c["name"] for c in d.columns if c.get("classification") == "PII"}
    out["sample"] = [{k: ("•••• hidden (PII)" if k in pii and v is not None else v) for k, v in r.items()}
                     for r in (d.rows or [])[:sample]]
    out["lineage"] = lineage(s, dataset_id)
    return out


def search(s: Session, query: str, limit: int = 8) -> list[dict]:
    """Plain keyword search over names, descriptions, columns and tags."""
    words = [w for w in re.findall(r"[a-z0-9]+", (query or "").lower()) if len(w) > 1]
    words = [w[:-1] if w.endswith("s") and len(w) > 3 else w for w in words]
    results = []
    for d in s.query(DatasetRow).all():
        hay_name = f"{d.id} {d.friendly_name}".lower()
        hay_desc = d.description.lower()
        hay_cols = " ".join(f"{c['name']} {c.get('description', '')}" for c in d.columns).lower()
        hay_tags = " ".join(d.tags).lower() + " " + d.platform
        score, why = 0, []
        for w in words:
            if w in hay_name:
                score += 5
                why.append(f"name matches '{w}'")
            if w in hay_desc:
                score += 3
                why.append(f"description mentions '{w}'")
            hit_cols = [c["name"] for c in d.columns
                        if w in c["name"].lower() or w in c.get("description", "").lower()]
            if hit_cols:
                score += 2
                why.append(f"column {hit_cols[0]}")
            if w in hay_tags:
                score += 1
        if not words:
            score = 1
        if score:
            if "Certified" in d.tags:
                score += 1
            results.append({**dataset_dict(d), "score": score, "why": sorted(set(why))[:4]})
    results.sort(key=lambda x: (-x["score"], x["friendly_name"]))
    return results[:limit]


def lineage(s: Session, dataset_id: str) -> dict:
    """Upstream and downstream datasets, walked transitively."""
    edges = s.query(LineageRow).all()

    def walk(start, forward):
        seen, out, frontier = {start}, [], [start]
        while frontier:
            nxt = []
            for e in edges:
                a, b = (e.source, e.target) if forward else (e.target, e.source)
                if a in frontier and b not in seen:
                    seen.add(b)
                    nxt.append(b)
                    out.append({"from": e.source, "to": e.target, "pipeline_id": e.pipeline_id,
                                "column_map": e.column_map})
            frontier = nxt
        return out

    return {"dataset": dataset_id, "upstream": walk(dataset_id, False), "downstream": walk(dataset_id, True)}


def runs(s: Session, pipeline_id: int | None = None, limit: int = 50) -> list[dict]:
    q = s.query(RunRow)
    if pipeline_id is not None:
        q = q.filter(RunRow.pipeline_id == pipeline_id)
    return [{"id": r.id, "pipeline_id": r.pipeline_id, "platform": r.platform, "job_id": r.job_id,
             "status": r.status, "rows_written": r.rows_written, "duration_ms": r.duration_ms,
             "log": r.log, "approved_by": r.approved_by, "at": r.at.isoformat() if r.at else None}
            for r in q.order_by(RunRow.id.desc()).limit(limit)]


def import_datasets(s: Session, *, platform: str, owner: str, tags: list[str], layer: str,
                    tables: list[dict], by: str = "") -> dict:
    """Onboard tables from another company's schema export. PII is tagged automatically from column
    names (a safe default: over-tagging blocks, under-tagging leaks); review it in Find data."""
    from . import actions, migration
    from .spec import _slug
    created, skipped = [], []
    for t in tables:
        tid = _slug(t.get("id") or t.get("table") or "")
        if s.get(DatasetRow, tid) is not None:
            skipped.append({"id": tid, "reason": "already in the catalog"})
            continue
        cols, pii = [], []
        for c in t.get("columns", []):
            name = _slug(c["name"])
            cls = c.get("classification") or migration.suggest_classification(name)
            if cls == "PII":
                pii.append(name)
            cols.append({"name": name, "type": migration.map_type(c.get("type", "")),
                         "description": c.get("description") or f"Imported ({c.get('type', 'unknown type')})",
                         **({"classification": cls} if cls else {})})
        if not cols:
            skipped.append({"id": tid, "reason": "no columns"})
            continue
        ds = DatasetRow(id=tid, friendly_name=t.get("friendly_name") or tid.replace("_", " ").title(),
                        description=t.get("description") or f"Imported from a schema export ({len(cols)} columns).",
                        layer=layer, platform=platform, owner=owner or "Unassigned",
                        tags=sorted(set(tags) | ({"PII"} if pii else set())), columns=cols, relationships=[],
                        last_loaded=None, refresh="daily", origin="seed", rows=list(t.get("rows") or []))
        s.add(ds)
        created.append({"id": tid, "columns": len(cols), "pii_columns": pii})
    actions.audit(s, by, "CATALOG_IMPORT", {"created": [c["id"] for c in created], "skipped": skipped})
    s.commit()
    return {"created": created, "skipped": skipped,
            "note": "PII was tagged from column names. Check it in Find data; no sample rows were loaded."}

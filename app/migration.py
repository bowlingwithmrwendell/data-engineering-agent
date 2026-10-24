"""Migration mode: bring an acquired company's data (or another platform's) onto yours.

A CONFORM pipeline maps each source onto ONE target model you define:

    model:   order_id · customer_id · order_date · status · amount   (your standard)
    sources: Lakeside orders  → copied as-is
             Summit orders    → ord_no→order_id, cust_no→customer_id via the ID crosswalk,
                                ord_date converted to a date, 'Closed'→'completed', ...

The compiler stacks the sources (UNION ALL) with a `source_system` column, for
every platform. The sandbox then RECONCILES: every source row arrives exactly
once, totals are preserved, unreadable values and crosswalk misses are counted,
keys don't collide between companies, required fields are filled and values
stay inside the allowed list. Each finding comes with a plain-English fix.

Also here: legacy-SQL equivalence (old query vs new pipeline on the same data),
reconciling any two tables, the re-platform plan, and schema import with PII
tagging. All deterministic; Claude only drafts specs and explains.
"""
from __future__ import annotations

import copy
import difflib
import json
import re
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from . import config, connectors
from .compiler import _filter, _lit, _mask
from .sandbox import FAIL, PASS, WARN, _check, _explain, _finish, _freshness_checks, _sha256
from .spec import IDENT, PLATFORM_NAMES, TEMPORAL, Filter, Issue, Mask, Scalar, Target, _slug

TYPES = ("string", "integer", "decimal", "date", "timestamp")
NUMERIC = ("integer", "decimal")


# --- the conform spec --------------------------------------------------------------------

class Lookup(BaseModel):
    dataset: str = Field(description="Crosswalk dataset, e.g. 'customer_crosswalk'")
    key: str = Field(description="Column in the crosswalk that holds the source's value, e.g. 'legacy_id'")
    value: str = Field(description="Column in the crosswalk that holds your value, e.g. 'customer_id'")


class Mapping(BaseModel):
    target: str = Field(description="Model column this fills, e.g. 'customer_id'")
    source: str | None = Field(None, description="Column of this source dataset (bare name); omit for a constant")
    cast: Literal["string", "integer", "decimal", "date", "timestamp"] | None = Field(
        None, description="Convert the value to this type; unreadable values become blank and are counted")
    value_map: dict[str, str] | None = Field(None, description="Translate values, e.g. {'NE': 'North'}")
    lookup: Lookup | None = Field(None, description="Replace the value through an ID crosswalk")
    default: Scalar | None = Field(None, description="Constant to use when source is omitted")


class ConformSource(BaseModel):
    dataset: str = Field(description="Catalog id of this source, e.g. 'summit_orders'")
    label: str = Field(description="Short name written to the source_system column, e.g. 'summit'")
    mappings: list[Mapping] = Field(default_factory=list)
    filters: list[Filter] = Field(default_factory=list)

    @field_validator("label")
    @classmethod
    def _label(cls, v):
        v = v.strip().lower()
        if not re.match(r"^[a-z][a-z0-9_]{0,30}$", v):
            raise ValueError("label must be a short lowercase name, e.g. 'summit'")
        return v


class ModelColumn(BaseModel):
    name: str
    type: Literal["string", "integer", "decimal", "date", "timestamp"]
    description: str = ""
    key: bool = Field(False, description="Part of the unique key")
    required: bool = Field(False, description="Must never be blank")
    accepted_values: list[str] | None = None


class ConformSpec(BaseModel):
    kind: Literal["conform"] = "conform"
    name: str
    description: str = ""
    platform: Literal["databricks", "snowflake", "fabric"]
    model: list[ModelColumn]
    sources: list[ConformSource]
    source_column: str = "source_system"
    overlap: Literal["keep_all", "prefer_first"] = Field(
        "keep_all", description="prefer_first: skip rows whose key already exists in the first source")
    masks: list[Mask] = Field(default_factory=list, description="Model columns to protect, e.g. 'email'")
    target: Target
    schedule: Literal["manual", "hourly", "daily", "weekly"] = "daily"

    @field_validator("name")
    @classmethod
    def _name(cls, v):
        v = v.strip()
        if not v:
            raise ValueError("name is required")
        return v[:120]


def is_conform(data) -> bool:
    if isinstance(data, ConformSpec):
        return True
    return isinstance(data, dict) and data.get("kind") == "conform"


# --- resolution and checks -------------------------------------------------------------------

@dataclass
class Resolved:
    spec: ConformSpec
    datasets: dict[str, dict]                         # every dataset touched (sources + crosswalks)
    plan: list[dict] = field(default_factory=list)    # per source: {src, ds, exprs:{target: info}}
    model: dict[str, ModelColumn] = field(default_factory=dict)


def _family(t):
    return "number" if t in NUMERIC else ("time" if t in TEMPORAL else t)


def resolve(spec: ConformSpec, catalog: dict[str, dict]) -> tuple[Resolved | None, list[Issue]]:
    issues: list[Issue] = []
    ids = list(catalog)
    if len(spec.sources) < 1:
        return None, [Issue("error", "NO_SOURCES", "Add at least one source.", "Pick the datasets to combine.")]
    names = [c.name for c in spec.model]
    for c in spec.model:
        if not IDENT.match(c.name):
            issues.append(Issue("error", "BAD_COLUMN_NAME", f"'{c.name}' isn't a valid column name.",
                                "Use lowercase letters, numbers and underscores."))
    dup = [n for n, k in Counter(names + [spec.source_column]).items() if k > 1]
    if dup:
        issues.append(Issue("error", "DUPLICATE_OUTPUT", f"Column name used twice: {', '.join(dup)}.",
                            "Give each model column a unique name."))
    labels = [s.label for s in spec.sources]
    if len(set(labels)) != len(labels):
        issues.append(Issue("error", "DUPLICATE_LABEL", "Two sources have the same label.",
                            "Give each source its own label, e.g. 'lakeside' and 'summit'."))
    missing = [s.dataset for s in spec.sources if s.dataset not in catalog]
    for m in missing:
        close = difflib.get_close_matches(m, ids, n=1)
        issues.append(Issue("error", "UNKNOWN_DATASET", f"The source '{m}' is not in the catalog.",
                            f"Did you mean '{close[0]}'?" if close else "Pick a dataset from Find data."))
    if missing:
        return None, issues

    res = Resolved(spec=spec, datasets={}, model={c.name: c for c in spec.model})
    masked = {m.column: m.method for m in spec.masks}
    for m in spec.masks:
        if m.column not in res.model:
            issues.append(Issue("error", "UNKNOWN_COLUMN", f"Can't protect '{m.column}': it isn't a model column.",
                                "Protect a column from the model."))

    for src in spec.sources:
        ds = catalog[src.dataset]
        res.datasets[src.dataset] = ds
        cols = {c["name"]: c for c in ds["columns"]}
        by_target = {}
        for mp in src.mappings:
            if mp.target not in res.model:
                close = difflib.get_close_matches(mp.target, names, n=1)
                issues.append(Issue("error", "UNKNOWN_TARGET",
                                    f"{ds['friendly_name']} maps to '{mp.target}', which isn't in the model.",
                                    f"Did you mean '{close[0]}'?" if close else "Add it to the model or remove it."))
                continue
            if mp.target in by_target:
                issues.append(Issue("error", "DUPLICATE_MAPPING",
                                    f"{ds['friendly_name']} maps '{mp.target}' twice.", "Keep one mapping."))
            by_target[mp.target] = mp

        exprs = {}
        for mc in spec.model:
            mp = by_target.get(mc.name)
            where = f"{ds['friendly_name']} → {mc.name}"
            if mp is None or (mp.source is None and mp.default is None and mp.lookup is None):
                if mc.required or mc.key:
                    issues.append(Issue("error", "MISSING_MAPPING",
                                        f"{ds['friendly_name']} has nothing mapped to required column '{mc.name}'.",
                                        f"Choose which {ds['friendly_name']} column fills '{mc.name}'.",
                                        _suggest_mapping(src, mc, cols)))
                else:
                    issues.append(Issue("warning", "UNMAPPED",
                                        f"{ds['friendly_name']} has no '{mc.name}'; it will be blank for these rows.",
                                        "Fine if the source doesn't have it; or map a column or a constant.",
                                        _suggest_mapping(src, mc, cols)))
                exprs[mc.name] = {"kind": "null", "type": mc.type}
                continue
            if mp.source is None and mp.lookup is None:
                exprs[mc.name] = {"kind": "const", "value": mp.default, "type": mc.type}
                continue
            col = cols.get(mp.source or "")
            if col is None:
                close = difflib.get_close_matches(mp.source or "", list(cols), n=1)
                issues.append(Issue("error", "UNKNOWN_COLUMN", f"{ds['friendly_name']} has no column '{mp.source}'.",
                                    f"Did you mean '{close[0]}'?" if close else f"Available: {', '.join(cols)}.",
                                    {"op": "set_mapping_source", "label": src.label, "target": mc.name,
                                     "to": close[0]} if close else None))
                exprs[mc.name] = {"kind": "null", "type": mc.type}
                continue
            info = {"kind": "col", "dataset": src.dataset, "column": col, "cast": mp.cast,
                    "value_map": mp.value_map, "lookup": None, "type": mc.type}
            out_type = col["type"]
            if mp.value_map:
                if col["type"] != "string":
                    issues.append(Issue("error", "VALUE_MAP_TYPE", f"{where}: value translation only works on text.",
                                        "Convert it first, or remove the translation."))
                out_type = "string"
            if mp.lookup:
                lk = mp.lookup
                xw = catalog.get(lk.dataset)
                if xw is None:
                    issues.append(Issue("error", "UNKNOWN_DATASET", f"{where}: crosswalk '{lk.dataset}' is not in the "
                                        "catalog.", "Pick the crosswalk table from Find data."))
                    exprs[mc.name] = {"kind": "null", "type": mc.type}
                    continue
                xcols = {c["name"]: c for c in xw["columns"]}
                bad = [c for c in (lk.key, lk.value) if c not in xcols]
                if bad:
                    issues.append(Issue("error", "UNKNOWN_COLUMN", f"{xw['friendly_name']} has no column "
                                        f"{', '.join(bad)}.", f"Available: {', '.join(xcols)}."))
                    exprs[mc.name] = {"kind": "null", "type": mc.type}
                    continue
                if _family(xcols[lk.key]["type"]) != _family(col["type"]):
                    issues.append(Issue("error", "JOIN_TYPE_MISMATCH", f"{where}: '{mp.source}' and the crosswalk's "
                                        f"'{lk.key}' hold different kinds of values.", "Match on the same kind of id."))
                res.datasets[lk.dataset] = xw
                info["lookup"] = {"dataset": lk.dataset, "key": lk.key, "value": lk.value,
                                  "value_column": xcols[lk.value]}
                out_type = xcols[lk.value]["type"]
            if mp.cast:
                out_type = mp.cast
            if _family(out_type) != _family(mc.type) or (out_type == "decimal" and mc.type == "integer"):
                issues.append(Issue("error", "TYPE_MISMATCH",
                                    f"{where}: the source holds {out_type} values but the model expects {mc.type}.",
                                    f"Convert it to {mc.type}. Values that can't be read become blank and are "
                                    "counted in the test.",
                                    {"op": "set_mapping_cast", "label": src.label, "target": mc.name, "cast": mc.type}))
            elif out_type != mc.type and not mp.cast:
                info["cast"] = mc.type            # same family (date vs timestamp, int vs decimal): widen silently
            source_cls = (info["lookup"]["value_column"] if info["lookup"] else col).get("classification")
            if source_cls == "PII" and not masked.get(mc.name):
                issues.append(Issue("error", "PII_EXPOSED",
                                    f"{ds['friendly_name']} › {mp.source.replace('_', ' ')} is personal information "
                                    f"(PII) and would land in '{mc.name}' as-is.",
                                    "Protect the column with a one-way hash (still usable for matching).",
                                    {"op": "add_mask", "column": mc.name, "method": "hash"}))
            exprs[mc.name] = info
        for f in src.filters:
            cname = f.column.split(".")[-1]
            if cname not in cols:
                issues.append(Issue("error", "UNKNOWN_COLUMN", f"Filter on {ds['friendly_name']}: no column '{cname}'.",
                                    "Check the column name."))
        res.plan.append({"src": src, "ds": ds, "exprs": exprs})

    keys = [c.name for c in spec.model if c.key]
    if spec.overlap == "prefer_first":
        first = res.plan[0]["exprs"].get(keys[0]) if len(keys) == 1 else None
        if len(keys) != 1 or not first or first["kind"] != "col" or first["lookup"] or first["value_map"]:
            issues.append(Issue("error", "OVERLAP_NEEDS_KEY",
                                "'Prefer the first source' needs exactly one key column, copied straight from the "
                                "first source.", "Mark one model column as the key."))

    t = spec.target
    if not IDENT.match(t.name):
        issues.append(Issue("error", "BAD_TARGET_NAME", f"'{t.name}' isn't a valid table name.",
                            "Use lowercase letters, numbers and underscores.", {"op": "rename_target", "to": _slug(t.name)}))
    elif t.name in catalog and catalog[t.name].get("origin") == "seed":
        issues.append(Issue("error", "TARGET_IS_SOURCE", f"'{t.name}' is an existing source table.",
                            "Choose a new name for the result.", {"op": "rename_target", "to": f"{t.name}_combined"}))
    for d, ds in res.datasets.items():
        if ds["platform"] != spec.platform and ds.get("origin") == "seed":
            issues.append(Issue("warning", "CROSS_PLATFORM",
                                f"{ds['friendly_name']} lives in {PLATFORM_NAMES[ds['platform']]}; the combined "
                                f"table is built on {PLATFORM_NAMES[spec.platform]}.",
                                "Normal during a migration: replicate or federate it first (Fivetran, Data Factory, "
                                "Lakehouse Federation, a data share)."))
    return res, issues


def _suggest_mapping(src: ConformSource, mc: ModelColumn, cols: dict) -> dict | None:
    names = list(cols)
    close = difflib.get_close_matches(mc.name, names, n=1, cutoff=0.5)
    if not close:
        short = mc.name.replace("_id", "").replace("_", "")
        close = [n for n in names if short and short in n.replace("_", "")][:1]
    return {"op": "set_mapping_source", "label": src.label, "target": mc.name, "to": close[0]} if close else None


# --- compilation ---------------------------------------------------------------------------------

_TYPE_SQL = {
    "databricks": {"string": "STRING", "integer": "INT", "decimal": "DECIMAL(18, 2)", "date": "DATE",
                   "timestamp": "TIMESTAMP"},
    "snowflake": {"string": "VARCHAR", "integer": "NUMBER(38, 0)", "decimal": "NUMBER(18, 2)", "date": "DATE",
                  "timestamp": "TIMESTAMP_NTZ"},
    "fabric": {"string": "VARCHAR(4000)", "integer": "INT", "decimal": "DECIMAL(18, 2)", "date": "DATE",
               "timestamp": "DATETIME2(6)"},
    "dbt": {"string": "dbt.type_string()", "integer": "dbt.type_int()", "decimal": "dbt.type_numeric()",
            "date": "'date'", "timestamp": "dbt.type_timestamp()"},
}


def _try_cast(x: str, t: str, d: str) -> str:
    if d == "sqlite":
        return f"try_cast({x}, '{t}')"
    if d == "dbt":
        return f"{{{{ dbt.safe_cast(\"{x}\", {_TYPE_SQL['dbt'][t]}) }}}}"
    if d == "snowflake":
        return f"TRY_CAST(CAST({x} AS VARCHAR) AS {_TYPE_SQL[d][t]})"
    return f"TRY_CAST({x} AS {_TYPE_SQL[d][t]})"


def _null(t: str, d: str) -> str:
    if d == "sqlite":
        return "NULL"
    if d == "dbt":
        return f"cast(null as {{{{ {_TYPE_SQL['dbt'][t]} }}}})" if t != "date" else "cast(null as date)"
    return f"CAST(NULL AS {_TYPE_SQL[d][t]})"


def _value_map(x: str, vm: dict[str, str]) -> str:
    q = lambda v: "'" + str(v).replace("'", "''") + "'"  # noqa: E731
    return "CASE " + x + " " + " ".join(f"WHEN {q(k)} THEN {q(v)}" for k, v in vm.items()) + f" ELSE {x} END"


def _expr(info: dict, alias: str, xw_alias: str | None, d: str) -> str:
    if info["kind"] == "null":
        return _null(info["type"], d)
    if info["kind"] == "const":
        return _lit(info["value"], d, info["type"]) if info["value"] is not None else _null(info["type"], d)
    x = f"{xw_alias}.{info['lookup']['value']}" if info["lookup"] else f"{alias}.{info['column']['name']}"
    if info["value_map"]:
        x = _value_map(x, info["value_map"])
    if info["cast"]:
        x = _try_cast(x, info["cast"], d)
    return x


def _source_select(r: Resolved, i: int, d: str, table_of) -> str:
    p = r.plan[i]
    src, alias = p["src"], p["src"].dataset
    masked = {m.column: m.method for m in r.spec.masks}
    sel, joins, xw_of = [], [], {}
    n = 0
    for mc in r.spec.model:
        info = p["exprs"][mc.name]
        xa = None
        if info.get("lookup"):
            n += 1
            xa = f"xw{n}"
            lk = info["lookup"]
            joins.append(f"LEFT JOIN {table_of(lk['dataset'])} AS {xa}\n    ON {alias}.{info['column']['name']} = {xa}.{lk['key']}")
            xw_of[mc.name] = xa
        e = _mask(_expr(info, alias, xa, d), masked.get(mc.name), d)
        sel.append(f"{e} AS {mc.name}")
    sel.append(f"{_lit(src.label, d, 'string')} AS {r.spec.source_column}")
    where = []
    cols = {c["name"]: c for c in p["ds"]["columns"]}
    for f in src.filters:
        cname = f.column.split(".")[-1]
        where.append(_filter(f.model_copy(update={"column": f"{alias}.{cname}"}), cols[cname], d))
    if r.spec.overlap == "prefer_first" and i > 0:
        key = next(c.name for c in r.spec.model if c.key)
        first = r.plan[0]
        k_expr = _expr(p["exprs"][key], alias, xw_of.get(key), d)
        fcol = first["exprs"][key]["column"]["name"]
        fa = f"{first['src'].dataset}_first"
        where.append(f"({k_expr} IS NULL OR {k_expr} NOT IN (SELECT {fa}.{fcol} FROM "
                     f"{table_of(first['src'].dataset)} AS {fa} WHERE {fa}.{fcol} IS NOT NULL))")
    sql = "SELECT\n" + ",\n".join("    " + s for s in sel) + f"\nFROM {table_of(src.dataset)} AS {alias}"
    if joins:
        sql += "\n" + "\n".join(joins)
    if where:
        sql += "\nWHERE " + "\n  AND ".join(where)
    return sql


def body_sql(r: Resolved, d: str, table_of) -> str:
    parts = [_source_select(r, i, d, table_of) for i in range(len(r.plan))]
    return ("\nUNION ALL\n".join(parts)) if d != "dbt" else ("\nunion all\n".join(parts))


def sandbox_sql(r: Resolved) -> str:
    return body_sql(r, "sqlite", lambda d: d)


def _table_for(conn, ds: dict) -> str:
    if ds.get("origin") == "pipeline":
        return conn.table_name(ds["layer"], ds["id"])
    return conn.source_name(ds)


def summarize(spec: ConformSpec, catalog: dict) -> str:
    srcs = [f"{catalog.get(s.dataset, {}).get('friendly_name', s.dataset)} ({s.label})" for s in spec.sources]
    did = []
    n_cast = sum(1 for s in spec.sources for m in s.mappings if m.cast)
    n_vm = sum(1 for s in spec.sources for m in s.mappings if m.value_map)
    n_lk = sum(1 for s in spec.sources for m in s.mappings if m.lookup)
    if n_vm:
        did.append(f"translating values in {n_vm} column(s)")
    if n_cast:
        did.append(f"converting {n_cast} column(s)")
    if n_lk:
        did.append(f"matching {n_lk} ID column(s) through a crosswalk")
    if spec.masks:
        did.append("protecting " + ", ".join(m.column.replace("_", " ") for m in spec.masks))
    sched = {"manual": "on demand", "hourly": "every hour", "daily": "every day", "weekly": "every week"}[spec.schedule]
    head = "Combines " + (" and ".join(srcs) if len(srcs) <= 2 else ", ".join(srcs))
    return (f"{head} into one standard table with {len(spec.model)} columns "
            f"({', '.join(c.name.replace('_', ' ') for c in spec.model)}) plus {spec.source_column.replace('_', ' ')}"
            + (", " + ", ".join(did) if did else "")
            + (", skipping rows whose key already exists in the first source" if spec.overlap == "prefer_first" else "")
            + f" — saved as {spec.target.layer} {spec.target.materialization} '{spec.target.name}' on "
              f"{PLATFORM_NAMES[spec.platform]}, refreshed {sched}.")


def _header(r: Resolved, catalog: dict, spec_hash: str, c: str = "--") -> str:
    return (f"{c} Pipeline: {r.spec.name} (conform / migration)\n"
            f"{c} Generated by the Data Engineering Agent from spec {spec_hash}.\n"
            f"{c} Change the spec, not this file.\n"
            f"{c} What it does: {summarize(r.spec, catalog)}\n")


def platform_sql(r: Resolved, catalog: dict, h: str, platform: str | None = None) -> str:
    platform = platform or r.spec.platform
    conn = connectors.get(platform)
    body = body_sql(r, platform, lambda d: _table_for(conn, catalog[d]))
    t = r.spec.target
    target = conn.target_name(t.layer, t.name)
    view = t.materialization == "view"
    if platform == "fabric":
        ddl = (f"CREATE OR ALTER VIEW {target} AS\n{body};\n" if view else
               f"DROP TABLE IF EXISTS {target};\nCREATE TABLE {target} AS\nSELECT * FROM (\n{body}\n) AS combined;\n")
    else:
        ddl = f"CREATE OR REPLACE {'VIEW' if view else 'TABLE'} {target} AS\n{body};\n"
    return _header(r, catalog, h) + "\n" + ddl


def pyspark(r: Resolved, catalog: dict, h: str) -> str | None:
    conn = connectors.get(r.spec.platform)
    if not conn.supports_pyspark:
        return None

    def spark_name(d):
        n = _table_for(conn, catalog[d])
        return n.replace(f"{conn.catalog_name}_wh.", f"{conn.catalog_name}_lh.", 1) if conn.key == "fabric" else n

    masked = {m.column: m.method for m in r.spec.masks}
    L = [_header(r, catalog, h, "#").rstrip(), "from pyspark.sql import functions as F", ""]
    for d in r.datasets:
        L.append(f'{d} = spark.table("{spark_name(d)}").alias("{d}")')
    parts = []
    for i, p in enumerate(r.plan):
        src, alias = p["src"], p["src"].dataset
        L.append("")
        L.append(f"# {catalog[src.dataset]['friendly_name']} ({src.label})")
        L.append(f"part_{i + 1} = {alias}")
        n = 0
        sel = []
        for mc in r.spec.model:
            info = p["exprs"][mc.name]
            xa = None
            if info.get("lookup"):
                n += 1
                xa = f"xw{n}"
                lk = info["lookup"]
                L.append(f'part_{i + 1} = part_{i + 1}.join({lk["dataset"]}.alias("{xa}"), '
                         f'F.col("{alias}.{info["column"]["name"]}") == F.col("{xa}.{lk["key"]}"), "left")')
            e = _mask(_expr(info, alias, xa, "databricks"), masked.get(mc.name), "databricks")
            sel.append(f'F.expr("{e}").alias("{mc.name}")' if info["kind"] != "col" or info["cast"] or
                       info["value_map"] or masked.get(mc.name) else f'F.col("{e}").alias("{mc.name}")')
        cols = {c["name"]: c for c in p["ds"]["columns"]}
        for f in src.filters:
            cname = f.column.split(".")[-1]
            L.append(f'part_{i + 1} = part_{i + 1}.where(F.expr("{_filter(f.model_copy(update={"column": f"{alias}.{cname}"}), cols[cname], "databricks")}"))')
        sel.append(f'F.lit("{src.label}").alias("{r.spec.source_column}")')
        L.append(f"part_{i + 1} = part_{i + 1}.select(\n" + "".join(f"    {s},\n" for s in sel) + ")")
        if r.spec.overlap == "prefer_first" and i > 0:
            key = next(c.name for c in r.spec.model if c.key)
            fcol = r.plan[0]["exprs"][key]["column"]["name"]
            L.append(f'part_{i + 1} = part_{i + 1}.join({r.plan[0]["src"].dataset}.select(F.col("{fcol}").alias("__k")), '
                     f'F.col("{key}") == F.col("__k"), "left_anti")')
        parts.append(f"part_{i + 1}")
    L += ["", "df = " + ".unionByName(".join(parts) + ")" * (len(parts) - 1)]
    target = spark_name_target = conn.target_name(r.spec.target.layer, r.spec.target.name)
    if conn.key == "fabric":
        target = spark_name_target.replace(f"{conn.catalog_name}_wh.", f"{conn.catalog_name}_lh.", 1)
    L.append(f'df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("{target}")')
    return "\n".join(L) + "\n"


def dbt(r: Resolved, catalog: dict, h: str) -> tuple[str, str]:
    def table_of(d):
        ds = catalog[d]
        if ds.get("origin") == "pipeline":
            return f"{{{{ ref('{d}') }}}}"
        return f"{{{{ source('{'raw' if ds['layer'] == 'bronze' else ds['layer']}', '{d}') }}}}"

    body = body_sql(r, "dbt", table_of)
    model = (f"{{{{ config(materialized='{r.spec.target.materialization}', schema='{r.spec.target.layer}') }}}}\n\n"
             + _header(r, catalog, h).rstrip() + "\n\n" + body + "\n")
    keys = [c.name for c in r.spec.model if c.key]
    Y = ["version: 2", "", "models:", f"  - name: {r.spec.target.name}",
         f"    description: {json.dumps(summarize(r.spec, catalog))}"]
    if len(keys) > 1:
        Y += ["    tests:", "      - dbt_utils.unique_combination_of_columns:",
              "          combination_of_columns: [" + ", ".join(keys) + "]"]
    Y.append("    columns:")
    for c in r.spec.model:
        Y.append(f"      - name: {c.name}")
        if c.description:
            Y.append(f"        description: {json.dumps(c.description)}")
        tests = []
        if c.key and len(keys) == 1:
            tests.append("unique")
        if c.required or c.key:
            tests.append("not_null")
        if c.accepted_values:
            tests.append("accepted_values:\n              values: " + json.dumps(c.accepted_values)
                         + "\n              config: {severity: warn}")
        if tests:
            Y.append("        tests:")
            Y += [f"          - {t}" for t in tests]
    Y += [f"      - name: {r.spec.source_column}", "        tests:", "          - not_null", "          - accepted_values:",
          "              values: " + json.dumps([s.label for s in r.spec.sources])]
    return model, "\n".join(Y) + "\n"


def mapping_csv(r: Resolved) -> str:
    """A reviewer-friendly mapping document: what fills each model column, per source."""
    rows = ["model_column,type,key,required," + ",".join(p["src"].label for p in r.plan)]
    for mc in r.spec.model:
        cells = []
        for p in r.plan:
            cells.append(_describe(p["exprs"][mc.name]).replace('"', "'"))
        rows.append(",".join([mc.name, mc.type, "yes" if mc.key else "", "yes" if mc.required else ""]
                             + [f'"{c}"' for c in cells]))
    return "\n".join(rows) + "\n"


def _describe(info: dict) -> str:
    if info["kind"] == "null":
        return "(blank)"
    if info["kind"] == "const":
        return f"constant {info['value']!r}"
    s = info["column"]["name"]
    if info["value_map"]:
        s += " translated (" + ", ".join(f"{k}→{v}" for k, v in info["value_map"].items()) + ")"
    if info["lookup"]:
        lk = info["lookup"]
        s += f" via {lk['dataset']}.{lk['key']}→{lk['value']}"
    if info["cast"]:
        s += f" as {info['cast']}"
    return s


def column_map(r: Resolved) -> list[dict]:
    out = []
    for p in r.plan:
        for mc in r.spec.model:
            info = p["exprs"][mc.name]
            if info["kind"] != "col":
                out.append({"output": mc.name, "from": None, "transform": _describe(info) + f" ({p['src'].label})"})
                continue
            frm = (f"{info['lookup']['dataset']}.{info['lookup']['value']}" if info["lookup"]
                   else f"{p['src'].dataset}.{info['column']['name']}")
            out.append({"output": mc.name, "from": frm, "transform": f"{_describe(info)} ({p['src'].label})"})
            if info["lookup"]:
                out.append({"output": mc.name, "from": f"{p['src'].dataset}.{info['column']['name']}",
                            "transform": f"matched through {info['lookup']['dataset']} ({p['src'].label})"})
    return out


def artifacts(r: Resolved, catalog: dict, h: str) -> list[dict]:
    spec = r.spec
    conn = connectors.get(spec.platform)
    name = spec.target.name
    sql = platform_sql(r, catalog, h)
    files = [{"path": f"sql/{name}.sql", "language": "sql",
              "title": f"{PLATFORM_NAMES[spec.platform]} SQL ({conn.sql_dialect})", "content": sql}]
    py = pyspark(r, catalog, h)
    if py:
        files.append({"path": f"notebooks/{name}.py", "language": "python", "title": "PySpark notebook", "content": py})
    model, schema = dbt(r, catalog, h)
    files.append({"path": f"dbt/models/{spec.target.layer}/{name}.sql", "language": "sql", "title": "dbt model",
                  "content": model})
    files.append({"path": f"dbt/models/{spec.target.layer}/{name}.yml", "language": "yaml",
                  "title": "dbt tests (schema.yml)", "content": schema})
    fn, lang, content = conn.schedule_artifact(spec.name, conn.target_name(spec.target.layer, name),
                                               sql.split("\n\n", 1)[1], spec.schedule)
    files.append({"path": f"deploy/{fn}", "language": lang, "title": f"{conn.label} schedule", "content": content})
    files.append({"path": f"docs/{name}_mapping.csv", "language": "csv", "title": "Mapping document (for data owners)",
                  "content": mapping_csv(r)})
    files.append({"path": f"specs/{name}.json", "language": "json", "title": "Pipeline spec (source of truth)",
                  "content": json.dumps(spec.model_dump(mode="json"), indent=2) + "\n"})
    files.append({"path": f"sandbox/{name}.sql", "language": "sql", "title": "Sandbox SQL (what the test ran)",
                  "content": sandbox_sql(r) + "\n"})
    return files


# --- the sandbox, with reconciliation ------------------------------------------------------------

def _py_try_cast(v, t):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        if t == "integer":
            f = float(s.replace(",", ""))
            return int(f) if f == int(f) else None
        if t == "decimal":
            return round(float(s.replace(",", "").replace("$", "")), 2)
        if t == "date":
            return date.fromisoformat(s[:10]).isoformat()
        if t == "timestamp":
            return datetime.fromisoformat(s).strftime("%Y-%m-%d %H:%M:%S")
        return s
    except (ValueError, TypeError):
        return None


def connect() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.create_function("sha256", 1, _sha256, deterministic=True)
    con.create_function("try_cast", 2, _py_try_cast, deterministic=True)
    deadline = time.monotonic() + config.SANDBOX_TIMEOUT_SECONDS
    con.set_progress_handler(lambda: 1 if time.monotonic() > deadline else 0, 10_000)
    return con


def load(con, ds: dict) -> None:
    from .sandbox import _load
    _load(con, ds)


def output_types(r: Resolved) -> list[dict]:
    masked = {m.column for m in r.spec.masks}
    out = []
    for mc in r.spec.model:
        cls = "PII-masked" if mc.name in masked else None
        out.append({"name": mc.name, "type": "string" if mc.name in masked else mc.type,
                    **({"classification": cls} if cls else {})})
    out.append({"name": r.spec.source_column, "type": "string"})
    return out


def run(r: Resolved, spec_issues: list[Issue], as_of: date | None = None) -> dict:
    as_of = as_of or config.AS_OF_DATE
    started = time.monotonic()
    checks = []
    errors = [i for i in spec_issues if i.level == "error"]
    for i in spec_issues:
        gov = i.code in ("PII_EXPOSED", "CROSS_PLATFORM")
        checks.append(_check(f"rule:{i.code}", "Governance rule" if gov else "Mapping check",
                             FAIL if i.level == "error" else WARN, i.message,
                             _explain("spec rule " + i.code, {}, "Checked against the catalog before running."),
                             i.fix, i.patch))
    sql = sandbox_sql(r) if not errors else None
    if errors:
        return _finish(r, checks, [], [], started, sql, ran=False)
    con = connect()
    try:
        for ds in r.datasets.values():
            load(con, ds)
        try:
            cur = con.execute(sql)
            names = [d[0] for d in cur.description]
            rows = [dict(zip(names, t)) for t in cur.fetchmany(config.SANDBOX_MAX_OUTPUT_ROWS + 1)]
        except sqlite3.Error as exc:
            checks.append(_check("runs", "Runs successfully", FAIL, f"The compiled code failed to run: {exc}",
                                 _explain("execute(sandbox_sql)", {"error": str(exc)}, "Executed on sample data.")))
            return _finish(r, checks, [], [], started, sql, ran=False)
        rows = rows[: config.SANDBOX_MAX_OUTPUT_ROWS]
        checks.append(_check("runs", "Runs successfully", PASS,
                             f"Combined {len(r.plan)} source(s) into {len(rows)} row(s).",
                             _explain("execute(sandbox_sql)", {"result_rows": len(rows)},
                                      "The exact SQL compiled from the spec ran on isolated sample data.")))
        checks += _reconcile_sources(con, r, rows)
    finally:
        con.close()
    checks += _model_checks(r, rows)
    checks += _freshness_checks(r, as_of)
    return _finish(r, checks, _interleave(rows, r.spec.source_column), output_types(r), started, sql, ran=True)


def _interleave(rows: list[dict], sc: str) -> list[dict]:
    """Round-robin rows by source so a preview shows every company's data, not just the first."""
    groups: dict[str, list] = {}
    for row in rows:
        groups.setdefault(row[sc], []).append(row)
    out, i = [], 0
    while len(out) < len(rows):
        for g in groups.values():
            if i < len(g):
                out.append(g[i])
        i += 1
    return out


def _where(r: Resolved, p: dict) -> tuple[str, str]:
    """(filter WHERE clause, overlap-exclusion condition) for one source, in sandbox SQL."""
    alias = p["src"].dataset
    cols = {c["name"]: c for c in p["ds"]["columns"]}
    w = [_filter(f.model_copy(update={"column": f"{alias}.{f.column.split('.')[-1]}"}),
                 cols[f.column.split(".")[-1]], "sqlite") for f in p["src"].filters]
    return (" WHERE " + " AND ".join(w)) if w else "", ""


def _reconcile_sources(con, r: Resolved, rows: list[dict]) -> list[dict]:
    out = []
    sc = r.spec.source_column
    for i, p in enumerate(r.plan):
        src, ds, alias = p["src"], p["ds"], p["src"].dataset
        fname = ds["friendly_name"]
        mine = [row for row in rows if row[sc] == src.label]
        where, _ = _where(r, p)
        rows_in = con.execute(f"SELECT COUNT(*) FROM {alias} AS {alias}{where}").fetchone()[0]
        excluded = 0
        if r.spec.overlap == "prefer_first" and i > 0:
            key = next(c.name for c in r.spec.model if c.key)
            first = r.plan[0]
            info = p["exprs"][key]
            fcol = first["exprs"][key]["column"]["name"]
            if info["kind"] == "col" and not info["lookup"]:
                k = _expr(info, alias, None, "sqlite")
                excluded = con.execute(
                    f"SELECT COUNT(*) FROM {alias} AS {alias}{where}{' AND ' if where else ' WHERE '}"
                    f"{k} IN (SELECT {fcol} FROM {first['src'].dataset} WHERE {fcol} IS NOT NULL)").fetchone()[0]
            elif info["lookup"]:
                lk = info["lookup"]
                excluded = con.execute(
                    f"SELECT COUNT(*) FROM {alias} AS {alias} LEFT JOIN {lk['dataset']} AS xw ON "
                    f"{alias}.{info['column']['name']} = xw.{lk['key']}{where}{' AND ' if where else ' WHERE '}"
                    f"xw.{lk['value']} IN (SELECT {fcol} FROM {first['src'].dataset} WHERE {fcol} IS NOT NULL)"
                ).fetchone()[0]
        expected = rows_in - excluded
        status = PASS if len(mine) == expected else FAIL
        msg = (f"All {expected} {fname} row(s) arrived exactly once"
               + (f" ({excluded} skipped because they already exist in {r.plan[0]['ds']['friendly_name']})"
                  if excluded else "") + "."
               if status == PASS else
               (f"{fname}: {expected} row(s) expected, {len(mine)} arrived. "
                + ("A crosswalk with repeated IDs is duplicating rows." if len(mine) > expected
                   else "Rows were lost.")))
        out.append(_check(f"recon:rows:{src.label}", f"Every {fname} row arrives once", status, msg,
                          _explain("rows_out(source) = rows_in(source, after filters) - skipped_overlap",
                                   {"rows_in": rows_in, "skipped_overlap": excluded, "rows_out": len(mine)},
                                   "Row counts are the first thing an auditor checks after a migration."),
                          "" if status == PASS else "Make the crosswalk one row per ID, or fix the filters."))

        for mc in r.spec.model:
            info = p["exprs"][mc.name]
            if info["kind"] != "col":
                continue
            col = f"{alias}.{info['column']['name']}"
            if info["cast"] and not info["lookup"]:
                bad = con.execute(
                    f"SELECT {col} FROM {alias} AS {alias}{where}{' AND ' if where else ' WHERE '}"
                    f"{col} IS NOT NULL AND TRIM(CAST({col} AS TEXT)) <> '' AND "
                    f"try_cast({_value_map(col, info['value_map']) if info['value_map'] else col}, '{info['cast']}') IS NULL"
                ).fetchall()
                if bad:
                    ex = sorted({str(b[0]) for b in bad})[:4]
                    sev = FAIL if (mc.required or mc.key) else WARN
                    out.append(_check(f"recon:cast:{src.label}:{mc.name}", f"{fname} › {info['column']['name']} reads "
                                      f"as {info['cast']}", sev,
                                      f"{len(bad)} value(s) couldn't be read as {info['cast']} "
                                      f"({', '.join(repr(e) for e in ex)}); '{mc.name}' will be blank for those rows.",
                                      _explain(f"count(rows where {info['column']['name']} is filled but "
                                               f"try_cast(..., {info['cast']}) is null)",
                                               {"unreadable": len(bad), "examples": ex},
                                               "Unreadable values are kept visible instead of silently becoming 0."),
                                      f"Ask the {ds.get('owner') or 'source'} owner to correct these values at the source."))
                else:
                    out.append(_check(f"recon:cast:{src.label}:{mc.name}", f"{fname} › {info['column']['name']} reads "
                                      f"as {info['cast']}", PASS, "Every value converted cleanly.",
                                      _explain("count(unreadable values)", {"unreadable": 0}, "All values parsed.")))
            if mc.type in NUMERIC and not info["lookup"] and not info["value_map"]:
                src_sum = con.execute(
                    f"SELECT ROUND(COALESCE(SUM(try_cast({col}, '{mc.type}')), 0), 2) FROM {alias} AS {alias}{where}"
                ).fetchone()[0]
                if excluded:
                    src_sum = None   # overlap skips rows on purpose; compare row counts instead
                out_sum = round(sum(row[mc.name] or 0 for row in mine), 2)
                if src_sum is not None:
                    ok = abs(src_sum - out_sum) < 0.005
                    out.append(_check(f"recon:sum:{src.label}:{mc.name}", f"{fname} total of {mc.name} is preserved",
                                      PASS if ok else FAIL,
                                      f"Source and combined table both total {_fmt(out_sum)}." if ok else
                                      f"Source totals {_fmt(src_sum)} but the combined table has {_fmt(out_sum)}.",
                                      _explain(f"sum(source.{info['column']['name']}) = sum(combined.{mc.name}) "
                                               f"where {sc} = '{src.label}'",
                                               {"source_total": src_sum, "combined_total": out_sum},
                                               "Totals must survive a migration to the cent."),
                                      "" if ok else "Check the joins and conversions for this column."))
            if info["lookup"]:
                lk = info["lookup"]
                miss = con.execute(
                    f"SELECT DISTINCT {col} FROM {alias} AS {alias}{where}{' AND ' if where else ' WHERE '}{col} IS NOT "
                    f"NULL AND {col} NOT IN (SELECT {lk['key']} FROM {lk['dataset']} WHERE {lk['key']} IS NOT NULL)"
                ).fetchall()
                n_rows = con.execute(
                    f"SELECT COUNT(*) FROM {alias} AS {alias}{where}{' AND ' if where else ' WHERE '}{col} IS NOT NULL "
                    f"AND {col} NOT IN (SELECT {lk['key']} FROM {lk['dataset']} WHERE {lk['key']} IS NOT NULL)"
                ).fetchone()[0]
                dupes = con.execute(f"SELECT COUNT(*) FROM (SELECT {lk['key']} FROM {lk['dataset']} "
                                    f"GROUP BY {lk['key']} HAVING COUNT(*) > 1)").fetchone()[0]
                xname = r.datasets[lk["dataset"]]["friendly_name"]
                if miss:
                    ex = [m[0] for m in miss[:4]]
                    sev = FAIL if (mc.required or mc.key) else WARN
                    out.append(_check(f"recon:lookup:{src.label}:{mc.name}", f"Every {info['column']['name']} is in "
                                      f"the {xname}", sev,
                                      f"{len(miss)} {fname} ID(s) aren't in the {xname} (e.g. {', '.join(map(str, ex))}); "
                                      f"{n_rows} row(s) will have a blank {mc.name}.",
                                      _explain(f"distinct {info['column']['name']} not found in {lk['dataset']}.{lk['key']}",
                                               {"missing_ids": len(miss), "rows_affected": n_rows, "examples": ex},
                                               "A crosswalk miss means a record can't be linked to your systems."),
                                      f"Ask the master-data owner to add these IDs to the {xname}."))
                else:
                    out.append(_check(f"recon:lookup:{src.label}:{mc.name}", f"Every {info['column']['name']} is in "
                                      f"the {xname}", PASS, "All IDs matched.",
                                      _explain("missing ids", {"missing_ids": 0}, "Every ID has a match.")))
                if dupes:
                    out.append(_check(f"recon:xwdup:{lk['dataset']}", f"The {xname} has one row per ID", FAIL,
                                      f"{dupes} ID(s) appear more than once in the {xname}, so rows get duplicated.",
                                      _explain(f"count(keys in {lk['dataset']}.{lk['key']} with more than one row)",
                                               {"duplicate_ids": dupes}, "A crosswalk must map each ID once."),
                                      "Remove the duplicates in the crosswalk."))
    return out


def _model_checks(r: Resolved, rows: list[dict]) -> list[dict]:
    out = []
    sc = r.spec.source_column
    keys = [c.name for c in r.spec.model if c.key]
    if keys:
        seen: dict[tuple, set] = {}
        for row in rows:
            k = tuple(row[c] for c in keys)
            if None in k:
                continue
            seen.setdefault(k, set()).add(row[sc])
        counts = Counter(tuple(row[c] for c in keys) for row in rows if None not in tuple(row[c] for c in keys))
        dup = [k for k, n in counts.items() if n > 1]
        cross = [k for k in dup if len(seen[k]) > 1]
        if dup:
            ex = ", ".join(" / ".join(map(str, k)) for k in dup[:3])
            first = r.plan[0]["ds"]["friendly_name"]
            msg = (f"{len(dup)} {' + '.join(keys)} value(s) appear more than once (e.g. {ex})"
                   + (f"; {len(cross)} of them exist in more than one company's data — the same record twice."
                      if cross else "."))
            patch = ({"op": "set_overlap", "value": "prefer_first"}
                     if cross and len(keys) == 1 and r.spec.overlap == "keep_all" else None)
            out.append(_check("model:key", f"{' + '.join(keys)} is unique across all sources", FAIL, msg,
                              _explain("count(key values with more than one row)",
                                       {"duplicate_keys": len(dup), "in_both_sources": len(cross),
                                        "examples": [list(k) for k in dup[:5]]},
                                       "Two companies often know the same customer or product under one ID."),
                              f"Keep the {first} record when a key exists in both (prefer the first source)."
                              if patch else "Make the key unique (e.g. include the source system).", patch))
        else:
            out.append(_check("model:key", f"{' + '.join(keys)} is unique across all sources", PASS,
                              "No key appears twice.", _explain("duplicate keys", {"duplicate_keys": 0}, "Unique.")))
    for mc in r.spec.model:
        if mc.required or mc.key:
            by = Counter(row[sc] for row in rows if row.get(mc.name) is None)
            n = sum(by.values())
            out.append(_check(f"model:required:{mc.name}", f"'{mc.name}' is always filled in", FAIL if n else PASS,
                              "No blanks." if not n else
                              f"{n} row(s) have a blank {mc.name} ({', '.join(f'{k}: {v}' for k, v in by.items())}). "
                              "It's required.",
                              _explain(f"count(rows where {mc.name} is null)", {"blank_rows": n, "by_source": dict(by)},
                                       "Required columns must be complete before the table is trusted."),
                              "" if not n else "Fix the source values, add the missing IDs to the crosswalk, or mark "
                                               "the column as not required."))
        if mc.accepted_values:
            bad = Counter((row[sc], row[mc.name]) for row in rows
                          if row.get(mc.name) is not None and row[mc.name] not in mc.accepted_values)
            if bad:
                vals = sorted({v for (_, v) in bad})
                out.append(_check(f"model:values:{mc.name}", f"'{mc.name}' uses only your standard values", WARN,
                                  f"{sum(bad.values())} row(s) have values outside your list: "
                                  f"{', '.join(repr(v) for v in vals[:6])} (allowed: {', '.join(mc.accepted_values)}).",
                                  _explain(f"count(rows where {mc.name} not in accepted_values)",
                                           {"unexpected_values": vals, "by_source": {f"{a}:{b}": n for (a, b), n in bad.items()}},
                                           "Untranslated codes break reports that filter on standard values."),
                                  f"Add translations for {', '.join(repr(v) for v in vals[:3])} to the value map."))
            else:
                out.append(_check(f"model:values:{mc.name}", f"'{mc.name}' uses only your standard values", PASS,
                                  "Every value is on the list.",
                                  _explain("values outside accepted list", {"unexpected": 0}, "All standard.")))
    by_src = Counter(row[sc] for row in rows)
    out.append(_check("model:sources", "Rows by source", PASS,
                      ", ".join(f"{k}: {v}" for k, v in by_src.items()) or "No rows.",
                      _explain(f"count(*) group by {sc}", dict(by_src), "How much each company contributes.")))
    return out


def _fmt(v):
    return f"{v:,.2f}" if isinstance(v, float) and not float(v).is_integer() else f"{v:,.0f}"


# --- fixes -------------------------------------------------------------------------------------------

def apply_patch(data: dict, patch: dict) -> dict:
    d = copy.deepcopy(data)
    op = patch.get("op")

    def mapping(label, target):
        src = next((s for s in d["sources"] if s["label"] == label), None)
        if src is None:
            raise ValueError(f"No source '{label}'")
        m = next((m for m in src.setdefault("mappings", []) if m["target"] == target), None)
        if m is None:
            m = {"target": target}
            src["mappings"].append(m)
        return m

    if op == "set_mapping_cast":
        mapping(patch["label"], patch["target"])["cast"] = patch["cast"]
    elif op == "set_mapping_source":
        m = mapping(patch["label"], patch["target"])
        m["source"] = patch["to"]
        m.pop("default", None)
    elif op == "set_overlap":
        d["overlap"] = patch["value"]
    elif op == "add_mask":
        d["masks"] = [m for m in d.get("masks", []) if m["column"] != patch["column"]] + [
            {"column": patch["column"], "method": patch.get("method", "hash")}]
    elif op == "rename_target":
        d["target"]["name"] = patch["to"]
    elif op == "replace_dataset":
        for s in d["sources"]:
            if s["dataset"] == patch["from"]:
                s["dataset"] = patch["to"]
    else:
        raise ValueError(f"Unknown fix '{op}'.")
    return d


# --- legacy SQL equivalence --------------------------------------------------------------------------

_ALLOWED = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION,
            getattr(sqlite3, "SQLITE_RECURSIVE", 33)}


def normalize_legacy_sql(sql: str, catalog: dict) -> str:
    """Strip comments/terminators and turn platform table names (db.schema.table) into catalog ids."""
    s = re.sub(r"--[^\n]*", "", sql)
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.S).strip().rstrip(";").strip()
    if ";" in s:
        raise ValueError("Paste one query (a single SELECT statement).")
    if not re.match(r"^(select|with)\b", s, re.I):
        raise ValueError("Only a SELECT query can be compared; nothing else is run.")
    ids = {k.lower(): k for k in catalog}

    def table(m):
        kw, name = m.group(1), m.group(2)
        last = re.split(r"\.", name)[-1].strip('`"[]').lower()
        return f"{kw} {ids[last]}" if last in ids else m.group(0)
    s = re.sub(r"\b(FROM|JOIN)\s+((?:[`\"\[]?[\w]+[`\"\]]?\.){0,2}[`\"\[]?[\w]+[`\"\]]?)", table, s, flags=re.I)
    return s


def _norm(v):
    if isinstance(v, float):
        return round(v, 2)
    if isinstance(v, int) and not isinstance(v, bool):
        return round(float(v), 2)
    return v


def run_legacy(sql: str, catalog: dict) -> tuple[list[str], list[tuple]]:
    s = normalize_legacy_sql(sql, catalog)
    con = connect()
    try:
        for ds in catalog.values():
            load(con, ds)
        con.set_authorizer(lambda action, *args: sqlite3.SQLITE_OK if action in _ALLOWED else sqlite3.SQLITE_DENY)
        cur = con.execute(s)
        cols = [d[0].lower() for d in cur.description]
        return cols, [tuple(r) for r in cur.fetchmany(config.SANDBOX_MAX_OUTPUT_ROWS)]
    finally:
        con.close()


def compare(legacy_cols, legacy_rows, new_rows: list[dict]) -> dict:
    new_cols = list(new_rows[0].keys()) if new_rows else []
    lc = [c.lower() for c in legacy_cols]
    nc = [c.lower() for c in new_cols]
    how = None
    if set(lc) == set(nc):
        order = [nc.index(c) for c in lc]
        new_t = [tuple(_norm(list(r.values())[i]) for i in order) for r in new_rows]
        how = "by column name"
    elif len(lc) == len(nc) and new_rows:
        new_t = [tuple(_norm(v) for v in r.values()) for r in new_rows]
        how = "by position (column names differ)"
    else:
        return {"status": "DIFFERENT", "headline": "Different columns",
                "message": f"The legacy query returns {', '.join(lc) or 'nothing'}; the pipeline returns "
                           f"{', '.join(nc) or 'nothing'}.", "legacy_rows": len(legacy_rows),
                "new_rows": len(new_rows), "legacy_columns": lc, "new_columns": nc}
    old_t = [tuple(_norm(v) for v in r) for r in legacy_rows]
    co, cn = Counter(old_t), Counter(new_t)
    only_old = list((co - cn).elements())
    only_new = list((cn - co).elements())
    same = sum((co & cn).values())
    eq = not only_old and not only_new
    return {"status": "EQUIVALENT" if eq else "DIFFERENT",
            "headline": (f"Equivalent: all {same} row(s) match the legacy query" if eq else
                         f"Different: {len(only_old)} legacy row(s) and {len(only_new)} new row(s) don't match"),
            "matched": how, "columns": lc, "legacy_rows": len(old_t), "new_rows": len(new_t), "matching_rows": same,
            "only_in_legacy": [list(r) for r in only_old[:5]], "only_in_new": [list(r) for r in only_new[:5]],
            "explanation": _explain("multiset(legacy rows) = multiset(pipeline rows), numbers rounded to 2 dp",
                                    {"legacy_rows": len(old_t), "pipeline_rows": len(new_t), "matching": same},
                                    "Both ran on the same sample data in the isolated sandbox; order doesn't matter.")}


# --- reconcile two tables ----------------------------------------------------------------------------------

def reconcile(left: dict, right: dict, key: str, measures: list[str]) -> dict:
    lk, _, rk = key.partition("=")
    lk, rk = lk.strip(), (rk or lk).strip()
    lcols = {c["name"] for c in left["columns"]}
    rcols = {c["name"] for c in right["columns"]}
    if lk not in lcols or rk not in rcols:
        raise ValueError(f"Key must exist in both tables ({left['id']}.{lk}, {right['id']}.{rk}).")
    bad = [m for m in measures if m not in lcols or m not in rcols]
    if bad:
        raise ValueError(f"Measures must exist in both tables: {', '.join(bad)}")
    con = connect()
    try:
        load(con, left)
        if right["id"] != left["id"]:
            load(con, right)
        L, R = left["id"], right["id"]
        q = lambda s: con.execute(s).fetchone()[0]  # noqa: E731
        out = {"left": L, "right": R, "key": f"{lk} = {rk}",
               "rows": {"left": q(f"SELECT COUNT(*) FROM {L}"), "right": q(f"SELECT COUNT(*) FROM {R}")},
               "distinct_keys": {"left": q(f"SELECT COUNT(DISTINCT {lk}) FROM {L}"),
                                 "right": q(f"SELECT COUNT(DISTINCT {rk}) FROM {R}")}}
        only_l = [x[0] for x in con.execute(f"SELECT DISTINCT {lk} FROM {L} WHERE {lk} IS NOT NULL AND {lk} NOT IN "
                                            f"(SELECT {rk} FROM {R} WHERE {rk} IS NOT NULL)")]
        only_r = [x[0] for x in con.execute(f"SELECT DISTINCT {rk} FROM {R} WHERE {rk} IS NOT NULL AND {rk} NOT IN "
                                            f"(SELECT {lk} FROM {L} WHERE {lk} IS NOT NULL)")]
        out["only_in_left"] = {"count": len(only_l), "examples": only_l[:5]}
        out["only_in_right"] = {"count": len(only_r), "examples": only_r[:5]}
        out["measures"] = []
        for m in measures:
            a = q(f"SELECT ROUND(COALESCE(SUM({m}), 0), 2) FROM {L}")
            b = q(f"SELECT ROUND(COALESCE(SUM({m}), 0), 2) FROM {R}")
            diff_keys = con.execute(
                f"SELECT l.{lk} FROM (SELECT {lk}, ROUND(SUM({m}), 2) AS v FROM {L} GROUP BY {lk}) l JOIN "
                f"(SELECT {rk}, ROUND(SUM({m}), 2) AS v FROM {R} GROUP BY {rk}) r ON l.{lk} = r.{rk} "
                f"WHERE COALESCE(l.v, 0) <> COALESCE(r.v, 0)").fetchall()
            out["measures"].append({"measure": m, "left_total": a, "right_total": b,
                                    "difference": round((b or 0) - (a or 0), 2),
                                    "keys_with_different_values": len(diff_keys),
                                    "examples": [d[0] for d in diff_keys[:5]]})
    finally:
        con.close()
    ok = (out["rows"]["left"] == out["rows"]["right"] and not only_l and not only_r
          and all(m["difference"] == 0 and m["keys_with_different_values"] == 0 for m in out["measures"]))
    out["status"] = "MATCH" if ok else "DIFFERENCES"
    parts = []
    if out["rows"]["left"] != out["rows"]["right"]:
        parts.append(f"row counts differ ({out['rows']['left']} vs {out['rows']['right']})")
    if only_l:
        parts.append(f"{len(only_l)} key(s) only in {left['friendly_name']}")
    if only_r:
        parts.append(f"{len(only_r)} key(s) only in {right['friendly_name']}")
    for m in out["measures"]:
        if m["difference"] or m["keys_with_different_values"]:
            parts.append(f"{m['measure']} differs by {m['difference']} across {m['keys_with_different_values']} key(s)")
    out["headline"] = ("Match: same rows, same keys, same totals" if ok else "Differences: " + "; ".join(parts))
    out["explanation"] = _explain("compare row counts, key sets and per-key sums of each measure",
                                  {"key": out["key"], "measures": measures},
                                  "Run after moving a table to prove the new copy matches the old one.")
    return out


# --- schema import ---------------------------------------------------------------------------------------

_PII = re.compile(r"(^|_)(e_?mail|phone|mobile|ssn|social_security|tax_id|passport|license_no|dob|birth|"
                  r"first_name|last_name|full_name|customer_name|contact_name|address|street|postcode|zip)(_|$)")


def map_type(t: str) -> str:
    t = (t or "").lower()
    if re.search(r"date ?time|timestamp|datetime2", t):
        return "timestamp"
    if t.startswith("date"):
        return "date"
    if re.search(r"number\(\s*\d+\s*,\s*0\s*\)|\bint|bigint|smallint|tinyint|integer|long", t):
        return "integer"
    if re.search(r"decimal|numeric|number|float|double|real|money", t):
        return "decimal"
    if re.search(r"bool|bit", t):
        return "integer"
    return "string"


def suggest_classification(name: str) -> str | None:
    return "PII" if _PII.search(name.lower()) else None


def parse_schema_csv(text: str) -> list[dict]:
    """'table,column,type' lines (an information_schema export) -> [{id, columns}]."""
    tables: dict[str, list] = {}
    for line in (text or "").splitlines():
        cells = [c.strip().strip('"') for c in line.split(",", 2)]   # the type may contain commas
        if len(cells) < 3 or not cells[0] or cells[0].lower() in ("table", "table_name"):
            continue
        tables.setdefault(cells[0], []).append({"name": cells[1], "type": cells[2]})
    return [{"id": t, "columns": cols} for t, cols in tables.items()]


# --- re-platform -----------------------------------------------------------------------------------------

def replatform_spec(data: dict, platform: str) -> dict:
    d = copy.deepcopy(data)
    old = d["platform"]
    d["platform"] = platform
    d["name"] = f"{d['name']} ({PLATFORM_NAMES[platform]})"
    d["target"]["name"] = f"{d['target']['name']}_{platform}"[:63]
    d["description"] = (d.get("description") or "") + f" Re-platformed from {PLATFORM_NAMES[old]}."
    return d

"""The pipeline spec: the one contract between people, the AI and the code.

Nobody -- not the user, not Claude -- writes production SQL by hand here. A
pipeline is described as a small, typed spec (start from this data, join that,
keep these rows, total this by that, land it there). The spec is checked
against the catalog, and the code for every platform is COMPILED from it
deterministically (see compiler.py). That is what makes the agent safe for
non-developers and reviewable for developers.

`check()` returns plain-English issues, each with a suggested fix. Fixes that
can be applied automatically carry a `patch` (see orchestration.apply_patch).
"""
from __future__ import annotations

import difflib
import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Literal, Union

from pydantic import BaseModel, Field, field_validator

PLATFORMS = ("databricks", "snowflake", "fabric")
PLATFORM_NAMES = {"databricks": "Databricks", "snowflake": "Snowflake", "fabric": "Microsoft Fabric"}
AGGS = ("sum", "avg", "count", "count_distinct", "min", "max")
AGG_WORDS = {"sum": "Total", "avg": "Average", "count": "Count", "count_distinct": "Count of unique",
             "min": "Lowest", "max": "Highest"}
# How a measure reads in front of a column name: "total of orders.amount", "lowest orders.amount".
AGG_PHRASE = {"sum": "total of", "avg": "average of", "count": "count of", "count_distinct": "count of unique",
              "min": "lowest", "max": "highest"}
GRAINS = ("day", "week", "month", "quarter", "year")
OPS = ("=", "!=", ">", ">=", "<", "<=", "in", "not in", "is null", "is not null")
OP_WORDS = {"=": "is", "!=": "is not", ">": "is more than", ">=": "is at least", "<": "is less than",
            "<=": "is at most", "in": "is one of", "not in": "is not one of", "is null": "is blank",
            "is not null": "is not blank"}
NUMERIC = ("integer", "decimal")
TEMPORAL = ("date", "timestamp")
IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")

Scalar = Union[str, int, float, bool]


class Join(BaseModel):
    dataset: str = Field(description="Catalog id of the dataset to bring in, e.g. 'customers'")
    left: str = Field(description="Key on the data already in the pipeline, e.g. 'orders.customer_id'")
    right: str = Field(description="Matching key on the new dataset, e.g. 'customers.customer_id'")
    how: Literal["left", "inner"] = Field(
        "left", description="left = keep every row even with no match; inner = keep matches only")


class Filter(BaseModel):
    column: str = Field(description="Column to test, e.g. 'orders.status'")
    op: Literal["=", "!=", ">", ">=", "<", "<=", "in", "not in", "is null", "is not null"]
    value: Scalar | list[Scalar] | None = Field(
        None, description="Value to compare with; a list for 'in'/'not in'; omit for null tests")


class GroupBy(BaseModel):
    column: str = Field(description="Column to group by, e.g. 'customers.region'")
    grain: Literal["day", "week", "month", "quarter", "year"] | None = Field(
        None, description="For dates only: roll up to this time period")
    name: str | None = Field(None, description="Output column name (snake_case). Optional.")


class Measure(BaseModel):
    agg: Literal["sum", "avg", "count", "count_distinct", "min", "max"]
    column: str | None = Field(None, description="Column to summarise; omit with 'count' to count rows")
    name: str = Field(description="Output column name in snake_case, e.g. 'revenue'")


class Mask(BaseModel):
    column: str = Field(description="Sensitive column to protect, e.g. 'customers.email'")
    method: Literal["hash", "redact"] = Field(
        "hash", description="hash = consistent one-way code (still joinable); redact = replace with ***")


class Target(BaseModel):
    name: str = Field(description="Name of the table to create, snake_case, e.g. 'monthly_revenue'")
    layer: Literal["silver", "gold"] = Field("gold", description="silver = cleaned data; gold = business-ready")
    materialization: Literal["table", "view"] = "table"


class PipelineSpec(BaseModel):
    name: str = Field(description="Plain-English name, e.g. 'Monthly revenue by region'")
    description: str = ""
    platform: Literal["databricks", "snowflake", "fabric"]
    source: str = Field(description="Catalog id of the starting dataset, e.g. 'orders'")
    joins: list[Join] = Field(default_factory=list)
    filters: list[Filter] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list,
                               description="For a row list (no measures): columns to keep")
    group_by: list[GroupBy] = Field(default_factory=list)
    measures: list[Measure] = Field(default_factory=list)
    masks: list[Mask] = Field(default_factory=list)
    target: Target
    schedule: Literal["manual", "hourly", "daily", "weekly"] = "daily"

    @field_validator("name")
    @classmethod
    def _name(cls, v):
        v = v.strip()
        if not v:
            raise ValueError("name is required")
        return v[:120]


def spec_hash(spec: PipelineSpec | dict) -> str:
    d = spec.model_dump(mode="json") if isinstance(spec, BaseModel) else spec
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode()).hexdigest()[:16]


# --- issues ---------------------------------------------------------------------------

@dataclass
class Issue:
    level: str          # error | warning
    code: str
    message: str        # plain English
    fix: str = ""       # plain-English suggestion
    patch: dict | None = None  # machine-applicable fix

    def as_dict(self) -> dict:
        d = {"level": self.level, "code": self.code, "message": self.message, "fix": self.fix}
        if self.patch:
            d["patch"] = self.patch
        return d


@dataclass
class Resolved:
    """A spec checked against the catalog: every column reference made explicit."""
    spec: PipelineSpec
    datasets: dict[str, dict]                      # id -> catalog entry, in FROM/JOIN order
    refs: dict[str, tuple[str, dict]] = field(default_factory=dict)   # raw ref -> (dataset, column)
    outputs: list[dict] = field(default_factory=list)  # [{name, kind, ...}]


def _friendly_col(ds: dict, col: str) -> str:
    return f"{ds['friendly_name']} › {col.replace('_', ' ')}"


def resolve(spec: PipelineSpec, catalog: dict[str, dict]) -> tuple[Resolved | None, list[Issue]]:
    issues: list[Issue] = []
    ids = list(catalog)

    def unknown_dataset(name: str, where: str):
        close = difflib.get_close_matches(name, ids, n=1)
        fix = f"Did you mean '{close[0]}'?" if close else "Pick a dataset from Find data."
        patch = {"op": "replace_dataset", "from": name, "to": close[0]} if close else None
        issues.append(Issue("error", "UNKNOWN_DATASET",
                            f"The {where} '{name}' is not in the catalog.", fix, patch))

    if spec.source not in catalog:
        unknown_dataset(spec.source, "starting dataset")
        return None, issues

    order = [spec.source]
    for j in spec.joins:
        if j.dataset not in catalog:
            unknown_dataset(j.dataset, "joined dataset")
        elif j.dataset in order:
            issues.append(Issue("error", "DUPLICATE_DATASET",
                                f"'{j.dataset}' is used twice. Joining a table to itself is not supported.",
                                "Remove the second copy."))
        else:
            order.append(j.dataset)
    if any(i.level == "error" for i in issues):
        return None, issues

    datasets = {d: catalog[d] for d in order}
    res = Resolved(spec=spec, datasets=datasets)

    def lookup(ref: str, scope: list[str] | None = None) -> tuple[str, dict] | None:
        scope = scope or order
        ref = (ref or "").strip()
        if "." in ref:
            ds, col = ref.split(".", 1)
            if ds not in scope:
                close = difflib.get_close_matches(ds, scope, n=1)
                issues.append(Issue("error", "DATASET_NOT_IN_PIPELINE",
                                    f"'{ref}' points at '{ds}', which is not part of this pipeline.",
                                    f"Add '{ds}' as related data first." if ds in catalog else
                                    (f"Did you mean '{close[0]}.{col}'?" if close else "Check the name."),
                                    {"op": "replace_column", "from": ref, "to": f"{close[0]}.{col}"}
                                    if close and ds not in catalog else None))
                return None
            cols = {c["name"]: c for c in catalog[ds]["columns"]}
            if col not in cols:
                close = difflib.get_close_matches(col, list(cols), n=1)
                issues.append(Issue("error", "UNKNOWN_COLUMN",
                                    f"{catalog[ds]['friendly_name']} has no column '{col}'.",
                                    f"Did you mean '{close[0]}'?" if close else
                                    f"Available: {', '.join(cols)}.",
                                    {"op": "replace_column", "from": ref, "to": f"{ds}.{close[0]}"}
                                    if close else None))
                return None
            res.refs[ref] = (ds, cols[col])
            return ds, cols[col]
        hits = [(d, c) for d in scope for c in catalog[d]["columns"] if c["name"] == ref]
        if len(hits) == 1:
            res.refs[ref] = hits[0]
            return hits[0]
        if len(hits) > 1:
            issues.append(Issue("error", "AMBIGUOUS_COLUMN",
                                f"'{ref}' exists in more than one dataset ({', '.join(d for d, _ in hits)}).",
                                f"Say which one, e.g. '{hits[0][0]}.{ref}'.",
                                {"op": "replace_column", "from": ref, "to": f"{hits[0][0]}.{ref}"}))
            return None
        allcols = [f"{d}.{c['name']}" for d in scope for c in catalog[d]["columns"]]
        close = difflib.get_close_matches(ref, allcols + [a.split(".")[1] for a in allcols], n=1)
        issues.append(Issue("error", "UNKNOWN_COLUMN", f"No column called '{ref}' in this pipeline.",
                            f"Did you mean '{close[0]}'?" if close else "Check Find data for column names.",
                            {"op": "replace_column", "from": ref, "to": close[0]} if close else None))
        return None

    # joins: keys must exist on the correct side and have compatible types
    for n, j in enumerate(spec.joins):
        left = lookup(j.left, order[: n + 1])
        right = lookup(j.right, [j.dataset])
        if left and right and _type_family(left[1]["type"]) != _type_family(right[1]["type"]):
            issues.append(Issue("error", "JOIN_TYPE_MISMATCH",
                                f"Can't match {j.left} ({left[1]['type']}) with {j.right} "
                                f"({right[1]['type']}): they hold different kinds of values.",
                                "Join on columns that hold the same kind of value (usually an id)."))

    for f in spec.filters:
        hit = lookup(f.column)
        if hit:
            _check_filter(f, hit[1], issues)

    summarising = bool(spec.measures)
    if summarising and spec.columns:
        issues.append(Issue("error", "MIXED_SHAPE",
                            "When you summarise, every column must be either grouped or totalled.",
                            "Move these columns into 'group by', or remove them: " + ", ".join(spec.columns),
                            {"op": "columns_to_group_by"}))
    if not summarising and not spec.columns and not spec.group_by:
        issues.append(Issue("error", "EMPTY_OUTPUT", "The result has no columns.",
                            "Pick columns to keep, or add a total (for example a count of rows)."))

    seen: dict[str, str] = {}

    def add_out(name: str, what: str, **kw):
        if not IDENT.match(name):
            issues.append(Issue("error", "BAD_COLUMN_NAME",
                                f"'{name}' isn't a valid column name.",
                                "Use lowercase letters, numbers and underscores, starting with a letter.",
                                {"op": "rename_output", "from": name, "to": _slug(name)}))
        elif name in seen:
            issues.append(Issue("error", "DUPLICATE_OUTPUT",
                                f"Two output columns are both called '{name}' ({seen[name]} and {what}).",
                                "Give one of them a different name."))
        seen[name] = what
        res.outputs.append({"name": name, **kw})

    if not summarising:
        for ref in spec.columns:
            hit = lookup(ref)
            if hit:
                add_out(hit[1]["name"] if hit[1]["name"] not in seen else f"{hit[0]}_{hit[1]['name']}",
                        ref, kind="column", ref=ref, dataset=hit[0], column=hit[1])

    for g in spec.group_by:
        hit = lookup(g.column)
        if not hit:
            continue
        if g.grain and hit[1]["type"] not in TEMPORAL:
            issues.append(Issue("error", "GRAIN_ON_NON_DATE",
                                f"'{g.column}' is not a date, so it can't be rolled up by {g.grain}.",
                                "Remove the time period, or group by a date column."))
        name = g.name or (f"{hit[1]['name']}_{g.grain}" if g.grain else hit[1]["name"])
        add_out(name, g.column, kind="group", ref=g.column, dataset=hit[0], column=hit[1], grain=g.grain)

    for m in spec.measures:
        if m.column is None:
            if m.agg != "count":
                issues.append(Issue("error", "MEASURE_NEEDS_COLUMN",
                                    f"'{AGG_WORDS[m.agg]}' needs a column to work on.",
                                    "Choose a column, or use Count to count rows."))
                continue
            add_out(m.name, f"{m.agg}(*)", kind="measure", agg=m.agg, ref=None, dataset=None, column=None)
            continue
        hit = lookup(m.column)
        if not hit:
            continue
        if m.agg in ("sum", "avg") and hit[1]["type"] not in NUMERIC:
            issues.append(Issue("error", "NOT_NUMERIC",
                                f"Can't {'total' if m.agg == 'sum' else 'average'} "
                                f"'{m.column}': it holds {hit[1]['type']} values, not numbers.",
                                "Use Count or Count of unique instead.",
                                {"op": "set_measure_agg", "name": m.name, "agg": "count_distinct"}))
        add_out(m.name, f"{m.agg}({m.column})", kind="measure", agg=m.agg, ref=m.column,
                dataset=hit[0], column=hit[1])

    # masks
    masked: dict[str, str] = {}
    for mk in spec.masks:
        hit = lookup(mk.column)
        if hit:
            masked[f"{hit[0]}.{hit[1]['name']}"] = mk.method
    for o in res.outputs:
        if o["kind"] in ("column", "group") and o.get("dataset"):
            o["mask"] = masked.get(f"{o['dataset']}.{o['column']['name']}")

    # target
    t = spec.target
    if not IDENT.match(t.name):
        issues.append(Issue("error", "BAD_TARGET_NAME", f"'{t.name}' isn't a valid table name.",
                            "Use lowercase letters, numbers and underscores.",
                            {"op": "rename_target", "to": _slug(t.name)}))
    elif t.name in catalog and catalog[t.name].get("origin") == "seed":
        issues.append(Issue("error", "TARGET_IS_SOURCE",
                            f"'{t.name}' is an existing source table; the pipeline would overwrite it.",
                            "Choose a new name for the result.",
                            {"op": "rename_target", "to": f"{t.name}_{t.layer}"}))

    _governance_checks(res, issues)
    return res, issues


def _governance_checks(res: Resolved, issues: list[Issue]) -> None:
    spec = res.spec
    # PII: a PII column may leave only if masked, or if it is aggregated away (counted).
    for o in res.outputs:
        col = o.get("column")
        if not col or col.get("classification") != "PII":
            continue
        if o["kind"] == "measure" and o["agg"] in ("count", "count_distinct"):
            continue
        if o["kind"] == "measure":
            continue
        if not o.get("mask"):
            ref = f"{o['dataset']}.{col['name']}"
            issues.append(Issue("error", "PII_EXPOSED",
                                f"{_friendly_col(res.datasets[o['dataset']], col['name'])} is personal "
                                "information (PII) and would be copied as-is.",
                                "Protect it with a one-way hash (still usable for matching), or remove it.",
                                {"op": "add_mask", "column": ref, "method": "hash"}))
    # Federation: reading data that lives on another platform.
    for d, ds in res.datasets.items():
        if ds["platform"] != spec.platform and ds.get("origin") == "seed":
            how = {"databricks": "Lakehouse Federation", "snowflake": "an external table / data share",
                   "fabric": "a OneLake shortcut or mirror"}[spec.platform]
            issues.append(Issue("warning", "CROSS_PLATFORM",
                                f"{ds['friendly_name']} lives in {PLATFORM_NAMES[ds['platform']]}; on "
                                f"{PLATFORM_NAMES[spec.platform]} it is read through {how}.",
                                "Fine for a demo; for production check cost and latency, or run the "
                                f"pipeline on {PLATFORM_NAMES[ds['platform']]}."))
    # Curated version available
    for d, ds in res.datasets.items():
        cur = ds.get("curated_version")
        if cur and cur not in res.datasets:
            issues.append(Issue("warning", "CURATED_AVAILABLE",
                                f"{ds['friendly_name']} is a raw table. A certified version exists: '{cur}'.",
                                f"Use '{cur}' instead.", {"op": "replace_dataset", "from": d, "to": cur}))


def _check_filter(f: Filter, col: dict, issues: list[Issue]) -> None:
    t = col["type"]
    if f.op in ("is null", "is not null"):
        return
    if f.value is None or f.value == "" or f.value == []:
        issues.append(Issue("error", "FILTER_NEEDS_VALUE",
                            f"The filter on '{f.column}' needs a value to compare with.", "Add a value."))
        return
    vals = f.value if isinstance(f.value, list) else [f.value]
    if f.op in ("in", "not in") and not isinstance(f.value, list):
        pass  # a single value is accepted and treated as a one-item list
    if f.op in (">", ">=", "<", "<=") and t not in NUMERIC + TEMPORAL:
        issues.append(Issue("error", "FILTER_TYPE",
                            f"'{OP_WORDS[f.op]}' only works on numbers and dates; '{f.column}' is text.",
                            "Use 'is' or 'is one of'."))
    for v in vals:
        if t in NUMERIC and not _is_number(v):
            issues.append(Issue("error", "FILTER_TYPE", f"'{v}' is not a number, but '{f.column}' is.",
                                "Enter a number."))
        if t in TEMPORAL and not re.match(r"^\d{4}-\d{2}-\d{2}", str(v)):
            issues.append(Issue("error", "FILTER_TYPE",
                                f"'{v}' is not a date. Dates are written YYYY-MM-DD.",
                                "For example 2026-07-01."))
    allowed = col.get("accepted_values")
    if allowed and t == "string" and f.op in ("=", "!=", "in", "not in"):
        bad = [v for v in vals if v not in allowed]
        if bad:
            close = difflib.get_close_matches(str(bad[0]), allowed, n=1)
            issues.append(Issue("warning", "UNEXPECTED_VALUE",
                                f"'{bad[0]}' is not a known value of '{f.column}' "
                                f"(known: {', '.join(allowed)}). No rows may match.",
                                f"Did you mean '{close[0]}'?" if close else "Check the spelling.",
                                {"op": "replace_filter_value", "column": f.column, "from": bad[0],
                                 "to": close[0]} if close else None))


def _is_number(v) -> bool:
    if isinstance(v, bool):
        return False
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False


def _type_family(t: str) -> str:
    return "number" if t in NUMERIC else ("time" if t in TEMPORAL else t)


def _slug(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    if not s or not s[0].isalpha():
        s = "t_" + s
    return s[:63]


# --- plain-English description ----------------------------------------------------------

def summarize(spec: PipelineSpec, catalog: dict[str, dict]) -> str:
    def fname(ref: str | None) -> str:
        if not ref:
            return "rows"
        ds, _, col = ref.rpartition(".")
        return col.replace("_", " ")

    def dname(d):
        return catalog.get(d, {}).get("friendly_name", d)

    parts = []
    if spec.measures:
        ms = []
        for m in spec.measures:
            how = ("number of rows" if m.column is None and m.agg == "count"
                   else f"{AGG_WORDS[m.agg].lower()} {fname(m.column)}")
            label = m.name.replace("_", " ")
            ms.append(how if label == how else f"{label} ({how})")
        what = _join_words(ms)
        if spec.group_by:
            gs = [f"{fname(g.column)} by {g.grain}" if g.grain else fname(g.column) for g in spec.group_by]
            what += " for each " + _join_words(gs, "and")
        parts.append(what[0].upper() + what[1:])
    else:
        cols = [fname(c) for c in spec.columns] or [fname(g.column) for g in spec.group_by]
        parts.append("A list of " + _join_words(cols))
    src = dname(spec.source)
    if spec.joins:
        src += " joined to " + _join_words([dname(j.dataset) for j in spec.joins])
    parts.append(f"from {src}")
    if spec.filters:
        fs = []
        for f in spec.filters:
            v = f.value
            if isinstance(v, list):
                v = ", ".join(map(str, v))
            fs.append(f"{fname(f.column)} {OP_WORDS[f.op]}" + ("" if v is None else f" {v}"))
        parts.append("only where " + _join_words(fs, "and"))
    if spec.masks:
        parts.append("with " + _join_words([fname(m.column) for m in spec.masks]) + " protected")
    sched = {"manual": "on demand", "hourly": "every hour", "daily": "every day",
             "weekly": "every week"}[spec.schedule]
    parts.append(f"— saved as {spec.target.layer} {spec.target.materialization} "
                 f"'{spec.target.name}' on {PLATFORM_NAMES[spec.platform]}, refreshed {sched}.")
    return " ".join(parts[:-1]) + " " + parts[-1]


def _join_words(items: list[str], last: str = "and") -> str:
    items = [i for i in items if i]
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + f" {last} " + items[-1]

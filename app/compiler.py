"""Deterministic code generation: one spec in, platform-correct code out.

Targets
  * sandbox  -- SQLite, executed by the code sandbox to test the pipeline
  * databricks / snowflake / fabric -- each platform's SQL dialect, with DDL
  * pyspark  -- DataFrame code for Databricks and Fabric Lakehouse notebooks
  * dbt      -- a model using dbt's cross-database macros, plus schema tests

The same resolved spec always produces byte-identical code, so a reviewer can
trust that what was tested in the sandbox is what gets deployed.
"""
from __future__ import annotations

import json

from . import connectors
from .spec import AGG_PHRASE, PLATFORM_NAMES, Resolved, spec_hash, summarize

INDENT = "    "


# --- dialect primitives ---------------------------------------------------------------

def _lit(v, dialect: str, col_type: str) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return ("1" if v else "0") if dialect in ("sqlite", "fabric") else ("TRUE" if v else "FALSE")
    if col_type in ("integer", "decimal"):
        return str(v)
    s = "'" + str(v).replace("'", "''") + "'"
    if col_type == "date" and dialect != "sqlite":
        return f"CAST({s} AS DATE)" if dialect != "dbt" else f"cast({s} as date)"
    if col_type == "timestamp" and dialect != "sqlite":
        t = {"fabric": "DATETIME2", "dbt": "timestamp"}.get(dialect, "TIMESTAMP")
        return f"CAST({s} AS {t})" if dialect != "dbt" else f"cast({s} as {t})"
    return s


def _trunc(x: str, grain: str | None, dialect: str) -> str:
    if not grain:
        return x
    if grain == "day":
        return f"date({x})" if dialect == "sqlite" else (
            f"cast({x} as date)" if dialect == "dbt" else f"CAST({x} AS DATE)")
    if dialect == "sqlite":
        if grain == "week":
            return f"date({x}, 'weekday 0', '-6 days')"
        if grain == "month":
            return f"date({x}, 'start of month')"
        if grain == "year":
            return f"date({x}, 'start of year')"
        return (f"date({x}, 'start of month', '-' || ((CAST(strftime('%m', {x}) AS INTEGER) - 1) % 3)"
                f" || ' months')")
    if dialect == "databricks":
        return f"CAST(date_trunc('{grain.upper()}', {x}) AS DATE)"
    if dialect == "snowflake":
        return f"CAST(DATE_TRUNC('{grain.upper()}', {x}) AS DATE)"
    if dialect == "fabric":
        part = "iso_week" if grain == "week" else grain
        return f"CAST(DATETRUNC({part}, {x}) AS DATE)"
    return f"cast({{{{ dbt.date_trunc('{grain}', '{x}') }}}} as date)"


def _mask(x: str, method: str | None, dialect: str) -> str:
    if not method:
        return x
    if method == "redact":
        return f"CASE WHEN {x} IS NULL THEN NULL ELSE '***' END"
    return {
        "sqlite": f"sha256({x})",
        "databricks": f"sha2(CAST({x} AS STRING), 256)",
        "snowflake": f"SHA2({x}, 256)",
        "fabric": f"LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CAST({x} AS VARCHAR(4000))), 2))",
        "dbt": f"{{{{ dbt.hash('{x}') }}}}",
    }[dialect]


def _agg(agg: str, x: str | None, col_type: str | None, dialect: str) -> str:
    if x is None:
        return "COUNT(*)"
    if agg == "count":
        return f"COUNT({x})"
    if agg == "count_distinct":
        return f"COUNT(DISTINCT {x})"
    if agg == "sum":
        return f"ROUND(SUM({x}), 2)" if col_type == "decimal" else f"SUM({x})"
    if agg == "avg":
        if dialect == "sqlite":
            return f"ROUND(AVG({x}), 2)"
        num = "{{ dbt.type_numeric() }}" if dialect == "dbt" else "DECIMAL(18, 4)"
        return f"ROUND(AVG(CAST({x} AS {num})), 2)"
    return f"{agg.upper()}({x})"


_OPS = {"=": "=", "!=": "<>", ">": ">", ">=": ">=", "<": "<", "<=": "<="}


def _filter(f, col: dict, dialect: str) -> str:
    x = f.column  # already a fully-qualified dataset.column reference
    t = col["type"]
    if f.op == "is null":
        return f"{x} IS NULL"
    if f.op == "is not null":
        return f"{x} IS NOT NULL"
    if f.op in ("in", "not in"):
        vals = f.value if isinstance(f.value, list) else [f.value]
        inner = ", ".join(_lit(v, dialect, t) for v in vals)
        return f"{x} {'NOT IN' if f.op == 'not in' else 'IN'} ({inner})"
    return f"{x} {_OPS[f.op]} {_lit(f.value, dialect, t)}"


# --- the SELECT -------------------------------------------------------------------------

class _Ctx:
    def __init__(self, r: Resolved):
        self.r = r

    def full(self, ref: str | None) -> str | None:
        if ref is None:
            return None
        ds, col = self.r.refs[ref]
        return f"{ds}.{col['name']}"


def select_parts(r: Resolved, dialect: str, table_of) -> dict:
    """Build the pieces of the SELECT for one dialect. table_of(dataset_id) -> SQL table text."""
    ctx = _Ctx(r)
    spec = r.spec
    sel, groups = [], []
    for o in r.outputs:
        if o["kind"] == "measure":
            e = _agg(o["agg"], ctx.full(o["ref"]), o["column"]["type"] if o["column"] else None, dialect)
        else:
            e = _mask(_trunc(ctx.full(o["ref"]), o.get("grain"), dialect), o.get("mask"), dialect)
            if o["kind"] == "group":
                groups.append(e)
        sel.append(f"{e} AS {o['name']}")

    frm = f"{table_of(spec.source)} AS {spec.source}"
    joins = []
    for j in spec.joins:
        kw = "LEFT JOIN" if j.how == "left" else "INNER JOIN"
        joins.append(f"{kw} {table_of(j.dataset)} AS {j.dataset}\n{INDENT}ON {ctx.full(j.left)} = {ctx.full(j.right)}")
    wheres = []
    for f in spec.filters:
        ds, col = r.refs[f.column]
        f2 = f.model_copy(update={"column": f"{ds}.{col['name']}"})
        wheres.append(_filter(f2, col, dialect))
    return {"select": sel, "from": frm, "joins": joins, "where": wheres,
            "group_by": groups if spec.group_by else []}


def render_select(parts: dict, lower: bool = False) -> str:
    kw = (lambda s: s.lower()) if lower else (lambda s: s)
    out = [kw("SELECT")]
    out.append(",\n".join(INDENT + s for s in parts["select"]))
    out.append(f"{kw('FROM')} {parts['from']}")
    for j in parts["joins"]:
        out.append(j if not lower else j.replace("LEFT JOIN", "left join").replace(
            "INNER JOIN", "inner join").replace("\n" + INDENT + "ON", "\n" + INDENT + "on"))
    if parts["where"]:
        out.append(kw("WHERE") + " " + f"\n{INDENT}{kw('AND')} ".join(parts["where"]))
    if parts["group_by"]:
        out.append(kw("GROUP BY") + "\n" + ",\n".join(INDENT + g for g in parts["group_by"]))
    return "\n".join(out)


# --- targets -----------------------------------------------------------------------------

def sandbox_sql(r: Resolved) -> str:
    return render_select(select_parts(r, "sqlite", lambda d: d))


def _header(r: Resolved, catalog: dict, comment: str = "--") -> str:
    return (f"{comment} Pipeline: {r.spec.name}\n"
            f"{comment} Generated by the Data Engineering Agent from spec {spec_hash(r.spec)}.\n"
            f"{comment} Change the spec, not this file.\n"
            f"{comment} What it does: {summarize(r.spec, catalog)}\n")


def platform_sql(r: Resolved, catalog: dict, platform: str | None = None) -> str:
    platform = platform or r.spec.platform
    conn = connectors.get(platform)
    body = render_select(select_parts(r, platform, lambda d: _table_for(conn, catalog[d])))
    t = r.spec.target
    target = conn.target_name(t.layer, t.name)
    kind = "VIEW" if t.materialization == "view" else "TABLE"
    if platform == "fabric":
        ddl = (f"CREATE OR ALTER VIEW {target} AS\n{body};\n" if kind == "VIEW" else
               f"DROP TABLE IF EXISTS {target};\nCREATE TABLE {target} AS\n{body};\n")
    else:
        ddl = f"CREATE OR REPLACE {kind} {target} AS\n{body};\n"
    return _header(r, catalog) + "\n" + ddl


def _table_for(conn, ds: dict) -> str:
    if ds.get("origin") == "pipeline":
        return conn.table_name(ds["layer"], ds["id"])
    return conn.source_name(ds)


def _spark_table(conn, ds: dict) -> str:
    name = _table_for(conn, ds)
    if conn.key == "fabric":   # Lakehouse (schema-enabled) instead of the Warehouse
        return name.replace(f"{conn.catalog_name}_wh.", f"{conn.catalog_name}_lh.", 1)
    return name


def pyspark(r: Resolved, catalog: dict, platform: str | None = None) -> str | None:
    platform = platform or r.spec.platform
    conn = connectors.get(platform)
    if not conn.supports_pyspark:
        return None
    spec = r.spec
    ctx = _Ctx(r)
    L = [_header(r, catalog, "#").rstrip(), "from pyspark.sql import functions as F", ""]
    for d in r.datasets:
        L.append(f'{d} = spark.table("{_spark_table(conn, catalog[d])}").alias("{d}")')
    L += ["", f"df = {spec.source}"]
    for j in spec.joins:
        L.append(f'df = df.join({j.dataset}, F.col("{ctx.full(j.left)}") == F.col("{ctx.full(j.right)}"), '
                 f'"{j.how}")')
    for f in spec.filters:
        ds, col = r.refs[f.column]
        L.append(f"df = df.where({_py_filter(f'{ds}.{col['name']}', f, col['type'])})")

    def col_expr(o):
        e = f'F.col("{ctx.full(o["ref"])}")'
        g = o.get("grain")
        if g == "day":
            e = f"F.to_date({e})"
        elif g:
            e = f'F.date_trunc("{g}", {e}).cast("date")'
        if o.get("mask") == "hash":
            e = f'F.sha2({e}.cast("string"), 256)'
        elif o.get("mask") == "redact":
            e = f'F.when({e}.isNull(), None).otherwise(F.lit("***"))'
        return f'{e}.alias("{o["name"]}")'

    def meas_expr(o):
        c = ctx.full(o["ref"])
        a = o["agg"]
        if c is None:
            e = "F.count(F.lit(1))"
        elif a == "count_distinct":
            e = f'F.countDistinct(F.col("{c}"))'
        elif a == "sum":
            e = f'F.sum(F.col("{c}"))'
            if o["column"]["type"] == "decimal":
                e = f"F.round({e}, 2)"
        elif a == "avg":
            e = f'F.round(F.avg(F.col("{c}")), 2)'
        else:
            e = f'F.{a}(F.col("{c}"))'
        return f'{e}.alias("{o["name"]}")'

    groups = [col_expr(o) for o in r.outputs if o["kind"] == "group"]
    meas = [meas_expr(o) for o in r.outputs if o["kind"] == "measure"]
    cols = [col_expr(o) for o in r.outputs if o["kind"] == "column"]
    if meas:
        L.append("df = df.groupBy(\n" + "".join(f"{INDENT}{g},\n" for g in groups) + ").agg(\n"
                 + "".join(f"{INDENT}{m},\n" for m in meas) + ")")
    elif groups:
        L.append("df = df.select(\n" + "".join(f"{INDENT}{g},\n" for g in groups) + ").distinct()")
    else:
        L.append("df = df.select(\n" + "".join(f"{INDENT}{c},\n" for c in cols) + ")")
    target = _spark_table(conn, {"origin": "pipeline", "layer": spec.target.layer, "id": spec.target.name})
    if spec.target.materialization == "view":
        L += ["", "# Materialization is 'view': the SQL script creates the view.",
              f'df.createOrReplaceTempView("{spec.target.name}")']
    else:
        L += ["", f'df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("{target}")']
    return "\n".join(L) + "\n"


def _py_filter(x: str, f, t: str) -> str:
    c = f'F.col("{x}")'

    def lit(v):
        if t == "date":
            return f'F.lit("{v}").cast("date")'
        if t == "timestamp":
            return f'F.lit("{v}").cast("timestamp")'
        return json.dumps(v) if not isinstance(v, (int, float)) or isinstance(v, bool) else str(v)

    if f.op == "is null":
        return f"{c}.isNull()"
    if f.op == "is not null":
        return f"{c}.isNotNull()"
    if f.op in ("in", "not in"):
        vals = f.value if isinstance(f.value, list) else [f.value]
        e = f"{c}.isin({', '.join(json.dumps(v) for v in vals)})"
        return f"~{e}" if f.op == "not in" else e
    op = "==" if f.op == "=" else f.op
    return f"{c} {op} {lit(f.value)}"


def dbt(r: Resolved, catalog: dict) -> tuple[str, str]:
    spec = r.spec

    def table_of(d):
        ds = catalog[d]
        if ds.get("origin") == "pipeline":
            return f"{{{{ ref('{d}') }}}}"
        schema = "raw" if ds["layer"] == "bronze" else ds["layer"]
        return f"{{{{ source('{schema}', '{d}') }}}}"

    body = render_select(select_parts(r, "dbt", table_of), lower=True)
    mat = spec.target.materialization
    model = (f"{{{{ config(materialized='{mat}', schema='{spec.target.layer}') }}}}\n\n"
             + _header(r, catalog).rstrip() + "\n\n" + body + "\n")

    groups = [o["name"] for o in r.outputs if o["kind"] == "group"]
    Y = ["version: 2", "", "models:", f"  - name: {spec.target.name}",
         f"    description: {json.dumps(summarize(spec, catalog))}"]
    if len(groups) > 1:
        Y += ["    tests:", "      - dbt_utils.unique_combination_of_columns:",
              "          combination_of_columns: [" + ", ".join(groups) + "]"]
    Y.append("    columns:")
    for o in r.outputs:
        Y.append(f"      - name: {o['name']}")
        tests = []
        if o["kind"] == "group":
            # Blank or unexpected values are warnings in the sandbox, so they warn in dbt too;
            # a broken grain (duplicates) is an error in both.
            tests.append("not_null:\n              config: {severity: warn}")
            if len(groups) == 1:
                tests.append("unique")
            av = (o.get("column") or {}).get("accepted_values")
            if av and not o.get("grain") and not o.get("mask"):
                tests.append("accepted_values:\n              values: " + json.dumps(av)
                             + "\n              config: {severity: warn}")
        if o["kind"] == "measure" and o["agg"] in ("count", "count_distinct"):
            tests.append("not_null")
        if o["kind"] == "measure":
            Y.append(f"        description: {json.dumps(_measure_words(o))}")
        if tests:
            Y.append("        tests:")
            Y += [f"          - {t}" for t in tests]
    return model, "\n".join(Y) + "\n"


def _measure_words(o: dict) -> str:
    if o["ref"] is None:
        return "Number of rows"
    return f"{AGG_PHRASE[o['agg']].capitalize()} {o['ref']}"


def column_map(r: Resolved) -> list[dict]:
    """Schema mapping for lineage: each output column and where it came from."""
    out = []
    for o in r.outputs:
        frm = None if o["ref"] is None else f"{o['dataset']}.{o['column']['name']}"
        if o["kind"] == "measure":
            how = "count of rows" if frm is None else AGG_PHRASE[o["agg"]]
        else:
            how = "copied"
            if o.get("grain"):
                how = f"truncated to {o['grain']}"
            if o.get("mask"):
                how += f", masked ({o['mask']})"
        out.append({"output": o["name"], "from": frm, "transform": how})
    return out


def artifacts(r: Resolved, catalog: dict) -> list[dict]:
    """Every file a developer would commit, for the pipeline's platform."""
    spec = r.spec
    conn = connectors.get(spec.platform)
    name = spec.target.name
    sql = platform_sql(r, catalog)
    files = [{"path": f"sql/{name}.sql", "language": "sql",
              "title": f"{PLATFORM_NAMES[spec.platform]} SQL ({conn.sql_dialect})", "content": sql}]
    py = pyspark(r, catalog)
    if py:
        files.append({"path": f"notebooks/{name}.py", "language": "python",
                      "title": "PySpark notebook" + (" (Fabric Lakehouse)" if spec.platform == "fabric" else ""),
                      "content": py})
    model, schema = dbt(r, catalog)
    files.append({"path": f"dbt/models/{spec.target.layer}/{name}.sql", "language": "sql",
                  "title": "dbt model", "content": model})
    files.append({"path": f"dbt/models/{spec.target.layer}/{name}.yml", "language": "yaml",
                  "title": "dbt tests (schema.yml)", "content": schema})
    fn, lang, content = conn.schedule_artifact(spec.name, conn.target_name(spec.target.layer, name),
                                               sql.split("\n\n", 1)[1], spec.schedule)
    files.append({"path": f"deploy/{fn}", "language": lang, "title": f"{conn.label} schedule",
                  "content": content})
    files.append({"path": f"specs/{name}.json", "language": "json", "title": "Pipeline spec (source of truth)",
                  "content": json.dumps(spec.model_dump(mode="json"), indent=2) + "\n"})
    files.append({"path": f"sandbox/{name}.sql", "language": "sql", "title": "Sandbox SQL (what the test ran)",
                  "content": sandbox_sql(r) + "\n"})
    return files

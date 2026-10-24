"""Code Sandbox: run the pipeline for real, in isolation, before anyone can deploy it.

Every test run gets a brand-new in-memory database that holds only the sample
rows of the datasets the pipeline touches. Nothing it does can reach the app
database, the platforms or the network, and a hard time limit stops runaway
queries. The compiled SQL is executed, then the result is put through
automated data-quality and compliance checks.

Each check returns a plain-English verdict plus an explanation
{formula, inputs, reasoning}, so a business user sees *what* is wrong and a
developer sees *exactly how* it was measured.
"""
from __future__ import annotations

import hashlib
import sqlite3
import time
from datetime import date

from . import config
from .compiler import sandbox_sql
from .spec import Issue, PLATFORM_NAMES, Resolved

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
_SQLITE_TYPE = {"string": "TEXT", "integer": "INTEGER", "decimal": "REAL", "date": "TEXT",
                "timestamp": "TEXT", "boolean": "INTEGER"}


def _explain(formula: str, inputs: dict, reasoning: str) -> dict:
    return {"formula": formula, "inputs": inputs, "reasoning": reasoning}


def _check(cid, title, status, message, explanation, fix="", patch=None) -> dict:
    d = {"id": cid, "title": title, "status": status, "message": message, "explanation": explanation}
    if fix:
        d["fix"] = fix
    if patch:
        d["patch"] = patch
    return d


def _sha256(v):
    return None if v is None else hashlib.sha256(str(v).encode()).hexdigest()


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.create_function("sha256", 1, _sha256, deterministic=True)
    deadline = time.monotonic() + config.SANDBOX_TIMEOUT_SECONDS
    con.set_progress_handler(lambda: 1 if time.monotonic() > deadline else 0, 10_000)
    return con


def _load(con: sqlite3.Connection, ds: dict) -> None:
    cols = ds["columns"]
    con.execute(f"CREATE TABLE {ds['id']} (" + ", ".join(
        f"{c['name']} {_SQLITE_TYPE.get(c['type'], 'TEXT')}" for c in cols) + ")")
    names = [c["name"] for c in cols]
    con.executemany(f"INSERT INTO {ds['id']} VALUES ({', '.join('?' * len(names))})",
                    [tuple(r.get(n) for n in names) for r in (ds.get("rows") or [])])


def output_types(r: Resolved) -> list[dict]:
    out = []
    for o in r.outputs:
        if o["kind"] == "measure":
            t = ("integer" if o["agg"] in ("count", "count_distinct") or
                 (o["agg"] == "sum" and o["column"]["type"] == "integer") else
                 "decimal" if o["agg"] in ("sum", "avg") else o["column"]["type"])
        elif o.get("mask"):
            t = "string"
        elif o.get("grain"):
            t = "date"
        else:
            t = o["column"]["type"]
        out.append({"name": o["name"], "type": t,
                    "classification": "PII-masked" if o.get("mask") else
                    ((o.get("column") or {}).get("classification") if o["kind"] != "measure" or
                     o["agg"] not in ("count", "count_distinct") else None)})
    return out


def run(r: Resolved, spec_issues: list[Issue], as_of: date | None = None) -> dict:
    as_of = as_of or config.AS_OF_DATE
    spec = r.spec
    started = time.monotonic()
    checks: list[dict] = []

    # 1. Design and governance rules from the spec check
    errors = [i for i in spec_issues if i.level == "error"]
    for i in spec_issues:
        gov = i.code in ("PII_EXPOSED", "CROSS_PLATFORM", "CURATED_AVAILABLE")
        checks.append(_check(f"rule:{i.code}", "Governance rule" if gov else "Design check",
                             FAIL if i.level == "error" else WARN, i.message,
                             _explain("spec rule " + i.code, {}, "Checked against the catalog before running."),
                             i.fix, i.patch))
    sql = sandbox_sql(r) if not errors else None
    if errors:
        return _finish(r, checks, [], [], started, sql, ran=False)

    con = _connect()
    try:
        for ds in r.datasets.values():
            _load(con, ds)
        try:
            cur = con.execute(sql)
            names = [d[0] for d in cur.description]
            rows = [dict(zip(names, t)) for t in cur.fetchmany(config.SANDBOX_MAX_OUTPUT_ROWS + 1)]
        except sqlite3.Error as exc:
            msg = str(exc)
            timeout = "interrupted" in msg.lower()
            checks.append(_check("runs", "Runs successfully", FAIL,
                                 f"The pipeline stopped after {config.SANDBOX_TIMEOUT_SECONDS} seconds."
                                 if timeout else f"The compiled code failed to run: {msg}",
                                 _explain("execute(sandbox_sql)", {"error": msg},
                                          "The SQL compiled from the spec was executed against sample data.")))
            return _finish(r, checks, [], [], started, sql, ran=False)
        truncated = len(rows) > config.SANDBOX_MAX_OUTPUT_ROWS
        rows = rows[: config.SANDBOX_MAX_OUTPUT_ROWS]
        base_n = con.execute(f"SELECT COUNT(*) FROM {spec.source}").fetchone()[0]
        checks.append(_check("runs", "Runs successfully", PASS,
                             f"Ran on sample data: read {base_n} {r.datasets[spec.source]['friendly_name']} "
                             f"row(s), produced {len(rows)} result row(s)."
                             + (" (Result capped for the sandbox.)" if truncated else ""),
                             _explain("execute(sandbox_sql)", {"source_rows": base_n, "result_rows": len(rows)},
                                      "The exact SQL compiled from the spec ran in an isolated in-memory "
                                      "database holding only this pipeline's sample data.")))
        checks += _join_checks(con, r)
    finally:
        con.close()

    checks += _result_checks(r, rows)
    checks += _freshness_checks(r, as_of)
    return _finish(r, checks, rows, output_types(r), started, sql, ran=True)


def _join_checks(con, r: Resolved) -> list[dict]:
    out = []
    spec = r.spec
    totals_affected = [o for o in r.outputs if o["kind"] == "measure" and o["agg"] in ("sum", "count", "avg")]
    for j in spec.joins:
        lds, lcol = r.refs[j.left]
        rds, rcol = r.refs[j.right]
        L, R = r.datasets[lds], r.datasets[rds]
        lk, rk = f"{lds}.{lcol['name']}", f"{rds}.{rcol['name']}"

        # Fan-out: duplicate keys on the right side repeat left rows.
        dup_keys = [k for (k,) in con.execute(
            f"SELECT {rk} FROM {rds} WHERE {rk} IS NOT NULL GROUP BY {rk} HAVING COUNT(*) > 1")]
        left_rows = con.execute(f"SELECT COUNT(*) FROM {lds}").fetchone()[0]
        if dup_keys:
            ph = ", ".join("?" * len(dup_keys))
            affected = con.execute(f"SELECT COUNT(*) FROM {lds} WHERE {lk} IN ({ph})", dup_keys).fetchone()[0]
            extra = con.execute(
                f"SELECT COUNT(*) FROM {lds} JOIN {rds} ON {lk} = {rk} WHERE {lk} IN ({ph})",
                dup_keys).fetchone()[0] - affected
        else:
            affected = extra = 0
        cur = R.get("curated_version")
        if affected:
            sev = FAIL if totals_affected else WARN
            what = ("Totals would be overstated" if totals_affected else "Some rows would appear twice")
            out.append(_check(
                f"fanout:{rds}", f"Joining {R['friendly_name']} doesn't duplicate rows", sev,
                f"{R['friendly_name']} has more than one row for {', '.join(map(str, dup_keys[:5]))}. "
                f"{affected} {L['friendly_name']} row(s) match it, adding {extra} duplicate row(s). {what}.",
                _explain(f"duplicate keys in {rk}; rows added = matched rows - matching {lds} rows",
                         {"duplicate_keys": dup_keys[:20], f"{lds}_rows_affected": affected,
                          "duplicate_rows_added": extra},
                         "A join repeats a row once for every match. A lookup table must have one row per key."),
                f"Use '{cur}', the certified one-row-per-key version." if cur else
                "Join to a table with one row per key, or remove the duplicates upstream.",
                {"op": "replace_dataset", "from": rds, "to": cur} if cur else None))
        else:
            out.append(_check(f"fanout:{rds}", f"Joining {R['friendly_name']} doesn't duplicate rows", PASS,
                              f"{R['friendly_name']} has exactly one row per {rcol['name']}.",
                              _explain(f"count of duplicate keys in {rk}", {"duplicate_keys": 0},
                                       "No key repeats, so the join cannot multiply rows.")))

        # Match rate: left keys with no partner.
        nulls = con.execute(f"SELECT COUNT(*) FROM {lds} WHERE {lk} IS NULL").fetchone()[0]
        orphans = con.execute(
            f"SELECT COUNT(*) FROM {lds} WHERE {lk} IS NOT NULL AND {lk} NOT IN "
            f"(SELECT {rk} FROM {rds} WHERE {rk} IS NOT NULL)").fetchone()[0]
        examples = [k for (k,) in con.execute(
            f"SELECT DISTINCT {lk} FROM {lds} WHERE {lk} IS NOT NULL AND {lk} NOT IN "
            f"(SELECT {rk} FROM {rds} WHERE {rk} IS NOT NULL) LIMIT 3")]
        unmatched = orphans + nulls
        pct = round(100 * (left_rows - unmatched) / left_rows, 1) if left_rows else 100.0
        consequence = ("they will be dropped from the result" if j.how == "inner" else
                       f"they will show blank {R['friendly_name']} details")
        if unmatched:
            parts = []
            if orphans:
                parts.append(f"{orphans} have a {lcol['name']} that doesn't exist in {R['friendly_name']}"
                             + (f" (e.g. {', '.join(map(str, examples))})" if examples else ""))
            if nulls:
                parts.append(f"{nulls} have no {lcol['name']} at all")
            out.append(_check(
                f"match:{rds}", f"Every {L['friendly_name']} row finds its {R['friendly_name']}", WARN,
                f"{pct}% of {L['friendly_name']} rows match. Of the rest, " + " and ".join(parts)
                + f"; {consequence}.",
                _explain("match % = (rows - unmatched) / rows x 100",
                         {"rows": left_rows, "orphan_keys": orphans, "null_keys": nulls, "match_pct": pct},
                         "Unmatched keys usually mean late-arriving or deleted reference data."),
                "Raise it with the data owner, or switch to 'keep every row' (left join) so nothing "
                "is silently dropped." if j.how == "inner" else
                "Raise it with the data owner; the rows are kept with blank details.",
                {"op": "set_join_how", "dataset": rds, "how": "left"} if j.how == "inner" else None))
        else:
            out.append(_check(f"match:{rds}", f"Every {L['friendly_name']} row finds its {R['friendly_name']}",
                              PASS, f"All {left_rows} {L['friendly_name']} rows match.",
                              _explain("match % = (rows - unmatched) / rows x 100",
                                       {"rows": left_rows, "unmatched": 0, "match_pct": 100.0},
                                       "Every key has a partner.")))
    return out


def _result_checks(r: Resolved, rows: list[dict]) -> list[dict]:
    out = []
    if not rows:
        out.append(_check("not_empty", "Result has rows", WARN,
                          "The result is empty: the filters removed every row.",
                          _explain("count(result rows) > 0", {"result_rows": 0},
                                   "An empty table usually means a filter value doesn't match the data."),
                          "Check filter values (spelling, case, date range)."))
        return out
    out.append(_check("not_empty", "Result has rows", PASS, f"{len(rows)} row(s).",
                      _explain("count(result rows) > 0", {"result_rows": len(rows)}, "The result has data.")))

    groups = [o["name"] for o in r.outputs if o["kind"] == "group"]
    if r.spec.measures and groups:
        keys = [tuple(row[g] for g in groups) for row in rows]
        dups = len(keys) - len(set(keys))
        out.append(_check("grain", "One row per " + " + ".join(groups), PASS if not dups else FAIL,
                          "Each combination appears once." if not dups else f"{dups} combination(s) repeat.",
                          _explain("count(rows) - count(distinct grain)", {"rows": len(keys),
                                                                           "duplicates": dups},
                                   "The grain is the set of columns that identifies one row.")))
    elif not r.spec.measures:
        sig = [tuple(sorted(row.items())) for row in rows]
        dups = len(sig) - len(set(sig))
        out.append(_check("duplicates", "No duplicate rows", PASS if not dups else WARN,
                          "Every row is distinct." if not dups else f"{dups} row(s) are exact duplicates.",
                          _explain("count(rows) - count(distinct rows)", {"rows": len(sig), "duplicates": dups},
                                   "Exact duplicate rows usually come from a join or a raw feed."),
                          "" if not dups else "Check the joins, or group by the columns instead."))

    for g in groups:
        n = sum(1 for row in rows if row.get(g) is None)
        if n:
            out.append(_check(f"nulls:{g}", f"'{g}' is always filled in", WARN,
                              f"{n} result row(s) have a blank {g}; their numbers are grouped together "
                              "under 'blank'.",
                              _explain(f"count(rows where {g} is null)", {"null_rows": n, "rows": len(rows)},
                                       "Blank grouping values hide data quality gaps in the source."),
                              f"Ask the data owner to fill in {g}, or filter out blanks."))
        else:
            out.append(_check(f"nulls:{g}", f"'{g}' is always filled in", PASS, "No blanks.",
                              _explain(f"count(rows where {g} is null)", {"null_rows": 0}, "No blanks.")))

    for o in r.outputs:
        if o["kind"] == "measure" and o["agg"] in ("sum", "avg") and o["column"]:
            neg = sum(1 for row in rows if isinstance(row.get(o["name"]), (int, float)) and row[o["name"]] < 0)
            if neg:
                out.append(_check(f"negative:{o['name']}", f"'{o['name']}' is never negative", WARN,
                                  f"{neg} row(s) have a negative {o['name']}.",
                                  _explain(f"count(rows where {o['name']} < 0)", {"rows": neg},
                                           "Negative totals often mean refunds are mixed in."),
                                  "Add a filter (for example status is completed)."))
    return out


def _freshness_checks(r: Resolved, as_of: date) -> list[dict]:
    out = []
    limit = {"hourly": 1, "daily": 1, "weekly": 7, "manual": 7}[r.spec.schedule]
    for ds in r.datasets.values():
        ll = ds.get("last_loaded")
        if not ll:
            continue
        ll = ll if isinstance(ll, date) else date.fromisoformat(str(ll))
        age = (as_of - ll).days
        ok = age <= limit
        out.append(_check(f"fresh:{ds['id']}", f"{ds['friendly_name']} is up to date", PASS if ok else WARN,
                          f"Last loaded {ll.isoformat()} ({age} day(s) before {as_of.isoformat()})."
                          + ("" if ok else f" A {r.spec.schedule} pipeline would publish stale data."),
                          _explain("age_days = as_of - last_loaded; ok if age_days <= allowed",
                                   {"last_loaded": ll.isoformat(), "as_of": as_of.isoformat(),
                                    "age_days": age, "allowed_days": limit},
                                   "Freshness is judged against how often the pipeline runs."),
                          "" if ok else f"Ask {ds.get('owner') or 'the data owner'} why the load is late."))
    return out


def _finish(r, checks, rows, schema, started, sql, ran: bool) -> dict:
    fails = [c for c in checks if c["status"] == FAIL]
    warns = [c for c in checks if c["status"] == WARN]
    status = "FAILED" if fails or not ran else ("PASSED_WITH_WARNINGS" if warns else "PASSED")
    if status == "FAILED":
        headline = (f"Not ready: {len(fails)} problem(s) to fix" if fails else "Not ready: it did not run")
    elif status == "PASSED_WITH_WARNINGS":
        headline = f"Works, with {len(warns)} thing(s) to know before you deploy"
    else:
        headline = "Ready: every check passed"
    order = {FAIL: 0, WARN: 1, PASS: 2}
    checks.sort(key=lambda c: order[c["status"]])
    return {"status": status, "headline": headline, "platform": PLATFORM_NAMES[r.spec.platform],
            "counts": {"fail": len(fails), "warn": len(warns),
                       "pass": sum(1 for c in checks if c["status"] == PASS)},
            "checks": checks, "row_count": len(rows), "schema": schema,
            "preview": rows[: config.SANDBOX_PREVIEW_ROWS], "rows": rows,
            "elapsed_ms": int((time.monotonic() - started) * 1000), "sandbox_sql": sql}

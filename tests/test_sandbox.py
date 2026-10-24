"""The sandbox finds real problems, explains them, and its fixes work."""
from app import orchestration as o
from app.seed import EXAMPLES


def _test(session, spec):
    p = o.create(session, spec, "tester")
    return o.validate(session, p["id"], "tester")


def _by_status(p, status):
    return [c for c in p["validation"]["checks"] if c["status"] == status]


def test_revenue_passes_with_honest_warnings(session):
    p = _test(session, EXAMPLES[0]["spec"])
    assert p["status"] == "VALIDATED"
    assert p["validation"]["status"] == "PASSED_WITH_WARNINGS"
    warns = " ".join(c["message"] for c in _by_status(p, "WARN"))
    assert "C-099" in warns                                   # orphan orders found
    assert "blank region" in warns                            # null regions found
    assert p["validation"]["row_count"] > 0


def test_duplicate_join_key_fails_and_fix_repairs_totals(session):
    p = _test(session, EXAMPLES[1]["spec"])
    assert p["status"] == "FAILED_VALIDATION"
    fan = [c for c in _by_status(p, "FAIL") if c["id"].startswith("fanout:")][0]
    assert "P-007" in fan["message"] and fan["patch"]["to"] == "products_current"
    before = {r["category"]: r["revenue"] for r in p["validation"]["preview"]}
    p = o.fix(session, p["id"], fan["patch"], "tester")
    assert p["validation"]["status"] != "FAILED"
    after = {r["category"]: r["revenue"] for r in p["validation"]["preview"]}
    assert after["Camping"] < before["Camping"]               # the duplicate rows are gone
    assert after["Footwear"] == before["Footwear"]            # unaffected categories unchanged


def test_pii_blocks_until_masked(session):
    p = _test(session, EXAMPLES[2]["spec"])
    fails = _by_status(p, "FAIL")
    assert {c["patch"]["column"] for c in fails} == {"customers.full_name", "customers.email"}
    assert p["validation"]["preview"] == []                   # PII never left the sandbox
    for c in fails:
        p = o.fix(session, p["id"], c["patch"])
    assert p["validation"]["status"] == "PASSED"
    row = p["validation"]["preview"][0]
    assert len(row["email"]) == 64 and "@" not in row["email"]


def test_stale_source_is_flagged(session):
    p = _test(session, EXAMPLES[4]["spec"])
    stale = [c for c in _by_status(p, "WARN") if c["id"] == "fresh:web_events"]
    assert stale and stale[0]["explanation"]["inputs"]["age_days"] == 6


def test_every_check_explains_itself(session):
    p = _test(session, EXAMPLES[0]["spec"])
    for c in p["validation"]["checks"]:
        assert set(c["explanation"]) == {"formula", "inputs", "reasoning"}


def test_design_errors_suggest_fixes(session):
    spec = dict(EXAMPLES[3]["spec"])
    spec["measures"] = [{"agg": "sum", "column": "support_tickets.chanel", "name": "x"}]
    p = _test(session, spec)
    assert p["status"] == "FAILED_VALIDATION"
    c = _by_status(p, "FAIL")[0]
    assert "channel" in c["fix"] and c["patch"]["op"] == "replace_column"


def test_sum_of_text_is_refused(session):
    spec = dict(EXAMPLES[3]["spec"])
    spec["measures"] = [{"agg": "sum", "column": "support_tickets.channel", "name": "x"}]
    p = _test(session, spec)
    assert any(c["id"] == "rule:NOT_NUMERIC" for c in _by_status(p, "FAIL"))


def test_misspelled_filter_value_warns(session):
    spec = dict(EXAMPLES[0]["spec"])
    spec["filters"] = [{"column": "orders.status", "op": "=", "value": "complete"}]
    p = _test(session, spec)
    w = [c for c in p["validation"]["checks"] if c["id"] == "rule:UNEXPECTED_VALUE"][0]
    assert w["patch"]["to"] == "completed"


def test_runaway_query_is_stopped(session, monkeypatch):
    from app import config, sandbox
    monkeypatch.setattr(config, "SANDBOX_TIMEOUT_SECONDS", 0)
    p = o.create(session, EXAMPLES[0]["spec"])
    r, issues, _ = o.check(session, o.parse_spec(p["spec"]))
    out = sandbox.run(r, issues)
    assert out["status"] == "FAILED"

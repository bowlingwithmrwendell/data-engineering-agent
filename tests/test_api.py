"""End-to-end through HTTP, the way the console uses it."""
from app.seed import EXAMPLES


def test_health_and_catalog(client):
    assert client.get("/health").json()["status"] == "ok"
    cat = client.get("/api/catalog").json()
    assert {d["id"] for d in cat} >= {"orders", "customers", "products_current", "web_events"}
    hits = client.get("/api/catalog", params={"q": "satisfaction"}).json()
    assert hits[0]["id"] == "support_tickets"


def test_pii_hidden_in_catalog_preview(client):
    d = client.get("/api/catalog/customers").json()
    assert all(r["email"].startswith("••••") for r in d["sample"])
    assert "rows" not in d


def test_full_journey(client):
    spec = EXAMPLES[1]["spec"]
    prev = client.post("/api/compile", json={"spec": spec}).json()
    assert prev["ready_to_test"] and any(f["path"].startswith("dbt/") for f in prev["artifacts"])
    p = client.post("/api/pipelines", json={"spec": spec, "by": "Ana"}).json()
    p = client.post(f"/api/pipelines/{p['id']}/test", json={"by": "Ana"}).json()
    assert p["status"] == "FAILED_VALIDATION"
    assert client.post(f"/api/pipelines/{p['id']}/request-deploy", json={"by": "Ana"}).status_code == 409
    patch = next(c["patch"] for c in p["validation"]["checks"] if c["status"] == "FAIL")
    p = client.post(f"/api/pipelines/{p['id']}/fix", json={"patch": patch, "by": "Ana"}).json()
    assert p["status"] == "VALIDATED"
    req = client.post(f"/api/pipelines/{p['id']}/request-deploy", json={"by": "Ana"}).json()["action"]
    assert client.post(f"/api/actions/{req['id']}/decision",
                       json={"approve": True, "decided_by": "Claude"}).status_code == 409
    ok = client.post(f"/api/actions/{req['id']}/decision", json={"approve": True, "decided_by": "Bea"})
    assert ok.status_code == 200 and ok.json()["status"] == "EXECUTED"
    full = client.get(f"/api/pipelines/{p['id']}").json()
    assert full["status"] == "DEPLOYED" and [w["state"] for w in full["workflow"]][-1] == "done"
    assert full["artifacts"] and full["runs"]
    assert client.get("/api/lineage/sales_by_category").json()["upstream"]


def test_bad_spec_is_explained(client):
    r = client.post("/api/pipelines", json={"spec": {"name": "x"}})
    assert r.status_code == 422 and "incomplete" in r.json()["detail"]


def test_agent_without_key_points_to_builder(client):
    r = client.post("/api/agent/ask", json={"question": "monthly revenue please"})
    assert r.status_code == 503 and "builder" in r.json()["detail"]


def test_governance_scorecard(client):
    g = client.get("/api/governance").json()
    assert "Writing SQL, PySpark or dbt (a deterministic compiler does)" in g["model_card"]["not_used_for"]
    assert g["limits"]["sandbox_timeout_seconds"] >= 1


def test_kill_switch_requires_named_human_and_token(client):
    body = {"engaged": True, "actor": "Wendell Killette", "reason": "maintenance"}
    assert client.post("/api/governance/kill-switch", json=body).status_code == 401
    bot = dict(body, actor="Ops Bot")
    assert client.post("/api/governance/kill-switch", json=bot,
                       headers={"X-Admin-Token": "test-admin"}).status_code == 403
    r = client.post("/api/governance/kill-switch", json=body, headers={"X-Admin-Token": "test-admin"})
    assert r.json()["engaged"] is True


def test_request_size_limit(client):
    r = client.post("/api/compile", content=b"{" + b" " * 40000 + b"}",
                    headers={"Content-Type": "application/json"})
    assert r.status_code == 413


def test_pages_served(client):
    assert "Data Engineering Agent" in client.get("/").text
    assert "Governance" in client.get("/governance").text


def test_preview_of_a_live_pipeline_does_not_flag_its_own_table(client):
    from app.seed import EXAMPLES as EX
    spec = EX[3]["spec"]
    p = client.post("/api/pipelines", json={"spec": spec, "by": "Ana"}).json()
    client.post(f"/api/pipelines/{p['id']}/test")
    a = client.post(f"/api/pipelines/{p['id']}/request-deploy", json={"by": "Ana"}).json()["action"]
    client.post(f"/api/actions/{a['id']}/decision", json={"approve": True, "decided_by": "Bea"})
    own = client.post("/api/compile", json={"spec": spec, "pipeline_id": p["id"]}).json()
    assert not [i for i in own["issues"] if i["code"] == "TARGET_TAKEN"]
    other = client.post("/api/compile", json={"spec": spec}).json()
    assert [i for i in other["issues"] if i["code"] == "TARGET_TAKEN"]

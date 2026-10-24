"""Migration mode: conform an acquired company's data, reconcile it, and prove legacy equivalence."""
import copy

import pytest

from app import actions, information, migration
from app import orchestration as o
from app.seed import CONFORM_EXAMPLES, LEGACY_EXAMPLE

ORDERS = CONFORM_EXAMPLES[0]["spec"]
CUSTOMERS = CONFORM_EXAMPLES[1]["spec"]


def _test(session, spec):
    return o.validate(session, o.create(session, spec, "Ana")["id"], "Ana")


def _checks(p, status=None):
    return [c for c in p["validation"]["checks"] if status is None or c["status"] == status]


def _fix_all(session, p, rounds=5):
    for _ in range(rounds):
        fx = next((c["patch"] for c in _checks(p, "FAIL") if c.get("patch")), None)
        if not fx:
            return p
        p = o.fix(session, p["id"], fx, "Ana")
    return p


def test_text_date_must_be_converted_and_fix_does_it(session):
    p = _test(session, ORDERS)
    fail = _checks(p, "FAIL")
    assert [c["id"] for c in fail] == ["rule:TYPE_MISMATCH"]
    assert fail[0]["patch"] == {"op": "set_mapping_cast", "label": "summit", "target": "order_date", "cast": "date"}
    p = _fix_all(session, p)
    assert p["status"] == "VALIDATED"


def test_reconciliation_proves_rows_and_totals(session):
    p = _fix_all(session, _test(session, ORDERS))
    by_id = {c["id"]: c for c in _checks(p)}
    assert by_id["recon:rows:lakeside"]["status"] == "PASS"
    assert by_id["recon:rows:summit"]["status"] == "PASS"
    assert by_id["recon:rows:summit"]["explanation"]["inputs"]["rows_out"] == 150
    assert by_id["recon:sum:summit:amount"]["status"] == "PASS"
    assert by_id["recon:sum:lakeside:amount"]["status"] == "PASS"
    assert p["validation"]["row_count"] == 550
    sources = {r["source_system"] for r in p["validation"]["preview"]}
    assert sources == {"lakeside", "summit"}                      # the preview shows both companies


def test_unreadable_values_and_crosswalk_misses_are_counted_not_hidden(session):
    p = _fix_all(session, _test(session, ORDERS))
    by_id = {c["id"]: c for c in _checks(p)}
    cast = by_id["recon:cast:summit:amount"]
    assert cast["status"] == "WARN" and "'N/A'" in cast["message"]
    assert cast["explanation"]["inputs"]["unreadable"] == 2
    miss = by_id["recon:lookup:summit:customer_id"]
    assert miss["status"] == "WARN" and "SC-131" in miss["message"]
    assert miss["explanation"]["inputs"]["rows_affected"] == 3


def test_value_translation_lands_on_standard_values(session):
    p = _fix_all(session, _test(session, ORDERS))
    assert {c["id"]: c for c in _checks(p)}["model:values:status"]["status"] == "PASS"
    summit = [r for r in p["validation"]["preview"] if r["source_system"] == "summit"]
    assert summit and all(r["status"] in ("completed", "returned", "cancelled") for r in summit)


def test_shared_customers_are_caught_and_prefer_first_resolves_them(session):
    p = _test(session, CUSTOMERS)
    assert {c["patch"]["column"] for c in _checks(p, "FAIL") if c["id"] == "rule:PII_EXPOSED"} == {"full_name",
                                                                                                    "email"}
    p = o.fix(session, p["id"], {"op": "add_mask", "column": "full_name", "method": "hash"})
    p = o.fix(session, p["id"], {"op": "add_mask", "column": "email", "method": "hash"})
    key = {c["id"]: c for c in _checks(p)}["model:key"]
    assert key["status"] == "FAIL" and key["explanation"]["inputs"]["in_both_sources"] == 8
    p = o.fix(session, p["id"], key["patch"])
    by_id = {c["id"]: c for c in _checks(p)}
    assert p["status"] == "VALIDATED"
    assert by_id["recon:rows:summit"]["explanation"]["inputs"]["skipped_overlap"] == 8
    assert by_id["model:values:region"]["status"] == "WARN" and "'INTL'" in by_id["model:values:region"]["message"]
    assert p["validation"]["row_count"] == 40 + 30 - 8


def test_conform_compiles_for_every_platform(session):
    spec = copy.deepcopy(ORDERS)
    spec["sources"][1]["mappings"][3]["cast"] = "date"
    cat = information.catalog_map(session)
    for platform, needle in [("snowflake", "TRY_CAST(CAST(summit_orders.total_usd AS VARCHAR) AS NUMBER(18, 2))"),
                             ("databricks", "TRY_CAST(summit_orders.total_usd AS DECIMAL(18, 2))"),
                             ("fabric", "TRY_CAST(summit_orders.total_usd AS DECIMAL(18, 2))")]:
        spec["platform"] = platform
        s = migration.ConformSpec.model_validate(spec)
        r, issues = migration.resolve(s, cat)
        assert not [i for i in issues if i.level == "error"]
        sql = migration.platform_sql(r, cat, "h")
        assert needle in sql and "UNION ALL" in sql and "'summit' AS source_system" in sql
        assert "LEFT JOIN" in sql and "xw1.legacy_id" in sql
    files = {f["path"] for f in migration.artifacts(r, cat, "h")}
    assert "docs/orders_combined_mapping.csv" in files and "notebooks/orders_combined.py" in files


def test_conform_deploys_with_lineage_to_every_source(session):
    p = _fix_all(session, _test(session, ORDERS))
    req = o.propose_deploy(session, p["id"], "Ana")["action"]
    assert req["payload"]["kind"] == "conform"
    actions.decide(session, req["id"], True, "Bea Approver")
    up = {e["from"] for e in information.lineage(session, "orders_combined")["upstream"]}
    assert up == {"orders", "summit_orders", "customer_crosswalk"}
    assert "Conformed" in information.get_dataset(session, "orders_combined")["tags"]


def test_legacy_sql_equivalence(session):
    p = o.create(session, LEGACY_EXAMPLE["spec"], "Ana")
    r = o.compare_legacy(session, p["id"], LEGACY_EXAMPLE["sql"], "Ana")
    assert r["status"] == "EQUIVALENT" and r["matching_rows"] == r["legacy_rows"] > 0
    wrong = LEGACY_EXAMPLE["sql"].replace("'Closed'", "'Returned'")
    assert o.compare_legacy(session, p["id"], wrong)["status"] == "DIFFERENT"
    full = o.pipeline_dict(session, o._get(session, p["id"]))
    assert full["legacy_comparison"]["status"] == "DIFFERENT" and full["legacy_comparison"]["current"]


@pytest.mark.parametrize("sql", ["DELETE FROM orders", "SELECT 1; DROP TABLE orders",
                                 "SELECT * FROM pragma_table_info('orders')", "ATTACH DATABASE 'x' AS y"])
def test_legacy_sandbox_only_reads(session, sql):
    p = o.create(session, LEGACY_EXAMPLE["spec"])
    assert o.compare_legacy(session, p["id"], sql)["status"] == "COULD_NOT_RUN"


def test_legacy_table_names_from_any_platform_are_understood():
    cat = {"summit_orders": {}, "orders": {}}
    sql = "select * from [summit].[dbo].[summit_orders] o join `lake`.raw.orders x on 1=1"
    out = migration.normalize_legacy_sql(sql, cat)
    assert "from summit_orders" in out and "join orders" in out


def test_reconcile_two_tables(session):
    r = o.reconcile_tables(session, "products", "products_current", "product_id", ["unit_price"])
    assert r["status"] == "DIFFERENCES"
    assert r["rows"] == {"left": 13, "right": 12}
    assert r["measures"][0]["examples"] == ["P-007"]
    same = o.reconcile_tables(session, "products_current", "products_current", "product_id", ["unit_price"])
    assert same["status"] == "MATCH"


def test_replatform_plan_and_draft(session):
    p = o.create(session, LEGACY_EXAMPLE["spec"])
    plan = o.replatform_plan(session, "fabric")
    item = plan["pipelines"][0]
    assert item["new_target"] == "lakeside_wh.gold.summit_units_by_territory_fabric"
    assert item["reads_from_other_platforms"] == ["Summit Customers", "Summit Orders"]
    d = o.replatform(session, p["id"], "fabric", "Ana")
    assert d["platform"] == "fabric" and d["status"] == "DRAFT"
    assert o.validate(session, d["id"])["validation"]["status"] != "FAILED"
    with pytest.raises(o.WorkflowError):
        o.replatform(session, d["id"], "fabric")


def test_catalog_import_tags_pii(session):
    tables = migration.parse_schema_csv("table,column,type\nsummit_invoices,invoice_no,varchar(20)\n"
                                        "summit_invoices,billing_email,nvarchar(200)\n"
                                        "summit_invoices,amount,decimal(12,2)\nsummit_invoices,issued_at,datetime2")
    out = information.import_datasets(session, platform="fabric", owner="Summit IT", tags=["Acquired"],
                                      layer="bronze", tables=tables)
    assert out["created"][0]["pii_columns"] == ["billing_email"]
    ds = information.get_dataset(session, "summit_invoices")
    assert {c["name"]: c["type"] for c in ds["columns"]} == {"invoice_no": "string", "billing_email": "string",
                                                            "amount": "decimal", "issued_at": "timestamp"}
    assert "PII" in ds["tags"] and "Acquired" in ds["tags"]
    again = information.import_datasets(session, platform="fabric", owner="", tags=[], layer="bronze", tables=tables)
    assert again["skipped"][0]["reason"] == "already in the catalog"


def test_migration_api(client):
    ex = client.get("/api/migration/examples").json()
    assert len(ex["conform"]) == 2 and "SELECT" in ex["legacy"]["sql"]
    pv = client.post("/api/compile", json={"spec": ex["conform"][1]["spec"]}).json()
    assert pv["summary"].startswith("Combines Customers (lakeside) and Summit Customers (summit)")
    r = client.post("/api/reconcile", json={"left": "orders", "right": "nope", "key": "order_id"})
    assert r.status_code == 404
    assert client.post("/api/catalog/import", json={"platform": "fabric", "schema_csv": ""}).status_code == 422


def test_agent_can_draft_a_migration_and_prove_legacy_equivalence(session):
    from app import agent
    from tests.test_governance import FakeClient, _text, _tool
    spec = copy.deepcopy(ORDERS)
    spec.pop("kind")
    spec["sources"][1]["mappings"][3]["cast"] = "date"
    fake = FakeClient([
        ([_tool(1, "draft_conform_pipeline", {"spec": spec})], "tool_use"),
        ([_tool(2, "test_pipeline", {"pipeline_id": 1})], "tool_use"),
        ([_tool(3, "draft_pipeline", {"spec": LEGACY_EXAMPLE["spec"]})], "tool_use"),
        ([_tool(4, "compare_with_legacy", {"pipeline_id": 2, "sql": LEGACY_EXAMPLE["sql"]})], "tool_use"),
        ([_text("Combined both companies' orders and the old report matches.")], "end_turn"),
    ])
    out = agent.ask(session, "Merge Summit's orders and check their old report", client=fake)
    assert [t["error"] for t in out["tools_used"]] == [False] * 4
    assert out["pipeline_ids"] == [1, 2]
    sent = fake.calls[4]["messages"][-1]["content"][0]["content"]
    assert '"status": "EQUIVALENT"' in sent
    assert out["status"] == "PENDING_REVIEW"

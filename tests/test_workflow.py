"""Deployment governance: tested version only, human approval, separation of duties, fail closed."""
import pytest

from app import actions, information
from app import orchestration as o
from app.models import AuditRow, RunRow
from app.models import AppendOnlyViolation
from app.seed import EXAMPLES

SPEC = EXAMPLES[3]["spec"]   # passes cleanly


def _ready(session, by="Ana Requester"):
    p = o.create(session, SPEC, by)
    o.validate(session, p["id"], by)
    return p["id"]


def test_cannot_request_untested_or_failed(session):
    p = o.create(session, SPEC)
    with pytest.raises(o.WorkflowError, match="Test this version"):
        o.propose_deploy(session, p["id"])
    bad = o.create(session, EXAMPLES[2]["spec"])
    o.validate(session, bad["id"])
    with pytest.raises(o.WorkflowError, match="failed"):
        o.propose_deploy(session, bad["id"])


def test_changing_spec_requires_retest_and_supersedes_request(session):
    pid = _ready(session)
    req = o.propose_deploy(session, pid, "Ana Requester")["action"]
    spec = dict(SPEC, schedule="weekly")
    p = o.update(session, pid, spec, "Ana Requester")
    assert p["status"] == "DRAFT" and p["pending_action"] is None
    assert actions.list_actions(session)[0]["status"] == "SUPERSEDED"
    with pytest.raises(actions.ActionError):
        actions.decide(session, req["id"], True, "Bea Approver")


@pytest.mark.parametrize("who", ["Claude", "claude-sonnet-5-5", "Deploy Bot", "system", "auto-approver"])
def test_ai_and_automation_cannot_approve(session, who):
    pid = _ready(session)
    req = o.propose_deploy(session, pid, "Ana Requester")["action"]
    with pytest.raises(actions.ActionError, match="cannot be approved"):
        actions.decide(session, req["id"], True, who)


def test_separation_of_duties(session):
    pid = _ready(session)
    req = o.propose_deploy(session, pid, "Ana Requester")["action"]
    with pytest.raises(actions.ActionError, match="Separation of duties"):
        actions.decide(session, req["id"], True, "ana requester")


def test_reject_needs_reason_and_is_final(session):
    pid = _ready(session)
    req = o.propose_deploy(session, pid, "Ana Requester")["action"]
    with pytest.raises(actions.ActionError, match="reason"):
        actions.decide(session, req["id"], False, "Bea Approver", "no")
    out = actions.decide(session, req["id"], False, "Bea Approver", "Wrong grain for finance")
    assert out["status"] == "REJECTED"
    assert o.pipeline_dict(session, o._get(session, pid))["status"] == "REJECTED"
    with pytest.raises(actions.ActionError, match="final"):
        actions.decide(session, req["id"], True, "Bea Approver")


def test_approved_deploy_publishes_dataset_lineage_and_run(session):
    pid = _ready(session)
    req = o.propose_deploy(session, pid, "Ana Requester")["action"]
    out = actions.decide(session, req["id"], True, "Bea Approver")
    assert out["status"] == "EXECUTED" and "simulated" in out["result"]
    ds = information.get_dataset(session, "support_csat_by_channel")
    assert ds["origin"] == "pipeline" and ds["row_count"] > 0
    assert "Bea Approver" in ds["owner"]
    up = information.lineage(session, "support_csat_by_channel")["upstream"]
    assert [e["from"] for e in up] == ["support_tickets"]
    runs = information.runs(session, pid)
    assert runs[0]["approved_by"] == "Bea Approver" and runs[0]["rows_written"] == ds["row_count"]
    events = [a.event for a in session.query(AuditRow)]
    assert {"PIPELINE_DRAFTED", "SANDBOX_TEST", "DEPLOY_PROPOSED", "DEPLOY_APPROVED",
            "PIPELINE_DEPLOYED"} <= set(events)


def test_deployed_tables_can_feed_new_pipelines(session):
    pid = _ready(session)
    actions.decide(session, o.propose_deploy(session, pid, "Ana")["action"]["id"], True, "Bea Approver")
    p = o.create(session, {"name": "Low satisfaction", "platform": "fabric",
                           "source": "support_csat_by_channel",
                           "filters": [{"column": "avg_csat", "op": "<", "value": 4}],
                           "columns": ["channel", "priority", "avg_csat"], "target": {"name": "low_csat"}})
    p = o.validate(session, p["id"])
    assert p["validation"]["status"] != "FAILED"
    assert all(r["avg_csat"] < 4 for r in p["validation"]["preview"])


def test_target_name_collisions(session):
    spec = dict(SPEC, target={"name": "orders"})
    p = o.validate(session, o.create(session, spec)["id"])
    assert any(c["id"] == "rule:TARGET_IS_SOURCE" for c in p["validation"]["checks"])


def test_run_log_is_append_only(session):
    pid = _ready(session)
    actions.decide(session, o.propose_deploy(session, pid, "Ana")["action"]["id"], True, "Bea Approver")
    run = session.query(RunRow).first()
    run.rows_written = 0
    with pytest.raises(AppendOnlyViolation):
        session.commit()
    session.rollback()

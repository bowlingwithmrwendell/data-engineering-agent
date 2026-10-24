"""AI governance: review gate, grounding, limiter, kill switch, audit."""
from types import SimpleNamespace

import pytest

from app import agent, config, governance
from app.grounding import is_grounded
from app.models import AICallRow, AppendOnlyViolation


# --- a scripted stand-in for the Anthropic client ----------------------------------
def _text(t):
    return SimpleNamespace(type="text", text=t)


def _tool(i, name, inp):
    return SimpleNamespace(type="tool_use", id=f"tu_{i}", name=name, input=inp)


class FakeClient:
    def __init__(self, turns):
        self.turns = list(turns)
        self.calls = []
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kw):
        self.calls.append(kw)
        content, stop = self.turns.pop(0)
        return SimpleNamespace(content=content, stop_reason=stop,
                               usage=SimpleNamespace(input_tokens=100, output_tokens=50))


SPEC = {"name": "CSAT by channel", "platform": "fabric", "source": "support_tickets",
        "group_by": [{"column": "support_tickets.channel"}],
        "measures": [{"agg": "count", "column": None, "name": "tickets"}],
        "target": {"name": "csat_by_channel"}}


def _happy_client(final_text):
    return FakeClient([
        ([_tool(1, "search_catalog", {"query": "support satisfaction"})], "tool_use"),
        ([_tool(2, "draft_pipeline", {"spec": SPEC})], "tool_use"),
        ([_tool(3, "test_pipeline", {"pipeline_id": 1})], "tool_use"),
        ([_text(final_text)], "end_turn"),
    ])


def test_agent_drafts_tests_and_answer_waits_for_review(session):
    out = agent.ask(session, "How many tickets per channel?", client=_happy_client(
        "Done. Your table has one row per channel. It is pipeline #1."))
    assert [t["tool"] for t in out["tools_used"]] == ["search_catalog", "draft_pipeline", "test_pipeline"]
    assert out["pipeline_ids"] == [1]
    assert out["status"] == "PENDING_REVIEW" and out["validated"] is False
    with pytest.raises(governance.ReviewError):
        governance.published_text(session.get(governance.AIReviewRow, out["review_id"]))


def test_agent_has_no_deploy_or_approve_tool():
    names = {t["name"] for t in agent.TOOLS}
    assert not names & {"deploy", "approve", "approve_deployment", "decide", "run_sql", "update_catalog"}


def test_invented_number_is_flagged_and_cannot_be_approved_unchanged(session):
    out = agent.ask(session, "Tickets per channel?", client=_happy_client(
        "There were 9999 tickets on chat."))
    assert out["grounded"] is False and "9999" in out["unverified_numbers"]
    with pytest.raises(governance.ReviewError, match="no engine produced|Edit them out"):
        governance.decide_review(session, out["review_id"], "approve", "Bea Approver")
    with pytest.raises(governance.ReviewError, match="still contains"):
        governance.decide_review(session, out["review_id"], "edit", "Bea Approver",
                                 edited_output="About 9999 tickets.")
    r = governance.decide_review(session, out["review_id"], "edit", "Bea Approver",
                                 edited_output="Ticket counts per channel are in the table.")
    assert r["status"] == "APPROVED_WITH_EDITS"


def test_ai_cannot_review_its_own_output(session):
    out = agent.ask(session, "Tickets per channel?", client=_happy_client("Done."))
    for who in ["Claude", "Claude (AI)", "agent", "Review Bot"]:
        with pytest.raises(governance.ReviewError):
            governance.decide_review(session, out["review_id"], "approve", who)


def test_grounding_allows_tool_numbers_and_list_markers():
    ok, unknown = is_grounded("1. Revenue was 1,234.50 in 2026", {"facts": [{"revenue": 1234.5}],
                                                                   "question": "2026"})
    assert ok and not unknown


def test_rate_limit_fails_closed_and_is_logged(session, monkeypatch):
    monkeypatch.setattr(config, "RATE_LIMIT_PER_MINUTE", 1)
    agent.ask(session, "q1", client=FakeClient([([_text("ok")], "end_turn")]), visitor="v-1")
    with pytest.raises(governance.AIBlocked) as e:
        agent.ask(session, "q2", client=FakeClient([([_text("ok")], "end_turn")]), visitor="v-1")
    assert e.value.status_code == 429
    assert session.query(AICallRow).filter(AICallRow.outcome == "RATE_LIMITED").count() == 1


def test_kill_switch_blocks_model_but_not_builder(session, client):
    governance.set_kill_switch(session, True, "Wendell Killette", "Testing the switch")
    fake = FakeClient([([_text("never")], "end_turn")])
    with pytest.raises(governance.AIBlocked):
        agent.ask(session, "anything", client=fake)
    assert fake.calls == []                                       # the model was never called
    r = client.post("/api/pipelines", json={"spec": SPEC})
    assert r.status_code == 201
    assert client.post(f"/api/pipelines/{r.json()['id']}/test").json()["status"] == "VALIDATED"


def test_tool_errors_are_reported_to_the_model_not_swallowed(session):
    fake = FakeClient([
        ([_tool(1, "request_deployment", {"pipeline_id": 999})], "tool_use"),
        ([_text("That pipeline doesn't exist.")], "end_turn"),
    ])
    out = agent.ask(session, "deploy 999", client=fake)
    assert out["tools_used"][0]["error"] is True
    sent = fake.calls[1]["messages"][-1]["content"][0]
    assert sent["is_error"] is True and "No pipeline #999" in sent["content"]


def test_ai_call_log_is_append_only(session):
    agent.ask(session, "q", client=FakeClient([([_text("ok")], "end_turn")]))
    row = session.query(AICallRow).first()
    row.outcome = "OK-edited"
    with pytest.raises(AppendOnlyViolation):
        session.commit()
    session.rollback()

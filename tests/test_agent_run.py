"""Pooled tasks as ArtifactBridge agent runs (AI-6656): the brief and tool list read with the task's dlc_, request.json
v1, and ``omp-run start --agent`` / ``wait`` / ``stop`` against a stand-in omp-run executable (tests/fake_omp_run.py)
and the in-process fake AB boundary."""
import logging
import os
import tempfile
import threading

import pytest

from hermes_artifactbridge.agent_run import AgentRuns, Outcome
from hermes_artifactbridge.pool_core import CANCELED, COMPLETED, FAILED
from tests import fake_ab
from tests.fake_omp_run import UNIT, FakeOmpRun, completed
from tests.fake_pool import envelope

ENVELOPE = envelope()
BINDING = ENVELOPE["binding"]
DLC = "dlc_" + "d" * 43
UNCONFIRMED = ("The task ended, but we couldn't confirm that its question was posted in the Room. The question is "
               "included below. Send the task again as a new request with your answer and the context it needs.")
ROOM_MESSAGE = "This workspace has no runner credit left this month. Ask a workspace owner to add credit."
# A structured question with no prompt/question string: sorted JSON, cut to the 20,000-code-point limit.
STRUCTURED = {"options": ["é" * 25_000], "kind": "choice", "prompt": " "}
STRUCTURED_TEXT = ('{"kind": "choice", "options": ["' + "\\u00e9" * 25_000)[:20_000]


@pytest.fixture
def delegation(ab):
    ab.delegations[BINDING["delegationId"]] = d = fake_ab.Delegation(id=BINDING["delegationId"])
    ab.pooled_credentials[DLC] = {"delegation_id": d.id}
    return d


@pytest.fixture
def omp(tmp_path):
    return FakeOmpRun(tmp_path)


@pytest.fixture
def runs(app, omp):
    return AgentRuns(tools_url=fake_ab.TOOLS_URL, binary=omp.binary, http_factory=fake_ab.http_factory(app))


def test_a_started_run_gets_exactly_the_twelve_key_owner_only_request_deleted_after_start(runs, omp, ab, delegation,
                                                                                         caplog):
    caplog.set_level(logging.DEBUG)
    assert runs.start(ENVELOPE, DLC) == UNIT
    call, = omp.calls
    assert call["argv"] == ["start", "--agent", BINDING["serviceId"], "--request", call["path"]]
    assert (call["mode"], call["regular"], call["owner"]) == (0o600, True, True)
    assert os.path.isabs(call["path"]) and not os.path.exists(call["path"])
    assert call["request"] == {
        "omp_agent_request": 1, "kind": "room_task", "workspace_id": BINDING["workspaceId"],
        "agent_id": BINDING["serviceId"], "delegation_id": BINDING["delegationId"],
        "execution_generation": BINDING["executionGeneration"], "assignment_id": BINDING["assignmentId"],
        "dispatch_id": ENVELOPE["dispatchId"], "start_by": ENVELOPE["startBy"],
        "brief": fake_ab.room_context(delegation), "tools": ab.catalog(), "conversation": None}
    # Both reads present only the task's dlc_; neither the request nor the brief reaches the logs.
    assert ab.tool_calls == [("artifactbridge_read_room_context", {})]
    assert {(r["authorization"], r["token"]) for r in ab.requests} == {(f"Bearer {DLC}", "")}
    assert "Draft the landing page" not in caplog.text and DLC not in caplog.text


@pytest.mark.parametrize("answer, outcome", [
    (completed("The page is drafted."), Outcome(COMPLETED, "The page is drafted.")),
    ({**completed(), "outcome": "question", "question": " Which tone? ", "result_text": None},
     Outcome(FAILED, f"{UNCONFIRMED}\n\nWhich tone?", {"reason": "clarification_unsupported", "question": "Which tone?"})),
    ({**completed(), "outcome": "question", "question": {"prompt": "Which tone?", "options": ["warm", "formal"]},
      "result_text": None},
     Outcome(FAILED, f"{UNCONFIRMED}\n\nWhich tone?", {"reason": "clarification_unsupported", "question": "Which tone?"})),
    ({**completed(), "outcome": "question", "question": STRUCTURED, "result_text": None},
     Outcome(FAILED, f"{UNCONFIRMED}\n\n{STRUCTURED_TEXT}",
             {"reason": "clarification_unsupported", "question": STRUCTURED_TEXT})),
    ({**completed(), "outcome": "failed", "error_code": "outcome_too_long", "result_text": None},
     Outcome(FAILED, "outcome_too_long")),
    ({"unit": UNIT, "active": True, "timeout": True}, Outcome(FAILED, "timeout", timed_out=True)),
    ({"unit": UNIT, "active": False, "result": "unknown_unit"}, Outcome(FAILED, "unknown_unit")),
    ({"unit": UNIT, "active": False, "outcome": "result_unreadable"}, Outcome(FAILED, "result_unreadable")),
    ({**completed(), "result_text": ""}, Outcome(FAILED, "omp_run_wait_failed")),
], ids=["completed", "question", "question-prompt", "question-json", "failed", "timeout", "unknown-unit", "unreadable",
        "empty-completed"])
def test_the_wait_line_maps_to_the_task_outcome(runs, omp, answer, outcome):
    omp.set(wait={"exit": 0 if answer.get("outcome") in ("completed", "question") else 1, "answer": answer})
    assert runs.wait(UNIT) == outcome
    assert omp.argvs("wait") == [["wait", "--unit", UNIT]]


@pytest.mark.parametrize("code", ["draining", "request_expired", "launch_failed"])
def test_a_start_refusal_fails_the_task_with_its_code_and_the_request_is_still_deleted(runs, omp, delegation, code):
    omp.set(start={"exit": 2, "answer": {"ok": False, "code": code, "detail": "refused"}})
    assert runs.start(ENVELOPE, DLC) == Outcome(FAILED, code)
    call, = omp.calls
    assert not os.path.exists(call["path"])


def test_already_running_reuses_the_live_unit(runs, omp, delegation):
    omp.set(start={"exit": 2, "answer": {"ok": False, "code": "already_running",
                                         "detail": "a run container for this issue is alive", "unit": UNIT}})
    assert runs.start(ENVELOPE, DLC) == UNIT
    assert runs.wait(UNIT) == Outcome(COMPLETED, "done")


def test_already_running_without_a_unit_fails_rather_than_guessing_one(runs, omp, delegation):
    omp.set(start={"exit": 2, "answer": {"ok": False, "code": "already_running",
                                         "detail": "another start of this delegation is in progress"}})
    assert runs.start(ENVELOPE, DLC) == Outcome(FAILED, "already_running")


REFUSAL = {"ok": False, "code": "runner_credential_refused", "detail": "the runner credential service refused this run",
           "unit": UNIT, "status": "credit_exhausted"}


@pytest.mark.parametrize("answer, text", [
    ({**REFUSAL, "room_message": ROOM_MESSAGE}, ROOM_MESSAGE),
    (REFUSAL, "runner_credential_refused"),
], ids=["room-message", "no-room-message"])
def test_a_token_service_refusal_fails_with_its_room_message_and_starts_nothing(runs, omp, delegation, answer, text):
    omp.set(start={"exit": 2, "answer": answer})
    assert runs.start(ENVELOPE, DLC) == Outcome(FAILED, text)  # never the refusal's unit
    assert [c["argv"][0] for c in omp.calls] == ["start"]


def test_a_missing_binary_fails_closed(app, tmp_path, delegation, monkeypatch):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    runs = AgentRuns(tools_url=fake_ab.TOOLS_URL, binary=str(tmp_path / "absent" / "omp-run"),
                     http_factory=fake_ab.http_factory(app))
    assert runs.start(ENVELOPE, DLC) == Outcome(FAILED, "omp_run_unavailable")
    assert runs.wait(UNIT) == Outcome(FAILED, "omp_run_unavailable")
    assert runs.stop(UNIT) is False
    assert os.listdir(tmp_path) == []  # the request written for the failed start is gone too


def test_a_refused_room_context_read_starts_nothing(runs, omp, ab, delegation):
    ab.pooled_credentials[DLC]["expired"] = True
    assert runs.start(ENVELOPE, DLC) == Outcome(FAILED, "room_context_unavailable")
    assert omp.calls == []


def test_stop_ends_a_waiting_run_as_canceled(runs, omp):
    omp.set(wait={"exit": 0, "answer": completed(), "block": True})
    outcome = []
    waiter = threading.Thread(target=lambda: outcome.append(runs.wait(UNIT)))
    waiter.start()
    while not omp.argvs("wait"):
        threading.Event().wait(0.02)
    assert runs.stop(UNIT) is True
    waiter.join(10)
    assert outcome == [Outcome(CANCELED, "")] and omp.argvs("stop") == [["stop", UNIT]]


def test_an_unconfirmed_stop_leaves_the_run_to_finish(runs, omp):
    omp.set(stop={"exit": 1}, wait={"exit": 0, "answer": completed()})
    assert runs.stop(UNIT) is False
    assert runs.wait(UNIT) == Outcome(COMPLETED, "done")


def test_a_stop_before_the_wait_began_cancels_without_waiting(runs, omp):
    assert runs.stop(UNIT) is True
    assert runs.wait(UNIT) == Outcome(CANCELED, "") and omp.argvs("wait") == []

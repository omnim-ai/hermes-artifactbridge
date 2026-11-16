"""Pooled (assignment-bound) execution core, without Hermes: envelope strictness, the durable journal,
start ownership, permit checks, restart recovery, sessions, cancel/report fencing, durable report
delivery, and the control client's status mapping against the server contract
(cloud-contract-r2-terminal-clarification-1)."""
import hashlib
import json
import logging
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import httpx2
import pytest

from hermes_artifactbridge import pool_protocol
from hermes_artifactbridge.pool_core import (COMPLETED, FAILED, POOL_PLATFORM, REJECTED,
                                             ControlUnavailable, CredentialBindings, PoolControl, PoolCore)
from hermes_artifactbridge.pool_journal import Journal
from hermes_artifactbridge.pool_protocol import (IMPLEMENTED_CAPABILITIES, Refused, envelope_from_metadata, report_body, start_request,
                                                 validate_binding, validate_envelope)
from tests import fake_pool
from tests.fake_pool import (BOOTSTRAP_PATH, EXECUTOR_ID, REPORT_PATH, REVISION, START_PATH, Clock, FakeControl,
                             envelope, fixture, grant_for)

CTX = "ctx-original"


def uid(n: int) -> str:
    return f"6f1f7d9e-0a1b-4c2d-8e3f-{n:012d}"


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def server(clock):
    return FakeControl(clock=clock)


@pytest.fixture
def journal_path(tmp_path):
    return tmp_path / "artifactbridge" / "pool-journal.sqlite3"


@pytest.fixture
def make_core(server, clock, journal_path):
    def make(credentials=None):
        return PoolCore(journal=Journal(journal_path),
                        control=PoolControl(tools_url=fake_pool.TOOLS_URL, http_factory=server.factory),
                        executor_id=EXECUTOR_ID, connection_revision=REVISION,
                        credentials=credentials or CredentialBindings(now=clock), now=clock)
    return make


@pytest.fixture
def core(make_core):
    return make_core()


def admit(core, metadata, context_id, task_id, **kwargs):
    """``core.admit`` with the start headers and ``message.messageId`` (= dispatchId) the server sends for the
    metadata's assignment."""
    env = metadata.get(pool_protocol.METADATA_KEY) if isinstance(metadata, dict) else None
    try:
        grant, message_id = grant_for(env), env["dispatchId"]
    except (TypeError, KeyError, AttributeError):
        grant, message_id = grant_for(envelope()), envelope()["dispatchId"]
    return core.admit(metadata, context_id, task_id, **{"grant": grant, "message_id": message_id, **kwargs})


def rows(path):
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute("SELECT * FROM dispatches ORDER BY updated_at")]


def continuation(n: int, generation: int = 2, dispatch: str = "dispatch-2") -> dict:
    return envelope(binding={"assignmentId": uid(n), "executionGeneration": generation}, dispatchId=dispatch)


def pending(path) -> list[list[tuple[int, str]]]:
    """Per journal row: the (reportSequence, kind) of each report not yet acknowledged."""
    return [[(b["reportSequence"], b["kind"]) for b in json.loads(r["pending_reports"] or "[]")] for r in rows(path)]


def sent(bodies) -> list[tuple[int, str]]:
    return [(b["reportSequence"], b["kind"]) for b in bodies]


def report_requests(server) -> list[int]:
    return [r["body"]["reportSequence"] for r in server.requests if r["path"] == REPORT_PATH]


# ---- fixtures and protocol shapes ----

def test_every_fixture_parses_through_the_plugins_own_parsers():
    p = pool_protocol
    send = fixture("cloud-send-request.json")
    env = envelope_from_metadata(send["body"]["params"]["metadata"])
    journal = send["headers"]["x-ab-start-journal-id"]
    assert p.start_journal_id(env["binding"]["assignmentId"]) == journal
    assert p.pool_grant(lambda name: [v for k, v in send["headers"].items() if k == name.lower()]) == (
        send["headers"]["x-ab-control-credential"], journal)
    assert start_request(env, journal) == fixture("cloud-start-replay-request.json")
    assert p.bootstrap_request(env, journal) == fixture("cloud-bootstrap-request.json")
    assert p.parse_start_response(fixture("cloud-start-replay-permit.json")) == ("permit", {**env, "startJournalId": journal})
    assert p.parse_start_response(fixture("cloud-start-owned-refusal.json")) == ("refused", "start_owned")
    assert p.parse_start_response(fixture("cloud-start-not-authorized-refusal.json")) == ("refused", "start_not_authorized")
    # cloud-continuation-*: historical generation-2 negative shapes; they parse, but are never a start capability.
    cont = fixture("cloud-continuation-request.json")
    cont_env = envelope_from_metadata(cont["body"]["params"]["metadata"])
    cont_journal = cont["headers"]["x-ab-start-journal-id"]
    assert cont_env == fixture("cloud-continuation-envelope.json")
    assert p.start_journal_id(cont_env["binding"]["assignmentId"]) == cont_journal
    assert start_request(cont_env, cont_journal) == fixture("cloud-continuation-start-request.json")
    assert p.bootstrap_request(cont_env, cont_journal) == fixture("cloud-continuation-bootstrap-request.json")
    for name in ("cloud-continuation-start-permit.json", "cloud-continuation-start-replay-permit.json"):
        assert p.parse_start_response(fixture(name)) == ("permit", {**cont_env, "startJournalId": cont_journal})
    # A connection edit moves the revision: a well-formed envelope this executor's configuration refuses.
    assert validate_envelope(fixture("configuration-revision-envelope.json"))["binding"]["executorConnectionRevision"] == "2"
    credential, expires_at = p.parse_bootstrap_response(fixture("bootstrap-response.json"))
    assert credential == fixture("bootstrap-response.json")["credential"] and expires_at.isoformat() == "2026-10-17T11:15:00+00:00"
    capability = fixture("cloud-capability-request.json")["body"]
    assert capability["method"] == p.CAPABILITY_METHOD
    assert capability["params"]["requiredCapabilities"] == list(IMPLEMENTED_CAPABILITIES)
    assert p.capability_answer(capability["params"], executor_id=EXECUTOR_ID, connection_revision=REVISION,
                               runtime_version="0.0.0-fixture") == fixture("cloud-capability-result.json")["result"]
    # Historical input (tests/fixtures/cloud_r2): the old seven-capability probe also requires the withdrawn
    # same_session_continuation, which this runner does not implement, so it is refused.
    old_seven = json.loads((fake_pool.FIXTURES.parent / "cloud_r2" / "cloud-capability-request.json").read_text())
    with pytest.raises(Refused, match="capability_mismatch"):
        p.capability_answer(old_seven["body"]["params"], executor_id=EXECUTOR_ID, connection_revision=REVISION,
                            runtime_version="0.0.0-fixture")
    # Legacy (integration-u3-r1) shapes, still generated: the same tuple; report shapes are unchanged.
    legacy = envelope_from_metadata(fixture("dispatch-metadata.json"))
    assert legacy == env == validate_envelope(fixture("mailbox-assignment-command.json")["envelope"])
    assert start_request(env, "journal-1") == fixture("start-request.json")
    assert p.bootstrap_request(env, "journal-1") == fixture("bootstrap-request.json")
    permit = {**env, "startJournalId": "journal-1"}
    assert p.parse_start_response(fixture("start-permit.json")) == ("permit", permit)
    # A tray start's controlCredential parses but is never returned, so it is never adopted or stored.
    assert p.parse_start_response(fixture("start-permit-with-control-credential.json")) == ("permit", permit)
    assert p.parse_start_response(fixture("start-owned-refusal.json")) == ("refused", "start_owned")
    assert p.parse_start_response(fixture("start-deadline-refusal.json")) == ("refused", "start_deadline_passed")
    assert report_body(env["binding"], 1, "stopped", "completed") == fixture("report.json")
    assert report_body(env["binding"], 2, "cancel_acknowledged", "canceled") == fixture("cancel-ack.json")
    assert p.parse_report_response(fixture("report-response.json")) == (True, True)
    assert p.parse_report_response(fixture("report-replay-response.json")) == (False, True)
    assert p.parse_error(fixture("refusal-response.json")) == ("assignment_tuple_mismatch", False)
    assert p.parse_error(fixture("unauthorized-response.json")) == ("unauthorized", False)
    assert p.parse_error(fixture("scheduler-busy-response.json")) == ("scheduler_busy", True)
    # Tray-only (mailbox tombstone, decline): a cloud executor neither sends nor reads them; their tuple parses.
    for name in ("mailbox-withdrawn-tombstone.json", "decline-request.json"):
        assert validate_binding(fixture(name)["binding"]) == env["binding"]
    covered = {
        "cloud-send-request.json", "cloud-start-replay-request.json", "cloud-bootstrap-request.json",
        "cloud-start-replay-permit.json", "cloud-start-owned-refusal.json", "cloud-start-not-authorized-refusal.json",
        "cloud-continuation-request.json", "cloud-continuation-envelope.json", "cloud-continuation-start-request.json",
        "cloud-continuation-bootstrap-request.json", "cloud-continuation-start-permit.json",
        "cloud-continuation-start-replay-permit.json", "configuration-revision-envelope.json", "bootstrap-response.json",
        "cloud-capability-request.json", "cloud-capability-result.json", "dispatch-metadata.json",
        "mailbox-assignment-command.json", "start-request.json", "bootstrap-request.json", "start-permit.json",
        "start-permit-with-control-credential.json", "start-owned-refusal.json", "start-deadline-refusal.json",
        "report.json", "cancel-ack.json", "report-response.json", "report-replay-response.json",
        "refusal-response.json", "unauthorized-response.json", "scheduler-busy-response.json",
        "mailbox-withdrawn-tombstone.json", "decline-request.json", "decline-response.json"}
    # Driven against the real listener in tests/test_pool_adapter.py.
    listener = {"cloud-send-response.json", "cloud-continuation-response.json", "cloud-get-task-request.json",
                "cloud-list-tasks-request.json", "cloud-cancel-request.json", "cloud-card-request.json",
                "cloud-capability-error-method-not-found.json", "cloud-terminal-clarification-task.json",
                "cloud-terminal-clarification-null-question-task.json", "cloud-terminal-continuation-task.json",
                "cloud-capability-verdict-old-seven.json", "cloud-capability-verdict-unknown-capability.json",
                "cloud-capability-verdict-duplicate-capability.json", "cloud-capability-verdict-missing-capability.json",
                "cloud-capability-verdict-wrong-nonce.json", "cloud-capability-verdict-wrong-protocol.json"}
    # Server-side only: the server's own probe record, SQL row schema examples (not runtime receipts),
    # connection API responses, the retired tray capability routes and the U2 revision fence.
    server_only = {"cloud-capability-proof-record.json", "cloud-transport-row.json",
                   "cloud-continuation-transport-row.json", "cloud-prepare-receipt-row.json",
                   "cloud-connections-due-check-row.json", "cloud-connection-response.json",
                   "cloud-probe-failed-response.json", "capability-challenge.json", "capability-proof.json",
                   "capability-response.json", "capability-refusal-response.json",
                   "capability-probe-required-response.json", "join-capability-probe-required-response.json",
                   "connection-changed-response.json", "manifest.json"}
    assert covered | listener | server_only == {f.name for f in fake_pool.FIXTURES.glob("*.json")}


ROOM = "room_space_context_v1"
PROTOCOL2 = Path(__file__).parent / "fixtures" / "runtime_protocol2"


def _answer(params: dict, **kwargs) -> dict:
    return pool_protocol.capability_answer(params, **{"executor_id": EXECUTOR_ID, "connection_revision": REVISION,
                                                      "runtime_version": "r", **kwargs})


def test_runtime_capability_protocol_2_answers_exactly_the_seven_and_protocol_1_stays_the_six():
    v1 = fixture("cloud-capability-request.json")["body"]["params"]
    six, seven = list(IMPLEMENTED_CAPABILITIES), [*IMPLEMENTED_CAPABILITIES, ROOM]
    assert pool_protocol.PROTOCOL_VERSION == 1  # the assignment protocol is not the runtime capability protocol
    assert pool_protocol.RUNTIME_PROTOCOL_CAPABILITIES == {1: tuple(six), 2: tuple(seven)}
    v2 = {**v1, "protocolVersion": 2, "requiredCapabilities": seven[::-1]}
    assert _answer(v2) == {"protocolVersion": 2, "executorId": EXECUTOR_ID, "executorConnectionRevision": REVISION,
                           "nonce": v1["nonce"], "capabilities": seven, "runtimeVersion": "r"}
    assert _answer({**v1, "requiredCapabilities": six[:1]}) == {**_answer(v1), "protocolVersion": 1, "capabilities": six}
    for required in ([*six, ROOM], [ROOM]):  # protocol 1 never claims the room capability
        with pytest.raises(Refused, match="capability_mismatch"):
            _answer({**v1, "requiredCapabilities": required})
    for change in ({"requiredCapabilities": six}, {"requiredCapabilities": seven[1:]},
                   {"requiredCapabilities": [*seven, seven[0]]}, {"requiredCapabilities": [*seven[:-1], six[0]]},
                   {"requiredCapabilities": [*seven, "same_session_continuation"]},
                   {"requiredCapabilities": [*six, "same_session_continuation"]}, {"requiredCapabilities": []},
                   {"requiredCapabilities": [*six, {"name": ROOM}]}, {"requiredCapabilities": tuple(seven)},
                   {"protocolVersion": 3}, {"protocolVersion": 0}, {"protocolVersion": "2"}, {"protocolVersion": True},
                   {"protocolVersion": 2.0}, {"protocolVersion": [2]}, {"executorId": uid(42)},
                   {"executorConnectionRevision": "2"}, {"nonce": "N" * 31}, {"nonce": "N" * 129}):
        with pytest.raises(Refused, match="capability_mismatch"):
            _answer({**v2, **change})
    for params in (_without(v2, "nonce"), {**v2, "extra": 1}, None, [v2]):
        with pytest.raises(Refused, match="capability_mismatch"):
            _answer(params)


def test_runtime_protocol_2_fixtures_match_the_plugin_sets_answer_and_recorded_hashes():
    server = json.loads((PROTOCOL2 / "plugin-fixtures.json").read_text())
    for version, count in (("1", 6), ("2", 7)):
        exact, ours = server["exactSets"]["cloud"][version], pool_protocol.RUNTIME_PROTOCOL_CAPABILITIES[int(version)]
        assert set(exact) == set(ours) and len(exact) == len(set(exact)) == len(ours) == count
    challenge = server["cloudV2Challenge"]
    request = json.loads((PROTOCOL2 / "capability-v2-request.json").read_text())["body"]
    assert request["method"] == pool_protocol.CAPABILITY_METHOD and request["params"] == {
        "protocolVersion": 2, "executorId": challenge["executorId"],
        "executorConnectionRevision": challenge["executorConnectionRevision"], "nonce": challenge["nonce"],
        "requiredCapabilities": server["exactSets"]["cloud"]["2"]}
    result = json.loads((PROTOCOL2 / "capability-v2-result.json").read_text())
    answered = _answer(request["params"], executor_id=challenge["executorId"],
                       connection_revision=challenge["executorConnectionRevision"], runtime_version="0.0.0-fixture")
    assert result == {"jsonrpc": "2.0", "id": request["id"], "result": answered}
    assert answered == {**server["cloudV2Answer"], "runtimeVersion": "0.0.0-fixture"}
    recorded = dict(re.findall(r"^\| `([^`]+)` \| `([0-9a-f]{64})` \|$", (PROTOCOL2 / "SOURCE.md").read_text(), re.M))
    assert recorded == {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in PROTOCOL2.glob("*.json")}


def _without(d: dict, key: str) -> dict:
    return {k: v for k, v in d.items() if k != key}


@pytest.mark.parametrize("metadata, code", [
    (None, "missing_assignment"),
    ({}, "missing_assignment"),
    ({"other": 1}, "missing_assignment"),
    (fake_pool.metadata({**envelope(), "protocolVersion": 2}), "unsupported_protocol_version"),
    (fake_pool.metadata({**envelope(), "delegationToken": "dlt_" + "a" * 43}), "invalid_envelope"),
    (fake_pool.metadata(_without(envelope(), "startBy")), "invalid_envelope"),
    (fake_pool.metadata(envelope(startBy="2026-10-17T12:00:00")), "invalid_envelope"),
    (fake_pool.metadata(envelope(dispatchId="")), "invalid_envelope"),
    (fake_pool.metadata(envelope(commandSequence=0)), "invalid_envelope"),
    (fake_pool.metadata(envelope(binding={"extra": "x"})), "invalid_binding"),
    (fake_pool.metadata(envelope(binding={"reservationGeneration": 1})), "invalid_binding"),
    (fake_pool.metadata(envelope(binding={"reservationGeneration": "9223372036854775808"})), "invalid_binding"),
    (fake_pool.metadata(envelope(binding={"executorConnectionRevision": "01"})), "invalid_binding"),
    (fake_pool.metadata(envelope(binding={"slotId": "not-a-uuid"})), "invalid_binding"),
    (fake_pool.metadata(envelope(binding={"executionGeneration": True})), "invalid_binding"),
], ids=lambda v: v if isinstance(v, str) else None)
def test_malformed_metadata_is_rejected_before_journal_or_network(core, server, journal_path, metadata, code):
    decision = admit(core, metadata, CTX, "task-1")
    assert (decision.kind, decision.outcome) == ("terminal", REJECTED) and code in decision.text
    assert rows(journal_path) == [] and server.requests == []


@pytest.mark.parametrize("sequence", [2, 3, True, 1.0, "1"])
def test_an_assignment_or_permit_whose_command_sequence_is_not_the_literal_1_is_refused(core, server, journal_path,
                                                                                         sequence):
    decision = admit(core, server.issue(envelope(commandSequence=sequence)), CTX, "task-1")
    assert decision.outcome == REJECTED and "invalid_envelope" in decision.text
    assert rows(journal_path) == [] and server.requests == []
    permit = {**fixture("cloud-start-replay-permit.json")["permit"], "commandSequence": sequence}
    assert pool_protocol.parse_start_response({"outcome": "permit", "permit": permit}) is None


def test_the_sequence_2_mailbox_tombstone_parses_as_its_own_shape_and_is_never_admitted(core, journal_path):
    tombstone = fixture("mailbox-withdrawn-tombstone.json")
    assert tombstone["commandSequence"] == 2 and validate_binding(tombstone["binding"]) == envelope()["binding"]
    assert validate_envelope({**envelope(), "commandSequence": 1}) == envelope()
    as_assignment = {k: v for k, v in {**envelope(), **tombstone}.items() if k != "kind"}
    decision = admit(core, fake_pool.metadata(as_assignment), CTX, "task-1")
    assert decision.outcome == REJECTED and "invalid_envelope" in decision.text and rows(journal_path) == []


@pytest.mark.parametrize("message_id", [None, "", "dispatch-2", "DISPATCH-1", " dispatch-1", 1])
def test_a_send_whose_message_id_is_not_exactly_the_dispatch_id_is_refused_before_any_control_call(
        core, server, journal_path, message_id):
    decision = admit(core, server.issue(envelope()), CTX, "task-1", message_id=message_id)
    assert decision.outcome == REJECTED and "message_id_mismatch" in decision.text
    assert rows(journal_path) == [] and server.requests == []


def test_the_message_id_is_journaled_with_the_context_and_task_and_an_altered_duplicate_is_refused(
        core, server, journal_path):
    metadata = server.issue(envelope())
    admit(core, metadata, CTX, "task-1")
    [row] = rows(journal_path)
    assert (row["message_id"], row["context_id"], row["task_id"]) == ("dispatch-1", CTX, "task-1")
    before = len(server.requests)
    decision = admit(core, metadata, CTX, "task-2", message_id="dispatch-1-altered")
    assert decision.outcome == REJECTED and "message_id_mismatch" in decision.text
    assert len(server.requests) == before and rows(journal_path) == [row]


@pytest.mark.parametrize("binding", [{"executorId": uid(99)}, {"executorConnectionRevision": "2"}])
def test_an_envelope_for_another_executor_is_refused_before_journaling(core, server, journal_path, binding):
    decision = admit(core, server.issue(envelope(binding=binding)), CTX, "task-1")
    assert decision.outcome == REJECTED and "executor_mismatch" in decision.text
    assert rows(journal_path) == [] and server.requests == []


# ---- start, bootstrap, reports ----

def test_permitted_dispatch_spawns_once_and_reports_running_then_stopped(core, server, journal_path):
    metadata = server.issue(envelope())
    decision = admit(core, metadata, CTX, "task-1")
    assert (decision.kind, decision.context_id, decision.task_id) == ("spawn", CTX, "task-1")
    # The server's journal from the start headers, never one the listener made up.
    assert rows(journal_path)[0]["start_journal_id"] == fixture("cloud-send-request.json")["headers"][
        "x-ab-start-journal-id"]
    assert server.starts == [fixture("cloud-start-replay-request.json")]
    assert server.bootstraps == [fixture("cloud-bootstrap-request.json")]
    credential = core.credentials.lookup(POOL_PLATFORM, CTX, "task-1")
    assert credential in server.issued and server.issued[credential] == envelope()["binding"]
    assert server.reports == [] and rows(journal_path)[0]["state"] == "spawned"  # spawn intent is not Running
    assert core.started("task-1") is True and core.started("task-1") is False
    assert server.reports == [report_body(envelope()["binding"], 1, "running", None)]

    core.finish("task-1", COMPLETED, "done")
    assert server.reports[-1] == {**fixture("report.json"), "reportSequence": 2}
    assert core.credentials.lookup(POOL_PLATFORM, CTX, "task-1") is None

    replay = admit(core, metadata, CTX, "task-2")  # lost response: the same recorded result, no second run
    assert (replay.kind, replay.outcome, replay.text) == ("terminal", COMPLETED, "done")
    assert len(server.starts) == 1 and len(server.bootstraps) == 1 and len(server.reports) == 2


@pytest.mark.parametrize("state, outcome", [(COMPLETED, "completed"), (FAILED, "failed")])
def test_turn_outcomes_map_to_report_outcomes(core, server, state, outcome):
    admit(core, server.issue(envelope()), CTX, "task-1")
    core.finish("task-1", state, "x")
    assert server.reports[-1]["kind"] == "stopped" and server.reports[-1]["outcome"] == outcome


ASKED = {"reason": "clarification_unsupported", "question": "Which tone?"}
CONTINUED = {"reason": "continuation_unsupported"}


def test_a_cloud_terminal_commits_with_its_failed_result_and_stopped_report_and_the_first_result_wins(
        core, server, journal_path):
    server.faults[REPORT_PATH] = ["down", "down"]  # running, then its retry before stopped: both stay pending
    metadata = server.issue(envelope())
    admit(core, metadata, CTX, "task-1")
    core.started("task-1")
    other = {"reason": "clarification_unsupported", "question": "Which colour?"}
    callbacks = [("asked", ASKED), ("asked", ASKED), ("other", other)]  # identical retries and a conflicting one
    barrier, wins = threading.Barrier(len(callbacks)), []

    def finish(text, terminal):
        barrier.wait()
        wins.append((core.finish("task-1", FAILED, text, terminal=terminal), text))

    threads = [threading.Thread(target=finish, args=c) for c in callbacks]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert core.finish("task-1", FAILED, "later", terminal=other) is False  # a late callback after the terminal
    row = rows(journal_path)[0]
    won = json.loads(row["cloud_terminal"])
    assert (won, row["result_text"]) in ((ASKED, "asked"), (other, "other"))  # one callback's pair, never mixed
    assert [text for ended, text in wins if ended] == [row["result_text"]]  # only the winner learns it won
    assert (row["state"], row["result_state"]) == ("finished", FAILED)
    assert pending(journal_path) == [[(1, "running"), (2, "stopped")]]  # one stopped report, in the same commit
    assert json.loads(row["pending_reports"])[1] == report_body(envelope()["binding"], 2, "stopped", "failed")
    decision = admit(core, metadata, CTX, "task-2")  # the lost Send response: the journaled terminal answers
    assert (decision.outcome, decision.text, decision.terminal) == (FAILED, row["result_text"], won)
    assert sent(server.reports) == [(1, "running"), (2, "stopped")] and pending(journal_path) == [[]]


def test_a_crash_after_the_terminal_commit_answers_it_after_restart_and_replays_the_report(make_core, server,
                                                                                         journal_path):
    server.faults[REPORT_PATH] = ["down", "down"]  # the process dies before the stopped report is acknowledged
    metadata = server.issue(envelope())
    first = make_core()
    admit(first, metadata, CTX, "task-1")
    first.started("task-1")
    first.finish("task-1", FAILED, "asked", terminal=ASKED)
    restarted = make_core()
    restarted.recover()  # a finished row is no lost turn: never uncertain, never a crash report
    assert rows(journal_path)[0]["state"] == "finished" and server.reports == []
    row = restarted.recovery_row(grant_for(envelope()))  # the server's authenticated Get restores the agx_
    view = restarted.projection(row)
    assert (view.outcome, view.text, view.terminal) == (FAILED, "asked", ASKED)
    assert server.reports == [report_body(envelope()["binding"], 1, "running", None),
                              report_body(envelope()["binding"], 2, "stopped", "failed")]


def test_a_lost_stopped_ack_is_replayed_in_order_and_never_reported_twice(core, server, journal_path):
    admit(core, server.issue(envelope()), CTX, "task-1")
    core.started("task-1")
    server.faults[REPORT_PATH] = ["lost"]  # the server records the stopped report; its response is lost
    core.finish("task-1", FAILED, "asked", terminal=ASKED)
    assert sent(server.reports) == [(1, "running"), (2, "stopped")] and pending(journal_path) == [[(2, "stopped")]]
    core.deliver_reports()
    assert sent(server.replays) == [(2, "stopped")] and len(server.reports) == 2 and pending(journal_path) == [[]]
    assert json.loads(rows(journal_path)[0]["cloud_terminal"]) == ASKED


def test_a_turn_the_runtime_never_answered_reports_timed_out_never_a_confirmed_failure(core, server, journal_path):
    admit(core, server.issue(envelope()), CTX, "task-1")
    core.started("task-1")
    core.finish("task-1", FAILED, "[agent did not reply in time]", timed_out=True)
    assert server.reports[-1] == report_body(envelope()["binding"], 2, "stopped", "timed_out")
    assert rows(journal_path)[0]["cloud_terminal"] is None


def test_the_journal_row_is_committed_before_the_start_request(core, server, journal_path):
    seen = []
    server.on_start = lambda body: seen.append([(r["state"], r["start_journal_id"]) for r in rows(journal_path)])
    admit(core, server.issue(envelope()), CTX, "task-1")
    assert seen == [[("start_requested", server.starts[0]["startJournalId"])]]


def test_lost_start_ack_then_restart_resends_the_same_journal_id_and_spawns_once(make_core, server, journal_path):
    server.faults[START_PATH] = ["lost"]
    metadata = server.issue(envelope())
    first = admit(make_core(), metadata, CTX, "task-1")
    assert (first.kind, first.outcome) == ("terminal", FAILED) and "start_unavailable" in first.text
    assert [r["state"] for r in rows(journal_path)] == ["start_requested"]

    restarted = make_core()  # new process: new Journal instance on the same file
    restarted.recover()
    second = admit(restarted, metadata, CTX, "task-2")
    assert second.kind == "spawn"
    assert [s["startJournalId"] for s in server.starts] == [rows(journal_path)[0]["start_journal_id"]] * 2
    assert len(server.bootstraps) == 1


def test_same_dispatch_with_a_different_envelope_is_refused_and_never_overwrites(core, server, journal_path):
    server.faults[START_PATH] = ["lost"]
    admit(core, server.issue(envelope()), CTX, "task-1")
    before = rows(journal_path)
    for changed in (envelope(binding={"slotId": uid(77)}), envelope(startBy="2026-10-18T12:00:00.000Z"),
                    envelope(setupRevision=2)):
        decision = admit(core, server.issue(changed), CTX, "task-2")
        assert decision.outcome == REJECTED and "tuple_mismatch" in decision.text
    assert rows(journal_path) == before and len(server.starts) == 1


def test_a_retry_after_start_by_never_spawns(core, server, clock, journal_path):
    server.faults[START_PATH] = ["lost"]
    metadata = server.issue(envelope())
    admit(core, metadata, CTX, "task-1")
    clock.advance(minutes=11)
    decision = admit(core, metadata, CTX, "task-2")
    assert decision.outcome == REJECTED and "start_deadline_passed" in decision.text
    assert len(server.starts) == 1 and server.bootstraps == []
    assert rows(journal_path)[0]["state"] == "refused"


def test_a_valid_permit_replayed_after_start_by_does_not_spawn(core, server, clock):
    server.on_start = lambda body: clock.advance(minutes=10)  # the permit arrives exactly at startBy
    decision = admit(core, server.issue(envelope()), CTX, "task-1")
    assert decision.outcome == REJECTED and "start_deadline_passed" in decision.text
    assert len(server.starts) == 1 and server.bootstraps == [] and server.reports == []


def test_start_by_passing_during_bootstrap_refuses_without_spawning(core, server, clock, journal_path):
    server.on_bootstrap = lambda body: clock.advance(minutes=10)  # the credential arrives exactly at startBy
    metadata = server.issue(envelope())
    decision = admit(core, metadata, CTX, "task-1")
    assert (decision.kind, decision.outcome) == ("terminal", REJECTED) and "start_deadline_passed" in decision.text
    assert core.credentials._bound == {} and len(server.issued) == 1
    assert [(r["state"], r["refusal"], r["result_state"]) for r in rows(journal_path)] == [
        ("refused", "start_deadline_passed", None)]
    assert server.reports == [report_body(envelope()["binding"], 1, "stopped", "failed")]

    again = admit(core, metadata, CTX, "task-2")
    assert (again.kind, again.outcome, again.text) == ("terminal", REJECTED, decision.text)
    assert len(server.starts) == len(server.bootstraps) == 1 and len(server.reports) == 1


@pytest.mark.parametrize("tamper", [{"setupRevision": 2}, {"startJournalId": "journal-other"},
                                    {"startBy": "2026-10-18T12:00:00.000Z"}])
def test_a_permit_differing_from_the_journaled_envelope_is_refused(core, server, tamper):
    server.tamper_permit = tamper
    decision = admit(core, server.issue(envelope()), CTX, "task-1")
    assert decision.outcome == REJECTED and "permit_mismatch" in decision.text
    assert server.bootstraps == []


@pytest.mark.parametrize("reason", ["start_owned", "start_deadline_passed", "start_not_authorized", "withdrawn"])
def test_start_refusals_are_terminal_and_replayed_without_restarting(core, server, journal_path, reason):
    server.refuse_start = reason
    metadata = server.issue(envelope())
    assert reason in admit(core, metadata, CTX, "task-1").text
    again = admit(core, metadata, CTX, "task-2")
    assert again.outcome == REJECTED and reason in again.text
    assert len(server.starts) == 1 and server.bootstraps == []
    assert [(r["state"], r["refusal"]) for r in rows(journal_path)] == [("refused", reason)]


def test_another_journal_owning_the_start_gets_start_owned(core, server):
    metadata = server.issue(envelope())
    server.journals[envelope()["binding"]["assignmentId"]] = "someone-elses-journal"
    decision = admit(core, metadata, CTX, "task-1")
    assert decision.outcome == REJECTED and "start_owned" in decision.text and server.bootstraps == []


@pytest.mark.parametrize("bootstrap", ["unavailable", "redirect"])
def test_control_failures_never_spawn(core, server, bootstrap):
    if bootstrap == "redirect":
        server.redirect = True
    else:
        server.faults[BOOTSTRAP_PATH] = ["down"]
    decision = admit(core, server.issue(envelope()), CTX, "task-1")
    assert (decision.kind, decision.outcome) == ("terminal", FAILED)
    assert all(r["url"].startswith(fake_pool.ORIGIN + "/api/agent-gateway/pool/") for r in server.requests)


# ---- retries, concurrency, restart ----

def test_a_retry_of_a_running_dispatch_answers_the_same_turn_instead_of_spawning(core, server):
    metadata = server.issue(envelope())
    assert admit(core, metadata, CTX, "task-1").kind == "spawn"
    working = admit(core, metadata, CTX, "task-2")
    assert (working.kind, working.task_id) == ("working", "task-1")
    core.finish("task-1", COMPLETED, "result")
    done = admit(core, metadata, CTX, "task-3")
    assert (done.kind, done.outcome, done.text) == ("terminal", COMPLETED, "result")
    assert len(server.bootstraps) == 1


def test_duplicate_concurrent_deliveries_spawn_exactly_once(make_core, server):
    metadata = server.issue(envelope())
    cores = [make_core() for _ in range(8)]  # separate connections, as separate processes would have
    barrier, decisions = threading.Barrier(8), []

    def deliver(i):
        barrier.wait()
        decisions.append(admit(cores[i], metadata, CTX, f"task-{i}"))

    threads = [threading.Thread(target=deliver, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert [d.kind for d in decisions].count("spawn") == 1
    assert {d.kind for d in decisions} == {"spawn", "working"}
    assert len(server.bootstraps) == 1 and server.reports == []


def test_restart_marks_a_spawned_turn_uncertain_reports_crashed_and_never_respawns(make_core, server, journal_path):
    metadata = server.issue(envelope())
    first = make_core()
    assert admit(first, metadata, CTX, "task-1").kind == "spawn"
    first.started("task-1")
    restarted = make_core()
    restarted.recover()
    assert rows(journal_path)[0]["state"] == "uncertain"
    # The restarted process holds no agx_: the crash report waits for the server to present the grant.
    assert len(server.reports) == 1 and pending(journal_path) == [[(2, "stopped")]]
    retry = admit(restarted, metadata, CTX, "task-2")
    assert (retry.kind, retry.outcome, retry.terminal) == ("terminal", FAILED, None) and "uncertain" in retry.text
    assert server.reports[-1] == report_body(envelope()["binding"], 2, "stopped", "crashed")
    restarted.recover()  # idempotent: no second crash report
    assert len(server.starts) == 1 and len(server.bootstraps) == 1 and len(server.reports) == 2


# ---- sessions ----

def test_a_known_dispatch_on_another_context_is_refused(core, server):
    metadata = server.issue(envelope())
    admit(core, metadata, CTX, "task-1")
    decision = admit(core, metadata, "ctx-other", "task-2")
    assert decision.outcome == REJECTED and "context_mismatch" in decision.text


def test_a_context_bound_to_another_delegation_is_refused(core, server):
    admit(core, server.issue(envelope()), CTX, "task-1")
    other = envelope(binding={"delegationId": uid(50), "assignmentId": uid(51)}, dispatchId="dispatch-9")
    decision = admit(core, server.issue(other), CTX, "task-2")
    assert decision.outcome == REJECTED and "context_bound_to_other_delegation" in decision.text


def test_an_unrelated_task_without_context_gets_a_fresh_session(core, server):
    decision = admit(core, server.issue(envelope()), "", "task-1")
    assert decision.kind == "spawn" and decision.context_id.startswith("ctx-")


@pytest.mark.parametrize("context_id", [CTX, "ctx-elsewhere", ""])
def test_a_continuation_after_a_clarification_is_continuation_unsupported_on_any_context(core, server, journal_path,
                                                                                        context_id):
    # Product ruling: initial tasks only; a clarification ended the first task as failed.
    admit(core, server.issue(envelope()), CTX, "task-1")
    core.finish("task-1", FAILED, "clarification_unsupported")
    before = len(server.requests)
    decision = admit(core, server.issue(continuation(42)), context_id, "task-2")
    assert (decision.kind, decision.outcome, decision.terminal) == ("terminal", REJECTED, CONTINUED)
    assert decision.text == ("This cloud task cannot resume. Send the task again as a new request with your answer "
                             "and the context it needs.")  # PM's exact copy (cloud-clarification-amendment/copy.md)
    assert len(server.requests) == before and len(rows(journal_path)) == 1  # no start, bootstrap or new row


def test_a_continuation_for_a_session_unknown_to_the_journal_is_continuation_unsupported(core, server, journal_path):
    decision = admit(core, server.issue(continuation(40)), CTX, "task-1")
    assert (decision.outcome, decision.terminal) == (REJECTED, CONTINUED)
    assert rows(journal_path) == [] and server.requests == []


def test_the_fixture_continuation_with_its_own_grant_is_refused_before_any_control_call(core, server, journal_path):
    context_id = fixture("cloud-send-request.json")["body"]["params"]["message"]["contextId"]
    admit(core, server.issue(envelope()), context_id, "task-1")
    core.finish("task-1", FAILED, "clarification_unsupported")
    request = fixture("cloud-continuation-request.json")
    params = request["body"]["params"]
    grant = (request["headers"]["x-ab-control-credential"], request["headers"]["x-ab-start-journal-id"])
    before = len(server.requests)
    decision = core.admit(server.issue(envelope_from_metadata(params["metadata"])), params["message"]["contextId"],
                          "task-2", grant=grant, message_id=params["message"]["messageId"])
    assert (decision.outcome, decision.terminal) == (REJECTED, CONTINUED)
    assert len(server.requests) == before and len(rows(journal_path)) == 1


@pytest.mark.parametrize("first_outcome", [COMPLETED, None])
def test_a_continuation_after_any_first_outcome_is_continuation_unsupported(core, server, first_outcome):
    admit(core, server.issue(envelope()), CTX, "task-1")
    if first_outcome:
        core.finish("task-1", first_outcome, "")
    before = len(server.requests)
    decision = admit(core, server.issue(continuation(42)), CTX, "task-2")
    assert (decision.outcome, decision.terminal) == (REJECTED, CONTINUED)
    assert len(server.requests) == before


# ---- start grant (X-AB-Control-Credential, X-AB-Start-Journal-Id) ----

def test_a_grant_naming_another_assignments_journal_is_refused_before_the_journal(core, server, journal_path):
    metadata = server.issue(envelope())
    agx, _ = grant_for(envelope())
    decision = core.admit(metadata, CTX, "task-1", grant=(agx, grant_for(continuation(42))[1]), message_id="dispatch-1")
    assert decision.outcome == REJECTED and "journal_mismatch" in decision.text
    assert rows(journal_path) == [] and server.requests == []


def test_a_journaled_assignment_presented_with_another_agx_is_refused_and_never_restarted(core, server, journal_path):
    metadata = server.issue(envelope())
    agx, journal = grant_for(envelope())
    admit(core, metadata, CTX, "task-1")
    before = len(server.requests)
    decision = core.admit(metadata, CTX, "task-2", grant=("agx_" + "Z" * 43, journal), message_id="dispatch-1")
    assert decision.outcome == REJECTED and "grant_mismatch" in decision.text and len(server.requests) == before
    assert rows(journal_path)[0]["grant_fingerprint"] == hashlib.sha256(agx.encode()).hexdigest()


def test_recovery_by_grant_needs_both_the_journal_and_its_agx(core, server, journal_path):
    admit(core, server.issue(envelope()), CTX, "task-1")
    agx, journal = grant_for(envelope())
    assert core.recovery_row(("agx_" + "Z" * 43, journal)) is None
    assert core.recovery_row((agx, grant_for(continuation(42))[1])) is None
    assert core.recovery_row((agx, journal))["start_journal_id"] == journal
    assert len(rows(journal_path)) == 1 and len(server.starts) == 1


# ---- cancel and late results ----

def test_cancel_acknowledges_with_its_own_binding_and_a_late_result_touches_nothing(core, server, journal_path):
    admit(core, server.issue(envelope()), CTX, "task-1")
    core.started("task-1")
    assert core.cancel("task-1") is True
    assert server.reports[-1] == fixture("cancel-ack.json")
    assert core.credentials.lookup(POOL_PLATFORM, CTX, "task-1") is None

    reports = list(server.reports)
    core.finish("task-1", COMPLETED, "late result")  # the canceled turn finishing anyway
    assert server.reports == reports
    # A continuation of a canceled turn never resumes the session.
    resumed = admit(core, server.issue(continuation(42)), CTX, "task-2")
    assert (resumed.outcome, resumed.terminal) == (REJECTED, CONTINUED)
    states = {r["assignment_id"]: (r["state"], r["result_state"]) for r in rows(journal_path)}
    assert states == {envelope()["binding"]["assignmentId"]: ("finished", "canceled")}
    assert core.cancel("task-1") is False


def test_refused_rows_are_kept_so_the_context_stays_known(core, server, journal_path):
    server.refuse_start = "withdrawn"
    admit(core, server.issue(envelope()), CTX, "task-1")
    server.refuse_start = None
    assert len(rows(journal_path)) == 1
    decision = admit(core, server.issue(continuation(42)), "ctx-elsewhere", "task-2")
    assert decision.terminal == CONTINUED and len(rows(journal_path)) == 1


# ---- secrets ----

def test_no_control_or_task_credential_reaches_the_journal_decisions_or_logs(core, server, journal_path, caplog):
    caplog.set_level(logging.DEBUG)
    metadata = server.issue(envelope())
    decisions = [admit(core, metadata, CTX, "task-1")]
    core.finish("task-1", COMPLETED, "done")
    decisions.append(admit(core, metadata, CTX, "task-2"))
    blob = b"".join(p.read_bytes() for p in journal_path.parent.iterdir())
    assert b"agx_" not in blob and b"dlc_" not in blob
    text = repr(decisions) + caplog.text + repr(core.credentials._bound)
    assert "agx_" not in text and "dlc_" not in text and server.issued


# ---- capability answer ----

@pytest.mark.parametrize("change", [{"executorId": uid(99)}, {"executorConnectionRevision": "2"},
                                    {"protocolVersion": 2}, {"protocolVersion": True}, {"nonce": "N" * 31},
                                    {"requiredCapabilities": ["open_pool_consent"]}, {"requiredCapabilities": []},
                                    {"extra": 1}])
def test_a_capability_request_for_another_executor_or_shape_gets_no_answer(change):
    params = {**fixture("cloud-capability-request.json")["body"]["params"], **change}
    with pytest.raises(Refused, match="capability_mismatch"):
        pool_protocol.capability_answer(params, executor_id=EXECUTOR_ID, connection_revision=REVISION,
                                        runtime_version="x")


# ---- control client: statuses and bodies against the server contract ----

def answering(*responses):
    """A PoolControl whose server answers ``(status, body)`` in order; ``seen`` collects the requests."""
    queue, seen = list(responses), []

    def handle(request):
        seen.append(request)
        status, body = queue.pop(0)
        return httpx2.Response(status, json=body) if body is not None else httpx2.Response(status)

    def factory(headers):
        return httpx2.Client(transport=httpx2.MockTransport(handle), headers=headers, follow_redirects=False)

    return PoolControl(tools_url=fake_pool.TOOLS_URL, http_factory=factory), seen


def outcome_of(call):
    try:
        return "ok", call()
    except Refused as refused:
        return "refused", refused.code
    except ControlUnavailable:
        return "unavailable", None


BUSY = (409, fixture("scheduler-busy-response.json"))
AGX = fixture("cloud-send-request.json")["headers"]["x-ab-control-credential"]
PERMIT = fixture("start-permit.json")
INVALID = (400, {"error": {"code": "invalid_request", "message": "Body must be a protocol version 1 start request."}})


@pytest.mark.parametrize("responses, expected, requests", [
    ([(200, PERMIT)], ("ok", PERMIT["permit"]), 1),
    ([(409, fixture("start-owned-refusal.json"))], ("refused", "start_owned"), 1),
    ([(409, fixture("start-deadline-refusal.json"))], ("refused", "start_deadline_passed"), 1),
    ([(409, {"outcome": "refused", "reason": "start_not_authorized"})], ("refused", "start_not_authorized"), 1),
    ([(409, fixture("refusal-response.json"))], ("refused", "assignment_tuple_mismatch"), 1),
    ([(401, fixture("unauthorized-response.json"))], ("refused", "unauthorized"), 1),
    ([INVALID], ("refused", "invalid_request"), 1),
    ([BUSY, (200, PERMIT)], ("ok", PERMIT["permit"]), 2),
    ([BUSY, BUSY, BUSY], ("unavailable", None), 3),
    ([(503, None)], ("unavailable", None), 1),
    ([(307, None)], ("unavailable", None), 1),
    ([(200, {**PERMIT, "extra": 1})], ("unavailable", None), 1),
    ([(200, {**PERMIT, "permit": {**PERMIT["permit"], "extra": 1}})], ("unavailable", None), 1),
    ([(200, {**PERMIT, "permit": {**PERMIT["permit"], "startJournalId": ""}})], ("unavailable", None), 1),
    ([(200, fixture("start-owned-refusal.json"))], ("unavailable", None), 1),
    ([(409, {**fixture("start-owned-refusal.json"), "message": "x"})], ("unavailable", None), 1),
    ([(409, {"error": {"code": "Not A Code!", "message": "x"}})], ("unavailable", None), 1),
    ([(401, {"detail": "nope"})], ("unavailable", None), 1),
], ids=["permit", "owned", "deadline", "not-authorized", "named", "401", "400", "busy-then-permit", "busy-exhausted",
        "503", "redirect", "extra-key", "permit-extra-key", "permit-empty-journal", "refusal-as-200",
        "refusal-extra-key", "bad-code", "401-unknown-body"])
def test_start_maps_statuses_and_bodies_strictly(responses, expected, requests):
    control, seen = answering(*responses)
    assert outcome_of(lambda: control.start(fixture("start-request.json"), AGX)) == expected
    assert len(seen) == requests and {r.headers["authorization"] for r in seen} == {f"Bearer {AGX}"}


BOOTSTRAPPED = fixture("bootstrap-response.json")


@pytest.mark.parametrize("responses, expected", [
    ([(200, BOOTSTRAPPED)], ("ok", (BOOTSTRAPPED["credential"], datetime(2026, 10, 17, 11, 15, tzinfo=timezone.utc)))),
    ([(200, {**BOOTSTRAPPED, "extra": 1})], ("unavailable", None)),
    ([(200, {**BOOTSTRAPPED, "credential": "dlc_short"})], ("unavailable", None)),
    ([(200, {**BOOTSTRAPPED, "credential": "dlt_" + "B" * 43})], ("unavailable", None)),
    ([(200, {**BOOTSTRAPPED, "expiresAt": "2026-10-17T11:15:00"})], ("unavailable", None)),
    ([(409, {"error": {"code": "assignment_not_live", "message": "x"}})], ("refused", "assignment_not_live")),
    ([(401, fixture("unauthorized-response.json"))], ("refused", "unauthorized")),
], ids=["fixture", "extra-key", "short", "dlt", "no-offset", "named", "401"])
def test_bootstrap_accepts_only_the_strict_credential_shape(responses, expected):
    control, seen = answering(*responses)
    assert outcome_of(lambda: control.bootstrap(fixture("cloud-bootstrap-request.json"), AGX)) == expected
    assert seen[0].url.path == "/api/agent-gateway/pool/bootstrap" and seen[0].headers["authorization"] == f"Bearer {AGX}"


@pytest.mark.parametrize("responses, expected", [
    ([(200, fixture("report-response.json"))], ("ok", None)),
    ([(200, fixture("report-replay-response.json"))], ("ok", None)),
    ([(200, {"recorded": True, "currentOccupant": False})], ("ok", None)),
    ([(200, {"ok": True})], ("unavailable", None)),
    ([(200, {"recorded": "yes", "currentOccupant": True})], ("unavailable", None)),
    ([(409, fixture("refusal-response.json"))], ("refused", "assignment_tuple_mismatch")),
    ([BUSY, BUSY, BUSY], ("unavailable", None)),
], ids=["recorded", "replay", "stale-occupant", "legacy-ok", "non-bool", "named", "busy-exhausted"])
def test_report_accepts_only_the_strict_acknowledgement(responses, expected, caplog):
    caplog.set_level(logging.INFO)
    control, seen = answering(*responses)
    assert outcome_of(lambda: control.report(fixture("report.json"), AGX)) == expected
    assert seen[0].headers["authorization"] == f"Bearer {AGX}"
    if responses[0][1] == {"recorded": True, "currentOccupant": False}:
        assert "superseded" in caplog.text


# ---- durable report delivery ----

def test_a_report_enqueued_before_a_restart_is_replayed_by_the_new_process(make_core, server, journal_path):
    server.faults[REPORT_PATH] = ["down"]
    metadata = server.issue(envelope())
    first = make_core()
    assert admit(first, metadata, CTX, "task-1").kind == "spawn"
    first.started("task-1")
    assert pending(journal_path) == [[(1, "running")]] and server.reports == []
    restarted = make_core()  # a new process: new Journal and PoolCore on the same file
    restarted.recover()
    assert server.reports == [] and pending(journal_path) == [[(1, "running"), (2, "stopped")]]
    # A GetTask with the start headers hands the grant back without inserting or starting anything.
    assert restarted.recovery_row(grant_for(envelope()))["state"] == "uncertain"
    assert sent(server.reports) == [(1, "running"), (2, "stopped")]  # the replay, then its own crash report
    assert pending(journal_path) == [[]] and len(server.starts) == 1


def test_a_report_whose_response_was_lost_is_cleared_by_its_replay(core, server, journal_path):
    server.faults[REPORT_PATH] = ["lost"]  # the server records it; the response never arrives
    admit(core, server.issue(envelope()), CTX, "task-1")
    core.started("task-1")
    assert sent(server.reports) == [(1, "running")] and pending(journal_path) == [[(1, "running")]]
    core.finish("task-1", COMPLETED, "done")
    assert sent(server.replays) == [(1, "running")] and sent(server.reports) == [(1, "running"), (2, "stopped")]
    assert pending(journal_path) == [[]]


def test_a_terminal_report_never_overtakes_a_pending_running_report(core, server, journal_path):
    server.faults[REPORT_PATH] = ["down", "down"]
    metadata = server.issue(envelope())
    admit(core, metadata, CTX, "task-1")
    core.started("task-1")
    core.finish("task-1", COMPLETED, "done")  # the running report fails again, so the stopped one waits
    assert server.reports == [] and report_requests(server) == [1, 1]
    assert pending(journal_path) == [[(1, "running"), (2, "stopped")]]
    replay = admit(core, metadata, CTX, "task-2")  # the next admit replays, oldest sequence first
    assert (replay.outcome, replay.text) == (COMPLETED, "done")
    assert sent(server.reports) == [(1, "running"), (2, "stopped")] and pending(journal_path) == [[]]


def test_a_scheduler_busy_report_is_retried_inline_then_stays_pending(core, server, journal_path):
    server.faults[REPORT_PATH] = ["busy"]
    admit(core, server.issue(envelope()), CTX, "task-1")
    core.started("task-1")
    assert sent(server.reports) == [(1, "running")] and report_requests(server) == [1, 1]
    server.faults[REPORT_PATH] = ["busy"] * 3
    core.finish("task-1", COMPLETED, "done")
    assert pending(journal_path) == [[(2, "stopped")]] and report_requests(server) == [1, 1, 2, 2, 2]
    core.deliver_reports()
    assert sent(server.reports) == [(1, "running"), (2, "stopped")] and pending(journal_path) == [[]]


def test_a_permanently_refused_report_stays_journaled_blocks_its_row_and_is_never_retried(core, server, journal_path):
    metadata = server.issue(envelope())
    server.refuse[REPORT_PATH] = "assignment_not_live"
    admit(core, metadata, CTX, "task-1")
    core.started("task-1")
    row = rows(journal_path)[0]
    assert (row["state"], row["report_refusal"]) == ("running", "assignment_not_live")
    del server.refuse[REPORT_PATH]
    core.finish("task-1", COMPLETED, "done")
    core.recover()
    # A refusal is no acknowledgment: both facts stay, in order, and nothing overtakes the refused sequence.
    assert pending(journal_path) == [[(1, "running"), (2, "stopped")]]
    assert server.reports == [] and report_requests(server) == [1]
    assert rows(journal_path)[0]["state"] == "finished"


def test_a_replay_waits_for_the_new_dispatchs_start_and_stops_at_an_unavailable_server(core, server, journal_path):
    server.faults[REPORT_PATH] = ["down"]
    admit(core, server.issue(envelope()), CTX, "task-1")
    core.started("task-1")  # its running report stays pending
    other = envelope(binding={"delegationId": uid(50), "assignmentId": uid(51)}, dispatchId="dispatch-9")
    server.faults[REPORT_PATH] = ["down"]
    before = len(server.requests)
    assert admit(core, server.issue(other), "", "task-2").kind == "spawn"
    assert [(r["path"], (r["body"] or {}).get("reportSequence")) for r in server.requests[before:]] == [
        (START_PATH, None), (BOOTSTRAP_PATH, None), (REPORT_PATH, 1)]  # one failed replay ends the pass
    server.faults[REPORT_PATH] = ["down"]
    core.started("task-2")
    assert pending(journal_path) == [[(1, "running")], [(1, "running")]]
    core.deliver_reports()
    assert pending(journal_path) == [[], []] and len(server.reports) == 2


def test_an_existing_journal_gains_the_report_columns_and_keeps_its_rows(make_core, server, journal_path):
    metadata = server.issue(envelope())
    assert admit(make_core(), metadata, CTX, "task-1").kind == "spawn"
    with sqlite3.connect(journal_path) as conn:  # the first 0.2.0.dev0 schema had none of these columns
        for column in ("pending_reports", "report_refusal", "grant_fingerprint", "message_id", "cloud_terminal"):
            conn.execute(f"ALTER TABLE dispatches DROP COLUMN {column}")
    make_core()  # opening twice must not re-add a column
    restarted = make_core()
    restarted.recover()
    assert [(r["state"], r["report_refusal"], r["grant_fingerprint"]) for r in rows(journal_path)] == [
        ("uncertain", None, None)]
    # A row journaled without a grant fingerprint never matches a presented grant: its report stays pending.
    assert restarted.recovery_row(grant_for(envelope())) is None
    assert server.reports == [] and pending(journal_path) == [[(1, "stopped")]]


# ---- start authority (the server-started journal), refusals, retries ----

def test_a_cloud_assignment_the_server_did_not_start_is_rejected_without_bootstrap(core, server, journal_path):
    metadata = server.issue(envelope())
    del server.journals[envelope()["binding"]["assignmentId"]]  # never started by the server lifecycle
    decision = admit(core, metadata, CTX, "task-1")
    assert (decision.kind, decision.outcome) == ("terminal", REJECTED) and "start_not_authorized" in decision.text
    again = admit(core, metadata, CTX, "task-2")
    assert again.text == decision.text and len(server.starts) == 1
    assert server.bootstraps == [] and server.reports == [] and core.credentials._bound == {}
    assert [(r["state"], r["refusal"]) for r in rows(journal_path)] == [("refused", "start_not_authorized")]


def test_a_cloud_assignment_started_under_another_journal_is_start_owned(core, server):
    metadata = server.issue(envelope())
    server.journals[envelope()["binding"]["assignmentId"]] = "server-chosen-journal"
    decision = admit(core, metadata, CTX, "task-1")
    assert decision.outcome == REJECTED and "start_owned" in decision.text and server.bootstraps == []


@pytest.mark.parametrize("refusal", ["named", "unauthorized"])
def test_a_refused_or_unauthorized_start_is_terminal_and_never_retried(make_core, server, journal_path, refusal, caplog):
    caplog.set_level(logging.DEBUG)
    if refusal == "named":
        server.refuse[START_PATH] = "assignment_not_live"
        grant, code = grant_for(envelope()), "assignment_not_live"
    else:  # another assignment's (or a revoked) agx_
        grant, code = ("agx_" + "W" * 40, grant_for(envelope())[1]), "unauthorized"
    core = make_core()
    metadata = server.issue(envelope())
    assert code in core.admit(metadata, CTX, "task-1", grant=grant, message_id="dispatch-1").text
    assert code in core.admit(metadata, CTX, "task-2", grant=grant, message_id="dispatch-1").text
    assert len(server.requests) == 1 and server.bootstraps == []
    assert [(r["state"], r["refusal"]) for r in rows(journal_path)] == [("refused", code)]
    assert "agx_" not in caplog.text


def test_a_busy_start_is_retried_inline_and_an_exhausted_one_stays_retryable(core, server, journal_path):
    server.faults[START_PATH] = ["busy", "busy", "busy"]
    metadata = server.issue(envelope())
    decision = admit(core, metadata, CTX, "task-1")
    assert (decision.outcome, decision.task_id) == (FAILED, "task-1") and "start_unavailable" in decision.text
    assert rows(journal_path)[0]["state"] == "start_requested" and server.starts == []
    server.faults[START_PATH] = ["busy"]
    again = admit(core, metadata, CTX, "task-2")
    assert (again.kind, again.task_id) == ("spawn", "task-1")
    assert [r["path"] for r in server.requests].count(START_PATH) == 5 and len(server.starts) == 1


def test_a_bootstrap_refusal_after_the_permit_never_spawns(core, server, journal_path):
    server.refuse[BOOTSTRAP_PATH] = "assignment_not_live"
    decision = admit(core, server.issue(envelope()), CTX, "task-1")
    assert (decision.kind, decision.outcome) == ("terminal", REJECTED) and "assignment_not_live" in decision.text
    assert [(r["state"], r["refusal"]) for r in rows(journal_path)] == [("refused", "assignment_not_live")]
    assert server.reports == [report_body(envelope()["binding"], 1, "stopped", "failed")]
    assert core.credentials._bound == {}


class Crash(BaseException):
    """The process dies: nothing in the plugin catches it."""


def test_a_crash_between_permit_and_spawn_replays_the_permit_and_bootstraps_a_fresh_credential(
        make_core, server, journal_path):
    def crash(body):
        raise Crash

    server.on_bootstrap = crash  # the server minted a dlc_; the response never arrives
    metadata = server.issue(envelope())
    with pytest.raises(Crash):
        admit(make_core(), metadata, CTX, "task-1")
    assert rows(journal_path)[0]["state"] == "permitted"
    server.on_bootstrap = None
    restarted = make_core()
    restarted.recover()
    assert rows(journal_path)[0]["state"] == "start_requested"
    decision = admit(restarted, metadata, CTX, "task-2")
    assert (decision.kind, decision.task_id) == ("spawn", "task-1")  # the journaled task id, not the retry's
    journal_id = rows(journal_path)[0]["start_journal_id"]
    assert [s["startJournalId"] for s in server.starts] == [journal_id] * 2
    assert [b["startJournalId"] for b in server.bootstraps] == [journal_id] * 2
    first, second = server.issued
    assert server.revoked == {first} and restarted.credentials.lookup(POOL_PLATFORM, CTX, "task-1") == second
    assert server.reports == []


def test_a_dispatch_id_reused_by_another_assignment_is_refused_without_reading_its_row(core, server, journal_path):
    admit(core, server.issue(envelope()), CTX, "task-1")
    core.finish("task-1", COMPLETED, "first assignment's result")
    other = envelope(binding={"assignmentId": uid(60), "slotId": uid(61)})  # same dispatchId "dispatch-1"
    before = len(server.requests)
    decision = admit(core, server.issue(other), "", "task-2")
    assert decision.outcome == REJECTED and "tuple_mismatch" in decision.text
    assert "first assignment" not in decision.text and decision.context_id == ""
    assert len(server.requests) == before and len(rows(journal_path)) == 1
    assert core.credentials.lookup(POOL_PLATFORM, decision.context_id, "task-2") is None

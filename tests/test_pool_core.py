"""Pooled (assignment-bound) execution core, without Hermes: envelope strictness, the durable journal,
start ownership, permit checks, restart recovery, sessions, cancel/report fencing, durable report
delivery, and the control client's status mapping against the server contract (integration-u3-r1)."""
import json
import logging
import sqlite3
import threading

import httpx2
import pytest

import hermes_artifactbridge as plugin_module
from hermes_artifactbridge import pool_protocol
from hermes_artifactbridge.pool_core import (COMPLETED, FAILED, INPUT_REQUIRED, POOL_PLATFORM, REJECTED,
                                             ControlUnavailable, CredentialBindings, PoolControl, PoolCore)
from hermes_artifactbridge.pool_journal import Journal
from hermes_artifactbridge.pool_protocol import (IMPLEMENTED_CAPABILITIES, Refused, answer_capability_challenge,
                                                 envelope_from_metadata, report_body, start_request,
                                                 validate_binding, validate_envelope)
from tests import fake_pool
from tests.fake_pool import (BOOTSTRAP_PATH, CAPABILITY_PATH, EXECUTOR_CREDENTIAL, EXECUTOR_ID, REPORT_PATH, REVISION,
                             START_PATH, Clock, FakeControl, envelope, fixture)

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
    def make(credentials=None, credential=EXECUTOR_CREDENTIAL):
        return PoolCore(journal=Journal(journal_path),
                        control=PoolControl(tools_url=fake_pool.TOOLS_URL, credential=credential,
                                            http_factory=server.factory),
                        executor_id=EXECUTOR_ID, connection_revision=REVISION,
                        credentials=credentials or CredentialBindings(), now=clock)
    return make


@pytest.fixture
def core(make_core):
    return make_core()


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
    env = envelope_from_metadata(fixture("dispatch-metadata.json"))
    permit = {**env, "startJournalId": "journal-1"}
    assert validate_envelope(fixture("mailbox-assignment-command.json")["envelope"]) == env
    assert start_request(env, "journal-1") == fixture("start-request.json")
    assert p.bootstrap_request(env, "journal-1") == fixture("bootstrap-request.json")
    assert p.parse_start_response(fixture("start-permit.json")) == ("permit", permit)
    # A tray start's controlCredential parses but is never returned, so it is never adopted or stored.
    assert p.parse_start_response(fixture("start-permit-with-control-credential.json")) == ("permit", permit)
    assert p.parse_start_response(fixture("start-owned-refusal.json")) == ("refused", "start_owned")
    assert p.parse_start_response(fixture("start-deadline-refusal.json")) == ("refused", "start_deadline_passed")
    assert p.parse_bootstrap_response(fixture("bootstrap-response.json")) == fixture("bootstrap-response.json")["credential"]
    assert report_body(env["binding"], 1, "stopped", "completed") == fixture("report.json")
    assert report_body(env["binding"], 2, "cancel_acknowledged", "canceled") == fixture("cancel-ack.json")
    assert p.parse_report_response(fixture("report-response.json")) == (True, True)
    assert p.parse_report_response(fixture("report-replay-response.json")) == (False, True)
    assert p.parse_error(fixture("refusal-response.json")) == ("assignment_tuple_mismatch", False)
    assert p.parse_error(fixture("unauthorized-response.json")) == ("unauthorized", False)
    assert p.parse_error(fixture("scheduler-busy-response.json")) == ("scheduler_busy", True)
    proof = answer_capability_challenge(fixture("capability-challenge.json"), executor_id=EXECUTOR_ID,
                                        connection_revision=REVISION, runtime_version="0.0.0-fixture")
    assert proof == fixture("capability-proof.json")
    assert set(fixture("capability-challenge.json")["requiredCapabilities"]) == set(IMPLEMENTED_CAPABILITIES)
    assert p.parse_capability_verdict(fixture("capability-response.json")) == fixture("capability-response.json")
    assert p.parse_capability_verdict(fixture("capability-refusal-response.json")) == fixture(
        "capability-refusal-response.json")
    # Tray-only (mailbox tombstone, decline): a cloud executor neither sends nor reads them; their tuple parses.
    for name in ("mailbox-withdrawn-tombstone.json", "decline-request.json"):
        assert validate_binding(fixture(name)["binding"]) == env["binding"]
    covered = {"dispatch-metadata.json", "mailbox-assignment-command.json", "start-request.json",
               "bootstrap-request.json", "start-permit.json", "start-permit-with-control-credential.json",
               "start-owned-refusal.json", "start-deadline-refusal.json", "bootstrap-response.json", "report.json",
               "cancel-ack.json", "report-response.json", "report-replay-response.json", "refusal-response.json",
               "unauthorized-response.json", "scheduler-busy-response.json", "capability-challenge.json",
               "capability-proof.json", "capability-response.json", "capability-refusal-response.json",
               "mailbox-withdrawn-tombstone.json", "decline-request.json", "decline-response.json"}
    assert covered == {f.name for f in fake_pool.FIXTURES.glob("*.json")}


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
    decision = core.admit(metadata, CTX, "task-1")
    assert (decision.kind, decision.outcome) == ("terminal", REJECTED) and code in decision.text
    assert rows(journal_path) == [] and server.requests == []


@pytest.mark.parametrize("binding", [{"executorId": uid(99)}, {"executorConnectionRevision": "2"}])
def test_an_envelope_for_another_executor_is_refused_before_journaling(core, server, journal_path, binding):
    decision = core.admit(server.issue(envelope(binding=binding)), CTX, "task-1")
    assert decision.outcome == REJECTED and "executor_mismatch" in decision.text
    assert rows(journal_path) == [] and server.requests == []


# ---- start, bootstrap, reports ----

def test_permitted_dispatch_spawns_once_and_reports_running_then_stopped(core, server, journal_path):
    metadata = server.issue(envelope())
    decision = core.admit(metadata, CTX, "task-1")
    assert (decision.kind, decision.context_id, decision.task_id) == ("spawn", CTX, "task-1")
    journal_id = rows(journal_path)[0]["start_journal_id"]
    assert server.starts == [{**fixture("start-request.json"), "startJournalId": journal_id}]
    assert server.bootstraps == [{**fixture("bootstrap-request.json"), "startJournalId": journal_id}]
    credential = core.credentials.lookup(POOL_PLATFORM, CTX, "task-1")
    assert credential in server.issued and server.issued[credential] == envelope()["binding"]
    assert server.reports == [report_body(envelope()["binding"], 1, "running", None)]

    core.finish("task-1", COMPLETED, "done")
    assert server.reports[-1] == {**fixture("report.json"), "reportSequence": 2}
    assert core.credentials.lookup(POOL_PLATFORM, CTX, "task-1") is None

    replay = core.admit(metadata, CTX, "task-2")  # lost response: the same recorded result, no second run
    assert (replay.kind, replay.outcome, replay.text) == ("terminal", COMPLETED, "done")
    assert len(server.starts) == 1 and len(server.bootstraps) == 1 and len(server.reports) == 2


@pytest.mark.parametrize("state, outcome", [(INPUT_REQUIRED, "input_wait"), (FAILED, "failed")])
def test_turn_outcomes_map_to_report_outcomes(core, server, state, outcome):
    core.admit(server.issue(envelope()), CTX, "task-1")
    core.finish("task-1", state, "x")
    assert server.reports[-1]["kind"] == "stopped" and server.reports[-1]["outcome"] == outcome


def test_the_journal_row_is_committed_before_the_start_request(core, server, journal_path):
    seen = []
    server.on_start = lambda body: seen.append([(r["state"], r["start_journal_id"]) for r in rows(journal_path)])
    core.admit(server.issue(envelope()), CTX, "task-1")
    assert seen == [[("start_requested", server.starts[0]["startJournalId"])]]


def test_lost_start_ack_then_restart_resends_the_same_journal_id_and_spawns_once(make_core, server, journal_path):
    server.faults[START_PATH] = ["lost"]
    metadata = server.issue(envelope())
    first = make_core().admit(metadata, CTX, "task-1")
    assert (first.kind, first.outcome) == ("terminal", FAILED) and "start_unavailable" in first.text
    assert [r["state"] for r in rows(journal_path)] == ["start_requested"]

    restarted = make_core()  # new process: new Journal instance on the same file
    restarted.recover()
    second = restarted.admit(metadata, CTX, "task-2")
    assert second.kind == "spawn"
    assert [s["startJournalId"] for s in server.starts] == [rows(journal_path)[0]["start_journal_id"]] * 2
    assert len(server.bootstraps) == 1


def test_same_dispatch_with_a_different_envelope_is_refused_and_never_overwrites(core, server, journal_path):
    server.faults[START_PATH] = ["lost"]
    core.admit(server.issue(envelope()), CTX, "task-1")
    before = rows(journal_path)
    for changed in (envelope(binding={"slotId": uid(77)}), envelope(startBy="2026-10-18T12:00:00.000Z"),
                    envelope(setupRevision=2)):
        decision = core.admit(server.issue(changed), CTX, "task-2")
        assert decision.outcome == REJECTED and "tuple_mismatch" in decision.text
    assert rows(journal_path) == before and len(server.starts) == 1


def test_a_retry_after_start_by_never_spawns(core, server, clock, journal_path):
    server.faults[START_PATH] = ["lost"]
    metadata = server.issue(envelope())
    core.admit(metadata, CTX, "task-1")
    clock.advance(minutes=11)
    decision = core.admit(metadata, CTX, "task-2")
    assert decision.outcome == REJECTED and "start_deadline_passed" in decision.text
    assert len(server.starts) == 1 and server.bootstraps == []
    assert rows(journal_path)[0]["state"] == "refused"


def test_a_valid_permit_replayed_after_start_by_does_not_spawn(core, server, clock):
    server.on_start = lambda body: clock.advance(minutes=10)  # the permit arrives exactly at startBy
    decision = core.admit(server.issue(envelope()), CTX, "task-1")
    assert decision.outcome == REJECTED and "start_deadline_passed" in decision.text
    assert len(server.starts) == 1 and server.bootstraps == [] and server.reports == []


def test_start_by_passing_during_bootstrap_refuses_without_spawning(core, server, clock, journal_path):
    server.on_bootstrap = lambda body: clock.advance(minutes=10)  # the credential arrives exactly at startBy
    metadata = server.issue(envelope())
    decision = core.admit(metadata, CTX, "task-1")
    assert (decision.kind, decision.outcome) == ("terminal", REJECTED) and "start_deadline_passed" in decision.text
    assert core.credentials._bound == {} and len(server.issued) == 1
    assert [(r["state"], r["refusal"], r["result_state"]) for r in rows(journal_path)] == [
        ("refused", "start_deadline_passed", None)]
    assert server.reports == [report_body(envelope()["binding"], 1, "stopped", "failed")]

    again = core.admit(metadata, CTX, "task-2")
    assert (again.kind, again.outcome, again.text) == ("terminal", REJECTED, decision.text)
    assert len(server.starts) == len(server.bootstraps) == 1 and len(server.reports) == 1


@pytest.mark.parametrize("tamper", [{"setupRevision": 2}, {"startJournalId": "journal-other"},
                                    {"startBy": "2026-10-18T12:00:00.000Z"}])
def test_a_permit_differing_from_the_journaled_envelope_is_refused(core, server, tamper):
    server.tamper_permit = tamper
    decision = core.admit(server.issue(envelope()), CTX, "task-1")
    assert decision.outcome == REJECTED and "permit_mismatch" in decision.text
    assert server.bootstraps == []


@pytest.mark.parametrize("reason", ["start_owned", "start_deadline_passed", "start_not_authorized", "withdrawn"])
def test_start_refusals_are_terminal_and_replayed_without_restarting(core, server, journal_path, reason):
    server.refuse_start = reason
    metadata = server.issue(envelope())
    assert reason in core.admit(metadata, CTX, "task-1").text
    again = core.admit(metadata, CTX, "task-2")
    assert again.outcome == REJECTED and reason in again.text
    assert len(server.starts) == 1 and server.bootstraps == []
    assert [(r["state"], r["refusal"]) for r in rows(journal_path)] == [("refused", reason)]


def test_another_journal_owning_the_start_gets_start_owned(core, server):
    metadata = server.issue(envelope())
    server.journals[envelope()["binding"]["assignmentId"]] = "someone-elses-journal"
    decision = core.admit(metadata, CTX, "task-1")
    assert decision.outcome == REJECTED and "start_owned" in decision.text and server.bootstraps == []


@pytest.mark.parametrize("bootstrap", ["unavailable", "redirect"])
def test_control_failures_never_spawn(core, server, bootstrap):
    if bootstrap == "redirect":
        server.redirect = True
    else:
        server.faults[BOOTSTRAP_PATH] = ["down"]
    decision = core.admit(server.issue(envelope()), CTX, "task-1")
    assert (decision.kind, decision.outcome) == ("terminal", FAILED)
    assert all(r["url"].startswith(fake_pool.ORIGIN + "/api/agent-gateway/pool/") for r in server.requests)


# ---- retries, concurrency, restart ----

def test_a_retry_of_a_running_dispatch_waits_for_the_same_turn_instead_of_spawning(core, server):
    metadata = server.issue(envelope())
    assert core.admit(metadata, CTX, "task-1").kind == "spawn"
    working = core.admit(metadata, CTX, "task-2")
    assert (working.kind, working.task_id) == ("working", "task-1")
    threading.Timer(0.05, core.finish, ("task-1", COMPLETED, "result")).start()
    waited = core.admit(metadata, CTX, "task-3", wait=5)
    assert (waited.kind, waited.outcome, waited.text) == ("terminal", COMPLETED, "result")
    assert len(server.bootstraps) == 1


def test_duplicate_concurrent_deliveries_spawn_exactly_once(make_core, server):
    metadata = server.issue(envelope())
    cores = [make_core() for _ in range(8)]  # separate connections, as separate processes would have
    barrier, decisions = threading.Barrier(8), []

    def deliver(i):
        barrier.wait()
        decisions.append(cores[i].admit(metadata, CTX, f"task-{i}"))

    threads = [threading.Thread(target=deliver, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert [d.kind for d in decisions].count("spawn") == 1
    assert {d.kind for d in decisions} == {"spawn", "working"}
    assert len(server.bootstraps) == 1 and [r["kind"] for r in server.reports] == ["running"]


def test_restart_marks_a_spawned_turn_uncertain_reports_crashed_and_never_respawns(make_core, server, journal_path):
    metadata = server.issue(envelope())
    assert make_core().admit(metadata, CTX, "task-1").kind == "spawn"
    restarted = make_core()
    restarted.recover()
    assert rows(journal_path)[0]["state"] == "uncertain"
    assert server.reports[-1] == report_body(envelope()["binding"], 2, "stopped", "crashed")
    retry = restarted.admit(metadata, CTX, "task-2")
    assert (retry.kind, retry.outcome) == ("terminal", FAILED) and "uncertain" in retry.text
    restarted.recover()  # idempotent: no second crash report
    assert len(server.starts) == 1 and len(server.bootstraps) == 1 and len(server.reports) == 2


# ---- sessions ----

def test_a_known_dispatch_on_another_context_is_refused(core, server):
    metadata = server.issue(envelope())
    core.admit(metadata, CTX, "task-1")
    decision = core.admit(metadata, "ctx-other", "task-2")
    assert decision.outcome == REJECTED and "context_mismatch" in decision.text


def test_a_context_bound_to_another_delegation_is_refused(core, server):
    core.admit(server.issue(envelope()), CTX, "task-1")
    other = envelope(binding={"delegationId": uid(50), "assignmentId": uid(51)}, dispatchId="dispatch-9")
    decision = core.admit(server.issue(other), CTX, "task-2")
    assert decision.outcome == REJECTED and "context_bound_to_other_delegation" in decision.text


def test_an_unrelated_task_without_context_gets_a_fresh_session(core, server):
    decision = core.admit(server.issue(envelope()), "", "task-1")
    assert decision.kind == "spawn" and decision.context_id.startswith("ctx-")


def test_continuation_resumes_only_on_the_original_context(core, server, journal_path):
    core.admit(server.issue(envelope()), CTX, "task-1")
    core.finish("task-1", INPUT_REQUIRED, "need more")
    moved = core.admit(server.issue(continuation(40)), "ctx-elsewhere", "task-2")
    assert moved.outcome == REJECTED and "resume_unavailable" in moved.text
    fresh = core.admit(server.issue(continuation(41, dispatch="dispatch-3")), "", "task-3")
    assert fresh.outcome == REJECTED and "resume_unavailable" in fresh.text
    resumed = core.admit(server.issue(continuation(42, dispatch="dispatch-4")), CTX, "task-4")
    assert (resumed.kind, resumed.context_id) == ("spawn", CTX)


def test_a_continuation_for_a_session_unknown_to_the_journal_is_resume_unavailable(core, server, journal_path):
    decision = core.admit(server.issue(continuation(40)), CTX, "task-1")
    assert decision.outcome == REJECTED and "resume_unavailable" in decision.text
    assert rows(journal_path) == [] and server.requests == []


# ---- cancel and late results ----

def test_cancel_acknowledges_with_its_own_binding_and_a_late_result_touches_nothing(core, server, journal_path):
    core.admit(server.issue(envelope()), CTX, "task-1")
    assert core.cancel("task-1") is True
    assert server.reports[-1] == fixture("cancel-ack.json")
    assert core.credentials.lookup(POOL_PLATFORM, CTX, "task-1") is None

    resumed = core.admit(server.issue(continuation(42)), CTX, "task-2")
    assert resumed.kind == "spawn"
    reports = list(server.reports)
    core.finish("task-1", COMPLETED, "late result")  # the canceled turn finishing anyway
    assert server.reports == reports
    states = {r["assignment_id"]: (r["state"], r["result_state"]) for r in rows(journal_path)}
    assert states == {envelope()["binding"]["assignmentId"]: ("finished", "canceled"), uid(42): ("spawned", None)}
    assert core.credentials.lookup(POOL_PLATFORM, CTX, "task-2") is not None
    assert core.cancel("task-1") is False


def test_refused_rows_are_kept_so_the_context_stays_known(core, server, journal_path):
    server.refuse_start = "withdrawn"
    core.admit(server.issue(envelope()), CTX, "task-1")
    server.refuse_start = None
    assert len(rows(journal_path)) == 1
    decision = core.admit(server.issue(continuation(42)), "ctx-elsewhere", "task-2")
    assert "resume_unavailable" in decision.text and len(rows(journal_path)) == 1


# ---- secrets ----

def test_no_control_or_task_credential_reaches_the_journal_decisions_or_logs(core, server, journal_path, caplog):
    caplog.set_level(logging.DEBUG)
    metadata = server.issue(envelope())
    decisions = [core.admit(metadata, CTX, "task-1")]
    core.finish("task-1", COMPLETED, "done")
    decisions.append(core.admit(metadata, CTX, "task-2"))
    blob = b"".join(p.read_bytes() for p in journal_path.parent.iterdir())
    assert b"agx_" not in blob and b"dlc_" not in blob
    text = repr(decisions) + caplog.text + repr(core.credentials._bound)
    assert "agx_" not in text and "dlc_" not in text and server.issued


# ---- capability proof ----

@pytest.mark.parametrize("change", [{"executorId": uid(99)}, {"executorConnectionRevision": "2"},
                                    {"protocolVersion": 2}, {"nonce": ""}, {"extra": 1}])
def test_capability_challenges_for_another_executor_or_shape_get_no_proof(change):
    challenge = {**fixture("capability-challenge.json"), **change}
    assert answer_capability_challenge(challenge, executor_id=EXECUTOR_ID, connection_revision=REVISION,
                                       runtime_version="x") is None


POOL_ENV = {"ARTIFACTBRIDGE_POOL_ENABLED": "1", "ARTIFACTBRIDGE_POOL_EXECUTOR_CREDENTIAL": EXECUTOR_CREDENTIAL,
            "ARTIFACTBRIDGE_POOL_EXECUTOR_ID": EXECUTOR_ID, "ARTIFACTBRIDGE_POOL_CONNECTION_REVISION": REVISION}


@pytest.mark.parametrize("missing", [None, *POOL_ENV])
def test_capability_proof_is_answered_only_when_pooled_mode_is_configured(monkeypatch, missing):
    for name, value in POOL_ENV.items():
        monkeypatch.setenv(name, value) if name != missing else monkeypatch.delenv(name, raising=False)
    proof = plugin_module.capability_proof(fixture("capability-challenge.json"))
    if missing is None:
        assert proof == {**fixture("capability-proof.json"), "runtimeVersion": plugin_module.VERSION}
        manifest = (fake_pool.FIXTURES.parents[2] / "plugin.yaml").read_text()
        assert f"\nversion: {plugin_module.VERSION}\n" in manifest
        assert "agx_" not in repr(proof)
    else:
        assert proof is None


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

    return PoolControl(tools_url=fake_pool.TOOLS_URL, credential=EXECUTOR_CREDENTIAL, http_factory=factory), seen


def outcome_of(call):
    try:
        return "ok", call()
    except Refused as refused:
        return "refused", refused.code
    except ControlUnavailable:
        return "unavailable", None


BUSY = (409, fixture("scheduler-busy-response.json"))
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
    assert outcome_of(lambda: control.start(fixture("start-request.json"))) == expected
    assert len(seen) == requests


BOOTSTRAPPED = fixture("bootstrap-response.json")


@pytest.mark.parametrize("responses, expected", [
    ([(200, BOOTSTRAPPED)], ("ok", BOOTSTRAPPED["credential"])),
    ([(200, {**BOOTSTRAPPED, "extra": 1})], ("unavailable", None)),
    ([(200, {**BOOTSTRAPPED, "credential": "dlc_short"})], ("unavailable", None)),
    ([(200, {**BOOTSTRAPPED, "credential": "dlt_" + "B" * 43})], ("unavailable", None)),
    ([(200, {**BOOTSTRAPPED, "expiresAt": "2026-10-17T11:15:00"})], ("unavailable", None)),
    ([(409, {"error": {"code": "assignment_not_live", "message": "x"}})], ("refused", "assignment_not_live")),
    ([(401, fixture("unauthorized-response.json"))], ("refused", "unauthorized")),
], ids=["fixture", "extra-key", "short", "dlt", "no-offset", "named", "401"])
def test_bootstrap_accepts_only_the_strict_credential_shape(responses, expected):
    control, seen = answering(*responses)
    assert outcome_of(lambda: control.bootstrap(fixture("bootstrap-request.json"))) == expected
    assert seen[0].url.path == "/api/agent-gateway/pool/bootstrap"


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
    control, _seen = answering(*responses)
    assert outcome_of(lambda: control.report(fixture("report.json"))) == expected
    if responses[0][1] == {"recorded": True, "currentOccupant": False}:
        assert "superseded" in caplog.text


# ---- durable report delivery ----

def test_a_report_enqueued_before_a_restart_is_replayed_by_the_new_process(make_core, server, journal_path):
    server.faults[REPORT_PATH] = ["down"]
    assert make_core().admit(server.issue(envelope()), CTX, "task-1").kind == "spawn"
    assert pending(journal_path) == [[(1, "running")]] and server.reports == []
    make_core().recover()  # a new process: new Journal and PoolCore on the same file
    assert sent(server.reports) == [(1, "running"), (2, "stopped")]  # the replay, then its own crash report
    assert pending(journal_path) == [[]]


def test_a_report_whose_response_was_lost_is_cleared_by_its_replay(core, server, journal_path):
    server.faults[REPORT_PATH] = ["lost"]  # the server records it; the response never arrives
    core.admit(server.issue(envelope()), CTX, "task-1")
    assert sent(server.reports) == [(1, "running")] and pending(journal_path) == [[(1, "running")]]
    core.finish("task-1", COMPLETED, "done")
    assert sent(server.replays) == [(1, "running")] and sent(server.reports) == [(1, "running"), (2, "stopped")]
    assert pending(journal_path) == [[]]


def test_a_terminal_report_never_overtakes_a_pending_running_report(core, server, journal_path):
    server.faults[REPORT_PATH] = ["down", "down"]
    metadata = server.issue(envelope())
    core.admit(metadata, CTX, "task-1")
    core.finish("task-1", COMPLETED, "done")  # the running report fails again, so the stopped one waits
    assert server.reports == [] and report_requests(server) == [1, 1]
    assert pending(journal_path) == [[(1, "running"), (2, "stopped")]]
    replay = core.admit(metadata, CTX, "task-2")  # the next admit replays, oldest sequence first
    assert (replay.outcome, replay.text) == (COMPLETED, "done")
    assert sent(server.reports) == [(1, "running"), (2, "stopped")] and pending(journal_path) == [[]]


def test_a_scheduler_busy_report_is_retried_inline_then_stays_pending(core, server, journal_path):
    server.faults[REPORT_PATH] = ["busy"]
    core.admit(server.issue(envelope()), CTX, "task-1")
    assert sent(server.reports) == [(1, "running")] and report_requests(server) == [1, 1]
    server.faults[REPORT_PATH] = ["busy"] * 3
    core.finish("task-1", COMPLETED, "done")
    assert pending(journal_path) == [[(2, "stopped")]] and report_requests(server) == [1, 1, 2, 2, 2]
    core.deliver_reports()
    assert sent(server.reports) == [(1, "running"), (2, "stopped")] and pending(journal_path) == [[]]


def test_a_permanently_refused_report_is_recorded_apart_from_the_state_and_never_retried(core, server, journal_path):
    metadata = server.issue(envelope())
    server.refuse[REPORT_PATH] = "assignment_not_live"
    core.admit(metadata, CTX, "task-1")
    row = rows(journal_path)[0]
    assert (row["state"], row["pending_reports"], row["report_refusal"]) == ("spawned", None, "assignment_not_live")
    del server.refuse[REPORT_PATH]
    core.finish("task-1", COMPLETED, "done")
    core.recover()
    assert sent(server.reports) == [(2, "stopped")] and report_requests(server) == [1, 2]


def test_a_replay_waits_for_the_new_dispatchs_start_and_stops_at_an_unavailable_server(core, server, journal_path):
    server.faults[REPORT_PATH] = ["down"]
    core.admit(server.issue(envelope()), CTX, "task-1")  # its running report stays pending
    other = envelope(binding={"delegationId": uid(50), "assignmentId": uid(51)}, dispatchId="dispatch-9")
    server.faults[REPORT_PATH] = ["down"]
    before = len(server.requests)
    assert core.admit(server.issue(other), "", "task-2").kind == "spawn"
    assert [(r["path"], (r["body"] or {}).get("reportSequence")) for r in server.requests[before:]] == [
        (START_PATH, None), (BOOTSTRAP_PATH, None), (REPORT_PATH, 1)]  # one failed replay ends the pass
    assert pending(journal_path) == [[(1, "running")], [(1, "running")]]
    core.deliver_reports()
    assert pending(journal_path) == [[], []] and len(server.reports) == 2


def test_an_existing_journal_gains_the_report_columns_and_keeps_its_rows(make_core, server, journal_path):
    metadata = server.issue(envelope())
    assert make_core().admit(metadata, CTX, "task-1").kind == "spawn"
    with sqlite3.connect(journal_path) as conn:  # the 0.2.0.dev0 schema had neither column
        conn.execute("ALTER TABLE dispatches DROP COLUMN pending_reports")
        conn.execute("ALTER TABLE dispatches DROP COLUMN report_refusal")
    make_core()  # opening twice must not re-add a column
    restarted = make_core()
    restarted.recover()
    assert [(r["state"], r["report_refusal"]) for r in rows(journal_path)] == [("uncertain", None)]
    assert sent(server.reports) == [(1, "running"), (2, "stopped")] and pending(journal_path) == [[]]


# ---- start authority (GAP-A/B scaffolding), refusals, retries ----

def test_a_cloud_assignment_the_server_did_not_start_is_rejected_without_bootstrap(core, server, journal_path):
    server.cloud = True
    metadata = server.issue(envelope())
    decision = core.admit(metadata, CTX, "task-1")
    assert (decision.kind, decision.outcome) == ("terminal", REJECTED) and "start_not_authorized" in decision.text
    again = core.admit(metadata, CTX, "task-2")
    assert again.text == decision.text and len(server.starts) == 1
    assert server.bootstraps == [] and server.reports == [] and core.credentials._bound == {}
    assert [(r["state"], r["refusal"]) for r in rows(journal_path)] == [("refused", "start_not_authorized")]


def test_a_cloud_assignment_started_under_the_servers_own_journal_is_start_owned(core, server):
    server.cloud = True
    metadata = server.issue(envelope())
    server.journals[envelope()["binding"]["assignmentId"]] = "server-chosen-journal"
    decision = core.admit(metadata, CTX, "task-1")
    assert decision.outcome == REJECTED and "start_owned" in decision.text and server.bootstraps == []


@pytest.mark.parametrize("refusal", ["named", "unauthorized"])
def test_a_refused_or_unauthorized_start_is_terminal_and_never_retried(make_core, server, journal_path, refusal, caplog):
    caplog.set_level(logging.DEBUG)
    if refusal == "named":
        server.refuse[START_PATH] = "assignment_not_live"
        core, code = make_core(), "assignment_not_live"
    else:
        core, code = make_core(credential="agx_" + "W" * 40), "unauthorized"
    metadata = server.issue(envelope())
    assert code in core.admit(metadata, CTX, "task-1").text
    assert code in core.admit(metadata, CTX, "task-2").text
    assert len(server.requests) == 1 and server.bootstraps == []
    assert [(r["state"], r["refusal"]) for r in rows(journal_path)] == [("refused", code)]
    assert "agx_" not in caplog.text


def test_a_busy_start_is_retried_inline_and_an_exhausted_one_stays_retryable(core, server, journal_path):
    server.faults[START_PATH] = ["busy", "busy", "busy"]
    metadata = server.issue(envelope())
    decision = core.admit(metadata, CTX, "task-1")
    assert decision.outcome == FAILED and "start_unavailable" in decision.text
    assert rows(journal_path)[0]["state"] == "start_requested" and server.starts == []
    server.faults[START_PATH] = ["busy"]
    assert core.admit(metadata, CTX, "task-2").kind == "spawn"
    assert [r["path"] for r in server.requests].count(START_PATH) == 5 and len(server.starts) == 1


def test_a_bootstrap_refusal_after_the_permit_never_spawns(core, server, journal_path):
    server.refuse[BOOTSTRAP_PATH] = "assignment_not_live"
    decision = core.admit(server.issue(envelope()), CTX, "task-1")
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
        make_core().admit(metadata, CTX, "task-1")
    assert rows(journal_path)[0]["state"] == "permitted"
    server.on_bootstrap = None
    restarted = make_core()
    restarted.recover()
    assert rows(journal_path)[0]["state"] == "start_requested"
    assert restarted.admit(metadata, CTX, "task-2").kind == "spawn"
    journal_id = rows(journal_path)[0]["start_journal_id"]
    assert [s["startJournalId"] for s in server.starts] == [journal_id] * 2
    assert [b["startJournalId"] for b in server.bootstraps] == [journal_id] * 2
    first, second = server.issued
    assert server.revoked == {first} and restarted.credentials.lookup(POOL_PLATFORM, CTX, "task-2") == second
    assert sent(server.reports) == [(1, "running")]


def test_a_dispatch_id_reused_by_another_assignment_is_refused_without_reading_its_row(core, server, journal_path):
    core.admit(server.issue(envelope()), CTX, "task-1")
    core.finish("task-1", COMPLETED, "first assignment's result")
    other = envelope(binding={"assignmentId": uid(60), "slotId": uid(61)})  # same dispatchId "dispatch-1"
    before = len(server.requests)
    decision = core.admit(server.issue(other), "", "task-2")
    assert decision.outcome == REJECTED and "tuple_mismatch" in decision.text
    assert "first assignment" not in decision.text and decision.context_id == ""
    assert len(server.requests) == before and len(rows(journal_path)) == 1
    assert core.credentials.lookup(POOL_PLATFORM, decision.context_id, "task-2") is None


# ---- capability transport ----

WORKSPACE = envelope()["binding"]["workspaceId"]


def test_the_capability_transport_answers_the_servers_challenge_and_never_claims_consent(core, server):
    verdict = core.control.prove_capability(WORKSPACE, executor_id=EXECUTOR_ID, connection_revision=REVISION,
                                            runtime_version="0.0.0-test")
    assert verdict == {"eligibility": "pool_capable", "capabilityRevision": "1", "executorConnectionRevision": REVISION}
    get, post = server.requests
    assert (get["method"], get["path"], dict(httpx2.URL(get["url"]).params)) == (
        "GET", CAPABILITY_PATH, {"workspaceId": WORKSPACE, "executorId": EXECUTOR_ID,
                                 "executorConnectionRevision": REVISION})
    assert (post["method"], dict(httpx2.URL(post["url"]).params)) == ("POST", {"workspaceId": WORKSPACE})
    assert set(post["body"]) == set(fixture("capability-proof.json"))  # no consent field exists to claim
    assert post["authorization"] == f"Bearer {EXECUTOR_CREDENTIAL}"


@pytest.mark.parametrize("responses, expected, requests", [
    ([(200, fixture("capability-challenge.json")), (409, fixture("capability-refusal-response.json"))],
     ("ok", fixture("capability-refusal-response.json")), 2),
    ([(200, {**fixture("capability-challenge.json"), "executorId": uid(99)})], ("unavailable", None), 1),
    ([(200, {**fixture("capability-challenge.json"), "nonce": "short"})], ("unavailable", None), 1),
    ([(200, {**fixture("capability-challenge.json"), "requiredCapabilities": ["open_pool_consent"]})],
     ("unavailable", None), 1),
    ([(401, fixture("unauthorized-response.json"))], ("refused", "unauthorized"), 1),
    ([(200, fixture("capability-challenge.json")), (200, fixture("capability-refusal-response.json"))],
     ("unavailable", None), 2),
    ([(200, fixture("capability-challenge.json")), (200, {**fixture("capability-response.json"), "consent": True})],
     ("unavailable", None), 2),
], ids=["refused-verdict", "other-executor", "short-nonce", "unknown-capability", "401", "refusal-as-200",
        "extra-key"])
def test_the_capability_transport_parses_only_the_contract_shapes(responses, expected, requests):
    control, seen = answering(*responses)
    assert outcome_of(lambda: control.prove_capability(
        WORKSPACE, executor_id=EXECUTOR_ID, connection_revision=REVISION, runtime_version="x")) == expected
    assert len(seen) == requests

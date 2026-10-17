"""Pooled (assignment-bound) execution core, without Hermes: envelope strictness, the durable journal,
start ownership, permit checks, restart recovery, sessions, cancel/report fencing, capability proof."""
import logging
import sqlite3
import threading

import pytest

import hermes_artifactbridge as plugin_module
from hermes_artifactbridge.pool_core import (COMPLETED, FAILED, INPUT_REQUIRED, POOL_PLATFORM, REJECTED,
                                             CredentialBindings, PoolControl, PoolCore)
from hermes_artifactbridge.pool_journal import Journal
from hermes_artifactbridge.pool_protocol import (IMPLEMENTED_CAPABILITIES, answer_capability_challenge,
                                                 envelope_from_metadata, report_body, start_request,
                                                 validate_envelope)
from tests import fake_pool
from tests.fake_pool import EXECUTOR_CREDENTIAL, EXECUTOR_ID, REVISION, Clock, FakeControl, envelope, fixture

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
                        control=PoolControl(tools_url=fake_pool.TOOLS_URL, credential=EXECUTOR_CREDENTIAL,
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


# ---- fixtures and protocol shapes ----

def test_u3_fixtures_parse_and_round_trip_through_the_protocol_builders():
    env = envelope_from_metadata(fixture("dispatch-metadata.json"))
    assert validate_envelope(fixture("mailbox-assignment-command.json")["envelope"]) == env
    assert start_request(env, "journal-1") == fixture("start-request.json")
    assert fixture("start-permit.json")["permit"] == {**env, "startJournalId": "journal-1"}
    assert report_body(env["binding"], 1, "stopped", "completed") == fixture("report.json")
    assert report_body(env["binding"], 2, "cancel_acknowledged", "canceled") == fixture("cancel-ack.json")
    proof = answer_capability_challenge(fixture("capability-challenge.json"), executor_id=EXECUTOR_ID,
                                        connection_revision=REVISION, runtime_version="0.0.0-fixture")
    assert proof == fixture("capability-proof.json")
    assert set(fixture("capability-challenge.json")["requiredCapabilities"]) == set(IMPLEMENTED_CAPABILITIES)


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
    assert server.bootstraps == server.starts  # provisional (G3): bootstrap body = start body
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
    server.lose_start_responses = 1
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
    server.lose_start_responses = 1
    core.admit(server.issue(envelope()), CTX, "task-1")
    before = rows(journal_path)
    for changed in (envelope(binding={"slotId": uid(77)}), envelope(startBy="2026-10-18T12:00:00.000Z"),
                    envelope(setupRevision=2)):
        decision = core.admit(server.issue(changed), CTX, "task-2")
        assert decision.outcome == REJECTED and "tuple_mismatch" in decision.text
    assert rows(journal_path) == before and len(server.starts) == 1


def test_a_retry_after_start_by_never_spawns(core, server, clock, journal_path):
    server.lose_start_responses = 1
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


@pytest.mark.parametrize("tamper", [{"setupRevision": 2}, {"startJournalId": "journal-other"},
                                    {"startBy": "2026-10-18T12:00:00.000Z"}, {"extra": 1}])
def test_a_permit_differing_from_the_journaled_envelope_is_refused(core, server, tamper):
    server.tamper_permit = tamper
    decision = core.admit(server.issue(envelope()), CTX, "task-1")
    assert decision.outcome == REJECTED and "permit_mismatch" in decision.text
    assert server.bootstraps == []


@pytest.mark.parametrize("reason", ["start_owned", "start_deadline_passed", "assignment_withdrawn"])
def test_start_refusals_are_terminal_and_replayed_without_restarting(core, server, journal_path, reason):
    server.refuse_with = reason
    metadata = server.issue(envelope())
    assert reason in core.admit(metadata, CTX, "task-1").text
    again = core.admit(metadata, CTX, "task-2")
    assert again.outcome == REJECTED and reason in again.text
    assert len(server.starts) == 1 and server.bootstraps == []
    assert [(r["state"], r["refusal"]) for r in rows(journal_path)] == [("refused", reason)]


def test_another_journal_owning_the_start_gets_start_owned(core, server):
    metadata = server.issue(envelope())
    server.owners[(envelope()["binding"]["assignmentId"], "dispatch-1")] = "someone-elses-journal"
    decision = core.admit(metadata, CTX, "task-1")
    assert decision.outcome == REJECTED and "start_owned" in decision.text and server.bootstraps == []


@pytest.mark.parametrize("bootstrap", ["unavailable", "redirect"])
def test_control_failures_never_spawn(core, server, bootstrap):
    if bootstrap == "redirect":
        server.redirect = True
    else:
        server.bootstrap_fails = True
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
    server.refuse_with = "assignment_withdrawn"
    core.admit(server.issue(envelope()), CTX, "task-1")
    server.refuse_with = None
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


# ---- capability proof (G5) ----

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

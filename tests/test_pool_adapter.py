"""The ``artifactbridge-pool`` listener over real HTTP against the real Hermes stock A2A adapter and the real
Hermes message pipeline (skipped without Hermes). Requests are the generated
cloud-contract-r2-terminal-clarification-1 fixtures (tests/fixtures/cloud_r2_terminal_clarification_1), sent
to the listener's real ``connect()`` socket with the server's headers; only the model (``_message_handler``)
and the server's control routes (FakeControl) are stand-ins."""
import asyncio
import contextlib
import copy
import http.client
import json
import sqlite3
import threading
import urllib.parse

import pytest

pytest.importorskip("plugins.platforms.a2a.adapter", exc_type=ImportError)

from gateway.config import PlatformConfig  # noqa: E402
from gateway.platform_registry import PlatformEntry, platform_registry  # noqa: E402
from plugins.platforms.a2a import _PLATFORM_HINT, protocol  # noqa: E402

import hermes_artifactbridge as plugin_module  # noqa: E402
from hermes_artifactbridge import pool_adapter, register  # noqa: E402
from hermes_artifactbridge.pool_core import POOL_PLATFORM, CredentialBindings, PoolControl, PoolCore  # noqa: E402
from hermes_artifactbridge.pool_journal import Journal  # noqa: E402
from hermes_artifactbridge.pool_protocol import report_body  # noqa: E402
from tests import fake_ab, fake_pool  # noqa: E402
from tests.conftest import FakeCtx  # noqa: E402
from tests.fake_pool import EXECUTOR_ID, REVISION, Clock, FakeControl, envelope, fixture, grant_for  # noqa: E402

POOL_ENV = {"ARTIFACTBRIDGE_POOL_ENABLED": "1", "ARTIFACTBRIDGE_POOL_EXECUTOR_ID": EXECUTOR_ID,
            "ARTIFACTBRIDGE_POOL_CONNECTION_REVISION": REVISION}
SEND = fixture("cloud-send-request.json")
BEARER = SEND["headers"]["authorization"].removeprefix("Bearer ")
CONTEXT = SEND["body"]["params"]["message"]["contextId"]
AGX, JOURNAL = SEND["headers"]["x-ab-control-credential"], SEND["headers"]["x-ab-start-journal-id"]
SECRETS = (b"agx_", b"dlc_", BEARER.encode())
TURN_BOUND = 30.0  # generous: the private loop thread can be starved on a loaded host


@pytest.fixture
def pool_env(monkeypatch, tmp_path):
    for name in ("A2A_PEER_TOKENS", "A2A_TRUSTED_PEERS", "A2A_ALLOW_ALL_USERS", "A2A_HOST", "A2A_PORT",
                 "A2A_PUBLIC_URL"):
        monkeypatch.delenv(name, raising=False)
    for name, value in POOL_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("A2A_BEARER_TOKEN", BEARER)
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))


@pytest.fixture
def registered(pool_env):
    """register() against the real PlatformEntry, so an unknown entry kwarg would raise TypeError."""
    ctx, entries = FakeCtx(), []

    def register_platform(*, name, label, adapter_factory, check_fn, validate_config=None, required_env=None,
                          install_hint="", **entry_kwargs):
        entry = PlatformEntry(name=name, label=label, adapter_factory=adapter_factory, check_fn=check_fn,
                              validate_config=validate_config, required_env=required_env or [],
                              install_hint=install_hint, source="plugin", **entry_kwargs)
        platform_registry.register(entry, scope=None)
        entries.append(entry)

    ctx.register_platform = register_platform
    register(ctx)
    yield ctx, entries
    for entry in entries:
        platform_registry.unregister(entry.name, scope=None)


@pytest.fixture
def server():
    return FakeControl(clock=Clock())


class Model:
    """The stand-in model behind Hermes' real message pipeline. ``behavior`` maps the A2A task id's turn
    number (0, 1, ...) to ``"block"`` (wait until released or canceled), ``"stubborn"`` (ignore a cancel
    until released), a reply text (default "done") or a function of the turn's bound dlc_ returning one."""

    def __init__(self, credentials):
        self.credentials = credentials
        self.turns, self.seen_credentials, self.behavior = [], [], {}
        self.started = [threading.Event() for _ in range(4)]
        self.release = threading.Event()

    async def __call__(self, event):
        n = len(self.turns)
        self.turns.append(event)
        self.seen_credentials.append(self.credentials.lookup(event.source.platform.value, event.source.chat_id,
                                                             event.source.message_id))
        self.started[n].set()
        behavior = self.behavior.get(n, "done")
        if behavior == "block":
            await asyncio.to_thread(self.release.wait, TURN_BOUND)
            return "done"
        if behavior == "stubborn":
            while not self.release.is_set():
                try:
                    await asyncio.to_thread(self.release.wait, TURN_BOUND)
                except asyncio.CancelledError:
                    continue
            return "done after all"
        return behavior(self.seen_credentials[n]) if callable(behavior) else behavior


class Listener:
    """One listener process: its own journal connection, core, adapter, loop thread and socket."""

    def __init__(self, journal_path, server, *, connect=True):
        self.credentials = CredentialBindings(now=server.clock)
        self.core = PoolCore(journal=Journal(journal_path),
                             control=PoolControl(tools_url=fake_pool.TOOLS_URL, http_factory=server.factory),
                             executor_id=EXECUTOR_ID, connection_revision=REVISION, credentials=self.credentials,
                             now=server.clock)
        self.adapter = pool_adapter.build_adapter_class()(PlatformConfig(enabled=True, extra={}), core=self.core,
                                                          port=0)
        self.model = Model(self.credentials)
        self.adapter.set_message_handler(self.model)
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.connected = self.run(self.adapter.connect()) if connect else None

    def run(self, coroutine, timeout=TURN_BOUND):
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout)

    @property
    def url(self) -> str:  # the a2aUrl the server would register: the card's pinned interface URL
        return f"http://127.0.0.1:{self.adapter._httpd.server_address[1]}/"

    def request(self, method: str, url: str, headers: dict, body) -> tuple[int, dict]:
        target = urllib.parse.urlsplit(url)
        conn = http.client.HTTPConnection("127.0.0.1", self.adapter._httpd.server_address[1], timeout=TURN_BOUND)
        try:
            payload = None if body is None else json.dumps(body).encode()
            conn.putrequest(method, target.path or "/", skip_accept_encoding=True)
            for name, value in headers:
                conn.putheader(name, value)
            if payload is not None:
                conn.putheader("content-length", str(len(payload)))
            conn.endheaders(payload)
            response = conn.getresponse()
            return response.status, json.loads(response.read() or b"null")
        finally:
            conn.close()

    def call(self, name: str, *, body=None, headers=None, drop=(), extra=()) -> tuple[int, dict]:
        """The fixture request ``name`` on this listener: its headers (minus ``drop``, plus ``extra``), its
        body or ``body``; the card's URL resolves from the registered a2aUrl as the server resolves it."""
        request = fixture(name)
        pairs = [(k, v) for k, v in (headers if headers is not None else request["headers"]).items() if k not in drop]
        url = (urllib.parse.urljoin(self.url, ".well-known/agent-card.json") if request["method"] == "GET"
               else self.url)
        return self.request(request["method"], url, [*pairs, *extra], request["body"] if body is None else body)

    def crash(self):
        """The process dies: the socket closes and the loop stops; no shutdown code runs, so the in-flight
        turn records nothing."""
        httpd, self.adapter._httpd = self.adapter._httpd, None
        httpd.shutdown()
        httpd.server_close()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)

    def stop(self):
        """Test teardown, also for a crashed listener: release the model, cancel and await every task still
        on the loop (released, stubborn, canceled or abandoned by the crash), then close the loop."""
        if self.adapter._httpd is not None:
            self.run(self.adapter.disconnect())
        self.model.release.set()
        if self.thread.is_alive():
            self.run(self._drain())
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(5)
        else:  # crashed: its loop stopped with the turn in flight
            self.loop.run_until_complete(self._drain())
        self.loop.run_until_complete(self.loop.shutdown_default_executor())
        self.loop.close()

    @staticmethod
    async def _drain():
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.fixture
def journal_path(tmp_path):
    return tmp_path / "journal.sqlite3"


@pytest.fixture
def listeners(registered, server, journal_path):
    started = []

    def start(**kwargs):
        started.append(Listener(journal_path, server, **kwargs))
        return started[-1]

    yield start
    for listener in reversed(started):
        if not listener.loop.is_closed():
            listener.stop()


@pytest.fixture
def listener(listeners):
    return listeners()


def with_id(name: str, task_id: str) -> dict:
    """A Get/Cancel fixture body for the remote task id the server recorded from the Send answer."""
    body = copy.deepcopy(fixture(name)["body"])
    body["params"]["id"] = task_id
    return body


def journaled(journal_path, journal=JOURNAL) -> list[dict]:
    with Journal(journal_path).tx() as tx:
        return tx.where("start_journal_id", journal)


def no_tasks(listener) -> bool:
    return listener.adapter.tasks.list()[0] == []


def task_of(answer: dict) -> dict:
    return answer["result"]["task"] if "task" in answer["result"] else answer["result"]


def wait_for(event: threading.Event):
    assert event.wait(TURN_BOUND), "the turn never reached the model"


def settle(listener, task_id: str):
    """Until this task's finalizer recorded its outcome."""
    finalizer = listener.adapter._finalizers.get(task_id)
    if finalizer is not None:
        finalizer.join(TURN_BOUND)
        assert not finalizer.is_alive()


# ---- shape and registration ----

def test_this_hermes_is_the_tested_shape_and_an_untested_version_is_refused(monkeypatch):
    assert pool_adapter.hermes_shape_supported() is True
    monkeypatch.setattr("hermes_cli.plugins_manifest.running_hermes_version", lambda: "0.23.0")
    assert pool_adapter.hermes_shape_supported() is False


def test_a_changed_stock_send_final_ledgered_refuses_pooled_registration(pool_env, monkeypatch, caplog):
    from plugins.platforms.a2a.adapter import A2AAdapter

    async def changed(self, event, session_key, text_content, metadata, *, reply_to):
        raise AssertionError("never called")

    monkeypatch.setattr(A2AAdapter, "send_final_ledgered", changed)
    assert pool_adapter.hermes_shape_supported() is False
    ctx, entries = FakeCtx(), []
    ctx.register_platform = lambda **entry: entries.append(entry)
    register(ctx)
    assert entries == [] and [t for t in ctx.tools if t["toolset"] == "artifactbridge-pool"] == []
    assert "outside the tested range" in caplog.text


def test_the_final_reply_reaches_the_stock_ledger_redacted_with_its_keywords_unchanged(listeners, monkeypatch):
    from gateway.platforms.base import BasePlatformAdapter
    seen = []

    async def stock(self, event, session_key, text_content, metadata, *, reply_to, is_ephemeral_response=False):
        seen.append((event, session_key, text_content, metadata, reply_to, is_ephemeral_response))
        return "sent", self

    monkeypatch.setattr(BasePlatformAdapter, "send_final_ledgered", stock)
    listener = listeners(connect=False)
    event, metadata, dlc = object(), {"notify": True}, "dlc_" + "B" * 43
    result = listener.run(listener.adapter.send_final_ledgered(event, "session", f"Keys: {AGX} and {dlc}.", metadata,
                                                               reply_to="task-1", is_ephemeral_response=True))
    assert result == ("sent", listener.adapter)
    assert seen == [(event, "session", "Keys: [redacted] and [redacted].", metadata, "task-1", True)]


def test_configured_registration_adds_a_separate_listener_and_pooled_tools(registered):
    ctx, entries = registered
    assert [(e.name, e.platform_hint, e.allow_update_command) for e in entries] == [(POOL_PLATFORM, _PLATFORM_HINT, False)]
    pooled = [t for t in ctx.tools if t["toolset"] == "artifactbridge-pool"]
    assert len(pooled) == 11 and all(t["check_fn"]() for t in pooled)
    assert len(ctx.tools) == 22 and entries[0].is_connected(None) is True


def test_factory_builds_on_the_pooled_platform_and_port_with_a_journal_under_hermes_home(registered, monkeypatch, tmp_path):
    monkeypatch.setenv("ARTIFACTBRIDGE_POOL_PORT", "9911")
    built = plugin_module._pool_adapter_factory(PlatformConfig(enabled=True, extra={"port": 9912}))
    assert (built.platform.value, built.port, built.name) == (POOL_PLATFORM, 9911, "ArtifactBridge pool")
    assert (tmp_path / "home" / "artifactbridge" / "pool-journal.sqlite3").exists()
    monkeypatch.delenv("ARTIFACTBRIDGE_POOL_PORT")
    assert plugin_module._pool_adapter_factory(PlatformConfig(enabled=True, extra={"port": 9912})).port == 9912


# ---- bearer, card, capability, method set ----

def test_without_a_remote_bearer_the_listener_never_binds(listeners, monkeypatch):
    monkeypatch.delenv("A2A_BEARER_TOKEN")
    listener = listeners(connect=False)
    assert listener.run(listener.adapter.connect()) is False
    assert listener.adapter._httpd is None and listener.adapter.fatal_error_code == "bearer_required"


@pytest.mark.parametrize("name", ["cloud-card-request.json", "cloud-capability-request.json",
                                  "cloud-send-request.json", "cloud-get-task-request.json",
                                  "cloud-list-tasks-request.json", "cloud-cancel-request.json"])
@pytest.mark.parametrize("authorization", [None, "Bearer " + "W" * 43], ids=["absent", "wrong"])
def test_the_card_and_every_method_require_the_remote_bearer(listener, server, journal_path, name, authorization):
    server.issue(envelope())
    extra = [("authorization", authorization)] if authorization else []
    status, answer = listener.call(name, drop=("authorization",), extra=extra)
    assert (status, answer["error"]["code"]) == (401, protocol.ERR_UNAUTHORIZED)
    assert listener.model.turns == [] and server.requests == [] and no_tasks(listener) and journaled(journal_path) == []


def test_the_card_pins_the_registered_listener_url_on_the_jsonrpc_binding(listener):
    status, card = listener.call("cloud-card-request.json")
    assert status == 200
    assert [(i["url"], i["protocolBinding"]) for i in card["supportedInterfaces"]] == [(listener.url, "JSONRPC")]


def test_the_capability_request_is_answered_from_this_executors_configuration(listener, server):
    status, answer = listener.call("cloud-capability-request.json")
    expected = fixture("cloud-capability-result.json")
    assert status == 200 and answer["id"] == expected["id"]
    assert answer["result"] == {**expected["result"], "runtimeVersion": plugin_module.VERSION}
    # Historical input (tests/fixtures/cloud_r2, cloud-contract-r2): the old seven-capability probe also requires
    # the withdrawn same_session_continuation and is refused, never answered with a capability this runner lacks.
    old_seven = json.loads((fake_pool.FIXTURES.parent / "cloud_r2" / "cloud-capability-request.json").read_text())
    status, refused = listener.call("cloud-capability-request.json", headers=old_seven["headers"],
                                    body=old_seven["body"])
    assert status == 200 and "result" not in refused
    assert refused["error"] == {**refused["error"], "code": protocol.ERR_INVALID_PARAMS,
                                "message": "ArtifactBridge pooled dispatch refused: capability_mismatch"}
    assert server.requests == [] and listener.model.turns == []


def test_the_capability_answer_is_none_of_the_shapes_the_servers_verdicts_judge_incompatible(listener):
    six = fixture("cloud-capability-request.json")["body"]["params"]["requiredCapabilities"]
    answered = listener.call("cloud-capability-request.json")[1]["result"]["capabilities"]
    assert answered == six and len(set(answered)) == 6 and "same_session_continuation" not in answered
    # The judged answers: old seven (six + same_session_continuation), six + an unknown name, six + a repeat.
    assert answered not in (six + ["same_session_continuation"], six + ["unknown_capability"], six + six[:1])
    incompatible = {"outcome": "failed", "reasonCode": "unsupported", "incompatible": True}
    for name in ("old-seven", "unknown-capability", "duplicate-capability", "missing-capability", "wrong-protocol"):
        assert fixture(f"cloud-capability-verdict-{name}.json") == incompatible
    # A wrong nonce fails the probe without marking the runtime incompatible.
    assert fixture("cloud-capability-verdict-wrong-nonce.json") == {**incompatible, "incompatible": False}


@pytest.mark.parametrize("change", [{"executorConnectionRevision": "2"}, {"protocolVersion": 2},
                                    {"requiredCapabilities": ["open_pool_consent", "pool_lease_v1"]}],
                         ids=["other-revision", "other-protocol", "missing-capability"])
def test_a_capability_request_this_executor_cannot_satisfy_gets_an_error_not_a_result(listener, change):
    body = copy.deepcopy(fixture("cloud-capability-request.json")["body"])
    body["params"].update(change)
    status, answer = listener.call("cloud-capability-request.json", body=body)
    assert status == 200 and "result" not in answer and answer["error"]["code"] == protocol.ERR_INVALID_PARAMS


@pytest.mark.parametrize("method", ["SendStreamingMessage", "message/send", "tasks/cancel", "SubscribeToTask",
                                    "CreateTaskPushNotificationConfig", "artifactbridge/pool/unknown"])
def test_every_other_method_is_method_not_found_and_never_reaches_the_stock_handlers(listener, server, method):
    body = {**SEND["body"], "method": method, "id": fixture("cloud-capability-error-method-not-found.json")["id"]}
    status, answer = listener.call("cloud-send-request.json", body=body)
    assert (status, answer) == (200, fixture("cloud-capability-error-method-not-found.json"))
    assert listener.model.turns == [] and server.requests == []


# ---- start headers ----

@pytest.mark.parametrize("drop, extra", [
    (("x-ab-control-credential",), ()),
    (("x-ab-start-journal-id",), ()),
    (("x-ab-pool-protocol-version",), ()),
    ((), (("x-ab-control-credential", AGX),)),
    ((), (("X-AB-Start-Journal-Id", JOURNAL),)),
    (("x-ab-control-credential",), (("x-ab-control-credential", ""),)),
    (("x-ab-control-credential",), (("x-ab-control-credential", "dlc_" + "A" * 43),)),
    (("x-ab-start-journal-id",), (("x-ab-start-journal-id", "agj_" + "0" * 129),)),
    (("x-ab-pool-protocol-version",), (("x-ab-pool-protocol-version", "2"),)),
], ids=["no-agx", "no-journal", "no-version", "duplicate-agx", "duplicate-journal", "empty-agx", "dlc-as-agx",
        "long-journal", "other-version"])
@pytest.mark.parametrize("name", ["cloud-send-request.json", "cloud-get-task-request.json",
                                  "cloud-list-tasks-request.json", "cloud-cancel-request.json"])
def test_missing_duplicate_or_malformed_start_headers_are_refused_before_any_task(listener, server, journal_path,
                                                                                  name, drop, extra):
    server.issue(envelope())
    status, answer = listener.call(name, drop=drop, extra=extra)
    assert status == 200 and answer["error"]["code"] == protocol.ERR_INVALID_PARAMS
    assert "invalid_pool_headers" in answer["error"]["message"] and "result" not in answer
    assert listener.model.turns == [] and server.requests == [] and no_tasks(listener) and journaled(journal_path) == []


def test_a_grant_for_another_assignment_is_refused_without_a_row_or_a_start(listener, server, journal_path):
    server.issue(envelope())
    other = envelope(binding={"assignmentId": "6f1f7d9e-0a1b-4c2d-8e3f-000000000042"})
    agx, journal = grant_for(other)
    headers = {**SEND["headers"], "x-ab-control-credential": agx, "x-ab-start-journal-id": journal}
    status, answer = listener.call("cloud-send-request.json", headers=headers)
    task = task_of(answer)
    assert (status, task["status"]["state"]) == (200, protocol.STATE_REJECTED) and "journal_mismatch" in json.dumps(task)
    assert listener.model.turns == [] and server.requests == []
    assert journaled(journal_path) == [] and journaled(journal_path, journal) == []


@pytest.mark.parametrize("change", [
    {"artifactbridge.assignment": "x"}, {},
    {"artifactbridge.assignment": {**envelope(), "protocolVersion": 2}},
    {"artifactbridge.assignment": envelope(binding={"executorId": "6f1f7d9e-0a1b-4c2d-8e3f-000000000099"})},
    {"artifactbridge.assignment": envelope(binding={"executorConnectionRevision": "2"})},
], ids=["malformed", "absent", "unknown-version", "other-executor", "other-revision"])
def test_every_send_is_intercepted_before_any_model_invocation(listener, server, change):
    body = copy.deepcopy(SEND["body"])
    body["params"]["metadata"] = change
    status, answer = listener.call("cloud-send-request.json", body=body)
    task = task_of(answer)
    assert (status, task["status"]["state"]) == (200, protocol.STATE_REJECTED) and "refused" in json.dumps(task)
    assert listener.model.turns == [] and listener.adapter._pending == {} and server.requests == []


# ---- Send ----

def test_send_answers_working_with_the_assignment_and_never_waits_for_the_reply(listener, server):
    listener.model.behavior[0] = "block"
    server.issue(envelope())
    status, answer = listener.call("cloud-send-request.json")
    expected = fixture("cloud-send-response.json")
    task = answer["result"]["task"]
    assert status == 200 and answer["id"] == SEND["body"]["id"] and set(answer) == set(expected)
    assert {k: v for k, v in task.items() if k not in ("id", "history")} == {
        k: v for k, v in expected["result"]["task"].items() if k != "id"} | {"status": task["status"]}
    assert task["status"]["state"] == expected["result"]["task"]["status"]["state"] == protocol.STATE_WORKING
    assert task["metadata"] == expected["result"]["task"]["metadata"] and task["contextId"] == CONTEXT
    event = listener.model.turns[0]
    assert (event.source.platform.value, event.source.chat_id, event.source.message_id) == (POOL_PLATFORM, CONTEXT,
                                                                                            task["id"])
    assert listener.model.seen_credentials[0] in server.issued
    assert server.starts == [fixture("cloud-start-replay-request.json")]
    assert [r["kind"] for r in server.reports] == ["running"]

    listener.model.release.set()
    settle(listener, task["id"])
    status, got = listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task["id"]))
    assert got["result"]["status"]["state"] == protocol.STATE_COMPLETED and got["result"]["metadata"] == task["metadata"]
    assert server.reports[-1] == {**fixture("report.json"), "reportSequence": 2}
    assert listener.credentials.lookup(POOL_PLATFORM, CONTEXT, task["id"]) is None

    status, again = listener.call("cloud-send-request.json")  # a lost ack: the same task, never a second turn
    assert (task_of(again)["id"], task_of(again)["status"]["state"]) == (task["id"], protocol.STATE_COMPLETED)
    assert len(listener.model.turns) == 1 and len(server.starts) == 1


def test_a_send_hermes_has_not_admitted_yet_answers_submitted_never_working(listener, server, monkeypatch):
    monkeypatch.setattr(pool_adapter, "_ADMISSION_WAIT", 0.2)
    gate = threading.Event()
    listener.loop.call_soon_threadsafe(gate.wait, TURN_BOUND)  # the gateway loop is busy: no turn can start
    server.issue(envelope())
    try:
        _, sent = listener.call("cloud-send-request.json")
    finally:
        gate.set()
    task = task_of(sent)
    assert task["status"]["state"] == protocol.STATE_SUBMITTED and server.reports == []
    wait_for(listener.model.started[0])
    settle(listener, task["id"])
    _, got = listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task["id"]))
    assert got["result"]["status"]["state"] == protocol.STATE_COMPLETED
    assert [r["kind"] for r in server.reports] == ["running", "stopped"]


def test_concurrent_duplicate_sends_run_one_turn_and_answer_one_task(listener, server):
    listener.model.behavior[0] = "block"
    server.issue(envelope())
    answers = []
    threads = [threading.Thread(target=lambda: answers.append(listener.call("cloud-send-request.json")))
               for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(TURN_BOUND)
    assert len(listener.model.turns) == 1 and len(server.bootstraps) == 1
    assert len({task_of(a)["id"] for _, a in answers}) == 1 and len(answers) == 4


@pytest.mark.parametrize("reply, shown", [
    ("done", "done"),
    ("[INPUT_REQUIRED] Which tone?", "Which tone?"),
    (lambda dlc: f"Keys: {AGX} and {dlc}.", "Keys: [redacted] and [redacted]."),  # the turn leaks its own
    (lambda dlc: f"[INPUT_REQUIRED] Is it {AGX} or {dlc}?", "Is it [redacted] or [redacted]?"),
], ids=["completed", "clarification", "completed-echoing-credentials", "clarification-echoing-credentials"])
def test_no_secret_reaches_the_model_tasks_journal_logs_or_answers(listener, server, journal_path, tmp_path, caplog,
                                                                   reply, shown):
    listener.model.behavior[0] = reply
    server.issue(envelope())
    _, sent = listener.call("cloud-send-request.json")
    task_id = task_of(sent)["id"]
    settle(listener, task_id)
    _, got = listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task_id))
    _, listed = listener.call("cloud-list-tasks-request.json")
    for task in (got["result"], *listed["result"]["tasks"]):  # the reply is visible, its credentials are not
        assert _text(task).endswith(shown)
        assert task["metadata"].get(CLOUD_TERMINAL, {}).get("question") in (None, shown)
    answers = [sent, got, listed]
    blob = (json.dumps(answers) + listener.model.turns[0].text + repr(listener.adapter.tasks.list())
            + caplog.text).encode()
    blob += b"".join(p.read_bytes() for p in tmp_path.rglob("*") if p.is_file())  # journal, audit, history
    assert [s for s in SECRETS if s in blob] == []
    assert listener.model.seen_credentials[0].startswith("dlc_")  # the turn held, and could echo, a real dlc_


# ---- clarification and continuation ----

# PM's exact sender copy (cloud-clarification-amendment/copy.md): the runner cannot confirm a Room post.
UNCONFIRMED = ("The task ended, but we couldn't confirm that its question was posted in the Room. The question is "
               "included below. Send the task again as a new request with your answer and the context it needs.")
UNAVAILABLE = ("The task ended, but we couldn't confirm that its question was posted in the Room. The question is "
               "unavailable. Send the task again as a new request with the context it needs.")
CANNOT_RESUME = ("This cloud task cannot resume. Send the task again as a new request with your answer and the "
                 "context it needs.")
CLOUD_TERMINAL = "artifactbridge.cloudTerminal"
ASKED = {"reason": "clarification_unsupported", "question": "Which tone?"}
REGION = fixture("cloud-terminal-clarification-task.json")["result"]["metadata"][CLOUD_TERMINAL]["question"]
WIDE = "\U0001F600" * 19_999 + "é"  # 20,000 code points: 40,000 UTF-16 units, yet within the code-point limit
BINDING = envelope()["binding"]


def _asked(listener, server, reply) -> str:
    """The six-capability handshake, then the fixture Send whose one turn replies ``reply``; returns its task id
    once its finalizer recorded the outcome."""
    assert "result" in listener.call("cloud-capability-request.json")[1]
    listener.model.behavior[0] = reply
    server.issue(envelope())
    _, sent = listener.call("cloud-send-request.json")
    task_id = task_of(sent)["id"]
    settle(listener, task_id)
    return task_id


def _text(task) -> str:
    return "".join(part["text"] for part in task["status"]["message"]["parts"])


@pytest.mark.parametrize("reply, terminal, text, typed", [
    ("[INPUT_REQUIRED] Which tone?", ASKED, f"{UNCONFIRMED}\n\nWhich tone?", None),
    (f"[INPUT_REQUIRED] {REGION}", {"reason": "clarification_unsupported", "question": REGION},
     f"{UNCONFIRMED}\n\n{REGION}", "cloud-terminal-clarification-task.json"),
    ("[input_required] \n ", {"reason": "clarification_unsupported", "question": None}, UNAVAILABLE,
     "cloud-terminal-clarification-null-question-task.json"),
    (f"[INPUT_REQUIRED]\n {WIDE} \n", {"reason": "clarification_unsupported", "question": WIDE},
     f"{UNCONFIRMED}\n\n{WIDE}", None),
    (f"[INPUT_REQUIRED] {WIDE}x", None, "outcome_too_long", None),  # 20,001: the generic failure, never cut short
], ids=["question", "fixture-question", "empty-is-null", "20000-code-points", "20001-is-outcome-too-long"])
def test_a_clarification_fails_with_its_journaled_cloud_terminal_on_get_list_and_a_resent_send(
        listener, server, journal_path, monkeypatch, reply, terminal, text, typed):
    seen, report = [], server._report

    def observed(body):  # the stopped report goes out after the journal commit, before the volatile TaskStore
        if body["kind"] == "stopped":
            row = journaled(journal_path)[0]
            seen.append((listener.adapter.tasks.get(row["task_id"])["state"], row["state"], row["cloud_terminal"]))
        return report(body)

    monkeypatch.setattr(server, "_report", observed)
    task_id = _asked(listener, server, reply)
    expected = fixture("cloud-send-response.json")["result"]["task"]["metadata"] | (
        {CLOUD_TERMINAL: terminal} if terminal else {})
    _, got = listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task_id))
    _, listed = listener.call("cloud-list-tasks-request.json")
    _, again = listener.call("cloud-send-request.json")  # the server lost the Send response: no second turn
    assert len(listed["result"]["tasks"]) == 1
    for task in (got["result"], listed["result"]["tasks"][0], task_of(again)):
        assert (task["id"], task["status"]["state"], task["metadata"]) == (task_id, protocol.STATE_FAILED, expected)
        assert _text(task) == text
        if typed:  # the server's generated typed terminal task
            want = fixture(typed)["result"]
            assert (task["status"]["state"], task["artifacts"], task["metadata"].keys(),
                    task["metadata"][CLOUD_TERMINAL]) == (want["status"]["state"], want["artifacts"],
                                                          want["metadata"].keys(), want["metadata"][CLOUD_TERMINAL])
    row, = journaled(journal_path)
    assert (row["state"], row["result_state"], row["result_text"]) == ("finished", "failed", text)
    assert seen == [(protocol.STATE_WORKING, "finished", row["cloud_terminal"])]
    assert (json.loads(row["cloud_terminal"]) if row["cloud_terminal"] else None) == terminal
    # Content-free reports only, and no Room post by the runner: the server ingests the question with the failure.
    assert server.reports == [report_body(BINDING, 1, "running", None), report_body(BINDING, 2, "stopped", "failed")]
    assert {r["path"] for r in server.requests} == {fake_pool.START_PATH, fake_pool.BOOTSTRAP_PATH, fake_pool.REPORT_PATH}
    assert len(listener.model.turns) == 1 and listener.credentials.lookup(POOL_PLATFORM, CONTEXT, task_id) is None


def test_a_clarification_journaled_before_a_crash_answers_get_and_list_after_restart_by_grant(
        listeners, server, journal_path):
    first = listeners()
    server.faults[fake_pool.REPORT_PATH] = ["down"] * 20  # no report is acknowledged before the crash
    task_id = _asked(first, server, "[INPUT_REQUIRED] Which tone?")
    first.crash()
    server.faults[fake_pool.REPORT_PATH].clear()
    try:
        second = listeners()
        assert [r["state"] for r in journaled(journal_path)] == ["finished"] and server.reports == []
        other_agx, other_journal = grant_for(envelope(binding={"assignmentId": "6f1f7d9e-0a1b-4c2d-8e3f-000000000042"}))
        foreign = {**SEND["headers"], "x-ab-control-credential": other_agx, "x-ab-start-journal-id": other_journal}
        _, unknown = second.call("cloud-get-task-request.json", headers=foreign,
                                 body=with_id("cloud-get-task-request.json", task_id))
        assert unknown["error"]["code"] == protocol.ERR_TASK_NOT_FOUND and server.reports == []
        _, got = second.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task_id))
        _, listed = second.call("cloud-list-tasks-request.json")
        for task in (got["result"], *listed["result"]["tasks"]):
            assert (task["id"], task["status"]["state"]) == (task_id, protocol.STATE_FAILED)
            assert task["metadata"][CLOUD_TERMINAL] == ASKED
            assert _text(task) == f"{UNCONFIRMED}\n\nWhich tone?"
        # The authenticated Get restored the agx_: both journaled reports go out in order, once each.
        assert server.reports == [report_body(BINDING, 1, "running", None),
                                  report_body(BINDING, 2, "stopped", "failed")]
        assert second.model.turns == [] and len(server.starts) == 1 and len(server.bootstraps) == 1
    finally:
        first.stop()


def test_a_turn_the_runtime_never_answered_reports_timed_out_never_a_confirmed_stop(listener, server,
                                                                                    monkeypatch):
    monkeypatch.setenv("A2A_REPLY_TIMEOUT", "1")  # the stock reply wait's floor
    listener.model.behavior[0] = "block"
    server.issue(envelope())
    _, sent = listener.call("cloud-send-request.json")
    task_id = task_of(sent)["id"]
    settle(listener, task_id)
    _, got = listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task_id))
    assert got["result"]["status"]["state"] == protocol.STATE_FAILED
    assert CLOUD_TERMINAL not in got["result"]["metadata"]
    assert server.reports[-1] == report_body(BINDING, 2, "stopped", "timed_out")


def _published(listener, monkeypatch) -> list:
    """Observes the stock finalizer's publish step (persist, audit, metrics, TaskStore, push) as (state, reply)."""
    published, record = [], listener.adapter._record_outcome
    monkeypatch.setattr(listener.adapter, "_record_outcome",
                        lambda *args, **kwargs: (published.append(args[3:5]), record(*args, **kwargs)))
    return published


def _running(listener, server) -> tuple[str, object]:
    """A sent task whose turn blocks until released; returns its id and its finalizer thread."""
    listener.model.behavior[0] = "block"
    server.issue(envelope())
    _, sent = listener.call("cloud-send-request.json")
    task_id = task_of(sent)["id"]
    return task_id, listener.adapter._finalizers[task_id]


def _journal_fails(journal_path, when: str):
    """The database aborts every dispatches update matching ``when`` (as a full disk would)."""
    db = sqlite3.connect(journal_path, isolation_level=None)
    db.execute(f"CREATE TRIGGER journal_fails BEFORE UPDATE ON dispatches WHEN {when} "
               "BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END")
    return contextlib.closing(db)


def test_a_result_the_journal_could_not_commit_is_never_published_and_get_answers_the_journal(
        listener, server, journal_path, monkeypatch):
    published = _published(listener, monkeypatch)
    task_id, finalizer = _running(listener, server)
    with _journal_fails(journal_path, "NEW.state = 'finished'") as db:  # the result's transaction aborts
        listener.model.release.set()
        finalizer.join(TURN_BOUND)
        db.execute("DROP TRIGGER journal_fails")
    assert published == [] and listener.adapter.tasks.get(task_id)["state"] == protocol.STATE_WORKING
    assert [r["state"] for r in journaled(journal_path)] == ["running"] and [r["kind"] for r in server.reports] == [
        "running"]
    assert listener.adapter._pending == {} and task_id not in listener.adapter._finalizers
    assert listener.credentials.lookup(POOL_PLATFORM, CONTEXT, task_id) is None  # the turn is over
    _, got = listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task_id))
    assert got["result"]["status"]["state"] == protocol.STATE_WORKING  # the journal's state, never a guess


def test_a_committed_result_whose_report_ack_cannot_be_written_is_still_published_once(
        listener, server, journal_path, monkeypatch):
    published = _published(listener, monkeypatch)
    task_id, finalizer = _running(listener, server)
    # Only an acknowledgment (a shorter pending list) aborts: the result and its report commit first.
    with _journal_fails(journal_path, "length(coalesce(NEW.pending_reports, '')) < "
                                      "length(coalesce(OLD.pending_reports, '')) AND OLD.state = 'finished'") as db:
        listener.model.release.set()
        finalizer.join(TURN_BOUND)
        db.execute("DROP TRIGGER journal_fails")
    assert published == [(protocol.STATE_COMPLETED, "done")]
    row, = journaled(journal_path)
    assert (row["state"], [r["kind"] for r in json.loads(row["pending_reports"])]) == ("finished", ["stopped"])
    _, got = listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task_id))
    assert got["result"]["status"]["state"] == protocol.STATE_COMPLETED
    # Recorded once; the unacknowledged copy is replayed (recorded: false) and then leaves the journal.
    assert [r["kind"] for r in server.reports] == ["running", "stopped"]
    assert [r["kind"] for r in server.replays] == ["stopped"] and journaled(journal_path)[0]["pending_reports"] is None


def test_conflicting_duplicate_finalizer_callbacks_publish_only_the_journaled_winner_once(
        listener, server, journal_path, monkeypatch):
    published, finalize = _published(listener, monkeypatch), listener.adapter._finalize_task
    results = [(protocol.STATE_COMPLETED, "done"), (protocol.STATE_COMPLETED, "done"),
               (protocol.STATE_COMPLETED, "[INPUT_REQUIRED] Which tone?"), (protocol.STATE_FAILED, "boom")]
    barrier = threading.Barrier(len(results))

    def duplicated(pending, state, reply):  # the runtime's one answer arrives as identical and conflicting copies
        def one(result):
            barrier.wait()
            finalize(pending, *result)
        threads = [threading.Thread(target=one, args=(result,)) for result in results]
        for t in threads:
            t.start()
        for t in threads:
            t.join(TURN_BOUND)

    monkeypatch.setattr(listener.adapter, "_finalize_task", duplicated)
    task_id, finalizer = _running(listener, server)
    listener.model.release.set()
    finalizer.join(TURN_BOUND)
    row, = journaled(journal_path)
    won = ({"completed": protocol.STATE_COMPLETED, "failed": protocol.STATE_FAILED}[row["result_state"]],
           row["result_text"])
    assert published == [won]  # one stock publish, and it is the journal's winner
    record = listener.adapter.tasks.get(task_id)
    assert (record["state"], record["reply"]) == won
    _, got = listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task_id))
    assert got["result"]["status"]["state"] == won[0]
    assert [r["kind"] for r in server.reports] == ["running", "stopped"] and listener.adapter._pending == {}


def test_the_fixture_continuation_after_a_clarification_is_rejected_with_no_turn_session_or_control_call(
        listener, server, journal_path):
    _asked(listener, server, "[INPUT_REQUIRED] Which tone?")
    # cloud-continuation-*: historical generation-2 negative inputs, never an allocation or a start capability.
    server.issue(fixture("cloud-continuation-envelope.json"))
    before = len(server.requests)
    for _ in range(2):
        _, answer = listener.call("cloud-continuation-request.json")
        task = task_of(answer)
        assert task["status"]["state"] == protocol.STATE_REJECTED and _text(task) == CANNOT_RESUME
        assert task["metadata"] == {CLOUD_TERMINAL: {"reason": "continuation_unsupported"}}
        # Unbound rejection: REJECTED with no assignment metadata (the server accepts FAILED or REJECTED).
        assert task["metadata"][CLOUD_TERMINAL] == (
            fixture("cloud-terminal-continuation-task.json")["result"]["metadata"][CLOUD_TERMINAL])
    assert len(listener.model.turns) == 1 and server.requests[before:] == []  # no start, bootstrap or report
    assert journaled(journal_path, fixture("cloud-continuation-request.json")["headers"]["x-ab-start-journal-id"]) == []


# ---- message.messageId ----

@pytest.mark.parametrize("message_id", [None, "dispatch-1-altered"], ids=["missing", "altered"])
def test_a_send_whose_message_id_is_not_the_dispatch_id_gets_no_turn_and_no_control_call(listener, server,
                                                                                         journal_path, message_id):
    server.issue(envelope())
    body = copy.deepcopy(SEND["body"])
    if message_id is None:
        del body["params"]["message"]["messageId"]
    else:
        body["params"]["message"]["messageId"] = message_id
    _, answer = listener.call("cloud-send-request.json", body=body)
    task = task_of(answer)
    assert task["status"]["state"] == protocol.STATE_REJECTED and "message_id_mismatch" in json.dumps(task["status"])
    assert listener.model.turns == [] and server.requests == [] and journaled(journal_path) == []


def test_a_duplicate_send_with_an_altered_message_id_is_refused_without_a_new_turn(listener, server, journal_path):
    server.issue(envelope())
    _, first = listener.call("cloud-send-request.json")
    settle(listener, task_of(first)["id"])
    before, rows = len(server.requests), journaled(journal_path)
    body = copy.deepcopy(SEND["body"])
    body["params"]["message"]["messageId"] = "dispatch-1-altered"
    _, answer = listener.call("cloud-send-request.json", body=body)
    task = task_of(answer)
    assert task["status"]["state"] == protocol.STATE_REJECTED and "message_id_mismatch" in json.dumps(task["status"])
    assert len(listener.model.turns) == 1 and server.requests[before:] == [] and journaled(journal_path) == rows
    assert [r["message_id"] for r in rows] == ["dispatch-1"]


# ---- identity of an unanswered start, ingress framing ----

def test_a_send_whose_start_got_no_answer_keeps_one_task_id_across_retries(listener, server, journal_path):
    server.faults[fake_pool.START_PATH] = ["down"]
    server.issue(envelope())
    _, lost = listener.call("cloud-send-request.json")
    task = task_of(lost)
    assert task["status"]["state"] == protocol.STATE_FAILED and listener.model.turns == []
    assert task["metadata"] == fixture("cloud-send-response.json")["result"]["task"]["metadata"]
    assert [(r["state"], r["task_id"]) for r in journaled(journal_path)] == [("start_requested", task["id"])]
    _, retried = listener.call("cloud-send-request.json")  # the start now answers: the same task runs
    assert task_of(retried)["id"] == task["id"]
    wait_for(listener.model.started[0])
    assert listener.model.turns[0].source.message_id == task["id"]


@pytest.mark.parametrize("framing", ["negative-length", "signed-length", "duplicate-authorization"])
def test_malformed_framing_is_refused_before_any_read_task_or_journal(listener, server, journal_path, framing):
    server.issue(envelope())
    port = listener.adapter._httpd.server_address[1]
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=TURN_BOUND)  # a hang here would be a negative read
    try:
        conn.putrequest("POST", "/", skip_accept_encoding=True)
        for name, value in SEND["headers"].items():
            conn.putheader(name, value)
        payload = json.dumps(SEND["body"]).encode()
        if framing == "duplicate-authorization":
            conn.putheader("authorization", SEND["headers"]["authorization"])
            conn.putheader("content-length", str(len(payload)))
        else:
            conn.putheader("content-length", "-1" if framing == "negative-length" else f"+{len(payload)}")
        conn.endheaders(payload)
        status = conn.getresponse().status
    finally:
        conn.close()
    assert status == (401 if framing == "duplicate-authorization" else 400)
    assert listener.model.turns == [] and server.requests == [] and no_tasks(listener) and journaled(journal_path) == []


# ---- cancel ----

def test_cancel_of_a_running_turn_is_canceled_only_after_the_runtime_stopped_it(listener, server):
    listener.model.behavior[0] = "block"
    server.issue(envelope())
    _, sent = listener.call("cloud-send-request.json")
    task_id = task_of(sent)["id"]
    wait_for(listener.model.started[0])
    status, answer = listener.call("cloud-cancel-request.json", body=with_id("cloud-cancel-request.json", task_id))
    task = answer["result"]
    assert (status, task["id"], task["status"]["state"]) == (200, task_id, protocol.STATE_CANCELED)
    assert "artifactbridge.cancel" not in task["metadata"] and task["metadata"] == sent["result"]["task"]["metadata"]
    assert server.reports[-1] == fixture("cancel-ack.json")
    assert listener.credentials.lookup(POOL_PLATFORM, CONTEXT, task_id) is None
    status, again = listener.call("cloud-cancel-request.json", body=with_id("cloud-cancel-request.json", task_id))
    assert again["result"]["status"]["state"] == protocol.STATE_CANCELED and len(server.reports) == 2


def test_cancel_the_runtime_did_not_confirm_stays_pending_and_the_real_outcome_is_recorded(listener, server,
                                                                                           journal_path, monkeypatch):
    monkeypatch.setattr(pool_adapter, "_CANCEL_CONFIRM_WAIT", 0.5)
    listener.model.behavior[0] = "stubborn"  # the worker swallows the cancellation and keeps running
    server.issue(envelope())
    _, sent = listener.call("cloud-send-request.json")
    task_id = task_of(sent)["id"]
    wait_for(listener.model.started[0])
    for _ in range(2):  # a repeated Cancel manufactures no stop either
        _, answer = listener.call("cloud-cancel-request.json", body=with_id("cloud-cancel-request.json", task_id))
        task = answer["result"]
        assert task["status"]["state"] == protocol.STATE_WORKING and task["metadata"]["artifactbridge.cancel"] == "pending"
    # Still occupied: the row is running, the task keeps its dlc_, and no cancel_acknowledged went out.
    assert [r["state"] for r in journaled(journal_path)] == ["running"]
    assert listener.credentials.lookup(POOL_PLATFORM, CONTEXT, task_id) is not None
    assert [r["kind"] for r in server.reports] == ["running"]
    listener.model.release.set()
    settle(listener, task_id)
    _, got = listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task_id))
    assert got["result"]["status"]["state"] == protocol.STATE_COMPLETED
    assert server.reports[-1] == report_body(envelope()["binding"], 2, "stopped", "completed")


def test_a_finished_task_is_not_cancelable(listener, server):
    server.issue(envelope())
    _, sent = listener.call("cloud-send-request.json")
    settle(listener, task_of(sent)["id"])
    _, answer = listener.call("cloud-cancel-request.json", body=with_id("cloud-cancel-request.json", task_of(sent)["id"]))
    assert answer["error"]["code"] == protocol.ERR_TASK_NOT_CANCELABLE and len(server.reports) == 2


# ---- restart recovery ----

def test_after_a_restart_get_list_and_cancel_recover_by_grant_and_nothing_respawns(listeners, server, journal_path):
    first = listeners()
    first.model.behavior[0] = "block"
    server.issue(envelope())
    _, sent = first.call("cloud-send-request.json")
    task_id = task_of(sent)["id"]
    wait_for(first.model.started[0])
    first.crash()  # with the turn in flight: its loop stops, its turn records nothing
    try:
        second = listeners()
        assert [r["state"] for r in journaled(journal_path)] == ["uncertain"]
        # The restarted process holds no agx_: the crash report waits for the server's next request.
        assert [r["kind"] for r in server.reports] == ["running"]

        other_agx, other_journal = grant_for(envelope(binding={"assignmentId": "6f1f7d9e-0a1b-4c2d-8e3f-000000000042"}))
        foreign = {**SEND["headers"], "x-ab-control-credential": other_agx, "x-ab-start-journal-id": other_journal}
        _, unknown = second.call("cloud-get-task-request.json", headers=foreign,
                                 body=with_id("cloud-get-task-request.json", task_id))
        assert unknown["error"]["code"] == protocol.ERR_TASK_NOT_FOUND
        _, listed = second.call("cloud-list-tasks-request.json", headers=foreign)
        assert listed["result"]["tasks"] == [] and [r["kind"] for r in server.reports] == ["running"]

        _, got = second.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task_id))
        assert (got["result"]["id"], got["result"]["status"]["state"]) == (task_id, protocol.STATE_FAILED)
        assert got["result"]["metadata"] == sent["result"]["task"]["metadata"]
        assert server.reports[-1] == report_body(envelope()["binding"], 2, "stopped", "crashed")
        _, listed = second.call("cloud-list-tasks-request.json")
        assert [t["id"] for t in listed["result"]["tasks"]] == [task_id]
        _, canceled = second.call("cloud-cancel-request.json", body=with_id("cloud-cancel-request.json", task_id))
        assert canceled["error"]["code"] == protocol.ERR_TASK_NOT_CANCELABLE
        _, resent = second.call("cloud-send-request.json")  # an ambiguous Send's replay never respawns
        assert task_of(resent)["status"]["state"] == protocol.STATE_FAILED and second.model.turns == []
        assert len(server.starts) == 1 and len(server.bootstraps) == 1
    finally:  # after the assertions: cancel and await the abandoned turn on the crashed loop, then close it
        first.stop()

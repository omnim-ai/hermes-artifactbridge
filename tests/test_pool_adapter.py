"""The ``artifactbridge-pool`` listener over real HTTP against the real Hermes stock A2A adapter and the real
Hermes message pipeline (skipped without Hermes). Requests are the generated cloud-contract-r2 fixtures
(tests/fixtures/cloud_r2), sent to the listener's real ``connect()`` socket with the server's headers; only
the model (``_message_handler``) and the server's control routes (FakeControl) are stand-ins."""
import asyncio
import copy
import http.client
import json
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
from hermes_artifactbridge.tools import PooledTools  # noqa: E402
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
    until released) or a reply text (default "done")."""

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
        return behavior


class Listener:
    """One listener process: its own journal connection, core, adapter, loop thread and socket."""

    def __init__(self, journal_path, server, *, connect=True):
        self.credentials = CredentialBindings(now=server.clock)
        self.core = PoolCore(journal=Journal(journal_path),
                             control=PoolControl(tools_url=fake_pool.TOOLS_URL, http_factory=server.factory),
                             executor_id=EXECUTOR_ID, connection_revision=REVISION, credentials=self.credentials,
                             now=server.clock)
        self.ab, self.server = fake_ab.FakeAB(), server  # the scoped tools route
        server.on_bootstrap = self._bootstrapped
        tools = PooledTools(tools_url=fake_ab.TOOLS_URL, credentials=self.credentials,
                            http_factory=fake_ab.http_factory(fake_ab.make_app(self.ab)))
        self.adapter = pool_adapter.build_adapter_class()(PlatformConfig(enabled=True, extra={}), core=self.core,
                                                          tools=tools, port=0)
        self.model = Model(self.credentials)
        self.adapter.set_message_handler(self.model)
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.connected = self.run(self.adapter.connect()) if connect else None

    def _bootstrapped(self, body):
        """The server minted a dlc_: the scoped tools accept it for the assignment's delegation."""
        delegation = body["binding"]["delegationId"]
        self.ab.delegations.setdefault(delegation, fake_ab.Delegation(id=delegation))
        self.ab.pooled_credentials[self.server.live[body["binding"]["assignmentId"]]] = {"delegation_id": delegation}

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
    # Product ruling: the six supported capabilities are answered; the generated probe still requires the
    # withdrawn same_session_continuation and is refused, never answered with a capability it lacks.
    body = copy.deepcopy(fixture("cloud-capability-request.json")["body"])
    body["params"]["requiredCapabilities"].remove("same_session_continuation")
    status, answer = listener.call("cloud-capability-request.json", body=body)
    expected = fixture("cloud-capability-result.json")
    assert status == 200 and answer["id"] == expected["id"]
    assert answer["result"] == {**expected["result"], "runtimeVersion": plugin_module.VERSION,
                                "capabilities": body["params"]["requiredCapabilities"]}
    status, refused = listener.call("cloud-capability-request.json")
    assert status == 200 and "result" not in refused
    assert refused["error"] == {**refused["error"], "code": protocol.ERR_INVALID_PARAMS,
                                "message": "ArtifactBridge pooled dispatch refused: capability_mismatch"}
    assert server.requests == [] and listener.model.turns == []


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


def test_no_secret_reaches_the_model_tasks_journal_logs_or_answers(listener, server, journal_path, tmp_path, caplog):
    server.issue(envelope())
    _, sent = listener.call("cloud-send-request.json")
    task_id = task_of(sent)["id"]
    settle(listener, task_id)
    answers = [sent, listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task_id))[1],
               listener.call("cloud-list-tasks-request.json")[1]]
    blob = (json.dumps(answers) + listener.model.turns[0].text + repr(listener.adapter.tasks.list())
            + caplog.text).encode()
    blob += b"".join(p.read_bytes() for p in tmp_path.rglob("*") if p.is_file())  # journal, audit, history
    assert [s for s in SECRETS if s in blob] == []


# ---- continuation ----

def _clarified(listener, server, monkeypatch) -> tuple[str, list]:
    """The six-capability handshake, then the fixture Send whose turn asks a clarification. Returns the task id
    and, per scoped tool call, what the server and a Get saw at that moment."""
    body = copy.deepcopy(fixture("cloud-capability-request.json")["body"])
    body["params"]["requiredCapabilities"].remove("same_session_continuation")
    assert "result" in listener.call("cloud-capability-request.json", body=body)[1]
    listener.model.behavior[0] = "[INPUT_REQUIRED] Which tone?"
    seen, call = [], listener.ab._call

    def observed(delegation, name, args):
        asked = listener.model.turns[0].message_id  # the finalizer can run before the Send answer is read
        get = listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", asked))[1]
        seen.append(([(r["kind"], r["outcome"]) for r in server.reports], get["result"]["status"]["state"]))
        return call(delegation, name, args)

    monkeypatch.setattr(listener.ab, "_call", observed)
    server.issue(envelope())
    _, sent = listener.call("cloud-send-request.json")
    task_id = task_of(sent)["id"]
    settle(listener, task_id)
    return task_id, seen


def test_a_clarification_posts_its_question_to_the_room_then_the_task_fails_with_resend_guidance(
        listener, server, journal_path, monkeypatch):
    task_id, seen = _clarified(listener, server, monkeypatch)
    delegation = envelope()["binding"]["delegationId"]
    assert [(e["delegation_id"], e["kind"], e["body"]) for e in listener.ab.events] == [
        (delegation, "question", "Which tone?")]
    assert seen == [([("running", None)], protocol.STATE_WORKING)]  # posted before any terminal outcome
    assert server.reports[-1] == report_body(envelope()["binding"], 2, "stopped", "failed")
    for _ in range(2):  # Get and a retried Send both answer the same terminal failure, with no second turn
        _, got = listener.call("cloud-get-task-request.json", body=with_id("cloud-get-task-request.json", task_id))
        _, again = listener.call("cloud-send-request.json")
        for task in (got["result"], task_of(again)):
            text = json.dumps(task["status"])
            assert (task["id"], task["status"]["state"]) == (task_id, protocol.STATE_FAILED)
            assert "clarification_unsupported" in text and "send the task again" in text and "Which tone?" in text
    assert len(listener.model.turns) == 1 and len(server.starts) == 1 and len(server.reports) == 2
    assert [(r["state"], r["result_state"]) for r in journaled(journal_path)] == [("finished", "failed")]
    assert listener.credentials.lookup(POOL_PLATFORM, CONTEXT, task_id) is None


def test_the_fixture_continuation_after_a_clarification_fails_with_no_turn_session_or_control_call(
        listener, server, journal_path, monkeypatch):
    _clarified(listener, server, monkeypatch)
    server.issue(fixture("cloud-continuation-envelope.json"))
    before = len(server.requests)
    for _ in range(2):
        _, answer = listener.call("cloud-continuation-request.json")
        task = task_of(answer)
        text = json.dumps(task["status"])
        assert task["status"]["state"] == protocol.STATE_REJECTED
        assert "continuation_unsupported" in text and "send the task again" in text
    assert len(listener.model.turns) == 1 and server.requests[before:] == []  # no start, bootstrap or report
    assert len(listener.ab.events) == 1  # only the clarification's question
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

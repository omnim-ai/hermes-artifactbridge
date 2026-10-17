"""The ``artifactbridge-pool`` adapter against the real Hermes stock A2A adapter (skipped without Hermes):
the intercept runs before any MessageEvent, a permitted dispatch runs exactly once on its own
(platform, contextId, task id) binding, cancel acknowledges, a late reply never answers a newer task."""
import asyncio
import json
import threading
import time

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
from tests.fake_pool import EXECUTOR_CREDENTIAL, EXECUTOR_ID, REVISION, Clock, FakeControl, envelope, fixture  # noqa: E402

POOL_ENV = {"ARTIFACTBRIDGE_POOL_ENABLED": "1", "ARTIFACTBRIDGE_POOL_EXECUTOR_CREDENTIAL": EXECUTOR_CREDENTIAL,
            "ARTIFACTBRIDGE_POOL_EXECUTOR_ID": EXECUTOR_ID, "ARTIFACTBRIDGE_POOL_CONNECTION_REVISION": REVISION}
CTX = "ctx-original"


@pytest.fixture
def pool_env(monkeypatch, tmp_path):
    for name, value in POOL_ENV.items():
        monkeypatch.setenv(name, value)
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


@pytest.fixture
def credentials():
    return CredentialBindings()


@pytest.fixture
def adapter(registered, server, credentials, tmp_path):
    core = PoolCore(journal=Journal(tmp_path / "journal.sqlite3"),
                    control=PoolControl(tools_url=fake_pool.TOOLS_URL, credential=EXECUTOR_CREDENTIAL,
                                        http_factory=server.factory),
                    executor_id=EXECUTOR_ID, connection_revision=REVISION, credentials=credentials, now=server.clock)
    adapter = pool_adapter.build_adapter_class()(PlatformConfig(enabled=True, extra={}), core=core, port=0)
    adapter.turns = []          # every MessageEvent that would have reached the model
    adapter.replies = {}        # task id -> reply the fake model sends (None = never reply)
    adapter.seen_credentials = []

    async def fake_handle_message(event):
        adapter.turns.append(event)
        adapter.seen_credentials.append(credentials.lookup(event.source.platform.value, event.source.chat_id,
                                                           event.source.message_id))
        reply = adapter.replies.get(event.message_id, "done")
        if reply is not None:
            await adapter.send(event.source.chat_id, reply, reply_to=event.message_id, metadata={"notify": True})

    adapter.handle_message = fake_handle_message
    adapter._message_handler = object()
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    adapter._loop = loop
    yield adapter
    loop.call_soon_threadsafe(loop.stop)
    thread.join(5)


def send(adapter, metadata, context_id="", text="Please do the task."):
    message = protocol.text_message(protocol.ROLE_USER, text, context_id=context_id)
    params = {"message": message} if metadata is None else {"message": message, "metadata": metadata}
    return adapter._rpc_message_send("1", params, "peer-1")["result"]


def state(task):
    return task["status"]["state"]


def wait_until(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while not predicate():
        assert time.time() < deadline
        time.sleep(0.01)


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


@pytest.mark.parametrize("metadata", [
    None, {}, {"artifactbridge.assignment": "x"},
    fake_pool.metadata({**envelope(), "protocolVersion": 2}),
    fake_pool.metadata(envelope(binding={"executorId": "6f1f7d9e-0a1b-4c2d-8e3f-000000000099"})),
], ids=["absent", "empty", "malformed", "unknown-version", "other-executor"])
def test_every_request_is_intercepted_before_any_model_invocation(adapter, server, metadata):
    task = send(adapter, metadata)
    assert state(task) == protocol.STATE_REJECTED and "refused" in json.dumps(task)
    assert adapter.turns == [] and adapter._pending == {} and server.requests == []
    assert adapter.tasks.get(task["id"])["state"] == protocol.STATE_REJECTED


def test_a_permitted_dispatch_runs_once_with_its_own_binding_and_reports_completion(adapter, server, credentials):
    metadata = server.issue(envelope())
    task = send(adapter, metadata, CTX)
    assert (state(task), task["contextId"]) == (protocol.STATE_COMPLETED, CTX)
    event = adapter.turns[0]
    assert (event.source.platform.value, event.source.chat_id, event.source.message_id) == (POOL_PLATFORM, CTX, task["id"])
    assert event.text != "Please do the task." and "Please do the task." in event.text  # stock inbound framing
    assert adapter.seen_credentials[0] in server.issued
    assert credentials.lookup(POOL_PLATFORM, CTX, task["id"]) is None
    assert [r["kind"] for r in server.reports] == ["running", "stopped"] and server.reports[-1]["outcome"] == "completed"

    again = send(adapter, metadata, CTX)  # duplicate delivery: the recorded result, no second turn
    assert (state(again), protocol.extract_text(again["artifacts"][0])) == (protocol.STATE_COMPLETED, "done")
    assert len(adapter.turns) == 1 and len(server.starts) == 1


def test_an_input_required_reply_reports_input_wait(adapter, server):
    metadata = server.issue(envelope())
    original = adapter.handle_message

    async def asks(event):
        adapter.replies[event.message_id] = "[INPUT_REQUIRED] Which document?"
        await original(event)

    adapter.handle_message = asks
    task = send(adapter, metadata, CTX)
    assert state(task) == protocol.STATE_INPUT_REQUIRED
    assert server.reports[-1]["outcome"] == "input_wait"


def test_concurrent_duplicate_deliveries_run_one_turn(adapter, server):
    metadata = server.issue(envelope())
    original = adapter.handle_message

    async def slow(event):
        await asyncio.sleep(0.2)
        await original(event)

    adapter.handle_message = slow
    results = []
    threads = [threading.Thread(target=lambda: results.append(send(adapter, metadata, CTX))) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert len(adapter.turns) == 1 and len(server.bootstraps) == 1
    assert {state(r) for r in results} == {protocol.STATE_COMPLETED}


def test_cancel_acknowledges_and_a_late_reply_never_answers_the_newer_task(adapter, server, credentials):
    original = adapter.handle_message

    async def never(event):
        adapter.replies[event.message_id] = None
        await original(event)

    adapter.handle_message = never
    first = []
    runner = threading.Thread(target=lambda: first.append(send(adapter, server.issue(envelope()), CTX)))
    runner.start()
    wait_until(lambda: adapter.turns)
    old_task = adapter.turns[0].message_id
    canceled = adapter._rpc_tasks_cancel("2", {"id": old_task})["result"]
    runner.join(5)
    assert state(canceled) == state(first[0]) == protocol.STATE_CANCELED
    assert server.reports[-1] == fixture("cancel-ack.json") and credentials.lookup(POOL_PLATFORM, CTX, old_task) is None

    newer = envelope(binding={"assignmentId": "6f1f7d9e-0a1b-4c2d-8e3f-000000000042", "executionGeneration": 2},
                     dispatchId="dispatch-2")
    second = []
    runner = threading.Thread(target=lambda: second.append(send(adapter, server.issue(newer), CTX)))
    runner.start()
    wait_until(lambda: len(adapter.turns) == 2)
    new_task = adapter.turns[1].message_id
    reports = list(server.reports)
    asyncio.run_coroutine_threadsafe(adapter.send(CTX, "late", reply_to=old_task, metadata={"notify": True}),
                                     adapter._loop).result(5)
    time.sleep(0.1)
    assert second == [] and adapter.tasks.get(new_task)["state"] == protocol.STATE_WORKING
    assert server.reports == reports and credentials.lookup(POOL_PLATFORM, CTX, new_task) is not None
    asyncio.run_coroutine_threadsafe(adapter.send(CTX, "resumed", reply_to=new_task, metadata={"notify": True}),
                                     adapter._loop).result(5)
    runner.join(5)
    assert state(second[0]) == protocol.STATE_COMPLETED
    assert server.reports[-1] == report_body(newer["binding"], 2, "stopped", "completed")


def test_a_continuation_on_another_context_is_resume_unavailable(adapter, server):
    send(adapter, server.issue(envelope()), CTX)
    newer = envelope(binding={"assignmentId": "6f1f7d9e-0a1b-4c2d-8e3f-000000000042", "executionGeneration": 2},
                     dispatchId="dispatch-2")
    task = send(adapter, server.issue(newer), "ctx-elsewhere")
    assert state(task) == protocol.STATE_REJECTED and "resume_unavailable" in json.dumps(task)
    assert len(adapter.turns) == 1

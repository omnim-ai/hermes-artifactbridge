"""Token-free pooled tools: per-task dlc_ resolution from the Hermes session, isolation between concurrent
sessions, no fallback, dlc_ refresh before expiry and exactly once after credential_expired; legacy dlt_ tools
refuse inside a pooled session."""
import asyncio
import json
import sys
import types
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone

import pytest

import hermes_artifactbridge as plugin_module
from hermes_artifactbridge import register
from hermes_artifactbridge.pool_core import POOL_PLATFORM, ControlUnavailable, CredentialBindings
from hermes_artifactbridge.schemas import POOL_TOOL_SCHEMAS, POOL_WIRE_NAMES, TOKEN_ARG, TOOL_SCHEMAS
from hermes_artifactbridge.tools import PooledTools
from tests import fake_ab
from tests.conftest import FakeCtx
from tests.fake_pool import EXECUTOR_ID, REVISION

POOL_READ_BRIEF = "artifactbridge_pool_delegation_read_brief"
POOL_CONTRIBUTE = "artifactbridge_pool_delegation_contribute"
POOL_READ_SKILL = "artifactbridge_pool_read_skill"
SESSION = ContextVar("session", default={})
DLC_A, DLC_B, DLC_A2 = "dlc_" + "a" * 43, "dlc_" + "b" * 43, "dlc_" + "c" * 43
EXPIRES = datetime(2026, 10, 17, 11, 15, tzinfo=timezone.utc)  # bootstrap-response.json's expiresAt


@pytest.fixture(autouse=True)
def session_context(monkeypatch):
    """Stands in for ``gateway.session_context``: per-asyncio-task session vars, as Hermes binds them."""
    module = types.ModuleType("gateway.session_context")
    module.get_session_env = lambda name, default="": SESSION.get().get(name, default)
    if "gateway" not in sys.modules:
        monkeypatch.setitem(sys.modules, "gateway", types.ModuleType("gateway"))
    monkeypatch.setitem(sys.modules, "gateway.session_context", module)


def in_session(platform: str, chat_id: str, message_id: str) -> None:
    SESSION.set({"HERMES_SESSION_PLATFORM": platform, "HERMES_SESSION_CHAT_ID": chat_id,
                 "HERMES_SESSION_MESSAGE_ID": message_id})


@pytest.fixture
def credentials(ab, clock):
    ab.delegations["del-2"] = fake_ab.Delegation(id="del-2", room_id="room-2")
    ab.pooled_credentials.update({DLC_A: {"delegation_id": "del-1"}, DLC_B: {"delegation_id": "del-2"},
                                  DLC_A2: {"delegation_id": "del-1"}})
    bound = CredentialBindings(now=lambda: clock[0])
    bound.renewals = []

    def renew():  # task-a's re-bootstrap: the server mints the next dlc_
        bound.renewals.append(clock[0])
        return DLC_A2, clock[0] + timedelta(minutes=15)

    bound.bind(POOL_PLATFORM, "ctx-a", "task-a", DLC_A, {"delegationId": "del-1"}, EXPIRES, renew)
    bound.bind(POOL_PLATFORM, "ctx-b", "task-b", DLC_B, {"delegationId": "del-2"}, EXPIRES, renew)
    return bound


@pytest.fixture
def clock():
    return [EXPIRES - timedelta(minutes=14)]


@pytest.fixture
def pooled(app, credentials):
    return PooledTools(tools_url=fake_ab.TOOLS_URL, credentials=credentials, http_factory=fake_ab.http_factory(app))


def test_pooled_schemas_mirror_the_legacy_tools_without_the_token():
    assert len(POOL_TOOL_SCHEMAS) == len(TOOL_SCHEMAS) == 11
    for pool, legacy in zip(POOL_TOOL_SCHEMAS, TOOL_SCHEMAS):
        assert POOL_WIRE_NAMES[pool["name"]] == legacy["name"] and pool["name"].startswith("artifactbridge_pool_")
        assert TOKEN_ARG not in pool["parameters"]["properties"] and TOKEN_ARG not in pool["parameters"]["required"]
        assert TOKEN_ARG not in pool["description"]
        assert pool["parameters"]["properties"] == {k: v for k, v in legacy["parameters"]["properties"].items()
                                                    if k != TOKEN_ARG}


async def test_two_concurrent_sessions_each_present_only_their_own_credential(pooled, ab):
    async def call(context_id, task_id):
        in_session(POOL_PLATFORM, context_id, task_id)
        await asyncio.sleep(0)
        return json.loads(await pooled.handler_for(POOL_READ_BRIEF)({}, task_id="t"))

    a, b = await asyncio.gather(call("ctx-a", "task-a"), call("ctx-b", "task-b"))
    assert (a["delegation_id"], b["delegation_id"]) == ("del-1", "del-2")
    assert {r["authorization"] for r in ab.requests} == {f"Bearer {DLC_A}", f"Bearer {DLC_B}"}
    assert all(r["token"] == "" and r["path"] == fake_ab.TOOLS_PATH for r in ab.requests)


@pytest.mark.parametrize("route", [
    ("", "", ""),                                   # outside any gateway turn
    ("a2a", "ctx-a", "task-a"),                     # the stock A2A platform
    (POOL_PLATFORM, "ctx-a", "task-b"),             # another task's id on this context
    (POOL_PLATFORM, "ctx-b", "task-a"),             # this task's id on another context
    (POOL_PLATFORM, "ctx-a", ""),                   # no task id
])
async def test_without_an_exact_binding_pooled_tools_refuse_without_network(pooled, ab, route):
    in_session(*route)
    assert await pooled.invoke(POOL_READ_BRIEF, {}) == {"error": "pooled_credential_unavailable"}
    assert ab.requests == []


async def test_a_dropped_binding_refuses(pooled, ab, credentials):
    credentials.drop(POOL_PLATFORM, "ctx-a", "task-a")
    in_session(POOL_PLATFORM, "ctx-a", "task-a")
    assert await pooled.invoke(POOL_READ_BRIEF, {}) == {"error": "pooled_credential_unavailable"}
    assert ab.requests == []


def tool_auths(ab):
    return [r["authorization"] for r in ab.requests if r["path"] == fake_ab.TOOLS_PATH]


async def test_credential_expired_is_re_bootstrapped_and_retried_exactly_once(pooled, ab, credentials):
    ab.pooled_credentials[DLC_A]["expired"] = True
    in_session(POOL_PLATFORM, "ctx-a", "task-a")
    result = await pooled.invoke(POOL_CONTRIBUTE, {"kind": "message", "body": "x"})
    assert result.get("error") is None and ab.tool_calls == [("artifactbridge_delegation_contribute",
                                                              {"kind": "message", "body": "x"})]
    auths = tool_auths(ab)  # an MCP call is several requests: the denied one, then the retry's
    assert auths[0] == f"Bearer {DLC_A}" and set(auths[1:]) == {f"Bearer {DLC_A2}"} and len(credentials.renewals) == 1
    assert credentials.lookup(POOL_PLATFORM, "ctx-a", "task-a") == DLC_A2 and ab.renew_calls == 0


async def test_a_renewed_credential_that_is_still_expired_is_not_renewed_again(pooled, ab, credentials):
    ab.pooled_credentials[DLC_A]["expired"] = ab.pooled_credentials[DLC_A2]["expired"] = True
    in_session(POOL_PLATFORM, "ctx-a", "task-a")
    assert await pooled.invoke(POOL_READ_BRIEF, {}) == {"error": "credential_expired", "status": 403}
    assert tool_auths(ab) == [f"Bearer {DLC_A}", f"Bearer {DLC_A2}"] and len(credentials.renewals) == 1


async def test_a_credential_near_its_expiry_is_re_bootstrapped_before_use(pooled, ab, credentials, clock):
    clock[0] = EXPIRES - timedelta(seconds=30)
    in_session(POOL_PLATFORM, "ctx-a", "task-a")
    assert (await pooled.invoke(POOL_READ_BRIEF, {}))["ok"] is True
    assert set(tool_auths(ab)) == {f"Bearer {DLC_A2}"} and credentials.renewals == [clock[0]]


async def test_a_failed_refresh_past_expiry_refuses_without_network(pooled, ab, credentials, clock):
    def unavailable():
        raise ControlUnavailable("down")

    credentials._bound[(POOL_PLATFORM, "ctx-a", "task-a")].renew = unavailable
    clock[0] = EXPIRES + timedelta(seconds=1)
    in_session(POOL_PLATFORM, "ctx-a", "task-a")
    assert await pooled.invoke(POOL_READ_BRIEF, {}) == {"error": "pooled_credential_unavailable"}
    assert ab.requests == []


async def test_concurrent_expired_calls_share_one_re_bootstrap(pooled, ab, credentials):
    ab.pooled_credentials[DLC_A]["expired"] = True
    in_session(POOL_PLATFORM, "ctx-a", "task-a")
    results = await asyncio.gather(*[pooled.invoke(POOL_READ_BRIEF, {}) for _ in range(4)])
    assert all(r["ok"] is True for r in results) and len(credentials.renewals) == 1


async def test_reserved_args_and_a_model_supplied_token_are_stripped_like_legacy(pooled, ab):
    in_session(POOL_PLATFORM, "ctx-a", "task-a")
    await pooled.invoke(POOL_CONTRIBUTE, {TOKEN_ARG: "dlt_" + "x" * 43, "delegation_id": "del-other",
                                          "room_id": "room-stolen", "kind": "message", "body": "hi"})
    await pooled.invoke(POOL_READ_SKILL, {"delegation_id": "del-1", "slug": "brand-voice"})
    assert ab.tool_calls == [("artifactbridge_delegation_contribute", {"kind": "message", "body": "hi"}),
                             ("artifactbridge_read_skill", {"delegation_id": "del-1", "slug": "brand-voice"})]
    assert all(r["token"] == "" for r in ab.requests)


POOL_SPACE_TOOLS = (("artifactbridge_pool_read_space_context", {}), (POOL_READ_SKILL, {"slug": "brand-voice"}))
SPACE_ID = "20000000-0000-4000-8000-000000000001"


def _server_replies(monkeypatch, wire: str, reply: dict) -> None:
    once = iter([reply])  # a second call would exhaust the script and fail
    monkeypatch.setitem(fake_ab.TOOLS, wire, lambda *a: next(once))


@pytest.mark.parametrize("selectors, reply", [
    ({}, fake_ab._text({"space": {"id": "space-agent"}})),
    ({"space_role": "agent"}, fake_ab._text({"space": {"id": "space-agent"}})),
    ({"space_role": "room"}, fake_ab._text({"space": {"id": "space-room"}})),
    ({"space_role": "both"}, fake_ab._text({"error": "invalid_arguments", "message": "space_role must be agent or room"}, True)),
    ({"space_role": "room"}, fake_ab._text({"error": "space_not_bound", "delegation_id": "del-1"}, True)),
    ({"space_role": "room", "space_id": SPACE_ID}, fake_ab._text({"error": "space_out_of_scope", "delegation_id": "del-1"}, True)),
], ids=["omitted", "agent", "room", "invalid-role", "room-not-bound", "room-foreign-space"])
@pytest.mark.parametrize("name, args", POOL_SPACE_TOOLS)
async def test_pooled_space_role_is_forwarded_verbatim_with_only_the_bound_dlc(pooled, ab, credentials, monkeypatch,
                                                                               name, args, selectors, reply):
    if name == POOL_READ_SKILL:
        selectors = {k: v for k, v in selectors.items() if k != "space_id"}  # read_skill takes no space_id
    wire = POOL_WIRE_NAMES[name]
    _server_replies(monkeypatch, wire, reply)
    in_session(POOL_PLATFORM, "ctx-a", "task-a")
    assert json.loads(await pooled.handler_for(name)({**args, **selectors}, task_id="t")) == reply
    assert ab.tool_calls == [(wire, {**args, **selectors})]  # omitted: the agent-only payload, no space_role key
    assert ab.requests and all((r["authorization"], r["token"], r["path"]) == (f"Bearer {DLC_A}", "", fake_ab.TOOLS_PATH)
                               for r in ab.requests)  # no dlt_, no service credential
    assert credentials.renewals == [] and ab.renew_calls == 0


async def test_legacy_tools_refuse_inside_a_pooled_session(registered_tools, ab, token):
    in_session(POOL_PLATFORM, "ctx-a", "task-a")
    for name, tool in registered_tools.items():
        result = json.loads(await tool["handler"]({TOKEN_ARG: token}, task_id="t"))
        assert result == {"error": "pooled_session_requires_pooled_tools"}, name
    assert ab.requests == []
    in_session("a2a", "ctx-a", "task-a")  # outside a pooled session legacy tools are unchanged
    result = json.loads(await registered_tools["artifactbridge_delegation_read_brief"]["handler"]({TOKEN_ARG: token}))
    assert result["ok"] is True


POOL_ENV = {"ARTIFACTBRIDGE_POOL_ENABLED": "1", "ARTIFACTBRIDGE_POOL_EXECUTOR_ID": EXECUTOR_ID,
            "ARTIFACTBRIDGE_POOL_CONNECTION_REVISION": REVISION}


def _registered(monkeypatch, env: dict) -> tuple[FakeCtx, list]:
    for name in POOL_ENV:
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    ctx, platforms = FakeCtx(), []
    ctx.register_platform = lambda **kw: platforms.append(kw)
    register(ctx)
    return ctx, platforms


@pytest.mark.parametrize("env", [
    {},
    {**POOL_ENV, "ARTIFACTBRIDGE_POOL_ENABLED": "0"},
    {**POOL_ENV, "ARTIFACTBRIDGE_POOL_EXECUTOR_ID": "not-a-uuid"},
    {**POOL_ENV, "ARTIFACTBRIDGE_POOL_CONNECTION_REVISION": "01"},
    {k: v for k, v in POOL_ENV.items() if k != "ARTIFACTBRIDGE_POOL_CONNECTION_REVISION"},
], ids=["unset", "disabled", "bad-executor", "bad-revision", "partial"])
def test_unconfigured_pooled_mode_registers_exactly_the_legacy_surface(monkeypatch, env):
    baseline, _ = _registered(monkeypatch, {})
    ctx, platforms = _registered(monkeypatch, env)
    assert platforms == []
    assert [(t["name"], t["toolset"]) for t in ctx.tools] == [(t["name"], t["toolset"]) for t in baseline.tools]
    assert [t["name"] for t in ctx.tools] == [s["name"] for s in TOOL_SCHEMAS] and ctx.skills == baseline.skills


def test_configured_pooled_mode_on_an_untested_hermes_registers_nothing_extra(monkeypatch, caplog):
    monkeypatch.setattr(plugin_module.pool_adapter, "hermes_shape_supported", lambda: False)
    ctx, platforms = _registered(monkeypatch, POOL_ENV)
    assert platforms == [] and [t["name"] for t in ctx.tools] == [s["name"] for s in TOOL_SCHEMAS]
    assert "outside the tested range" in caplog.text and "agx_" not in caplog.text

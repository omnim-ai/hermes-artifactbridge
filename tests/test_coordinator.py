"""Coordinator profile surface: exactly three fixed-Space reads plus a confined session_search, through the
public registration boundary and real MCP serialization against an in-process broker."""
import asyncio
import functools
import json
import logging
import sys
import types
from unittest.mock import Mock

import httpx2
import pytest
from jsonschema import Draft202012Validator

import hermes_artifactbridge as plugin_module
from hermes_artifactbridge import register
from hermes_artifactbridge.schemas import TOOL_NAMES, TOOLSET
from tests import fake_ab
from tests import fake_coordinator_broker as fb
from tests.conftest import FakeCtx

READS = ("artifactbridge_read_space_context", "artifactbridge_read_document", "artifactbridge_read_skill")
STOCK_SEARCH_PROPS = ("query", "limit", "sort", "detail", "after", "before", "exclude_session_ids", "session_id",
                      "around_message_id", "window", "role_filter", "profile")


class CoordinatorCtx(FakeCtx):
    def __init__(self, settings=None, capabilities=("tools.override",)):
        super().__init__()
        self.settings = {"role": "coordinator"} if settings is None else settings
        self.capabilities = set(capabilities)

    def get_config(self, key, default=None):
        return self.settings.get(key, default)

    def has_capability(self, capability):
        return capability in self.capabilities


@pytest.fixture
def stock_search(monkeypatch):
    """Hermes' public tools.registry holding the stock session_search entry."""
    handler = Mock(return_value=json.dumps({"success": True, "results": []}))
    check_fn = Mock(return_value=True)
    entry = types.SimpleNamespace(
        name="session_search", toolset="session_search", handler=handler, check_fn=check_fn, emoji="🔍",
        schema={"name": "session_search", "description": "stock",
                "parameters": {"type": "object", "properties": {k: {"type": "string"} for k in STOCK_SEARCH_PROPS}}})
    registry = Mock()
    registry.snapshot_registration = lambda name, scope=None: entry if name == "session_search" and scope is None else None
    module = types.ModuleType("tools.registry")
    module.registry = registry
    monkeypatch.setitem(sys.modules, "tools", types.ModuleType("tools"))
    monkeypatch.setitem(sys.modules, "tools.registry", module)
    return entry


@pytest.fixture
def broker():
    return fb.FakeBroker()


@pytest.fixture
def coordinator(monkeypatch, broker, stock_search):
    monkeypatch.setenv("ARTIFACTBRIDGE_COORDINATOR_READ_CREDENTIAL", fb.CREDENTIAL)
    monkeypatch.setenv("ARTIFACTBRIDGE_TOOLS_URL", fake_ab.TOOLS_URL)
    monkeypatch.delenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", raising=False)
    monkeypatch.setattr(plugin_module, "CoordinatorReads", functools.partial(
        plugin_module.CoordinatorReads, http_factory=fake_ab.http_factory(fb.make_app(broker))))
    ctx = CoordinatorCtx()
    register(ctx)
    return {tool["name"]: tool for tool in ctx.tools}


async def call(tools, name, args):
    return json.loads(await tools[name]["handler"](args))


def payload(result):
    return json.loads(result["content"][0]["text"])


def test_coordinator_role_registers_exactly_the_reads_and_confined_search(coordinator, stock_search, monkeypatch):
    assert list(coordinator) == ["session_search", *READS]
    search = coordinator["session_search"]
    assert search["toolset"] == "session_search" and search["override"] is True and search["is_async"] is False
    assert search["check_fn"] is stock_search.check_fn
    assert "profile" not in search["schema"]["parameters"]["properties"]
    assert set(search["schema"]["parameters"]["properties"]) == set(STOCK_SEARCH_PROPS) - {"profile"}
    for name in READS:
        tool = coordinator[name]
        params = tool["schema"]["parameters"]
        Draft202012Validator.check_schema(params)
        assert tool["toolset"] == "ab_coordinator_reads" and tool["is_async"] is True and not tool.get("override")
        assert tool["requires_env"] == ["ARTIFACTBRIDGE_COORDINATOR_READ_CREDENTIAL"]
        assert params["additionalProperties"] is False
        assert tool["check_fn"]() is True
    props = {name: set(coordinator[name]["schema"]["parameters"]["properties"]) for name in READS}
    assert props == {READS[0]: {"cursor", "include_archived", "limit"}, READS[1]: {"document_id"},
                     READS[2]: {"slug", "version_policy"}}
    for bad in ("", "agw_not_a_read_credential"):
        monkeypatch.setenv("ARTIFACTBRIDGE_COORDINATOR_READ_CREDENTIAL", bad)
        assert all(coordinator[name]["check_fn"]() is False for name in READS)


@pytest.mark.parametrize("settings, capabilities", [
    ({"role": "coordinator"}, ()),  # no operator grant: session_search would stay unconfined
    ({"role": "Coordinator"}, ("tools.override",)),  # unknown role never falls back to delegation tools
], ids=["no-override-grant", "unknown-role"])
def test_coordinator_role_fails_closed(monkeypatch, stock_search, settings, capabilities):
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    ctx = CoordinatorCtx(settings, capabilities)
    register(ctx)
    assert ctx.tools == [] and ctx.skills == []


def test_coordinator_role_without_stock_search_registers_nothing(monkeypatch):
    registry = Mock()
    registry.snapshot_registration = lambda name, scope=None: None
    module = types.ModuleType("tools.registry")
    module.registry = registry
    monkeypatch.setitem(sys.modules, "tools", types.ModuleType("tools"))
    monkeypatch.setitem(sys.modules, "tools.registry", module)
    ctx = CoordinatorCtx()
    register(ctx)
    assert ctx.tools == []


def test_default_role_keeps_the_delegation_surface(monkeypatch, stock_search):
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    ctx = CoordinatorCtx(settings={})
    register(ctx)
    assert [t["name"] for t in ctx.tools] == list(TOOL_NAMES)
    assert all(t["toolset"] == TOOLSET and not t.get("override") for t in ctx.tools) and len(ctx.skills) == 1


async def test_reads_send_only_the_read_credential_and_forward_args_unchanged(coordinator, broker):
    space = await call(coordinator, READS[0], {"limit": 5, "cursor": "c1", "include_archived": True})
    assert space["isError"] is False and payload(space)["args"] == {"limit": 5, "cursor": "c1", "include_archived": True}
    doc = await call(coordinator, READS[1], {"document_id": fb.DOCUMENT_ID})
    assert payload(doc) == {"document_id": fb.DOCUMENT_ID, "content_md": "# Runbook"}
    skill = await call(coordinator, READS[2], {"slug": "triage", "version_policy": "current"})
    assert payload(skill)["slug"] == "triage"
    assert broker.tool_calls == [(READS[0], {"limit": 5, "cursor": "c1", "include_archived": True}),
                                 (READS[1], {"document_id": fb.DOCUMENT_ID}),
                                 (READS[2], {"slug": "triage", "version_policy": "current"})]
    assert {r["path"] for r in broker.requests} == {fb.READ_PATH}
    for request in broker.requests:
        assert request["headers"]["authorization"] == f"Bearer {fb.CREDENTIAL}"
        assert fake_ab.TOKEN_HEADER not in request["headers"]


@pytest.mark.parametrize("name, args", [
    (READS[0], {"space_id": "s"}), (READS[0], {"workspace": "w"}), (READS[1], {"document_id": fb.DOCUMENT_ID,
                                                                             "member_id": "m"}),
    (READS[2], {"slug": "triage", "delegation_token": "dlt_x"}), (READS[1], {"document_id": fb.DOCUMENT_ID,
                                                                            "session_id": "s"}),
])
async def test_identity_and_scope_arguments_are_refused_before_the_broker(coordinator, broker, name, args):
    result = await call(coordinator, name, args)
    assert result["error"] == "unexpected_arguments" and result["arguments"]
    assert broker.requests == []


@pytest.mark.parametrize("credential", [None, "", "agw_service_credential", "dlc_pooled"])
async def test_missing_or_foreign_credential_refuses_without_network(coordinator, broker, monkeypatch, credential):
    if credential is None:
        monkeypatch.delenv("ARTIFACTBRIDGE_COORDINATOR_READ_CREDENTIAL")
    else:
        monkeypatch.setenv("ARTIFACTBRIDGE_COORDINATOR_READ_CREDENTIAL", credential)
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    for name, args in [(READS[0], {}), (READS[1], {"document_id": fb.DOCUMENT_ID}), (READS[2], {"slug": "triage"})]:
        assert (await call(coordinator, name, args)) == {"error": "not_configured", "tool": name}
    assert broker.requests == []


@pytest.mark.parametrize("code", ["space_unavailable", "action_not_allowed", "access_revoked", "invalid_cursor"])
async def test_broker_refusals_are_returned_unchanged_without_retry(coordinator, broker, code):
    broker.refuse[READS[0]] = code
    result = await call(coordinator, READS[0], {"cursor": "stale"})
    assert result["isError"] is True and payload(result) == {"error": code}
    assert len(broker.tool_calls) == 1


async def test_unknown_credential_is_401_without_retry(coordinator, broker, monkeypatch):
    monkeypatch.setenv("ARTIFACTBRIDGE_COORDINATOR_READ_CREDENTIAL", "cri_revoked")
    assert (await call(coordinator, READS[2], {"slug": "triage"})) == {"error": "unauthorized", "status": 401}
    assert len(broker.requests) == 1


async def test_tool_outside_the_saved_subset_preserves_the_json_rpc_error(coordinator, broker):
    broker.subset.discard(READS[1])
    result = await call(coordinator, READS[1], {"document_id": fb.DOCUMENT_ID})
    assert result["error"]["code"] == -32602 and READS[1] in result["error"]["message"]


async def test_transport_failure_is_structured_and_leaks_no_credential(coordinator, broker, monkeypatch, caplog):
    def failing(headers):
        return httpx2.AsyncClient(transport=httpx2.MockTransport(lambda r: (_ for _ in ()).throw(
            httpx2.ConnectError(f"boom {headers['authorization']}"))), headers=headers)

    reads = plugin_module.CoordinatorReads.func(tools_url=fake_ab.TOOLS_URL, credential=lambda: fb.CREDENTIAL,
                                                http_factory=failing)
    caplog.set_level(logging.DEBUG)
    result = await reads.handler_for(READS[0])({})
    assert json.loads(result) == {"error": "scoped_call_failed"}
    assert fb.CREDENTIAL not in result and fb.CREDENTIAL not in caplog.text


async def test_cancel_before_the_call_makes_no_broker_request(coordinator, broker):
    task = asyncio.ensure_future(coordinator[READS[0]]["handler"]({}))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert broker.requests == []


async def test_cancel_in_flight_returns_no_content(coordinator, broker):
    broker.hold = asyncio.Event()
    task = asyncio.ensure_future(coordinator[READS[1]]["handler"]({"document_id": fb.DOCUMENT_ID}))
    await asyncio.wait_for(broker.held.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    broker.hold.set()
    assert task.cancelled()


@pytest.mark.parametrize("args, error", [
    ({"query": "x", "profile": "swe"}, "profile is not allowed"),
    ({"query": "x", "profile": ""}, "profile is not allowed"),
    ({"session_id": "swe/20260612_123456_abcdef"}, "profile-qualified"),
    ({"session_id": "@session:swe/20260612_123456_abcdef"}, "profile-qualified"),
    ({"session_id": "..\\..\\x"}, "profile-qualified"),
    ({"session_id": "a..b"}, "invalid session id"),
    ({"session_id": "@session:abc"}, "invalid session id"),
    ({"session_id": ""}, "invalid session id"),
    ({"session_id": 7}, "must be strings"),
    ({"query": "x", "exclude_session_ids": ["20260612_123456_abcdef", "swe/x"]}, "profile-qualified"),
    ({"query": "x", "exclude_session_ids": "20260612_123456_abcdef"}, "must be a list"),
    ({"query": "x", "db_path": "/tmp/state.db"}, "unsupported argument: db_path"),
])
def test_confined_search_refuses_other_profiles_before_the_stock_handler(coordinator, stock_search, args, error):
    result = json.loads(coordinator["session_search"]["handler"](args, db="own-db", current_session_id="cur"))
    assert result["success"] is False and error in result["error"]
    stock_search.handler.assert_not_called()


@pytest.mark.parametrize("args", [
    {}, {"query": "deploy", "limit": 3, "exclude_session_ids": ["room_" + "a" * 32]},
    {"session_id": "20260612_123456_abcdef", "around_message_id": 42, "window": 5},
    {"session_id": "cron_job-1.v2_20260612_123456"}, {"session_id": "0b0c3c5e-1f0a-4b8e-9c1d-2a3b4c5d6e7f"},
])
def test_confined_search_passes_own_session_shapes_to_the_stock_handler(coordinator, stock_search, args):
    result = coordinator["session_search"]["handler"](args, db="own-db", current_session_id="cur")
    assert json.loads(result)["success"] is True
    stock_search.handler.assert_called_once_with(args, db="own-db", current_session_id="cur")

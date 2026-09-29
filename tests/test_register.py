import copy
import json
from unittest.mock import Mock

from jsonschema import Draft202012Validator

import pytest

import hermes_artifactbridge as plugin_module
from hermes_artifactbridge import SKILL_PATH, register
from hermes_artifactbridge.schemas import RESERVED_ARGS, TOKEN_ARG, TOOL_NAMES, TOOL_SCHEMAS, TOOLSET
from tests import fake_ab


class FakeCtx:
    def __init__(self):
        self.tools, self.skills = [], []

    def register_tool(self, **kwargs):
        self.tools.append(kwargs)

    def register_skill(self, name, path, description=""):
        self.skills.append((name, path, description))


@pytest.mark.parametrize("tools_url", ["", " ", "not-a-url", "http://app.artifactbridge.com/mcp/agent-gateway",
                                       "https://u:p@ab.test/mcp", "https://ab.test/mcp#fragment", "https://["])
async def test_register_without_a_trusted_tools_url_registers_gated_refusing_tools(monkeypatch, tools_url):
    """Declared tools stay registered, but invalid overrides never build a client or use production."""
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    monkeypatch.setenv("ARTIFACTBRIDGE_TOOLS_URL", tools_url)
    build = Mock(side_effect=AssertionError("Invalid overrides must not build a client"))
    monkeypatch.setattr(plugin_module, "build_plugin", build)
    ctx = FakeCtx()
    register(ctx)
    assert [t["name"] for t in ctx.tools] == list(TOOL_NAMES) and len(ctx.skills) == 1
    assert all(t["check_fn"]() is False for t in ctx.tools)
    for tool in ctx.tools:
        result = json.loads(await tool["handler"]({"delegation_token": "dlt_" + "a" * 43}))
        assert result["error"] == "not_configured"
    build.assert_not_called()
    assert plugin_module.PLUGIN is None


@pytest.mark.parametrize("tools_url", [None, fake_ab.TOOLS_URL, "https://custom.example/mcp/agent-gateway"])
def test_register_static_tools_and_namespaced_skill(monkeypatch, tools_url):
    if tools_url is None:
        monkeypatch.delenv("ARTIFACTBRIDGE_TOOLS_URL", raising=False)
    else:
        monkeypatch.setenv("ARTIFACTBRIDGE_TOOLS_URL", tools_url)
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    ctx = FakeCtx()
    register(ctx)
    expected_url = tools_url if tools_url is not None else "https://app.artifactbridge.com/mcp/agent-gateway"
    assert plugin_module.PLUGIN is not None
    assert plugin_module.PLUGIN.tools_url == expected_url
    assert [t["name"] for t in ctx.tools] == list(TOOL_NAMES)
    for tool in ctx.tools:
        assert tool["toolset"] == TOOLSET and tool["is_async"] is True
        assert tool["check_fn"]() is True
        assert tool["requires_env"] == ["ARTIFACTBRIDGE_SERVICE_CREDENTIAL"]
    assert ctx.skills == [("delegation", SKILL_PATH, ctx.skills[0][2])]
    assert SKILL_PATH.is_file() and SKILL_PATH.read_text().startswith("---\nname: delegation")


@pytest.mark.parametrize("credential", [None, ""])
@pytest.mark.parametrize("tools_url", [None, fake_ab.TOOLS_URL])
async def test_missing_credential_gates_tools_and_refuses_before_network(monkeypatch, credential, tools_url):
    for name, value in [("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", credential),
                        ("ARTIFACTBRIDGE_TOOLS_URL", tools_url)]:
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    ctx = FakeCtx()
    register(ctx)
    network = Mock(side_effect=AssertionError("Missing credential must not reach the network"))
    monkeypatch.setattr(plugin_module.PLUGIN, "_http_factory", network)
    for tool in ctx.tools:
        assert tool["check_fn"]() is False
        with pytest.raises(RuntimeError, match="ARTIFACTBRIDGE_SERVICE_CREDENTIAL is not set"):
            await tool["handler"]({"delegation_token": "dlt_" + "a" * 43})
    network.assert_not_called()


def test_manifest_matches_registered_tools_and_required_environment(monkeypatch):
    monkeypatch.delenv("ARTIFACTBRIDGE_TOOLS_URL", raising=False)
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    ctx = FakeCtx()
    register(ctx)
    # These manifest fields are block lists; stop at the next top-level field/comment.
    manifest = (SKILL_PATH.parents[2] / "plugin.yaml").read_text()

    def block_list(field):
        lines = manifest.split(field + ":\n", 1)[1].splitlines()
        values = []
        for line in lines:
            if not line.startswith("  - "):
                break
            values.append(line.removeprefix("  - "))
        return values

    assert block_list("requires_env") == ["ARTIFACTBRIDGE_SERVICE_CREDENTIAL"]
    assert all(tool["requires_env"] == block_list("requires_env") for tool in ctx.tools)
    assert block_list("provides_tools") == [tool["name"] for tool in ctx.tools]


def test_schemas_are_static_require_token_and_restrict_authority_selectors():
    before = copy.deepcopy(TOOL_SCHEMAS)
    for schema in TOOL_SCHEMAS:
        params = schema["parameters"]
        assert TOKEN_ARG in params["properties"] and params["required"][0] == TOKEN_ARG, schema["name"]
        reserved = RESERVED_ARGS - {"room_id"} if schema["name"] == "artifactbridge_delegation_create_document" else RESERVED_ARGS
        assert reserved.isdisjoint(params["properties"]), schema["name"]
        assert params["additionalProperties"] is False
        json.dumps(schema)  # provider-serializable
    assert TOOL_SCHEMAS == before
    with_key = {s["name"] for s in TOOL_SCHEMAS if "idempotency_key" in s["parameters"]["properties"]}
    assert with_key == {"artifactbridge_delegation_contribute", "artifactbridge_delegation_create_document",
                        "artifactbridge_delegation_propose_change"}


CREATE_DOCUMENT = "artifactbridge_delegation_create_document"
ROOM = "10000000-0000-4000-8000-000000000001"
OTHER_ROOM = "10000000-0000-4000-8000-000000000002"


@pytest.fixture
def registered_tools(monkeypatch, app):
    """Use the public registration boundary and real MCP serialization, with no network."""
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    monkeypatch.setenv("ARTIFACTBRIDGE_TOOLS_URL", fake_ab.TOOLS_URL)
    # build_plugin forwards this transport into ScopedTools; no production-only test seam.
    build = plugin_module.build_plugin
    monkeypatch.setattr(plugin_module, "build_plugin", lambda **kw: build(**kw, http_factory=fake_ab.http_factory(app)))
    ctx = FakeCtx()
    register(ctx)
    return {tool["name"]: tool for tool in ctx.tools}


@pytest.mark.parametrize("body", [
    ' \ufeff<!doctype html>\r\n<style>p::after{content:"é 😀"}</style>\r\n'
    '<script>const x = "\\\\n";</script><p>e\u0301 &amp; x</p>\r\n ',
    "é" * 200_001,  # More than the old Markdown character cap.
    "😀" * 2_500_000,  # Exactly 10,000,000 UTF-8 bytes.
], ids=["verbatim", "above-prose-cap", "html-byte-cap"])
async def test_registered_html_create_preserves_file_bytes_and_room(registered_tools, ab, token, tmp_path, body):
    path = tmp_path / "design.html"
    original = body.encode("utf-8")
    path.write_bytes(original)
    args = {"delegation_token": token, "title": "Design", "format": "html", "room_id": ROOM,
            "content_md": path.read_bytes().decode("utf-8"), "idempotency_key": "design-1"}
    tool = registered_tools[CREATE_DOCUMENT]
    Draft202012Validator(tool["schema"]["parameters"]).validate(args)
    # Even callers bypassing model schema cannot override the delegation.
    result = json.loads(await tool["handler"]({**args, "delegation_id": "forged"}))
    assert result["ok"] is True
    assert len(ab.tool_calls) == 1
    name, sent = ab.tool_calls[0]
    assert name == CREATE_DOCUMENT
    assert sent == {k: v for k, v in args.items() if k != "delegation_token"}
    assert sent["content_md"].encode("utf-8") == original
    assert ab.renew_calls == 0


@pytest.mark.parametrize("format_args", [{}, {"format": "markdown"}, {"format": "html"}])
async def test_registered_library_create_keeps_room_omitted(registered_tools, ab, token, format_args):
    args = {"delegation_token": token, "title": "Library", "content_md": "x" * 200_000, **format_args}
    tool = registered_tools[CREATE_DOCUMENT]
    Draft202012Validator(tool["schema"]["parameters"]).validate(args)
    assert json.loads(await tool["handler"](args))["ok"] is True
    assert ab.tool_calls == [(CREATE_DOCUMENT, {k: v for k, v in args.items() if k != "delegation_token"})]


@pytest.mark.parametrize("name", [name for name in TOOL_NAMES if name != CREATE_DOCUMENT])
async def test_registered_other_tools_still_strip_authority(registered_tools, ab, token, name):
    await registered_tools[name]["handler"]({"delegation_token": token, "delegation_id": "forged", "room_id": OTHER_ROOM})
    assert ab.tool_calls == [(name, {})]


# These replies are backend contract fixtures, not a second implementation of
# backend validation or SQL receipts. The plugin owns forwarding and passthrough.
@pytest.mark.parametrize("extra, body, reply", [
    ({"format": "html", "room_id": OTHER_ROOM}, "<p>x</p>", {"error": "room_out_of_scope"}),
    ({"format": "markdown", "room_id": ROOM}, "x", {
        "error": "validation_error", "message": 'room_id is supported only with format "html".'}),
    ({}, "x" * 200_001, {"error": "validation_error", "message": "content_md must be 200,000 characters or smaller."}),
    ({"format": "html", "room_id": ROOM}, "é" * 5_000_000 + "x", {
        "error": "validation_error", "message": "HTML design artifacts must be 10,000,000 bytes or smaller."}),
    ({"format": "html", "room_id": ROOM}, " \r\n", {"error": "validation_error", "message": "The HTML file is empty."}),
    ({"format": "html", "room_id": ROOM}, "<p>\x00</p>", {
        "error": "validation_error", "message": "The HTML file must be well-formed UTF-8 text without NUL bytes."}),
    ({"format": "html"}, "<p>x</p>", {"error": "destination_not_allowed"}),
    ({"format": "html", "room_id": ROOM}, "<p>x</p>", {"error": "action_not_allowed"}),
], ids=["foreign-room", "markdown-room", "markdown-too-large", "html-too-large", "empty-html", "nul-html", "no-folder", "read-only"])
async def test_registered_create_returns_backend_refusal_without_fallback(
        registered_tools, ab, token, monkeypatch, extra, body, reply):
    expected = {**reply, "delegation_id": "del-1"}
    monkeypatch.setitem(fake_ab.TOOLS, CREATE_DOCUMENT, lambda *a: fake_ab._text(expected, True))
    args = {"title": "Refused", "content_md": body, "idempotency_key": "one-write", **extra}
    result = json.loads(await registered_tools[CREATE_DOCUMENT]["handler"]({"delegation_token": token, **args}))
    assert result == expected
    assert ab.tool_calls == [(CREATE_DOCUMENT, args)]
    assert ab.renew_calls == 0
    assert all(r["path"] == fake_ab.TOOLS_PATH for r in ab.requests)


@pytest.mark.parametrize("attach_failed", [False, True])
@pytest.mark.parametrize("expired", [False, True])
async def test_registered_create_preserves_receipt_and_key_on_retry(
        registered_tools, ab, token, monkeypatch, attach_failed, expired):
    if expired:
        ab.tokens[token]["expires_at"] = ab.now() - 1
    receipt = {"ok": True, "delegation_id": "del-1", "document_id": "doc-created", "version_id": "v1",
               "document_version_id": "v1", "room_id": ROOM, "library_scope": "room",
               "attachment_created": not attach_failed,
               "attachment": None if attach_failed else {"id": "edge-1", "attachedVersionId": "v1"}}
    if attach_failed:
        receipt["room_attach_failed"] = {
            "code": "attach_failed",
            "message": "The document was created, but attaching it to the room failed. Retry the attach with the arguments below.",
            "retry": {"tool": "artifactbridge_attach_document_to_agent_room",
                      "arguments": {"room_id": ROOM, "document_id": "doc-created"}}}
    replies = iter([{**receipt, "replayed": False}, {**receipt, "replayed": True}])
    monkeypatch.setitem(fake_ab.TOOLS, CREATE_DOCUMENT, lambda *a: fake_ab._text(next(replies)))
    args = {"title": "Design", "content_md": "<p>x</p>", "format": "html", "room_id": ROOM, "idempotency_key": "stable-key"}
    for replayed in (False, True):
        result = json.loads(await registered_tools[CREATE_DOCUMENT]["handler"]({"delegation_token": token, **args}))
        assert result == {**receipt, "replayed": replayed}
    assert ab.tool_calls == [(CREATE_DOCUMENT, args), (CREATE_DOCUMENT, args)]
    assert ab.renew_calls == int(expired)


async def test_registered_html_create_ambiguous_failure_is_not_retried(registered_tools, ab, token, monkeypatch):
    monkeypatch.setitem(fake_ab.TOOLS, CREATE_DOCUMENT, Mock(side_effect=ConnectionError("response lost")))
    args = {"title": "Design", "content_md": "<p>x</p>", "format": "html", "room_id": ROOM, "idempotency_key": "stable-key"}
    result = json.loads(await registered_tools[CREATE_DOCUMENT]["handler"]({"delegation_token": token, **args}))
    assert result == {"error": "scoped_call_failed"}
    assert ab.tool_calls == [(CREATE_DOCUMENT, args)]
    assert ab.renew_calls == 0

import copy
import json
from unittest.mock import Mock

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


def test_schemas_are_static_require_the_token_and_expose_no_authority_fields():
    before = copy.deepcopy(TOOL_SCHEMAS)
    for schema in TOOL_SCHEMAS:
        params = schema["parameters"]
        assert TOKEN_ARG in params["properties"] and params["required"][0] == TOKEN_ARG, schema["name"]
        assert RESERVED_ARGS.isdisjoint(params["properties"]), schema["name"]
        assert params["additionalProperties"] is False
        json.dumps(schema)  # provider-serializable
    assert TOOL_SCHEMAS == before
    with_key = {s["name"] for s in TOOL_SCHEMAS if "idempotency_key" in s["parameters"]["properties"]}
    assert with_key == {"artifactbridge_delegation_contribute", "artifactbridge_delegation_create_document",
                        "artifactbridge_delegation_propose_change"}

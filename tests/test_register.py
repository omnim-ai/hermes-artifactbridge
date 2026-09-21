import copy
import json

import pytest

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


@pytest.mark.parametrize("tools_url", [None, "", "http://app.artifactbridge.com/mcp/agent-gateway", "https://u:p@ab.test/mcp"])
async def test_register_without_a_trusted_tools_url_registers_gated_refusing_tools(monkeypatch, tools_url):
    """Stock convention: declared tools are always registered (no manifest drift) and hidden by the
    requires_env/check_fn gate; with an untrusted URL every handler refuses and builds no client."""
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    if tools_url is None:
        monkeypatch.delenv("ARTIFACTBRIDGE_TOOLS_URL", raising=False)
    else:
        monkeypatch.setenv("ARTIFACTBRIDGE_TOOLS_URL", tools_url)
    ctx = FakeCtx()
    register(ctx)
    assert [t["name"] for t in ctx.tools] == list(TOOL_NAMES) and len(ctx.skills) == 1
    assert all(t["check_fn"]() is False for t in ctx.tools)
    result = json.loads(await ctx.tools[0]["handler"]({"delegation_token": "dlt_" + "a" * 43}))
    assert result["error"] == "not_configured"


def test_register_static_tools_and_namespaced_skill(monkeypatch):
    monkeypatch.setenv("ARTIFACTBRIDGE_TOOLS_URL", fake_ab.TOOLS_URL)
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    ctx = FakeCtx()
    register(ctx)
    assert [t["name"] for t in ctx.tools] == list(TOOL_NAMES)
    for tool in ctx.tools:
        assert tool["toolset"] == TOOLSET and tool["is_async"] is True
        assert tool["check_fn"]() is True
        assert tool["requires_env"] == ["ARTIFACTBRIDGE_SERVICE_CREDENTIAL", "ARTIFACTBRIDGE_TOOLS_URL"]
    assert ctx.skills == [("delegation", SKILL_PATH, ctx.skills[0][2])]
    assert SKILL_PATH.is_file() and SKILL_PATH.read_text().startswith("---\nname: delegation")


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

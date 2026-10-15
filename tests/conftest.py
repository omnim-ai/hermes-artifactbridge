import pytest

import hermes_artifactbridge as plugin_module
from hermes_artifactbridge import build_plugin, register
from tests import fake_ab


class FakeCtx:
    def __init__(self):
        self.tools, self.skills = [], []

    def register_tool(self, **kwargs):
        self.tools.append(kwargs)

    def register_skill(self, name, path, description=""):
        self.skills.append((name, path, description))


@pytest.fixture
def ab():
    fake = fake_ab.FakeAB()
    fake.delegations["del-1"] = fake_ab.Delegation(id="del-1", writable_document_ids=["doc-w"])
    fake.documents["doc-w"] = {"document_id": "doc-w", "title": "Writable", "content_md": "# w"}
    return fake


@pytest.fixture
def app(ab):
    return fake_ab.make_app(ab)


@pytest.fixture
def plugin(app, monkeypatch):
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", fake_ab.SERVICE_CREDENTIAL)
    return build_plugin(tools_url=fake_ab.TOOLS_URL, http_factory=fake_ab.http_factory(app))


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


@pytest.fixture
def token(ab):
    return ab.mint("del-1")

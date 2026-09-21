import pytest

from hermes_artifactbridge import build_plugin
from tests import fake_ab


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
def token(ab):
    return ab.mint("del-1")


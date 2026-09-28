"""Profile-scoped settings: a multiplexed Hermes gateway serves several profiles from one process,
so os.environ holds only the DEFAULT profile's .env. The serving profile's credential and tools URL
must come from Hermes' per-turn secret scope (agent.secret_scope.get_secret), never from another
profile's environment. Without Hermes (or with an older Hermes) plain os.environ still applies.
"""
import sys
import types

import pytest

import hermes_artifactbridge as plugin_module
from tests import fake_ab

DEFAULT_PROFILE_CREDENTIAL = "agw_default_profile_credential"
SERVING_PROFILE_CREDENTIAL = "agw_serving_profile_credential"


class UnscopedSecretError(RuntimeError):
    pass


def install_fake_secret_scope(monkeypatch, scope, *, multiplex=True):
    """A stand-in for Hermes' agent.secret_scope with its documented get_secret contract."""
    agent_pkg = types.ModuleType("agent")
    module = types.ModuleType("agent.secret_scope")

    def get_secret(name, default=None):
        if scope is not None:
            value = scope.get(name)
            return value if value is not None else default
        if multiplex:
            raise UnscopedSecretError(name)
        import os
        return os.environ.get(name, default)

    module.get_secret = get_secret
    agent_pkg.secret_scope = module
    monkeypatch.setitem(sys.modules, "agent", agent_pkg)
    monkeypatch.setitem(sys.modules, "agent.secret_scope", module)


def test_a_serving_profile_scope_supplies_its_own_credential_not_the_process_environment(monkeypatch):
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", DEFAULT_PROFILE_CREDENTIAL)
    install_fake_secret_scope(monkeypatch, {"ARTIFACTBRIDGE_SERVICE_CREDENTIAL": SERVING_PROFILE_CREDENTIAL})

    assert plugin_module._configured() is True
    assert plugin_module._service_credential() == SERVING_PROFILE_CREDENTIAL


def test_a_scope_without_the_credential_is_not_configured_even_when_another_profile_has_one(monkeypatch):
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", DEFAULT_PROFILE_CREDENTIAL)
    install_fake_secret_scope(monkeypatch, {})

    assert plugin_module._configured() is False
    with pytest.raises(RuntimeError, match="not set"):
        plugin_module._service_credential()


def test_no_profile_scope_while_multiplexing_fails_closed(monkeypatch):
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", DEFAULT_PROFILE_CREDENTIAL)
    install_fake_secret_scope(monkeypatch, None, multiplex=True)

    assert plugin_module._configured() is False


def test_the_tools_url_override_is_read_from_the_serving_profile(monkeypatch):
    monkeypatch.setenv("ARTIFACTBRIDGE_TOOLS_URL", "https://default-profile.example/mcp/agent-gateway")
    install_fake_secret_scope(monkeypatch, {"ARTIFACTBRIDGE_SERVICE_CREDENTIAL": SERVING_PROFILE_CREDENTIAL,
                                            "ARTIFACTBRIDGE_TOOLS_URL": fake_ab.TOOLS_URL})

    assert plugin_module._tools_url() == fake_ab.TOOLS_URL


def test_a_scope_without_a_tools_url_uses_production(monkeypatch):
    monkeypatch.setenv("ARTIFACTBRIDGE_TOOLS_URL", "https://default-profile.example/mcp/agent-gateway")
    install_fake_secret_scope(monkeypatch, {"ARTIFACTBRIDGE_SERVICE_CREDENTIAL": SERVING_PROFILE_CREDENTIAL})

    assert plugin_module._tools_url() == plugin_module.DEFAULT_TOOLS_URL


def test_an_explicit_empty_tools_url_in_the_scope_still_fails_closed(monkeypatch):
    install_fake_secret_scope(monkeypatch, {"ARTIFACTBRIDGE_SERVICE_CREDENTIAL": SERVING_PROFILE_CREDENTIAL,
                                            "ARTIFACTBRIDGE_TOOLS_URL": ""})

    assert plugin_module._tools_url() == ""
    assert plugin_module._configured() is False


def test_without_hermes_secret_scope_the_process_environment_is_used(monkeypatch):
    monkeypatch.setitem(sys.modules, "agent", None)  # import agent.secret_scope -> ImportError
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", SERVING_PROFILE_CREDENTIAL)
    monkeypatch.delenv("ARTIFACTBRIDGE_TOOLS_URL", raising=False)

    assert plugin_module._configured() is True
    assert plugin_module._service_credential() == SERVING_PROFILE_CREDENTIAL
    assert plugin_module._tools_url() == plugin_module.DEFAULT_TOOLS_URL


async def test_a_scoped_call_presents_the_serving_profiles_credential(monkeypatch, ab, app):
    """End to end against the fake boundary: the request carries the scoped credential."""
    monkeypatch.setenv("ARTIFACTBRIDGE_SERVICE_CREDENTIAL", DEFAULT_PROFILE_CREDENTIAL)
    install_fake_secret_scope(monkeypatch, {"ARTIFACTBRIDGE_SERVICE_CREDENTIAL": fake_ab.SERVICE_CREDENTIAL})
    plugin = plugin_module.build_plugin(tools_url=fake_ab.TOOLS_URL, http_factory=fake_ab.http_factory(app))

    result = await plugin.invoke("artifactbridge_delegation_read_brief", {"delegation_token": ab.mint("del-1")})

    assert "error" not in result

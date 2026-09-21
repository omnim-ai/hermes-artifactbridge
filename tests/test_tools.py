import json

import pytest

from hermes_artifactbridge.schemas import TOOL_NAMES
from tests import fake_ab

READ_BRIEF = "artifactbridge_delegation_read_brief"
CONTRIBUTE = "artifactbridge_delegation_contribute"


def _no_secrets(value: object, *secrets: str) -> None:
    dumped = json.dumps(value)
    for secret in secrets:
        assert secret not in dumped


# ---- token handling before any network call ----

async def test_missing_token_refused_without_network(plugin, ab):
    assert await plugin.invoke(READ_BRIEF, {}) == {"error": "missing_delegation_token"}
    assert ab.requests == []


@pytest.mark.parametrize("bad", ["", "dlt_short", "dlc_" + "a" * 43, "dlt_" + "a" * 42, "dlt_" + "a" * 44,
                                 "dlt_" + "a" * 42 + "+", 42, None])
async def test_malformed_token_refused_without_network(plugin, ab, bad):
    result = await plugin.invoke(READ_BRIEF, {"delegation_token": bad})
    assert result["error"] in ("invalid_delegation_token", "missing_delegation_token")
    assert ab.requests == []


async def test_token_travels_as_header_never_as_argument(plugin, ab, token):
    result = await plugin.invoke(READ_BRIEF, {"delegation_token": token})
    assert result["ok"] is True and result["delegation_id"] == "del-1"
    assert ab.requests and all(r["path"] == fake_ab.TOOLS_PATH for r in ab.requests)
    assert all(r["authorization"] == f"Bearer {fake_ab.SERVICE_CREDENTIAL}" and r["token"] == token for r in ab.requests)
    assert ab.tool_calls == [(READ_BRIEF, {})]


async def test_reserved_args_are_stripped_and_room_defaults_server_side(plugin, ab, token):
    result = await plugin.invoke(CONTRIBUTE, {"delegation_token": token, "delegation_id": "del-other",
                                              "room_id": "room-stolen", "kind": "message", "body": "hi"})
    assert result["ok"] is True and result["event_id"] == "evt-1"
    _, args = ab.tool_calls[0]
    assert "delegation_id" not in args and "room_id" not in args and "delegation_token" not in args
    assert ab.events[0]["room_id"] == "room-1"


async def test_only_the_configured_origin_is_ever_contacted(plugin, ab, token):
    await plugin.invoke(READ_BRIEF, {"delegation_token": token})
    assert plugin.tools_url == fake_ab.TOOLS_URL
    assert plugin.renew_url == fake_ab.ORIGIN + fake_ab.RENEW_PATH


# ---- boundary denials ----

async def test_unknown_token_is_401_unauthorized(plugin, ab):
    result = await plugin.invoke(READ_BRIEF, {"delegation_token": "dlt_" + "A" * 43})
    assert result == {"error": "unauthorized", "status": 401}
    assert ab.tool_calls == [] and ab.renew_calls == 0


async def test_other_service_token_is_401_unauthorized(plugin, ab):
    foreign = ab.mint("del-1", service=fake_ab.OTHER_SERVICE_CREDENTIAL)
    result = await plugin.invoke(READ_BRIEF, {"delegation_token": foreign})
    assert result == {"error": "unauthorized", "status": 401}
    assert ab.tool_calls == []


@pytest.mark.parametrize("setup, code", [
    (lambda ab: ab.revoke_all("del-1"), "credential_revoked"),
    (lambda ab: setattr(ab.delegations["del-1"], "generation", 2), "generation_stale"),
    (lambda ab: setattr(ab.delegations["del-1"], "state", "completed"), "delegation_not_active"),
    (lambda ab: setattr(ab.delegations["del-1"], "room_open", False), "room_closed"),
    (lambda ab: setattr(ab, "service_disabled", True), "service_disabled"),
])
async def test_final_403_codes_fail_without_renewal_or_retry(plugin, ab, token, setup, code):
    setup(ab)
    result = await plugin.invoke(CONTRIBUTE, {"delegation_token": token, "kind": "message", "body": "x"})
    assert result == {"error": code, "status": 403}
    assert ab.tool_calls == [] and ab.renew_calls == 0 and ab.events == []


# ---- renewal ----

async def test_expired_token_is_renewed_once_and_the_call_retried_once(plugin, ab, token):
    ab.tokens[token]["expires_at"] = ab.now() - 1
    result = await plugin.invoke(CONTRIBUTE, {"delegation_token": token, "kind": "message", "body": "x", "idempotency_key": "op-1"})
    assert result["ok"] is True and result["replayed"] is False
    assert ab.renew_calls == 1 and len(ab.events) == 1
    assert [r["path"] for r in ab.requests if r["path"] == fake_ab.RENEW_PATH] == [fake_ab.RENEW_PATH]
    renew = next(r for r in ab.requests if r["path"] == fake_ab.RENEW_PATH)
    assert renew["authorization"] == f"Bearer {fake_ab.SERVICE_CREDENTIAL}" and renew["token"] == token
    assert ab.tokens[token]["expires_at"] > ab.now() + fake_ab.RENEWAL_SECONDS - 5


async def test_renewal_refusal_is_returned_without_retry(plugin, ab, token):
    ab.tokens[token]["expires_at"] = ab.now() - 1
    ab.delegations["del-1"].generation = 2  # expired AND stale: renewal reports the live-state denial
    result = await plugin.invoke(CONTRIBUTE, {"delegation_token": token, "kind": "message", "body": "x"})
    assert result == {"error": "generation_stale", "status": 403}
    assert ab.renew_calls == 1 and ab.tool_calls == []


async def test_renewal_redirect_is_refused_not_followed(plugin, ab, token):
    ab.tokens[token]["expires_at"] = ab.now() - 1
    ab.redirect_renew = True
    result = await plugin.invoke(READ_BRIEF, {"delegation_token": token})
    assert result == {"error": "renewal_refused", "status": 307}
    assert ab.renew_calls == 1 and ab.tool_calls == []
    assert all(r["path"] in (fake_ab.TOOLS_PATH, fake_ab.RENEW_PATH) for r in ab.requests)


async def test_still_expired_after_renewal_does_not_loop(plugin, ab, token, monkeypatch):
    ab.tokens[token]["expires_at"] = ab.now() - 1
    monkeypatch.setattr(fake_ab, "RENEWAL_SECONDS", -10)  # renewal "succeeds" but leaves the token expired
    result = await plugin.invoke(READ_BRIEF, {"delegation_token": token})
    assert result == {"error": "credential_expired", "status": 403}
    assert ab.renew_calls == 1
    assert len([r for r in ab.requests if r["path"] == fake_ab.TOOLS_PATH]) == 2


# ---- writes ----

async def test_model_idempotency_key_is_forwarded_and_replayed(plugin, ab, token):
    args = {"delegation_token": token, "kind": "evidence", "body": "same", "idempotency_key": "op-7"}
    first = await plugin.invoke(CONTRIBUTE, dict(args))
    second = await plugin.invoke(CONTRIBUTE, dict(args))
    assert first["event_id"] == second["event_id"] == "evt-1" and second["replayed"] is True
    assert len(ab.events) == 1 and ab.tool_calls[0][1]["idempotency_key"] == "op-7"


async def test_identical_writes_without_key_are_intentional_duplicates(plugin, ab, token):
    for _ in range(2):
        await plugin.invoke(CONTRIBUTE, {"delegation_token": token, "kind": "message", "body": "same"})
    assert [e["id"] for e in ab.events] == ["evt-1", "evt-2"]


async def test_transport_failure_is_structured_and_not_retried(plugin, ab, token, monkeypatch):
    calls = 0

    async def broken(*a, **kw):
        nonlocal calls
        calls += 1
        raise ConnectionError("boom")

    monkeypatch.setattr("hermes_artifactbridge.tools.call_scoped_tool", broken)
    result = await plugin.invoke(CONTRIBUTE, {"delegation_token": token, "kind": "message", "body": "x"})
    assert result == {"error": "scoped_call_failed"} and calls == 1 and ab.renew_calls == 0


async def test_scoped_tool_errors_pass_through(plugin, ab, token):
    result = await plugin.invoke("artifactbridge_delegation_propose_change",
                                 {"delegation_token": token, "document_id": "doc-ro", "proposed_md": "x"})
    assert result["error"] == "document_not_writable"


# ---- hygiene ----

async def test_service_credential_never_appears_in_results(plugin, ab, token):
    ab.tokens[token]["expires_at"] = ab.now() - 1
    ab.redirect_renew = True
    for args in ({}, {"delegation_token": "dlt_" + "B" * 43}, {"delegation_token": token}):
        _no_secrets(await plugin.invoke(READ_BRIEF, args), fake_ab.SERVICE_CREDENTIAL, token)
    ab.redirect_renew = False
    ab.tokens[token]["expires_at"] = ab.now() + 100
    _no_secrets(await plugin.invoke(READ_BRIEF, {"delegation_token": token}), fake_ab.SERVICE_CREDENTIAL)
    assert fake_ab.SERVICE_CREDENTIAL not in repr(plugin)


@pytest.mark.parametrize("name", TOOL_NAMES)
async def test_every_canonical_tool_routes(plugin, ab, token, name):
    args = {"delegation_token": token, "document_id": "doc-w", "query": "q", "kind": "message", "body": "b",
            "content_type": "image/png", "data_base64": "AA==", "title": "t", "content_md": "c", "proposed_md": "p"}
    handler = plugin.handler_for(name)
    raw = await handler(args, task_id="t", session_id="s")
    assert isinstance(raw, str)  # Hermes' registry accepts only string results
    result = json.loads(raw)
    assert result.get("ok") is True, result
    assert ab.tool_calls[-1][0] == name and "delegation_token" not in ab.tool_calls[-1][1]

"""Scoped tool handlers: validate the model-supplied delegation token, move it into
the request header, make one scoped MCP call to the configured endpoint, and
renew the token once when ArtifactBridge reports it expired.

Refusals are structured (``{"error": code}``) and never fall back to personal
ArtifactBridge access. The service credential never enters arguments, results
or logs; the delegation token is model-visible by design but is only ever sent
to the configured ArtifactBridge origin.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Awaitable, Callable

from .endpoint import renew_url_for
from .mcp_client import (HttpFactory, call_scoped_tool, default_http_factory, renew_delegation_token,
                         scoped_headers)
from .pool_core import POOL_PLATFORM, CredentialBindings
from .schemas import POOL_WIRE_NAMES, RESERVED_ARGS, SPACE_TOOL_NAMES, TOKEN_ARG, TOOL_NAMES

logger = logging.getLogger(__name__)

TOKEN_RE = re.compile(r"^dlt_[A-Za-z0-9_-]{43}$")
# The only boundary denial that renewal can cure; every other 401/403 is final for this token.
RENEWABLE_CODE = "credential_expired"


def refusal(code: str, **detail: object) -> dict:
    return {"error": code, **detail}


def session_route() -> tuple[str, str, str]:
    """(platform, chat_id, message_id) of the Hermes turn running this tool call; for A2A listeners
    chat_id is the contextId and message_id the A2A task id. Empty outside a Hermes gateway turn."""
    try:
        from gateway.session_context import get_session_env
    except ImportError:
        return "", "", ""
    return tuple(get_session_env(name, "") or "" for name in
                 ("HERMES_SESSION_PLATFORM", "HERMES_SESSION_CHAT_ID", "HERMES_SESSION_MESSAGE_ID"))


def _payload(name: str, args: dict) -> tuple[dict, bool]:
    # room_id opts into a Room artifact only here. Never default, normalize, or
    # drop it based on format: the backend must reject invalid/out-of-scope requests.
    reserved = RESERVED_ARGS - {"room_id"} if name == "artifactbridge_delegation_create_document" else RESERVED_ARGS
    # Space tools forward the optional delegation_id selector; the backend refuses any but the token's own.
    preserve = name in SPACE_TOOL_NAMES
    if preserve:
        reserved = reserved - {"delegation_id"}
    return {k: v for k, v in args.items() if k not in reserved}, preserve


class ScopedTools:
    def __init__(self, *, tools_url: str, service_credential: Callable[[], str],
                 http_factory: HttpFactory = default_http_factory):
        self.tools_url = tools_url
        self.renew_url = renew_url_for(tools_url)
        self._service_credential = service_credential
        self._http_factory = http_factory

    def handler_for(self, name: str) -> Callable[..., Awaitable[str]]:
        """The Hermes tool handler: Hermes requires a string result (its registry rejects dicts)."""
        if name not in TOOL_NAMES:
            raise KeyError(name)

        async def handler(args: dict, **kwargs) -> str:
            return json.dumps(await self.invoke(name, args))

        handler.__name__ = name
        return handler

    async def invoke(self, name: str, args: dict) -> dict:
        if session_route()[0] == POOL_PLATFORM:  # no dlt_ mode inside a pooled session
            return refusal("pooled_session_requires_pooled_tools")
        args = dict(args or {})
        token = args.pop(TOKEN_ARG, None)
        if token is None:
            return refusal("missing_delegation_token")
        if not isinstance(token, str) or not TOKEN_RE.match(token):
            return refusal("invalid_delegation_token")
        payload, preserve = _payload(name, args)
        headers = scoped_headers(self._service_credential(), token)

        result = await self._call(name, payload, headers, preserve)
        if result.get("error") != RENEWABLE_CODE or result.get("status") != 403:
            return result
        # The boundary refused before any tool ran, so one retry after renewal cannot duplicate a write.
        try:
            renewed = await renew_delegation_token(self.renew_url, headers, http_factory=self._http_factory)
        except Exception as exc:
            logger.warning("artifactbridge token renewal failed: %s", type(exc).__name__)
            return refusal("renewal_unavailable")
        if "error" in renewed:
            return renewed
        return await self._call(name, payload, headers, preserve)

    async def _call(self, name: str, payload: dict, headers: dict[str, str], preserve: bool) -> dict:
        try:
            return await call_scoped_tool(self.tools_url, headers, name, payload, http_factory=self._http_factory,
                                          preserve=preserve)
        except Exception as exc:
            # Outcome unknown for a write: the skill says read back before repeating, same idempotency_key.
            logger.warning("artifactbridge scoped call failed: %s %s", name, type(exc).__name__)
            return refusal("scoped_call_failed")


class PooledTools:
    """Token-free scoped tools for pooled sessions. Each call presents only the ``dlc_`` bound to the
    current (artifactbridge-pool, contextId, task id) session: no binding is a refusal, never a fallback to
    a dlt_, a service credential or personal access. No dlt_ renewal applies: a ``dlc_`` near its expiresAt
    is re-bootstrapped by the lookup, and a ``credential_expired`` denial is re-bootstrapped and retried
    exactly once (never ``access_revoked`` or a canceled task)."""

    def __init__(self, *, tools_url: str, credentials: CredentialBindings, http_factory: HttpFactory = default_http_factory):
        self.tools_url = tools_url
        self._credentials = credentials
        self._http_factory = http_factory

    def handler_for(self, name: str) -> Callable[..., Awaitable[str]]:
        if name not in POOL_WIRE_NAMES:
            raise KeyError(name)

        async def handler(args: dict, **kwargs) -> str:
            return json.dumps(await self.invoke(name, args))

        handler.__name__ = name
        return handler

    async def invoke(self, name: str, args: dict) -> dict:
        return await self.invoke_for(session_route(), name, args)

    async def invoke_for(self, route: tuple[str, str, str], name: str, args: dict) -> dict:
        """``invoke`` for the (platform, contextId, task id) ``route`` the caller names, outside the turn."""
        platform, context_id, task_id = route
        credential = (await asyncio.to_thread(self._credentials.lookup, platform, context_id, task_id)
                      if platform == POOL_PLATFORM else None)
        if credential is None:
            return refusal("pooled_credential_unavailable")
        wire = POOL_WIRE_NAMES[name]
        args = {k: v for k, v in (args or {}).items() if k != TOKEN_ARG}
        payload, preserve = _payload(wire, args)
        try:
            result = await call_scoped_tool(self.tools_url, {"authorization": f"Bearer {credential}"}, wire, payload,
                                            http_factory=self._http_factory, preserve=preserve)
            if result.get("error") != RENEWABLE_CODE:
                return result
            renewed = await asyncio.to_thread(self._credentials.refresh, platform, context_id, task_id, credential)
            if renewed is None:
                return result
            return await call_scoped_tool(self.tools_url, {"authorization": f"Bearer {renewed}"}, wire, payload,
                                          http_factory=self._http_factory, preserve=preserve)
        except Exception as exc:
            logger.warning("artifactbridge pooled scoped call failed: %s %s", wire, type(exc).__name__)
            return refusal("scoped_call_failed")

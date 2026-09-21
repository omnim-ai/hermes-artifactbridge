"""Scoped tool handlers: validate the model-supplied delegation token, move it into
the request header, make one scoped MCP call to the configured endpoint, and
renew the token once when ArtifactBridge reports it expired.

Refusals are structured (``{"error": code}``) and never fall back to personal
ArtifactBridge access. The service credential never enters arguments, results
or logs; the delegation token is model-visible by design but is only ever sent
to the configured ArtifactBridge origin.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Awaitable, Callable

from .endpoint import renew_url_for
from .mcp_client import (HttpFactory, call_scoped_tool, default_http_factory, renew_delegation_token,
                         scoped_headers)
from .schemas import RESERVED_ARGS, TOKEN_ARG, TOOL_NAMES

logger = logging.getLogger(__name__)

TOKEN_RE = re.compile(r"^dlt_[A-Za-z0-9_-]{43}$")
# The only boundary denial that renewal can cure; every other 401/403 is final for this token.
RENEWABLE_CODE = "credential_expired"


def refusal(code: str, **detail: object) -> dict:
    return {"error": code, **detail}


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
        args = dict(args or {})
        token = args.pop(TOKEN_ARG, None)
        if token is None:
            return refusal("missing_delegation_token")
        if not isinstance(token, str) or not TOKEN_RE.match(token):
            return refusal("invalid_delegation_token")
        payload = {k: v for k, v in args.items() if k not in RESERVED_ARGS}
        headers = scoped_headers(self._service_credential(), token)

        result = await self._call(name, payload, headers)
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
        return await self._call(name, payload, headers)

    async def _call(self, name: str, payload: dict, headers: dict[str, str]) -> dict:
        try:
            return await call_scoped_tool(self.tools_url, headers, name, payload, http_factory=self._http_factory)
        except Exception as exc:
            # Outcome unknown for a write: the skill says read back before repeating, same idempotency_key.
            logger.warning("artifactbridge scoped call failed: %s %s", name, type(exc).__name__)
            return refusal("scoped_call_failed")

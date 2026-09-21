"""One short-lived scoped MCP call per tool invocation, plus token renewal.

ArtifactBridge's delegation boundary is stateless per request and re-resolves
the credentials on every call, so a fresh streamable-HTTP session per call is
the honest shape: no client outlives the authority it was opened with, and no
process-wide header is ever swapped. Both requests carry the same two headers:
``Authorization: Bearer <service credential>`` and
``X-ArtifactBridge-Delegation-Token: <token>``.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

import httpx2
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client

TOKEN_HEADER = "x-artifactbridge-delegation-token"
HttpFactory = Callable[[dict[str, str]], httpx2.AsyncClient]


class BoundaryRefused(Exception):
    """AB refused the credentials at the transport (401/403 with ``{"error": {"code"}}``)
    before any tool ran: unknown token, revoked, expired, stale generation, ..."""

    def __init__(self, status: int, code: str):
        super().__init__(f"boundary refused: {status} {code}")
        self.status = status
        self.code = code


def _denial_code(status: int, body: object) -> str:
    code = "unauthorized" if status == 401 else "forbidden"
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        code = str(body["error"].get("code") or code)
    return code


async def _raise_on_boundary_denial(response: httpx2.Response) -> None:
    if response.status_code not in (401, 403):
        return
    await response.aread()
    try:
        body = response.json()
    except ValueError:
        body = None
    raise BoundaryRefused(response.status_code, _denial_code(response.status_code, body))


def _find(exc: BaseException, kind: type) -> BaseException | None:
    if isinstance(exc, kind):
        return exc
    if isinstance(exc, BaseExceptionGroup):
        for inner in exc.exceptions:
            found = _find(inner, kind)
            if found is not None:
                return found
    return None


def scoped_headers(service_credential: str, delegation_token: str) -> dict[str, str]:
    return {"authorization": f"Bearer {service_credential}", TOKEN_HEADER: delegation_token}


def default_http_factory(headers: dict[str, str]) -> httpx2.AsyncClient:
    return httpx2.AsyncClient(headers=headers, follow_redirects=False, timeout=httpx2.Timeout(30, read=300))


def adapt_result(result: Any) -> dict:
    """AB replies with one JSON text block (``{"error": code, ...}`` when isError).
    Return that object as-is; anything else (images, resources, several blocks)
    is passed through as content blocks so nothing is dropped."""
    content = list(getattr(result, "content", None) or [])
    is_error = bool(getattr(result, "isError", False))
    if len(content) == 1 and getattr(content[0], "type", None) == "text":
        try:
            parsed = json.loads(content[0].text)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            if is_error and "error" not in parsed:
                parsed["error"] = "scoped_call_failed"
            return parsed
    blocks = [block.model_dump(mode="json", exclude_none=True) if hasattr(block, "model_dump") else block
              for block in content]
    return {"error": "scoped_call_failed", "content": blocks} if is_error else {"content": blocks}


async def call_scoped_tool(
    tools_url: str, headers: dict[str, str], name: str, arguments: dict, *,
    http_factory: HttpFactory = default_http_factory, timeout: float = 300,
) -> dict:
    http = http_factory(headers)
    hooks = http.event_hooks
    hooks.setdefault("response", []).append(_raise_on_boundary_denial)
    http.event_hooks = hooks
    try:
        async with http:
            async with streamable_http_client(tools_url, http_client=http) as streams:
                read_stream, write_stream = streams[0], streams[1]
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    result = await session.call_tool(name, arguments, read_timeout_seconds=timeout)
    except BaseException as exc:  # the mcp transport wraps failures in exception groups
        refused = _find(exc, BoundaryRefused)
        if refused is None:
            raise
        return {"error": refused.code, "status": refused.status}
    finally:
        # Over real HTTP the transport leaves one async-generator finalizer scheduled on the loop.
        # Hermes runs tool calls on a persistent loop that only spins during dispatch, so give the
        # finalizer its tick here instead of leaving a pending task behind (seen at interpreter exit).
        await asyncio.sleep(0)
    return adapt_result(result)


async def renew_delegation_token(
    renew_url: str, headers: dict[str, str], *, http_factory: HttpFactory = default_http_factory,
) -> dict:
    """POST the renewal endpoint with the same headers and no body. Returns AB's JSON on
    success or ``{"error": code, "status": n}``; a redirect is refused, never followed."""
    http = http_factory(headers)
    async with http:
        response = await http.post(renew_url, content=b"")
    if 300 <= response.status_code < 400:
        return {"error": "renewal_refused", "status": response.status_code}
    try:
        body = response.json()
    except ValueError:
        body = None
    if response.status_code in (401, 403):
        return {"error": _denial_code(response.status_code, body), "status": response.status_code}
    if response.status_code >= 400 or not isinstance(body, dict):
        return {"error": "renewal_unavailable", "status": response.status_code}
    return body

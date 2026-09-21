"""In-process fake ArtifactBridge: the delegation-scoped MCP boundary in its
service-bearer-plus-token mode and the token renewal route, reproducing the
denial codes agreed with the backend. Served over httpx2's ASGITransport; the
MCP side is a minimal stateless streamable-HTTP JSON-RPC handler so every
request's headers are re-resolved exactly as AB does."""
from __future__ import annotations

import base64
import json
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

import httpx2
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

SERVICE_CREDENTIAL = "agw_test_service_credential_do_not_leak"
OTHER_SERVICE_CREDENTIAL = "agw_other_service_credential"
ORIGIN = "https://ab.test"
TOOLS_PATH = "/mcp/agent-gateway"
RENEW_PATH = "/api/agent-gateway/delegation-token/renew"
TOOLS_URL = ORIGIN + TOOLS_PATH
TOKEN_HEADER = "x-artifactbridge-delegation-token"
RENEWAL_SECONDS = 3600
ROOM_TOOLS = ("artifactbridge_delegation_read_room", "artifactbridge_delegation_contribute",
              "artifactbridge_delegation_upload_image")


def _error(code: str, message: str = "", status: int = 403) -> JSONResponse:
    return JSONResponse({"error": {"code": code, "message": message}}, status_code=status)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class Delegation:
    id: str
    room_id: str = "room-1"
    generation: int = 1
    state: str = "active"
    room_open: bool = True
    document_ids: list[str] = field(default_factory=list)
    writable_document_ids: list[str] = field(default_factory=list)
    destination_folder_id: str | None = "folder-1"

    def scope(self) -> dict:
        return {"workspaceId": "ws-1", "roomId": self.room_id, "documentIds": self.document_ids,
                "writableDocumentIds": self.writable_document_ids,
                "destinationFolderId": self.destination_folder_id,
                "allowedActions": ["room.read", "room.contribute", "document.read", "document.create", "document.propose"]}


@dataclass
class FakeAB:
    delegations: dict[str, Delegation] = field(default_factory=dict)
    tokens: dict[str, dict] = field(default_factory=dict)  # plaintext -> row (test-only)
    tool_calls: list[tuple[str, dict]] = field(default_factory=list)
    requests: list[dict] = field(default_factory=list)  # every HTTP request: path + which headers were present
    renew_calls: int = 0
    events: list[dict] = field(default_factory=list)
    documents: dict[str, dict] = field(default_factory=dict)
    ttl_seconds: float = 900
    redirect_renew: bool = False
    service_disabled: bool = False
    now: Callable[[], float] = time.time

    # ---- mint at dispatch ----
    def mint(self, delegation_id: str, service: str = SERVICE_CREDENTIAL) -> str:
        delegation = self.delegations[delegation_id]
        token = "dlt_" + base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
        self.tokens[token] = {"delegation_id": delegation.id, "generation": delegation.generation,
                              "expires_at": self.now() + self.ttl_seconds, "revoked": False, "service": service}
        return token

    def revoke_all(self, delegation_id: str) -> None:
        for row in self.tokens.values():
            if row["delegation_id"] == delegation_id:
                row["revoked"] = True

    # ---- resolution shared by the boundary and renewal ----
    def _resolve(self, request: Request, *, renewing: bool = False) -> tuple[dict | None, Response | None]:
        auth = request.headers.get("authorization", "")
        token = request.headers.get(TOKEN_HEADER, "")
        self.requests.append({"path": request.url.path, "authorization": auth, "token": token})
        if auth != f"Bearer {SERVICE_CREDENTIAL}":
            return None, _error("unauthorized", "Unknown service credential.", 401)
        row = self.tokens.get(token)
        if row is None or row["service"] != SERVICE_CREDENTIAL:
            return None, _error("unauthorized", "Unknown delegation token.", 401)
        delegation = self.delegations[row["delegation_id"]]
        reason = None
        if row["revoked"]:
            reason = "credential_revoked"
        elif not renewing and row["expires_at"] <= self.now():
            reason = "credential_expired"
        elif row["generation"] != delegation.generation:
            reason = "generation_stale"
        elif self.service_disabled:
            reason = "service_disabled"
        elif not delegation.room_open:
            reason = "room_closed"
        elif delegation.state != "active":
            reason = "delegation_not_active"
        if reason:
            return None, _error(reason, "The delegation token cannot act.")
        return {"row": row, "delegation": delegation}, None

    # ---- POST /api/agent-gateway/delegation-token/renew ----
    async def renew(self, request: Request) -> Response:
        self.renew_calls += 1
        if self.redirect_renew:
            return Response(status_code=307, headers={"location": "https://elsewhere.test/steal"})
        resolved, denial = self._resolve(request, renewing=True)
        if denial:
            return denial
        row, delegation = resolved["row"], resolved["delegation"]
        row["expires_at"] = self.now() + RENEWAL_SECONDS
        return JSONResponse({"delegationId": delegation.id, "executionGeneration": delegation.generation,
                             "expiresAt": _iso(row["expires_at"])})

    # ---- MCP boundary (ALL /mcp/agent-gateway) ----
    async def mcp(self, request: Request) -> Response:
        if request.method == "DELETE":
            return Response(status_code=200)
        if request.method != "POST":
            return Response(status_code=405)
        resolved, denial = self._resolve(request)
        if denial:
            return denial
        message = await request.json()
        method, rid, params = message.get("method"), message.get("id"), message.get("params") or {}
        if rid is None:  # notification
            return Response(status_code=202)
        if method == "initialize":
            result = {"protocolVersion": params.get("protocolVersion", "2025-06-18"),
                      "capabilities": {"tools": {}}, "serverInfo": {"name": "artifactbridge-agent-gateway", "version": "2"}}
        elif method == "ping":
            result = {}
        elif method == "tools/list":
            result = {"tools": [{"name": n, "inputSchema": {"type": "object"}} for n in TOOLS]}
        elif method == "tools/call":
            result = self._call(resolved["delegation"], params.get("name", ""), params.get("arguments") or {})
        else:
            return JSONResponse({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": "Method not found"}})
        return JSONResponse({"jsonrpc": "2.0", "id": rid, "result": result})

    def _call(self, delegation: Delegation, name: str, args: dict) -> dict:
        self.tool_calls.append((name, args))
        if name not in TOOLS:
            return _text({"error": "unknown_tool"}, True)
        requested = args.get("delegation_id") or delegation.id
        if requested != delegation.id:
            return _text({"error": "delegation_mismatch", "delegation_id": requested}, True)
        # room_id is optional and defaults to the token's Room; any other explicit Room is denied.
        if name in ROOM_TOOLS and args.get("room_id", delegation.room_id) != delegation.room_id:
            return _text({"error": "room_out_of_scope", "delegation_id": delegation.id}, True)
        return TOOLS[name](self, delegation, args)


def _text(payload: dict, is_error: bool = False) -> dict:
    return {"content": [{"type": "text", "text": json.dumps(payload)}], "isError": is_error}


def _read_brief(ab: FakeAB, d: Delegation, args: dict) -> dict:
    return _text({"ok": True, "delegation_id": d.id, "room_id": d.room_id, "task": "Summarize the brief.",
                  "scope": d.scope()})


def _read_room(ab: FakeAB, d: Delegation, args: dict) -> dict:
    return _text({"ok": True, "delegation_id": d.id, "events": [e for e in ab.events if e["room_id"] == d.room_id]})


def _read_document(ab: FakeAB, d: Delegation, args: dict) -> dict:
    doc = ab.documents.get(args.get("document_id", ""))
    if doc is None:
        return _text({"error": "not_found", "delegation_id": d.id}, True)
    return _text({"ok": True, "delegation_id": d.id, **doc})


def _list_documents(ab: FakeAB, d: Delegation, args: dict) -> dict:
    return _text({"ok": True, "delegation_id": d.id, "documents": list(ab.documents.values())})


def _contribute(ab: FakeAB, d: Delegation, args: dict) -> dict:
    key = args.get("idempotency_key")
    if key:  # scoped per (delegation, generation) as in contributeAsAgentGatewayDelegation
        for event in ab.events:
            if event.get("idempotency_key") == key and event["delegation_id"] == d.id and event["generation"] == d.generation:
                return _text({"ok": True, "delegation_id": d.id, "event_id": event["id"], "replayed": True})
    event = {"id": f"evt-{len(ab.events) + 1}", "room_id": d.room_id, "delegation_id": d.id, "generation": d.generation,
             "kind": args.get("kind"), "body": args.get("body"), "idempotency_key": key}
    ab.events.append(event)
    return _text({"ok": True, "delegation_id": d.id, "event_id": event["id"], "replayed": False})


def _upload_image(ab: FakeAB, d: Delegation, args: dict) -> dict:
    url = f"{ORIGIN}/room-images/{secrets.token_hex(4)}"
    return _text({"ok": True, "delegation_id": d.id, "room_id": d.room_id, "id": "img-1", "url": url,
                  "markdown": f"![image]({url})"})


def _create_document(ab: FakeAB, d: Delegation, args: dict) -> dict:
    if not d.destination_folder_id:
        return _text({"error": "destination_not_allowed", "delegation_id": d.id}, True)
    doc_id = f"doc-{len(ab.documents) + 1}"
    ab.documents[doc_id] = {"document_id": doc_id, "title": args.get("title"), "content_md": args.get("content_md")}
    return _text({"ok": True, "delegation_id": d.id, "document_id": doc_id, "version_id": "v1"})


def _propose_change(ab: FakeAB, d: Delegation, args: dict) -> dict:
    if args.get("document_id") not in d.writable_document_ids:
        return _text({"error": "document_not_writable", "delegation_id": d.id}, True)
    return _text({"ok": True, "delegation_id": d.id, "review_request_id": "rr-1"})


TOOLS = {
    "artifactbridge_delegation_read_brief": _read_brief,
    "artifactbridge_delegation_read_room": _read_room,
    "artifactbridge_delegation_read_document": _read_document,
    "artifactbridge_delegation_list_documents": _list_documents,
    "artifactbridge_delegation_search_documents": _list_documents,
    "artifactbridge_delegation_contribute": _contribute,
    "artifactbridge_delegation_upload_image": _upload_image,
    "artifactbridge_delegation_create_document": _create_document,
    "artifactbridge_delegation_propose_change": _propose_change,
}


def make_app(ab: FakeAB) -> Starlette:
    return Starlette(routes=[
        Route(RENEW_PATH, ab.renew, methods=["POST"]),
        Route(TOOLS_PATH, ab.mcp, methods=["POST", "GET", "DELETE"]),
    ])


def http_factory(app: Starlette):
    """A ``mcp_client.HttpFactory`` that routes the scoped client into the fake app.
    No base_url: the plugin must produce absolute URLs on the configured origin itself."""
    def factory(headers: dict[str, str]) -> httpx2.AsyncClient:
        return httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), headers=headers, follow_redirects=False)
    return factory

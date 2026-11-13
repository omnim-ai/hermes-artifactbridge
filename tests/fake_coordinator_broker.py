"""In-process fake of ArtifactBridge's coordinator read broker (``ALL /mcp/coordinator-reads``): a stateless
streamable-HTTP JSON-RPC endpoint authenticated by ``Authorization: Bearer cri_...`` only. An unknown
credential is a 401; a tool outside the coordinator's saved subset is a JSON-RPC tool-not-found error;
refusals are ``isError`` text ``{"error": code}``. ``hold`` parks tool calls until released so tests can
cancel a call in flight."""
from __future__ import annotations

import asyncio
import json

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from tests.fake_ab import ORIGIN

READ_PATH = "/mcp/coordinator-reads"
READ_URL = ORIGIN + READ_PATH
CREDENTIAL = "cri_test_coordinator_read_credential_do_not_leak"
DOCUMENT_ID = "20000000-0000-4000-8000-000000000001"
TOOLS = ("artifactbridge_read_space_context", "artifactbridge_read_document", "artifactbridge_read_skill")


def _text(payload: dict, is_error: bool = False) -> dict:
    return {"content": [{"type": "text", "text": json.dumps(payload)}], "isError": is_error}


class FakeBroker:
    def __init__(self):
        self.credentials = {CREDENTIAL}
        self.subset = set(TOOLS)
        self.refuse: dict[str, str] = {}  # tool -> refusal code
        self.requests: list[dict] = []
        self.tool_calls: list[tuple[str, dict]] = []
        self.hold: asyncio.Event | None = None
        self.held = asyncio.Event()

    async def mcp(self, request: Request) -> Response:
        if request.method == "DELETE":
            return Response(status_code=200)
        if request.method != "POST":
            return Response(status_code=405)
        auth = request.headers.get("authorization", "")
        self.requests.append({"path": request.url.path, "headers": dict(request.headers)})
        if not auth.startswith("Bearer cri_") or auth.removeprefix("Bearer ") not in self.credentials:
            return JSONResponse({"error": {"code": "unauthorized", "message": "Unknown coordinator read credential."}},
                                status_code=401)
        message = await request.json()
        method, rid, params = message.get("method"), message.get("id"), message.get("params") or {}
        if rid is None:
            return Response(status_code=202)
        if method == "initialize":
            result = {"protocolVersion": params.get("protocolVersion", "2025-06-18"), "capabilities": {"tools": {}},
                      "serverInfo": {"name": "artifactbridge-coordinator-reads", "version": "1"}}
        elif method == "tools/list":
            result = {"tools": [{"name": n, "inputSchema": {"type": "object"}} for n in TOOLS if n in self.subset]}
        elif method == "tools/call" and params.get("name") not in self.subset:
            return JSONResponse({"jsonrpc": "2.0", "id": rid,
                                 "error": {"code": -32602, "message": f"Tool {params.get('name')} not found"}})
        elif method == "tools/call":
            name, args = params["name"], params.get("arguments") or {}
            self.tool_calls.append((name, args))
            if self.hold is not None:
                self.held.set()
                await self.hold.wait()
            result = _text({"error": self.refuse[name]}, True) if name in self.refuse else _text(_reply(name, args))
        else:
            return JSONResponse({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": "Method not found"}})
        return JSONResponse({"jsonrpc": "2.0", "id": rid, "result": result})


def _reply(name: str, args: dict) -> dict:
    if name == "artifactbridge_read_space_context":
        return {"space": {"name": "Ops"}, "skills": {"items": [{"slug": "triage"}], "next_cursor": None}, "args": args}
    if name == "artifactbridge_read_document":
        return {"document_id": args["document_id"], "content_md": "# Runbook"}
    return {"slug": args["slug"], "content_md": "# triage", "modules": []}


def make_app(broker: FakeBroker) -> Starlette:
    return Starlette(routes=[Route(READ_PATH, broker.mcp, methods=["POST", "GET", "DELETE"])])

"""Coordinator profile surface: the three fixed-Space reads and a confined ``session_search``.

A coordinator is one shared Hermes agent per ArtifactBridge workspace. It reads its own configured
Space through ArtifactBridge's coordinator read broker (``/mcp/coordinator-reads`` on the tools URL's
origin), authenticated only by the profile's read installation credential (``cri_``). ArtifactBridge
derives the workspace, coordinator, Space and allowed tools from that credential: the model passes
content selectors and paging only, never a member, Space, workspace or conversation. Every member
gets the same reads, whoever sent the message. Results go back to the invoking tool call only:
nothing is cached, retried or delivered later.

Hermes' stock ``session_search`` can open another profile's store (``profile=`` or a
``<profile>/<id>`` session id). The confined override refuses both, and any argument or session id
it does not know, before calling the stock handler, which then reads only the serving profile's store.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Awaitable, Callable

from .endpoint import url_on_origin
from .mcp_client import HttpFactory, call_scoped_tool, default_http_factory
from .tools import refusal

logger = logging.getLogger(__name__)

COORDINATOR_TOOLSET = "ab_coordinator_reads"
COORDINATOR_READ_PATH = "/mcp/coordinator-reads"
CREDENTIAL_PREFIX = "cri_"

# The broker's strict input schemas (artifact-forge coordinator-read-boundary.ts); descriptions are ours.
COORDINATOR_TOOL_SCHEMAS: tuple[dict, ...] = (
    {"name": "artifactbridge_read_space_context",
     "description": "Read this coordinator's own configured Space, as the coordinator can see it now (the same "
                    "for everyone you talk with). Returns a map, not content: the Space, its description, an "
                    "overview on the first call, its references and pages of direct_documents, "
                    "included_documents and skills. Read a document with artifactbridge_read_document and a "
                    "skill with artifactbridge_read_skill. Pass a returned next_cursor as cursor for the next page.",
     "parameters": {"type": "object", "properties": {
         "cursor": {"type": "string", "minLength": 1,
                    "description": "A next_cursor returned by an earlier read of this Space, unchanged."},
         "include_archived": {"type": "boolean",
                              "description": "true includes archived documents (default false); keep one value across pages."},
         "limit": {"type": "integer", "minimum": 1, "maximum": 100,
                   "description": "Items per collection page, 1 to 100 (default 50)."},
     }, "required": [], "additionalProperties": False}},
    {"name": "artifactbridge_read_document",
     "description": "Read one document that this coordinator's configured Space reaches, as the coordinator can "
                    "read it now.",
     "parameters": {"type": "object", "properties": {
         "document_id": {"type": "string", "format": "uuid", "description": "A document id from the Space map."},
     }, "required": ["document_id"], "additionalProperties": False}},
    {"name": "artifactbridge_read_skill",
     "description": "Read one skill that this coordinator's configured Space lists, by slug: its SKILL.md and the "
                    "modules the coordinator can read now. Use version_policy current for a skill the Space lists. "
                    "Nothing is installed.",
     "parameters": {"type": "object", "properties": {
         "slug": {"type": "string", "minLength": 1, "maxLength": 64, "pattern": "^[a-z0-9][a-z0-9-]{0,63}$",
                  "description": "The skill slug from the Space's skills page."},
         "version_policy": {"type": "string", "enum": ["registered", "current"],
                            "description": "registered (default): the pinned versions. current: the latest versions."},
     }, "required": ["slug"], "additionalProperties": False}},
)
COORDINATOR_TOOL_NAMES: tuple[str, ...] = tuple(s["name"] for s in COORDINATOR_TOOL_SCHEMAS)
_ALLOWED_ARGS = {s["name"]: frozenset(s["parameters"]["properties"]) for s in COORDINATOR_TOOL_SCHEMAS}


class CoordinatorReads:
    """One stateless MCP call to the broker per tool call, with ``Authorization: Bearer cri_...`` and
    nothing else. A broker refusal is returned unchanged (``{"content", "isError"}``, a JSON-RPC error
    as ``{"error": {"code", "message"}}``, or a 401 as ``{"error": "unauthorized", "status": 401}``);
    nothing is retried and no other credential is ever tried. Cancelling the call's task stops it:
    a cancelled handler raises ``CancelledError`` and returns no content."""

    def __init__(self, *, tools_url: str, credential: Callable[[], str], http_factory: HttpFactory = default_http_factory):
        self.read_url = url_on_origin(tools_url, COORDINATOR_READ_PATH)
        self._credential = credential
        self._http_factory = http_factory

    def handler_for(self, name: str) -> Callable[..., Awaitable[str]]:
        async def handler(args: dict, **kwargs) -> str:
            return json.dumps(await self.invoke(name, args))

        return handler

    async def invoke(self, name: str, args: dict) -> dict:
        args = dict(args or {})
        unexpected = sorted(set(args) - _ALLOWED_ARGS[name])
        if unexpected:  # identity, Space or conversation selectors never reach the broker
            return refusal("unexpected_arguments", arguments=unexpected)
        credential = self._credential()
        if not credential.startswith(CREDENTIAL_PREFIX):
            return refusal("not_configured", tool=name)
        try:
            return await call_scoped_tool(self.read_url, {"authorization": f"Bearer {credential}"}, name, args,
                                          http_factory=self._http_factory, preserve=True)
        except Exception as exc:  # CancelledError is not an Exception: it propagates and nothing is returned
            logger.warning("artifactbridge coordinator read failed: %s %s", name, type(exc).__name__)
            return refusal("scoped_call_failed")


# ---- confined session_search ----

SEARCH_TOOL = "session_search"
# The stock arguments that act on the serving profile's own store. ``profile`` and any argument a
# later Hermes adds are left out of the schema and refused.
SEARCH_ARGS = ("query", "limit", "sort", "detail", "after", "before", "exclude_session_ids", "session_id",
               "around_message_id", "window", "role_filter")
# Every shape Hermes mints (hermes_state_ids.SESSION_ID_RECOGNIZERS): no "/", "\\", ":" or "@".
SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
SEARCH_DESCRIPTION = (
    "Search or read this coordinator's own past conversations (FTS5), or scroll inside one. Shapes, picked by "
    "args: `query` = discovery (top matching sessions, top result fully hydrated); `session_id` + "
    "`around_message_id` = scroll (messages around an anchor); `session_id` alone = read a whole session; no "
    "args = browse recent sessions. Only this coordinator's sessions are searchable: pass a bare session id "
    "(for a `@session:<profile>/<id>` link, only the part after the '/'), never a profile. Results are actual "
    "stored messages. Searches conversation history only; never conclude 'not found' from history alone."
)


def _search_refusal(args: object) -> str | None:
    if not isinstance(args, dict):
        return "arguments must be an object"
    for key in args:
        if key == "profile":
            return "profile is not allowed: this coordinator can search only its own sessions"
        if key not in SEARCH_ARGS:
            return f"unsupported argument: {key}"
    ids = []
    session_id = args.get("session_id")
    if session_id is not None:
        ids.append(session_id)
    excluded = args.get("exclude_session_ids")
    if excluded is not None:
        if not isinstance(excluded, list):
            return "exclude_session_ids must be a list of session ids"
        ids.extend(excluded)
    for value in ids:
        if not isinstance(value, str):
            return "session ids must be strings"
        if "/" in value or "\\" in value:
            return "profile-qualified session ids are not allowed: pass the bare session id"
        if ".." in value or not SESSION_ID_RE.match(value):
            return "invalid session id"
    return None


def confined_search_schema(stock_schema: dict) -> dict:
    properties = (stock_schema.get("parameters") or {}).get("properties") or {}
    kept = {name: properties[name] for name in SEARCH_ARGS if name in properties}
    if "session_id" in kept:
        kept["session_id"] = {**kept["session_id"], "description": (
            "Scroll or read shape. A bare session id from a prior result of this tool, never "
            "'<profile>/<id>'. With around_message_id, scroll; alone, read the whole session.")}
    return {"name": SEARCH_TOOL, "description": SEARCH_DESCRIPTION,
            "parameters": {"type": "object", "properties": kept, "required": [], "additionalProperties": False}}


def confined_search_handler(stock_handler: Callable[..., str]) -> Callable[..., str]:
    """Refuse before the stock handler runs; otherwise call it unchanged with the dispatcher's own
    context (the serving session's store), so search, read and scroll keep the stock behaviour."""
    def session_search(args: dict, **kwargs) -> str:
        problem = _search_refusal(args)
        if problem is not None:
            return json.dumps({"success": False, "error": problem})
        return stock_handler(args, **kwargs)

    return session_search

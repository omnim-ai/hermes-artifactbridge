"""Static model-facing schemas for the delegation-scoped tools.

Tool names and argument bounds are the ArtifactBridge scoped-tool interface (protocol
facts, see THIRD_PARTY.md); descriptions are this plugin's own wording. Every tool takes
the opaque ``delegation_token`` from the task brief; the plugin moves it into a
request header. Only the two Space-bound read tools accept ``delegation_id``
(optional; the backend refuses any delegation but the token's own). Only document
creation exposes ``room_id`` to opt into an HTML Room artifact; the backend
checks that it is the token's Room.
Schemas are constants: no per-task registration, stable across turns.
"""
from __future__ import annotations

TOOLSET = "artifactbridge-delegation"

TOKEN_ARG = "delegation_token"
_TOKEN_PROPERTY = {
    "type": "string",
    "pattern": "^dlt_[A-Za-z0-9_-]{43}$",
    "description": "The delegation token given in your task brief, verbatim.",
}
_IDEMPOTENCY_PROPERTY = {
    "type": "string", "minLength": 1, "maxLength": 255,
    "description": "Your stable id for this one write. Pass the same value when retrying it; "
                   "ArtifactBridge returns the write already recorded under it instead of a duplicate.",
}


def _schema(name: str, description: str, properties: dict, required: list[str], *, write: bool = False) -> dict:
    properties = {TOKEN_ARG: _TOKEN_PROPERTY, **properties}
    if write:
        properties["idempotency_key"] = _IDEMPOTENCY_PROPERTY
    return {
        "name": name,
        "description": description,
        "parameters": {"type": "object", "properties": properties, "required": [TOKEN_ARG, *required],
                       "additionalProperties": False},
    }


_DELEGATION_SCHEMAS: tuple[dict, ...] = (
    _schema("artifactbridge_delegation_read_brief",
            "Fetch the delegation you are executing: its task text, scope (Room, documents, folder, allowed actions) and how it is completed.",
            {}, []),
    _schema("artifactbridge_delegation_read_room",
            "Return the latest events of the Room this delegation belongs to.",
            {"limit": {"type": "integer", "minimum": 1, "maximum": 200}}, []),
    _schema("artifactbridge_delegation_read_document",
            "Fetch one document by id: any workspace-visible document, or a private one that was shared with this delegation.",
            {"document_id": {"type": "string"}}, ["document_id"]),
    _schema("artifactbridge_delegation_list_documents",
            "Enumerate the documents readable by this delegation (workspace-visible plus the private ones shared with it), optionally filtered by title.",
            {"q": {"type": "string", "maxLength": 200}, "limit": {"type": "integer", "minimum": 1, "maximum": 100}}, []),
    _schema("artifactbridge_delegation_search_documents",
            "Full-text search over the documents readable by this delegation; all query words must match in title or body.",
            {"query": {"type": "string", "minLength": 1, "maxLength": 200},
             "limit": {"type": "integer", "minimum": 1, "maximum": 50}}, ["query"]),
    _schema("artifactbridge_delegation_contribute",
            "Add one Room event as the shared service agent: a message, a piece of evidence, or a question for the humans. "
            "Posting is not finishing: the A2A task's final answer is the delegation's result.",
            {"kind": {"type": "string", "enum": ["evidence", "question", "message"]},
             "body": {"type": "string", "minLength": 1, "maxLength": 20000}}, ["kind", "body"], write=True),
    _schema("artifactbridge_delegation_upload_image",
            "Store an image (PNG, JPEG, WebP or GIF, at most 3 MiB, plain base64) in the delegation's Room; returns a URL "
            "and a Markdown snippet to reuse inside a message contribution.",
            {"content_type": {"type": "string", "minLength": 1}, "data_base64": {"type": "string", "minLength": 1}},
            ["content_type", "data_base64"]),
    _schema("artifactbridge_delegation_create_document",
            "Create Markdown or HTML in the authorized destination folder, or pass format html and room_id "
            "to create and attach an unfiled artifact to this delegation's Room. "
            "Omitting room_id creates a Library document; it does not default to the Room.",
            {"title": {"type": "string", "minLength": 1, "maxLength": 200},
             # Match the backend's flat schema. JSON Schema maxLength counts characters, not UTF-8 bytes;
             # format-dependent body limits and Room validation remain authoritative on the backend.
             "content_md": {"type": "string", "description":
                            "Markdown (at most 200,000 characters) or complete HTML (at most 10,000,000 UTF-8 bytes). "
                            "Pass HTML verbatim, not a file path, Markdown fence, or summary."},
             "format": {"type": "string", "enum": ["markdown", "html"], "default": "markdown"},
             "room_id": {"type": "string", "description":
                         "Only with format html: the Room UUID from read_brief. Creates unfiled Room context. "
                         "Other Rooms are refused. Omit for a Library document."}}, ["title", "content_md"], write=True),
    _schema("artifactbridge_delegation_propose_change",
            "Submit a whole-document proposal for one of the documents this delegation may propose changes to. "
            "Humans review it; the service can never approve its own proposal.",
            {"document_id": {"type": "string"}, "proposed_md": {"type": "string", "maxLength": 200000},
             "summary": {"type": "string", "maxLength": 2000}}, ["document_id", "proposed_md"], write=True),
)

# Space-bound reads: the hosted proxy schemas of the AI-3948 plugin tool contract, verbatim apart from
# descriptions. Optional fields carry the server's defaults as documentation; the plugin never inserts them.
_NONEMPTY_TOKEN_PROPERTY = {"type": "string", "minLength": 1}
SPACE_TOOL_SCHEMAS: tuple[dict, ...] = (
    {"name": "artifactbridge_read_space_context",
     "description": "Read the Space attached to this delegation: its Description, overview, skills and documents, "
                    "one page per collection. Pass delegation_token from your task brief verbatim. Pass a returned "
                    "cursor unchanged to read the next page; page through every skill page before deciding no skill "
                    "matches. space_id and delegation_id default to the delegation's own.",
     "parameters": {"type": "object", "properties": {
         TOKEN_ARG: _NONEMPTY_TOKEN_PROPERTY,
         "delegation_id": {"type": "string"},
         "space_id": {"type": "string", "format": "uuid"},
         "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 50},
         "cursor": {"type": "string", "minLength": 1},
         "include_archived": {"type": "boolean", "default": False},
     }, "required": [TOKEN_ARG]}},
    {"name": "artifactbridge_read_skill",
     "description": "Read one skill listed by the delegation's Space, with its modules and provenance. Pass "
                    "delegation_token from your task brief verbatim. Use version_policy current for a skill the Space "
                    "lists. Nothing is installed.",
     "parameters": {"type": "object", "properties": {
         TOKEN_ARG: _NONEMPTY_TOKEN_PROPERTY,
         "delegation_id": {"type": "string"},
         "slug": {"type": "string", "minLength": 1, "maxLength": 64, "pattern": "^[a-z0-9][a-z0-9-]{0,63}$"},
         "version_policy": {"type": "string", "enum": ["registered", "current"], "default": "registered"},
     }, "required": [TOKEN_ARG, "slug"]}},
)
SPACE_TOOL_NAMES = frozenset(s["name"] for s in SPACE_TOOL_SCHEMAS)

TOOL_SCHEMAS: tuple[dict, ...] = _DELEGATION_SCHEMAS + SPACE_TOOL_SCHEMAS
TOOL_NAMES: tuple[str, ...] = tuple(s["name"] for s in TOOL_SCHEMAS)

# Authority selectors stripped before dispatch, except room_id on create_document and the
# optional delegation_id on Space tools. room_id there selects Room context rather than a Library
# document; delegation_id defaults server-side to the token's own delegation and any other is refused.
RESERVED_ARGS = frozenset({"delegation_id", "room_id"})

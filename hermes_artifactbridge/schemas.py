"""Static model-facing schemas for the delegation-scoped tools.

Tool names and argument bounds are the ArtifactBridge scoped-tool interface (protocol
facts, see THIRD_PARTY.md); descriptions are this plugin's own wording. Every tool takes
the opaque ``delegation_token`` from the task brief; the plugin moves it into a
request header. ``delegation_id`` and ``room_id`` are not model arguments: the
token binds the delegation and ArtifactBridge defaults the Room from it.
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


TOOL_SCHEMAS: tuple[dict, ...] = (
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
            "Write a new document into the destination folder this delegation is allowed to create in.",
            {"title": {"type": "string", "minLength": 1, "maxLength": 200},
             "content_md": {"type": "string", "maxLength": 200000}}, ["title", "content_md"], write=True),
    _schema("artifactbridge_delegation_propose_change",
            "Submit a whole-document proposal for one of the documents this delegation may propose changes to. "
            "Humans review it; the service can never approve its own proposal.",
            {"document_id": {"type": "string"}, "proposed_md": {"type": "string", "maxLength": 200000},
             "summary": {"type": "string", "maxLength": 2000}}, ["document_id", "proposed_md"], write=True),
)

TOOL_NAMES: tuple[str, ...] = tuple(s["name"] for s in TOOL_SCHEMAS)

# Authority-bearing arguments the model must never supply; stripped before dispatch.
RESERVED_ARGS = frozenset({"delegation_id", "room_id"})

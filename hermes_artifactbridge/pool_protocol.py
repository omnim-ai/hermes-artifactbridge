"""Pooled execution protocol v1: the assignment tuple, the dispatch envelope, the cloud start headers and
the capability answer.

Shapes follow the ArtifactBridge cloud-contract-r2-terminal-clarification-1 fixtures
(tests/fixtures/cloud_r2_terminal_clarification_1). Every object is
strict: an unknown or missing key is a refusal, never a best-effort parse. The dispatch metadata never
carries a ``dlt_`` token or any credential; this module never accepts one there.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Callable

PROTOCOL_VERSION = 1
METADATA_KEY = "artifactbridge.assignment"
TERMINAL_KEY = "artifactbridge.cloudTerminal"  # task metadata only, never read from a request

# The control routes of artifact-forge src/routes/agent-gateway-pool.ts (a26eb977c; not yet merged or
# deployed): same origin as the configured tools URL, Bearer <the assignment's agx_ from the start headers>.
START_PATH = "/api/agent-gateway/pool/start"
BOOTSTRAP_PATH = "/api/agent-gateway/pool/bootstrap"
REPORT_PATH = "/api/agent-gateway/pool/report"
# The JSON-RPC method the server's capability probe calls on this listener (cloud-capability-request.json).
CAPABILITY_METHOD = "artifactbridge/pool/capability"
# Request-scoped start headers on every pooled Send/Get/List/Cancel (cloud-send-request.json).
CREDENTIAL_HEADER = "X-AB-Control-Credential"
JOURNAL_HEADER = "X-AB-Start-Journal-Id"
PROTOCOL_HEADER = "X-AB-Pool-Protocol-Version"

_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_BIGINT_RE = re.compile(r"^[1-9][0-9]*$")
_BIGINT_MAX = 9223372036854775807
_UUID_FIELDS = ("workspaceId", "serviceId", "delegationId", "assignmentId", "executorId", "slotId")
_BIGINT_FIELDS = ("reservationGeneration", "executorConnectionRevision")
BINDING_KEYS = frozenset(_UUID_FIELDS + _BIGINT_FIELDS + ("executionGeneration",))
ENVELOPE_KEYS = frozenset({"protocolVersion", "binding", "dispatchId", "commandSequence", "setupRevision", "startBy"})

_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")  # a refusal or error code: content-free, never echoed text
_DLC_RE = re.compile(r"^dlc_[A-Za-z0-9_-]{32,}$")
_AGX_RE = re.compile(r"^agx_[A-Za-z0-9_-]{32,}$")
_JOURNAL_RE = re.compile(r"^[\x20-\x7e]{1,128}$")  # printable ASCII, no control characters
# Anywhere in text, the agx_/dlc_ shapes above; Hermes' egress scrub (security.redact_outbound) knows neither.
_CREDENTIAL_IN_TEXT_RE = re.compile(r"(?:agx|dlc)_[A-Za-z0-9_-]{32,}")


def redact_credentials(text: str) -> str:
    """``text`` with every agx_/dlc_-shaped value replaced, as Hermes' own scrub writes ``[redacted]``."""
    return _CREDENTIAL_IN_TEXT_RE.sub("[redacted]", text)


# Only what this plugin implements; the proof is separate from open-pool consent, which the plugin never
# claims or sets. ``same_session_continuation`` is withdrawn (product ruling 2026-10-17; AI-4105): a probe
# requiring it is refused, a probe for these six is answered, and every continuation is refused.
IMPLEMENTED_CAPABILITIES = (
    "assignment_tuple_start_ownership",
    "scoped_space_document_skill",
    "per_task_credentials_fresh_sessions",
    "durable_inspect_recovery",
    "cancel_report_fencing",
    "cloud_start_headers_v1",
)
_CAPABILITY_KEYS = frozenset({"protocolVersion", "executorId", "executorConnectionRevision", "nonce",
                              "requiredCapabilities"})


class Refused(Exception):
    """A pooled request refused with a stable, content-free code."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def is_uuid(value: object) -> bool:
    return isinstance(value, str) and bool(_UUID_RE.match(value))


def is_bigint_string(value: object) -> bool:
    return isinstance(value, str) and bool(_BIGINT_RE.match(value)) and int(value) <= _BIGINT_MAX


def _positive_int(value: object) -> bool:
    return type(value) is int and value > 0


def _strict(obj: object, keys: frozenset, code: str) -> dict:
    if not isinstance(obj, dict) or set(obj) != keys:
        raise Refused(code)
    return obj


def validate_binding(binding: object) -> dict:
    binding = _strict(binding, BINDING_KEYS, "invalid_binding")
    if not (all(is_uuid(binding[k]) for k in _UUID_FIELDS) and all(is_bigint_string(binding[k]) for k in _BIGINT_FIELDS)
            and _positive_int(binding["executionGeneration"])):
        raise Refused("invalid_binding")
    return binding


def parse_start_by(value: object) -> datetime:
    """ISO datetime with an explicit offset (``Z`` or ``+hh:mm``)."""
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else None
    except ValueError:
        parsed = None
    if parsed is None or parsed.tzinfo is None:
        raise Refused("invalid_envelope")
    return parsed


def validate_envelope(envelope: object) -> dict:
    if isinstance(envelope, dict) and envelope.get("protocolVersion") != PROTOCOL_VERSION:
        raise Refused("unsupported_protocol_version")
    envelope = _strict(envelope, ENVELOPE_KEYS, "invalid_envelope")
    if type(envelope["protocolVersion"]) is not int:
        raise Refused("unsupported_protocol_version")
    validate_binding(envelope["binding"])
    if not (isinstance(envelope["dispatchId"], str) and envelope["dispatchId"]
            and type(envelope["commandSequence"]) is int and envelope["commandSequence"] == 1  # the literal 1
            and _positive_int(envelope["setupRevision"])):
        raise Refused("invalid_envelope")
    parse_start_by(envelope["startBy"])
    return envelope


def envelope_from_metadata(metadata: object) -> dict:
    """The envelope from A2A ``SendMessage`` ``params.metadata``; missing metadata is a refusal."""
    if not isinstance(metadata, dict) or METADATA_KEY not in metadata:
        raise Refused("missing_assignment")
    return validate_envelope(metadata[METADATA_KEY])


def start_request(envelope: dict, start_journal_id: str) -> dict:
    """Start request body: start-request.json."""
    return {"protocolVersion": PROTOCOL_VERSION, "binding": envelope["binding"], "dispatchId": envelope["dispatchId"],
            "startJournalId": start_journal_id}


def bootstrap_request(envelope: dict, start_journal_id: str) -> dict:
    """Bootstrap request body: bootstrap-request.json (no dispatchId)."""
    return {"protocolVersion": PROTOCOL_VERSION, "binding": envelope["binding"], "startJournalId": start_journal_id}


def report_body(binding: dict, sequence: int, kind: str, outcome: str | None) -> dict:
    """Content-free report: report.json / cancel-ack.json."""
    return {"binding": binding, "reportSequence": sequence, "kind": kind, "outcome": outcome}


def _is_code(value: object) -> bool:
    return isinstance(value, str) and bool(_CODE_RE.match(value))


def parse_error(body: object) -> tuple[str, bool] | None:
    """``(code, retryable)`` for ``{"error": {"code", "message"}}`` (plus ``"retryable": true`` on
    scheduler-busy-response.json), else None."""
    if not isinstance(body, dict) or set(body) - {"retryable"} != {"error"} or body.get("retryable", True) is not True:
        return None
    error = body["error"]
    if not (isinstance(error, dict) and set(error) == {"code", "message"} and _is_code(error["code"])
            and isinstance(error["message"], str)):
        return None
    return error["code"], "retryable" in body


def parse_start_response(body: object) -> tuple[str, object] | None:
    """``("permit", permit)`` for start-permit.json, ``("refused", reason)`` for a start refusal, else None.
    A tray start's ``controlCredential`` is accepted as shape and never returned, so it is never adopted."""
    if not isinstance(body, dict):
        return None
    if body.get("outcome") == "refused" and set(body) == {"outcome", "reason"} and _is_code(body["reason"]):
        return "refused", body["reason"]
    if body.get("outcome") != "permit" or not ({"outcome", "permit"} <= set(body) <= {"outcome", "permit",
                                                                                      "controlCredential"}):
        return None
    if "controlCredential" in body and not (isinstance(body["controlCredential"], str)
                                            and _AGX_RE.match(body["controlCredential"])):
        return None
    permit = body["permit"]
    if not isinstance(permit, dict) or not isinstance(permit.get("startJournalId"), str) or not permit["startJournalId"]:
        return None
    try:
        validate_envelope({k: v for k, v in permit.items() if k != "startJournalId"})
    except Refused:
        return None
    return "permit", permit


def parse_bootstrap_response(body: object) -> tuple[str, datetime] | None:
    """``(dlc_, expiresAt)`` of bootstrap-response.json (exactly ``credential`` and an offset ``expiresAt``),
    else None."""
    if not isinstance(body, dict) or set(body) != {"credential", "expiresAt"}:
        return None
    try:
        expires_at = parse_start_by(body["expiresAt"])
    except Refused:
        return None
    credential = body["credential"]
    return (credential, expires_at) if isinstance(credential, str) and _DLC_RE.match(credential) else None


def parse_report_response(body: object) -> tuple[bool, bool] | None:
    """``(recorded, currentOccupant)`` of report-response.json / report-replay-response.json, else None."""
    if not isinstance(body, dict) or set(body) != {"recorded", "currentOccupant"}:
        return None
    if type(body["recorded"]) is not bool or type(body["currentOccupant"]) is not bool:
        return None
    return body["recorded"], body["currentOccupant"]


def start_journal_id(assignment_id: str) -> str:
    """The server's defined locator for an assignment: ``agj_`` + the assignment id without hyphens."""
    return "agj_" + assignment_id.replace("-", "")


def pool_grant(values: Callable[[str], list[str] | None]) -> tuple[str, str]:
    """``(agx_, startJournalId)`` from the start headers; ``values(name)`` returns every value of one
    case-insensitive header (``http.client.HTTPMessage.get_all``). Exactly one non-empty, well-formed value
    each, or ``Refused("invalid_pool_headers")``."""
    def one(name: str, pattern: re.Pattern) -> str:
        found = values(name) or []
        if len(found) != 1 or not pattern.match(found[0]):
            raise Refused("invalid_pool_headers")
        return found[0]

    if one(PROTOCOL_HEADER, re.compile(r"^1$")) != str(PROTOCOL_VERSION):
        raise Refused("invalid_pool_headers")
    return one(CREDENTIAL_HEADER, _AGX_RE), one(JOURNAL_HEADER, _JOURNAL_RE)


def capability_answer(params: object, *, executor_id: str, connection_revision: str, runtime_version: str) -> dict:
    """cloud-capability-result.json ``result`` for a strict cloud-capability-request.json ``params``
    addressed to this executor, else ``Refused("capability_mismatch")``. The answer lists only what this
    plugin implements and claims no open-pool consent."""
    if not isinstance(params, dict) or set(params) != _CAPABILITY_KEYS:
        raise Refused("capability_mismatch")
    nonce, required = params["nonce"], params["requiredCapabilities"]
    if (type(params["protocolVersion"]) is not int or params["protocolVersion"] != PROTOCOL_VERSION
            or params["executorId"] != executor_id or params["executorConnectionRevision"] != connection_revision
            or not isinstance(nonce, str) or not 32 <= len(nonce) <= 128
            or not isinstance(required, list) or not required
            or not all(isinstance(c, str) and c in IMPLEMENTED_CAPABILITIES for c in required)):
        raise Refused("capability_mismatch")
    return {"protocolVersion": PROTOCOL_VERSION, "executorId": executor_id,
            "executorConnectionRevision": connection_revision, "nonce": nonce,
            "capabilities": list(IMPLEMENTED_CAPABILITIES), "runtimeVersion": runtime_version}

"""Pooled execution protocol v1: the assignment tuple, the dispatch envelope, and the capability proof.

Shapes follow the ArtifactBridge integration-u3-r1 fixtures (tests/fixtures/pooled_v1). Every object is
strict: an unknown or missing key is a refusal, never a best-effort parse. The dispatch metadata never
carries a ``dlt_`` token; this module never accepts one.
"""
from __future__ import annotations

import re
from datetime import datetime

PROTOCOL_VERSION = 1
METADATA_KEY = "artifactbridge.assignment"

# The cloud control routes of artifact-forge src/routes/agent-gateway-pool.ts (c0b4de56c; not yet merged or
# deployed): same origin as the configured tools URL, Bearer agx_ executor control credential.
START_PATH = "/api/agent-gateway/pool/start"
BOOTSTRAP_PATH = "/api/agent-gateway/pool/bootstrap"
REPORT_PATH = "/api/agent-gateway/pool/report"
CAPABILITY_PATH = "/api/agent-gateway/pool/capability"  # GET the challenge, POST the proof

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
_REVISION_RE = re.compile(r"^[1-9][0-9]{0,18}$")

# Only what this plugin implements; the proof is separate from open-pool consent, which the plugin never
# claims or sets.
IMPLEMENTED_CAPABILITIES = (
    "assignment_tuple_start_ownership",
    "scoped_space_document_skill",
    "per_task_credentials_fresh_sessions",
    "same_session_continuation",
    "durable_inspect_recovery",
    "cancel_report_fencing",
)
_CHALLENGE_KEYS = frozenset({"protocolVersion", "executorId", "executorConnectionRevision", "nonce",
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
            and _positive_int(envelope["commandSequence"]) and _positive_int(envelope["setupRevision"])):
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


def parse_bootstrap_response(body: object) -> str | None:
    """The ``dlc_`` of bootstrap-response.json (exactly ``credential`` and an offset ``expiresAt``), else None."""
    if not isinstance(body, dict) or set(body) != {"credential", "expiresAt"}:
        return None
    try:
        parse_start_by(body["expiresAt"])
    except Refused:
        return None
    credential = body["credential"]
    return credential if isinstance(credential, str) and _DLC_RE.match(credential) else None


def parse_report_response(body: object) -> tuple[bool, bool] | None:
    """``(recorded, currentOccupant)`` of report-response.json / report-replay-response.json, else None."""
    if not isinstance(body, dict) or set(body) != {"recorded", "currentOccupant"}:
        return None
    if type(body["recorded"]) is not bool or type(body["currentOccupant"]) is not bool:
        return None
    return body["recorded"], body["currentOccupant"]


def parse_capability_verdict(body: object) -> dict | None:
    """capability-response.json or capability-refusal-response.json, else None."""
    if not isinstance(body, dict):
        return None
    if body.get("eligibility") == "pool_capable" and set(body) == {"eligibility", "capabilityRevision",
                                                                   "executorConnectionRevision"}:
        revisions = (body["capabilityRevision"], body["executorConnectionRevision"])
        return body if all(isinstance(r, str) and _REVISION_RE.match(r) for r in revisions) else None
    if body.get("eligibility") == "own_agent_only" and set(body) == {"eligibility", "reason"} and body["reason"] in (
            "proof_invalid", "challenge_mismatch", "missing_capability"):
        return body
    return None


def answer_capability_challenge(challenge: object, *, executor_id: str, connection_revision: str,
                                runtime_version: str) -> dict | None:
    """capability-proof.json for a strict capability-challenge.json addressed to this executor, else None.
    The proof lists only what this plugin implements and claims no open-pool consent."""
    if not isinstance(challenge, dict) or set(challenge) != _CHALLENGE_KEYS:
        return None
    nonce, required = challenge["nonce"], challenge["requiredCapabilities"]
    if (challenge["protocolVersion"] != PROTOCOL_VERSION or type(challenge["protocolVersion"]) is not int
            or challenge["executorId"] != executor_id or challenge["executorConnectionRevision"] != connection_revision
            or not isinstance(nonce, str) or not 32 <= len(nonce) <= 128
            or not isinstance(required, list) or not required
            or not all(isinstance(c, str) and c in IMPLEMENTED_CAPABILITIES for c in required)):
        return None
    return {"protocolVersion": PROTOCOL_VERSION, "executorId": executor_id,
            "executorConnectionRevision": connection_revision, "nonce": nonce,
            "capabilities": list(IMPLEMENTED_CAPABILITIES), "runtimeVersion": runtime_version}

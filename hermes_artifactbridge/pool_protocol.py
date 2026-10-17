"""Pooled execution protocol v1: the assignment tuple, the dispatch envelope, and the capability proof.

Shapes follow the ArtifactBridge U3 checkpoint fixtures (tests/fixtures/pooled_v1). Every object is
strict: an unknown or missing key is a refusal, never a best-effort parse. The dispatch metadata never
carries a ``dlt_`` token; this module never accepts one.
"""
from __future__ import annotations

import re
from datetime import datetime

PROTOCOL_VERSION = 1
METADATA_KEY = "artifactbridge.assignment"

# Provisional, pending server route (U2/U4): no cloud HTTP routes exist yet (gap G2). Same origin as the
# configured tools URL, Bearer agx_ executor control credential, bodies exactly the fixture shapes.
START_PATH = "/api/agent-gateway/pool/start"
BOOTSTRAP_PATH = "/api/agent-gateway/pool/bootstrap"
REPORT_PATH = "/api/agent-gateway/pool/report"

_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_BIGINT_RE = re.compile(r"^[1-9][0-9]*$")
_BIGINT_MAX = 9223372036854775807
_UUID_FIELDS = ("workspaceId", "serviceId", "delegationId", "assignmentId", "executorId", "slotId")
_BIGINT_FIELDS = ("reservationGeneration", "executorConnectionRevision")
BINDING_KEYS = frozenset(_UUID_FIELDS + _BIGINT_FIELDS + ("executionGeneration",))
ENVELOPE_KEYS = frozenset({"protocolVersion", "binding", "dispatchId", "commandSequence", "setupRevision", "startBy"})

# Only what this plugin implements (gap G5); the proof is separate from open-pool consent, which the
# plugin never claims or sets.
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
    """Start request body (start-request.json); also the bootstrap body, provisional, pending server route
    (U2/U4), gap G3."""
    return {"protocolVersion": PROTOCOL_VERSION, "binding": envelope["binding"], "dispatchId": envelope["dispatchId"],
            "startJournalId": start_journal_id}


def report_body(binding: dict, sequence: int, kind: str, outcome: str | None) -> dict:
    """Content-free report: report.json / cancel-ack.json."""
    return {"binding": binding, "reportSequence": sequence, "kind": kind, "outcome": outcome}


def answer_capability_challenge(challenge: object, *, executor_id: str, connection_revision: str,
                                runtime_version: str) -> dict | None:
    """capability-proof.json for a challenge addressed to this executor, else None. Pure: no channel
    for the challenge is specified (gap G5: provisional, pending server route (U2/U4)), so none is invented."""
    if not isinstance(challenge, dict) or set(challenge) != _CHALLENGE_KEYS:
        return None
    if (challenge["protocolVersion"] != PROTOCOL_VERSION or type(challenge["protocolVersion"]) is not int
            or challenge["executorId"] != executor_id or challenge["executorConnectionRevision"] != connection_revision
            or not isinstance(challenge["nonce"], str) or not challenge["nonce"]
            or not isinstance(challenge["requiredCapabilities"], list)):
        return None
    return {"protocolVersion": PROTOCOL_VERSION, "executorId": executor_id,
            "executorConnectionRevision": connection_revision, "nonce": challenge["nonce"],
            "capabilities": list(IMPLEMENTED_CAPABILITIES), "runtimeVersion": runtime_version}

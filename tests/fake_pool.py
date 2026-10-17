"""In-process fake of the ArtifactBridge pooled control routes (src/routes/agent-gateway-pool.ts at
artifact-forge c0b4de56c), answering with the integration-u3-r1 fixture statuses and bodies.

- 401 ``unauthorized-response.json`` for a missing or unknown ``agx_``; 400 ``invalid_request`` for a body
  that is not the strict protocol v1 shape; 409 ``scheduler-busy-response.json`` (retryable) or a named
  refusal (``refusal-response.json`` shape).
- Start follows ``decidePooledStart``: unknown assignment ``start_not_authorized``, a tuple or dispatch
  mismatch, another journal ``start_owned``, a passed startBy before spawn, then the permit for the same
  journal. Scaffolding mode (default): the first journal owns an unstarted assignment. Cloud mode
  (``cloud=True``): the server lifecycle owns start, so an assignment it did not start is
  ``start_not_authorized`` and one it started under its own journal is ``start_owned``.
- Bootstrap needs the start journal; a retry mints a fresh ``dlc_`` and revokes the earlier one.
- Report is idempotent by reportSequence (recorded true, then false on a replay); ``currentOccupant`` is
  false for a superseded assignment.
- Capability: GET derives the nonce from the credential hash, executor and revision; POST verifies like
  ``verifyPooledCapabilityProof`` and never touches open-pool consent.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import httpx2

# The server's routes, written out (not imported from the plugin) so a wrong plugin route fails.
START_PATH = "/api/agent-gateway/pool/start"
BOOTSTRAP_PATH = "/api/agent-gateway/pool/bootstrap"
REPORT_PATH = "/api/agent-gateway/pool/report"
CAPABILITY_PATH = "/api/agent-gateway/pool/capability"
FIXTURES = Path(__file__).parent / "fixtures" / "pooled_v1"
EXECUTOR_CREDENTIAL = "agx_test_executor_credential_do_not_leak"
ORIGIN = "https://ab.test"
TOOLS_URL = ORIGIN + "/mcp/agent-gateway"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def envelope(**changes) -> dict:
    """The dispatch-metadata fixture's envelope; ``binding`` changes merge into the binding."""
    env = copy.deepcopy(fixture("dispatch-metadata.json")["artifactbridge.assignment"])
    env["binding"].update(changes.pop("binding", {}))
    env.update(changes)
    return env


def metadata(env: dict) -> dict:
    return {"artifactbridge.assignment": env}


EXECUTOR_ID = envelope()["binding"]["executorId"]
REVISION = envelope()["binding"]["executorConnectionRevision"]
# The fixture's startBy minus ten minutes: a clock where the fixtures are still startable.
BEFORE_START_BY = datetime(2026, 10, 17, 11, 50, tzinfo=timezone.utc)
BINDING_KEYS = set(envelope()["binding"])
REQUIRED_CAPABILITIES = fixture("capability-challenge.json")["requiredCapabilities"]
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_REVISION = re.compile(r"^[1-9][0-9]{0,18}$")
_REPORT_KINDS = {"running", "stopped", "cancel_acknowledged"}
_REPORT_OUTCOMES = {"completed", "input_wait", "failed", "canceled", "timed_out", "crashed", None}


class Clock:
    def __init__(self, now: datetime = BEFORE_START_BY):
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **delta) -> None:
        self.now += timedelta(**delta)


def _error(status: int, code: str, message: str = "The request was refused.") -> httpx2.Response:
    return httpx2.Response(status, json={"error": {"code": code, "message": message}})


def _start_refusal(reason: str) -> httpx2.Response:
    return httpx2.Response(409, json={"outcome": "refused", "reason": reason})


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@dataclass
class FakeControl:
    clock: Clock
    cloud: bool = False
    assignments: dict[str, dict] = field(default_factory=dict)  # assignmentId -> the dispatched envelope
    journals: dict[str, str] = field(default_factory=dict)  # assignmentId -> start journal
    live: dict[str, str] = field(default_factory=dict)  # assignmentId -> its one live dlc_
    issued: dict[str, dict] = field(default_factory=dict)  # every dlc_ minted -> binding (test-only)
    revoked: set[str] = field(default_factory=set)
    superseded: set[str] = field(default_factory=set)  # assignmentIds no longer the slot's occupant
    last_sequence: dict[str, int] = field(default_factory=dict)
    starts: list[dict] = field(default_factory=list)  # processed request bodies, per route
    bootstraps: list[dict] = field(default_factory=list)
    reports: list[dict] = field(default_factory=list)  # recorded reports only
    replays: list[dict] = field(default_factory=list)  # reports answered recorded:false
    proofs: list[dict] = field(default_factory=list)
    capability_revision: int = 0
    requests: list[dict] = field(default_factory=list)  # every request, before any fault
    # path -> queue of faults, one per request: "busy" (409 scheduler_busy, nothing changed), "down" (503,
    # nothing changed), "lost" (processed, then the response is lost).
    faults: dict[str, list[str]] = field(default_factory=dict)
    refuse: dict[str, str] = field(default_factory=dict)  # path -> named 409 refusal code (persistent)
    refuse_start: str | None = None  # a 409 start refusal in the start-response shape (persistent)
    tamper_permit: dict | None = None
    redirect: bool = False
    on_start: Callable[[dict], None] | None = None
    on_bootstrap: Callable[[dict], None] | None = None  # after the dlc_ is minted, before the response

    def issue(self, env: dict) -> dict:
        """Dispatch ``env`` server-side; returns the A2A request metadata carrying it."""
        self.assignments[env["binding"]["assignmentId"]] = copy.deepcopy(env)
        return metadata(env)

    def transport(self) -> httpx2.MockTransport:
        return httpx2.MockTransport(self._handle)

    def factory(self, headers: dict[str, str]) -> httpx2.Client:
        return httpx2.Client(transport=self.transport(), headers=headers, follow_redirects=False)

    def _handle(self, request: httpx2.Request) -> httpx2.Response:
        path = request.url.path
        self.requests.append({"method": request.method, "path": path, "url": str(request.url),
                              "authorization": request.headers.get("authorization"),
                              "delegation_token": request.headers.get("x-artifactbridge-delegation-token"),
                              "body": json.loads(request.content) if request.content else None})
        if request.headers.get("authorization") != f"Bearer {EXECUTOR_CREDENTIAL}":
            return httpx2.Response(401, json=fixture("unauthorized-response.json"))
        if self.redirect:
            return httpx2.Response(307, headers={"location": "https://elsewhere.test/steal"})
        fault = self.faults.get(path, []).pop(0) if self.faults.get(path) else None
        if fault == "busy":
            return httpx2.Response(409, json=fixture("scheduler-busy-response.json"))
        if fault == "down":
            return httpx2.Response(503)
        if request.method == "GET":
            response = self._challenge(dict(request.url.params)) if path == CAPABILITY_PATH else httpx2.Response(404)
        else:
            try:
                body = json.loads(request.content)
            except ValueError:
                return _error(400, "invalid_request", "Body must be a protocol version 1 request.")
            if path in self.refuse:
                return _error(409, self.refuse[path])
            handler = {START_PATH: self._start, BOOTSTRAP_PATH: self._bootstrap, REPORT_PATH: self._report,
                       CAPABILITY_PATH: lambda b: self._prove(b, dict(request.url.params))}.get(path)
            response = handler(body) if handler else httpx2.Response(404)
        if fault == "lost":
            raise httpx2.ReadError("response lost")
        return response

    def _known(self, binding: object) -> str | None:
        """The assignmentId when ``binding`` is exactly the dispatched tuple."""
        aid = binding.get("assignmentId") if isinstance(binding, dict) else None
        env = self.assignments.get(aid)
        return aid if env is not None and env["binding"] == binding else None

    def _start(self, body: dict) -> httpx2.Response:
        if set(body) != {"protocolVersion", "binding", "dispatchId", "startJournalId"} or body["protocolVersion"] != 1 \
                or not isinstance(body["binding"], dict) or set(body["binding"]) != BINDING_KEYS:
            return _error(400, "invalid_request", "Body must be a protocol version 1 start request.")
        self.starts.append(body)
        if self.on_start:
            self.on_start(body)
        if self.refuse_start:
            return _start_refusal(self.refuse_start)
        env = self.assignments.get(body["binding"]["assignmentId"])
        if env is None:
            return _start_refusal("start_not_authorized")
        aid = env["binding"]["assignmentId"]
        if body["dispatchId"] != env["dispatchId"] or body["binding"] != env["binding"]:
            return _start_refusal("assignment_tuple_mismatch")
        journal = self.journals.get(aid)
        if journal is not None and journal != body["startJournalId"]:
            return _start_refusal("start_owned")
        if aid not in self.last_sequence and self.clock() >= datetime.fromisoformat(env["startBy"]):
            return httpx2.Response(409, json=fixture("start-deadline-refusal.json"))
        if journal is None:
            if self.cloud:  # the server lifecycle starts cloud work; an executor never claims it
                return _start_refusal("start_not_authorized")
            self.journals[aid] = body["startJournalId"]
        permit = {**env, "startJournalId": self.journals[aid], **(self.tamper_permit or {})}
        return httpx2.Response(200, json={"outcome": "permit", "permit": permit})

    def _bootstrap(self, body: dict) -> httpx2.Response:
        if set(body) != {"protocolVersion", "binding", "startJournalId"} or body["protocolVersion"] != 1:
            return _error(400, "invalid_request", "Body must be a protocol version 1 bootstrap request.")
        self.bootstraps.append(body)
        aid = self._known(body["binding"])
        if aid is None:
            return _error(409, "assignment_tuple_mismatch")
        if aid not in self.journals:
            return _error(409, "assignment_not_started")
        if self.journals[aid] != body["startJournalId"]:
            return _error(409, "start_owned")
        credential = "dlc_" + secrets.token_urlsafe(32)
        if aid in self.live:
            self.revoked.add(self.live[aid])  # at most one live dlc_ per assignment
        self.live[aid], self.issued[credential] = credential, body["binding"]
        if self.on_bootstrap:
            self.on_bootstrap(body)
        expires = (self.clock() + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        return httpx2.Response(200, json={"credential": credential, "expiresAt": expires})

    def _report(self, body: dict) -> httpx2.Response:
        sequence = body.get("reportSequence")
        if set(body) != {"binding", "reportSequence", "kind", "outcome"} or type(sequence) is not int or sequence < 1 \
                or body["kind"] not in _REPORT_KINDS or body["outcome"] not in _REPORT_OUTCOMES:
            return _error(400, "invalid_request", "Body must be a protocol version 1 report.")
        aid = self._known(body["binding"])
        if aid is None:
            return _error(409, "assignment_tuple_mismatch")
        recorded = sequence > self.last_sequence.get(aid, 0)
        if recorded:
            self.last_sequence[aid] = sequence
        (self.reports if recorded else self.replays).append(body)
        return httpx2.Response(200, json={"recorded": recorded, "currentOccupant": aid not in self.superseded})

    def _nonce(self, executor_id: str, revision: str) -> str:
        return _sha256("\n".join(["ab-pool-capability-v1", _sha256(EXECUTOR_CREDENTIAL), executor_id, revision]))

    def _challenge(self, query: dict) -> httpx2.Response:
        if not (_UUID.match(query.get("workspaceId", "")) and _UUID.match(query.get("executorId", ""))
                and _REVISION.match(query.get("executorConnectionRevision", ""))):
            return _error(400, "invalid_request", "workspaceId, executorId and executorConnectionRevision are required.")
        executor_id, revision = query["executorId"], query["executorConnectionRevision"]
        return httpx2.Response(200, json={"protocolVersion": 1, "executorId": executor_id,
                                          "executorConnectionRevision": revision,
                                          "nonce": self._nonce(executor_id, revision),
                                          "requiredCapabilities": list(REQUIRED_CAPABILITIES)})

    def _prove(self, proof: dict, query: dict) -> httpx2.Response:
        executor_id, revision = proof.get("executorId"), proof.get("executorConnectionRevision")
        if not (_UUID.match(query.get("workspaceId", "")) and isinstance(executor_id, str) and _UUID.match(executor_id)
                and isinstance(revision, str) and _REVISION.match(revision) and proof.get("protocolVersion") == 1):
            return _error(400, "invalid_request", "Body must be a protocol version 1 capability proof with ?workspaceId.")
        self.proofs.append(proof)
        capabilities = proof.get("capabilities")
        if set(proof) != set(fixture("capability-proof.json")) or not isinstance(capabilities, list):
            reason = "proof_invalid"
        elif proof["nonce"] != self._nonce(executor_id, revision):
            reason = "challenge_mismatch"
        elif not set(REQUIRED_CAPABILITIES) <= set(capabilities):
            reason = "missing_capability"
        else:
            self.capability_revision += 1
            return httpx2.Response(200, json={"eligibility": "pool_capable",
                                              "capabilityRevision": str(self.capability_revision),
                                              "executorConnectionRevision": revision})
        return httpx2.Response(409, json={"eligibility": "own_agent_only", "reason": reason})

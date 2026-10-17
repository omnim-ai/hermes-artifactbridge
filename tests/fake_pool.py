"""In-process fake of the ArtifactBridge pooled control routes (src/routes/agent-gateway-pool.ts at
artifact-forge a26eb977c), answering with the cloud-contract-r2 fixture statuses and bodies.

- Every route authenticates with the ``agx_`` of the assignment its body names (``Bearer`` of the start
  headers' ``X-AB-Control-Credential``): 401 ``unauthorized-response.json`` for a missing, unknown or other
  assignment's ``agx_``; 400 ``invalid_request`` for a body that is not the strict protocol v1 shape; 409
  ``scheduler-busy-response.json`` (retryable) or a named refusal (``refusal-response.json`` shape).
- ``issue`` is the server lifecycle starting cloud work: it mints the assignment's ``agx_`` and records the
  journal ``agj_`` + assignmentId without hyphens. Start follows ``decidePooledStart``: unknown assignment
  ``start_not_authorized``, a tuple or dispatch mismatch, another journal ``start_owned``, a passed startBy
  before spawn, then the replay permit for that journal.
- Bootstrap needs the start journal; a retry mints a fresh ``dlc_`` and revokes the earlier one.
- Report is idempotent by reportSequence (recorded true, then false on a replay); ``currentOccupant`` is
  false for a superseded assignment.
"""
from __future__ import annotations

import copy
import hashlib
import json
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
FIXTURES = Path(__file__).parent / "fixtures" / "cloud_r2"
ORIGIN = "https://ab.test"
TOOLS_URL = ORIGIN + "/mcp/agent-gateway"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def envelope(**changes) -> dict:
    """The dispatch-metadata fixture's envelope; ``binding`` changes merge into the binding."""
    env = copy.deepcopy(fixture("cloud-send-request.json")["body"]["params"]["metadata"]["artifactbridge.assignment"])
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
# The fixtures' agx_ for their two assignments (cloud-send-request.json, cloud-continuation-request.json).
_FIXTURE_GRANTS = {
    fixture(name)["body"]["params"]["metadata"]["artifactbridge.assignment"]["binding"]["assignmentId"]:
        fixture(name)["headers"]["x-ab-control-credential"]
    for name in ("cloud-send-request.json", "cloud-continuation-request.json")}


def grant_for(env: dict) -> tuple[str, str]:
    """``(agx_, startJournalId)`` the server presents for ``env``'s assignment."""
    aid = env["binding"]["assignmentId"]
    agx = _FIXTURE_GRANTS.get(aid) or "agx_" + hashlib.sha256(aid.encode()).hexdigest()[:43]
    return agx, "agj_" + aid.replace("-", "")


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


@dataclass
class FakeControl:
    clock: Clock
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
        """Start ``env`` server-side under its journal; returns the A2A request metadata carrying it."""
        aid = env["binding"]["assignmentId"]
        self.assignments[aid] = copy.deepcopy(env)
        self.journals.setdefault(aid, grant_for(env)[1])
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
        try:
            body = json.loads(request.content)
            env = self.assignments[body["binding"]["assignmentId"]]
        except (ValueError, KeyError, TypeError):
            env = None
        if env is None or request.headers.get("authorization") != f"Bearer {grant_for(env)[0]}":
            return httpx2.Response(401, json=fixture("unauthorized-response.json"))
        if self.redirect:
            return httpx2.Response(307, headers={"location": "https://elsewhere.test/steal"})
        fault = self.faults.get(path, []).pop(0) if self.faults.get(path) else None
        if fault == "busy":
            return httpx2.Response(409, json=fixture("scheduler-busy-response.json"))
        if fault == "down":
            return httpx2.Response(503)
        if path in self.refuse:
            return _error(409, self.refuse[path])
        handler = {START_PATH: self._start, BOOTSTRAP_PATH: self._bootstrap, REPORT_PATH: self._report}.get(path)
        response = handler(body) if handler and request.method == "POST" else httpx2.Response(404)
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
        if journal is None:  # the server lifecycle starts cloud work; an executor never claims it
            return _start_refusal("start_not_authorized")
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

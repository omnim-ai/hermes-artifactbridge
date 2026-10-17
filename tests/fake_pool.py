"""In-process fake of the provisional pooled control routes (G2/G3), driven by the U3 fixtures.

Start ownership follows the U3 checkpoint: the first startJournalId owns a dispatch, the same journal
replays the same permit, a different one gets start_owned, and after startBy every start is refused.
"""
from __future__ import annotations

import copy
import json
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import httpx2

from hermes_artifactbridge.pool_protocol import BOOTSTRAP_PATH, REPORT_PATH, START_PATH

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


class Clock:
    def __init__(self, now: datetime = BEFORE_START_BY):
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **delta) -> None:
        self.now += timedelta(**delta)


@dataclass
class FakeControl:
    clock: Clock
    assignments: dict[tuple[str, str], dict] = field(default_factory=dict)  # (assignment, dispatch) -> envelope
    owners: dict[tuple[str, str], str] = field(default_factory=dict)  # (assignment, dispatch) -> journal id
    starts: list[dict] = field(default_factory=list)
    bootstraps: list[dict] = field(default_factory=list)
    reports: list[dict] = field(default_factory=list)
    issued: dict[str, dict] = field(default_factory=dict)  # dlc_ -> binding (test-only)
    requests: list[dict] = field(default_factory=list)
    lose_start_responses: int = 0  # record ownership, then fail the response (lost ack)
    refuse_with: str | None = None
    tamper_permit: dict | None = None
    bootstrap_fails: bool = False
    redirect: bool = False
    on_start: Callable[[dict], None] | None = None

    def issue(self, env: dict) -> dict:
        """Dispatch ``env`` server-side; returns the A2A request metadata carrying it."""
        self.assignments[(env["binding"]["assignmentId"], env["dispatchId"])] = copy.deepcopy(env)
        return metadata(env)

    def transport(self) -> httpx2.MockTransport:
        return httpx2.MockTransport(self._handle)

    def factory(self, headers: dict[str, str]) -> httpx2.Client:
        return httpx2.Client(transport=self.transport(), headers=headers, follow_redirects=False)

    def _handle(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append({"url": str(request.url), "authorization": request.headers.get("authorization"),
                              "delegation_token": request.headers.get("x-artifactbridge-delegation-token")})
        if request.headers.get("authorization") != f"Bearer {EXECUTOR_CREDENTIAL}":
            return httpx2.Response(401, json={"error": {"code": "unauthorized"}})
        if self.redirect:
            return httpx2.Response(307, headers={"location": "https://elsewhere.test/steal"})
        body = json.loads(request.content)
        path = request.url.path
        if path == START_PATH:
            return self._start(body)
        if path == BOOTSTRAP_PATH:
            self.bootstraps.append(body)
            if self.bootstrap_fails:
                return httpx2.Response(503)
            credential = "dlc_" + secrets.token_urlsafe(32)
            self.issued[credential] = body["binding"]
            return httpx2.Response(200, json={"credential": credential, "expiresAt": "2026-10-17T13:00:00.000Z"})
        if path == REPORT_PATH:
            self.reports.append(body)
            return httpx2.Response(200, json={"ok": True})
        return httpx2.Response(404)

    def _start(self, body: dict) -> httpx2.Response:
        self.starts.append(body)
        if self.on_start:
            self.on_start(body)
        if self.refuse_with:
            return httpx2.Response(200, json={"outcome": "refused", "reason": self.refuse_with})
        key = (body["binding"]["assignmentId"], body["dispatchId"])
        env = self.assignments.get(key)  # the server's own copy of what it dispatched
        if env is None or env["binding"] != body["binding"]:
            return httpx2.Response(200, json={"outcome": "refused", "reason": "unknown_assignment"})
        if self.clock() >= datetime.fromisoformat(env["startBy"]):
            return httpx2.Response(200, json=fixture("start-deadline-refusal.json"))
        owner = self.owners.setdefault(key, body["startJournalId"])
        if owner != body["startJournalId"]:
            return httpx2.Response(200, json=fixture("start-owned-refusal.json"))
        if self.lose_start_responses:
            self.lose_start_responses -= 1
            raise httpx2.ReadError("response lost")
        permit = {**env, "startJournalId": owner, **(self.tamper_permit or {})}
        return httpx2.Response(200, json={"outcome": "permit", "permit": permit})

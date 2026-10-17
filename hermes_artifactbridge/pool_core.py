"""Pooled (assignment-bound) execution core, independent of Hermes.

``PoolCore.admit`` is the intercept every request on the pooled listener goes through before any agent
turn exists: validate the envelope and executor, journal it (committed before any network call), ask
for a start permit with the row's stable startJournalId, check the permit against the journaled
envelope, bootstrap the per-task ``dlc_`` credential, and only then answer "spawn". Retries of a
dispatch read the journal row instead of starting again. ``finish``/``cancel`` report with the task's
own binding and the next report sequence; ``recover`` marks turns lost to a restart ``uncertain``.

The ``agx_`` control credential authenticates start/bootstrap/report only and is held in memory; the
``dlc_`` credential lives only in ``CredentialBindings``. Neither is logged, returned or journaled.
"""
from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

import httpx2

from .endpoint import url_on_origin
from .pool_journal import TERMINAL_STATES, Journal, Tx
from .pool_protocol import (BOOTSTRAP_PATH, REPORT_PATH, START_PATH, Refused, envelope_from_metadata, parse_start_by,
                            report_body, start_request)

logger = logging.getLogger(__name__)

POOL_PLATFORM = "artifactbridge-pool"
# Core outcome names; the adapter maps them to A2A task states.
COMPLETED, INPUT_REQUIRED, FAILED, CANCELED, REJECTED = "completed", "input_required", "failed", "canceled", "rejected"
_REPORT_OUTCOME = {COMPLETED: "completed", INPUT_REQUIRED: "input_wait", FAILED: "failed", CANCELED: "canceled"}
_UNCERTAIN_TEXT = "ArtifactBridge pooled dispatch outcome uncertain: the executor restarted while it ran."

ControlFactory = Callable[[dict[str, str]], httpx2.Client]


def refusal_text(code: str) -> str:
    return f"ArtifactBridge pooled dispatch refused: {code}"


def default_control_factory(headers: dict[str, str]) -> httpx2.Client:
    return httpx2.Client(headers=headers, follow_redirects=False, timeout=httpx2.Timeout(30))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ControlUnavailable(Exception):
    """The control call's outcome is unknown or malformed; the journal row stays retryable."""


class PoolControl:
    """Executor control client. Routes and the bootstrap response shape are provisional, pending server
    route (U2/U4): gaps G2/G3. Redirects are never followed."""

    def __init__(self, *, tools_url: str, credential: str, http_factory: ControlFactory = default_control_factory):
        self._tools_url = tools_url
        self._credential = credential
        self._http_factory = http_factory

    def _post(self, path: str, body: dict) -> dict:
        headers = {"authorization": f"Bearer {self._credential}"}
        try:
            with self._http_factory(headers) as http:
                response = http.post(url_on_origin(self._tools_url, path), json=body)
        except Exception as exc:
            raise ControlUnavailable(type(exc).__name__) from None
        if not 200 <= response.status_code < 300:
            raise ControlUnavailable(f"status {response.status_code}")
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if not isinstance(payload, dict):
            raise ControlUnavailable("malformed response")
        return payload

    def start(self, request: dict) -> dict:
        """start-permit.json or a start refusal; anything else is ControlUnavailable."""
        body = self._post(START_PATH, request)
        if body.get("outcome") == "refused" and set(body) == {"outcome", "reason"} and isinstance(body["reason"], str):
            return body
        if body.get("outcome") == "permit" and set(body) == {"outcome", "permit"} and isinstance(body["permit"], dict):
            return body
        raise ControlUnavailable("malformed start response")

    def bootstrap(self, request: dict) -> str:
        """Provisional, pending server route (U2/U4), gap G3: request = the start request body, response
        {"credential": "dlc_...", "expiresAt"}."""
        body = self._post(BOOTSTRAP_PATH, request)
        credential = body.get("credential")
        try:
            parse_start_by(body.get("expiresAt"))
        except Refused:
            raise ControlUnavailable("malformed bootstrap response") from None
        if not (isinstance(credential, str) and credential.startswith("dlc_") and len(credential) > 4):
            raise ControlUnavailable("malformed bootstrap response")
        return credential

    def report(self, body: dict) -> bool:
        """Provisional, pending server route (U2/U4), gap G4: the server's report transport is RPC-only
        (agent_gateway_pooled_report). A failed report is logged and not retried (no outbox)."""
        try:
            self._post(REPORT_PATH, body)
        except ControlUnavailable as exc:
            logger.warning("artifactbridge pooled report %s/%s not delivered: %s", body["kind"], body["reportSequence"], exc)
            return False
        return True


@dataclass
class _Bound:
    credential: str = field(repr=False)
    binding: dict


class CredentialBindings:
    """In-memory ``dlc_`` per (platform, A2A contextId, A2A task id); a lookup never falls back."""

    def __init__(self):
        self._lock = threading.Lock()
        self._bound: dict[tuple[str, str, str], _Bound] = {}

    def bind(self, platform: str, context_id: str, task_id: str, credential: str, binding: dict) -> None:
        with self._lock:
            self._bound[(platform, context_id, task_id)] = _Bound(credential, binding)

    def lookup(self, platform: str, context_id: str, task_id: str) -> str | None:
        with self._lock:
            bound = self._bound.get((platform, context_id, task_id))
        return bound.credential if bound else None

    def drop(self, platform: str, context_id: str, task_id: str) -> None:
        with self._lock:
            self._bound.pop((platform, context_id, task_id), None)


CREDENTIALS = CredentialBindings()


@dataclass(frozen=True)
class Decision:
    """``spawn``: run the turn as ``task_id`` on ``context_id``. ``working``: the dispatch already runs as
    ``task_id``. ``terminal``: answer ``outcome``/``text`` and run nothing."""
    kind: str
    context_id: str
    task_id: str = ""
    outcome: str = ""
    text: str = ""


def _key(envelope: dict) -> tuple[str, str]:
    return envelope["binding"]["assignmentId"], envelope["dispatchId"]


class PoolCore:
    def __init__(self, *, journal: Journal, control: PoolControl, executor_id: str, connection_revision: str,
                 credentials: CredentialBindings = CREDENTIALS, now: Callable[[], datetime] = _utcnow):
        self.journal = journal
        self.control = control
        self.executor_id = executor_id
        self.connection_revision = connection_revision
        self.credentials = credentials
        self._now = now
        self._lock = threading.Lock()
        self._live: dict[str, tuple[str, str]] = {}  # A2A task id -> dispatch key, turns of THIS process
        self._done: dict[tuple[str, str], threading.Event] = {}

    # ---- intercept ----
    def admit(self, metadata: object, context_id: str, task_id: str, *, wait: float = 0.0) -> Decision:
        """``context_id`` is the request's A2A contextId ("" when absent); ``task_id`` is the A2A task id
        the turn will run as if this request spawns it; ``wait`` bounds waiting on an in-process turn."""
        try:
            envelope = envelope_from_metadata(metadata)
            binding = envelope["binding"]
            if binding["executorId"] != self.executor_id or binding["executorConnectionRevision"] != self.connection_revision:
                raise Refused("executor_mismatch")
            with self.journal.tx() as tx:
                row = self._journal_dispatch(tx, envelope, context_id)
        except Refused as refused:
            logger.info("artifactbridge pooled dispatch refused: %s", refused.code)
            return Decision("terminal", context_id, outcome=REJECTED, text=refusal_text(refused.code))
        return self._advance(row, task_id, wait)

    @staticmethod
    def _journal_dispatch(tx: Tx, envelope: dict, context_id: str) -> dict:
        if any(row["envelope"] != envelope for row in tx.where("dispatch_id", envelope["dispatchId"])):
            raise Refused("tuple_mismatch")  # the first journaled envelope (startBy included) is immutable
        row = tx.get(_key(envelope))
        if row is not None:
            if context_id and context_id != row["context_id"]:
                raise Refused("context_mismatch")
            return row
        delegation_id, generation = envelope["binding"]["delegationId"], envelope["binding"]["executionGeneration"]
        if context_id and any(r["delegation_id"] != delegation_id for r in tx.where("context_id", context_id)):
            raise Refused("context_bound_to_other_delegation")
        prior = tx.where("delegation_id", delegation_id)
        # Continuation affinity only: a known delegation resumes on its original context, never on a fresh one.
        if (prior and context_id != prior[0]["context_id"]) or (not prior and generation > 1):
            raise Refused("resume_unavailable")
        return tx.insert(envelope, str(uuid.uuid4()), context_id or "ctx-" + uuid.uuid4().hex[:16])

    def _advance(self, row: dict, task_id: str, wait: float) -> Decision:
        if row["state"] in TERMINAL_STATES:
            return self._terminal(row)
        if row["state"] in ("permitted", "spawned"):
            return self._await(row, wait)
        envelope, key, journal_id = row["envelope"], _key(row["envelope"]), row["start_journal_id"]
        start_by = parse_start_by(envelope["startBy"])
        if self._now() >= start_by:
            return self._refuse(key, "start_deadline_passed", task_id, wait)
        with self.journal.tx() as tx:
            tx.update(key, from_states=("journaled",), state="start_requested")
        request = start_request(envelope, journal_id)
        try:
            response = self.control.start(request)
        except ControlUnavailable as exc:
            logger.warning("artifactbridge pooled start unavailable: %s", exc)
            return Decision("terminal", row["context_id"], outcome=FAILED, text=refusal_text("start_unavailable"))
        if response["outcome"] == "refused":
            return self._refuse(key, response["reason"], task_id, wait)
        if response["permit"] != {**envelope, "startJournalId": journal_id}:
            return self._refuse(key, "permit_mismatch", task_id, wait)
        if self._now() >= start_by:  # a replayed permit never starts a turn after startBy
            return self._refuse(key, "start_deadline_passed", task_id, wait)
        with self.journal.tx() as tx:
            claimed = tx.update(key, from_states=("journaled", "start_requested"), state="permitted", task_id=task_id)
            current = tx.get(key)
        if not claimed:  # a concurrent delivery owns this permit
            return self._advance(current, task_id, wait)
        with self._lock:
            self._live[task_id] = key
            self._done[key] = threading.Event()
        try:
            credential = self.control.bootstrap(request)
        except ControlUnavailable as exc:
            logger.warning("artifactbridge pooled bootstrap unavailable: %s", exc)
            self.finish(task_id, FAILED, refusal_text("bootstrap_unavailable"))
            return Decision("terminal", row["context_id"], outcome=FAILED, text=refusal_text("bootstrap_unavailable"))
        if self._now() >= start_by:  # bootstrap can outlast startBy; the unbound credential is discarded
            self._end(task_id, FAILED, "", kind="stopped", refusal="start_deadline_passed")
            with self.journal.tx() as tx:
                return self._terminal(tx.get(key))
        with self.journal.tx() as tx:
            spawned = tx.update(key, from_states=("permitted",), state="spawned")
            current = tx.get(key)
        if not spawned:
            return self._terminal(current)
        self.credentials.bind(POOL_PLATFORM, row["context_id"], task_id, credential, envelope["binding"])
        self._report(key, "running", None)
        return Decision("spawn", row["context_id"], task_id=task_id)

    def _refuse(self, key: tuple[str, str], code: str, task_id: str, wait: float) -> Decision:
        with self.journal.tx() as tx:
            tx.update(key, from_states=("journaled", "start_requested"), state="refused", refusal=code)
            row = tx.get(key)
        return self._advance(row, task_id, wait)

    def _await(self, row: dict, wait: float) -> Decision:
        """Never a second turn: wait on this process's turn (if any), then report what the journal holds."""
        key = _key(row["envelope"])
        with self._lock:
            done = self._done.get(key)
        if done is not None and wait > 0:
            done.wait(wait)
        with self.journal.tx() as tx:
            row = tx.get(key)
        if row["state"] in TERMINAL_STATES:
            return self._terminal(row)
        return Decision("working", row["context_id"], task_id=row["task_id"] or "")

    @staticmethod
    def _terminal(row: dict) -> Decision:
        if row["state"] == "finished":
            return Decision("terminal", row["context_id"], outcome=row["result_state"], text=row["result_text"] or "")
        if row["state"] == "refused":
            return Decision("terminal", row["context_id"], outcome=REJECTED, text=refusal_text(row["refusal"]))
        return Decision("terminal", row["context_id"], outcome=FAILED, text=_UNCERTAIN_TEXT)

    # ---- turn lifecycle ----
    def finish(self, task_id: str, outcome: str, text: str) -> None:
        """Record a turn's result on its OWN row only; a row already canceled (or replaced) stays as it is
        and gets no further report."""
        self._end(task_id, outcome, text, kind="stopped")

    def cancel(self, task_id: str) -> bool:
        """Cancel a live pooled turn: cancel_acknowledged with its own binding; drops its credential."""
        return self._end(task_id, CANCELED, "", kind="cancel_acknowledged")

    def _end(self, task_id: str, outcome: str, text: str, *, kind: str, refusal: str | None = None) -> bool:
        """``refusal`` ends a permitted, never-spawned row as refused; it still reports, as it held a permit."""
        with self._lock:
            key = self._live.get(task_id)
        if key is None:
            return False
        with self.journal.tx() as tx:
            if refusal:
                ended = tx.update(key, from_states=("permitted",), state="refused", refusal=refusal)
            else:
                ended = tx.update(key, from_states=("permitted", "spawned"), state="finished", result_state=outcome,
                                  result_text=text)
            sequence = tx.next_report_sequence(key) if ended else None
            row = tx.get(key)
        self.credentials.drop(POOL_PLATFORM, row["context_id"], task_id)
        with self._lock:
            self._live.pop(task_id, None)
            done = self._done.pop(key, None)
        if done is not None:
            done.set()
        if sequence is not None:
            self.control.report(report_body(row["envelope"]["binding"], sequence, kind, _REPORT_OUTCOME[outcome]))
        return ended

    def _report(self, key: tuple[str, str], kind: str, outcome: str | None) -> None:
        with self.journal.tx() as tx:
            sequence = tx.next_report_sequence(key)
            row = tx.get(key)
        self.control.report(report_body(row["envelope"]["binding"], sequence, kind, outcome))

    def recover(self) -> None:
        """On adapter start: a spawned row without a live turn here lost its in-memory Hermes turn. It becomes
        ``uncertain`` (never respawned) and is reported stopped/crashed. A permit claimed but never spawned
        goes back to start_requested so a retry re-asks with the same startJournalId."""
        with self._lock:
            live = set(self._live)
        reports = []
        with self.journal.tx() as tx:
            for row in tx.where("state", "permitted"):
                if row["task_id"] not in live:
                    tx.update(_key(row["envelope"]), from_states=("permitted",), state="start_requested", task_id=None)
            for row in tx.where("state", "spawned"):
                key = _key(row["envelope"])
                if row["task_id"] not in live and tx.update(key, from_states=("spawned",), state="uncertain"):
                    reports.append(report_body(row["envelope"]["binding"], tx.next_report_sequence(key), "stopped", "crashed"))
        for body in reports:
            self.control.report(body)

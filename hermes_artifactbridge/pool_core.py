"""Pooled (assignment-bound) execution core, independent of Hermes.

``PoolCore.admit`` is the intercept every request on the pooled listener goes through before any agent
turn exists: validate the envelope and executor, journal it (committed before any network call), ask
for a start permit with the row's stable startJournalId, check the permit against the journaled
envelope, bootstrap the per-task ``dlc_`` credential, and only then answer "spawn". Retries of a
dispatch read the journal row instead of starting again. ``finish``/``cancel`` report with the task's
own binding and the next report sequence; ``recover`` marks turns lost to a restart ``uncertain``.
Reports are journaled with their transition and sent afterwards, oldest sequence first (``deliver_reports``).

The assignment's ``agx_`` arrives in the request's start headers (``X-AB-Control-Credential``), authenticates
start/bootstrap/report for that assignment only, and is held in memory (the journal keeps its SHA-256
fingerprint); the ``dlc_`` credential lives only in ``CredentialBindings``. Neither is logged, returned or
journaled. A spawned turn is Submitted until Hermes admits it (``started``), then Working.
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable

import httpx2

from .endpoint import url_on_origin
from .pool_journal import TERMINAL_STATES, Journal, Tx
from .pool_protocol import (BOOTSTRAP_PATH, REPORT_PATH, START_PATH, Refused, bootstrap_request,
                            envelope_from_metadata, parse_bootstrap_response, parse_error, parse_report_response,
                            parse_start_by, parse_start_response, start_journal_id, start_request)

logger = logging.getLogger(__name__)

POOL_PLATFORM = "artifactbridge-pool"
# Core outcome names; the adapter maps them to A2A task states.
COMPLETED, FAILED, CANCELED, REJECTED = "completed", "failed", "canceled", "rejected"
_REPORT_OUTCOME = {COMPLETED: "completed", FAILED: "failed", CANCELED: "canceled"}
# Product ruling (2026-10-17): a pooled runner takes initial tasks only, never a continuation (native
# continuation support is AI-4105). The sender is told to send the task again with the answer.
_RESEND = ("This runner cannot continue a conversation: send the task again as a new request, with the answer "
           "included.")
_UNCERTAIN_TEXT = "ArtifactBridge pooled dispatch outcome uncertain: the executor restarted while it ran."

ControlFactory = Callable[[dict[str, str]], httpx2.Client]
Grant = tuple[str, str]  # (the assignment's agx_, the server's startJournalId) from the request's start headers
# scheduler_busy changed nothing on the server; it is retried after these pauses, then the call is unavailable.
_BUSY_BACKOFF_SECONDS = (0.05, 0.2)
# Reports sent per delivery pass; the rest wait for the next pass (admit, finish, cancel, recover).
_REPORTS_PER_PASS = 8


def refusal_text(code: str) -> str:
    text = f"ArtifactBridge pooled dispatch refused: {code}"
    return f"{text}. {_RESEND}" if code == "continuation_unsupported" else text


def clarification_text(question: str) -> str:
    """The FAILED result of a turn that asked for clarification; its question went to the Room first."""
    asked = f" The runner asked: {question}" if question else ""
    return f"ArtifactBridge pooled task failed: clarification_unsupported. {_RESEND}{asked}"


def default_control_factory(headers: dict[str, str]) -> httpx2.Client:
    return httpx2.Client(headers=headers, follow_redirects=False, timeout=httpx2.Timeout(30))


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ControlUnavailable(Exception):
    """The control call's outcome is unknown or malformed; the journal row stays retryable."""


class PoolControl:
    """Executor control client for the pooled routes (artifact-forge src/routes/agent-gateway-pool.ts at
    a26eb977c). Each call presents the ``agx_`` of the assignment it is about (the request's start
    headers); the plugin holds no executor-wide credential. Redirects are never followed. A named refusal
    (a 4xx with a contract body) raises ``Refused``; ``scheduler_busy`` is retried a bounded number of
    times; anything else unknown or malformed raises ``ControlUnavailable``."""

    def __init__(self, *, tools_url: str, http_factory: ControlFactory = default_control_factory):
        self._tools_url = tools_url
        self._http_factory = http_factory

    def _call(self, path: str, body: dict, credential: str) -> tuple[int, dict]:
        """``(200 | 409, JSON object)`` for a route body; the caller checks which shape the status allows."""
        headers = {"authorization": f"Bearer {credential}"}
        for backoff in (*_BUSY_BACKOFF_SECONDS, None):
            try:
                with self._http_factory(headers) as http:
                    response = http.request("POST", url_on_origin(self._tools_url, path), json=body)
            except Exception as exc:
                raise ControlUnavailable(type(exc).__name__) from None
            try:
                payload = response.json()
            except ValueError:
                payload = None
            error = parse_error(payload) if response.status_code in (400, 401, 403, 409) else None
            if error and error[0] == "scheduler_busy" and response.status_code == 409:
                if backoff is None:
                    raise ControlUnavailable("scheduler_busy")
                time.sleep(backoff)
                continue
            if error:
                raise Refused(error[0])
            if response.status_code in (200, 409) and isinstance(payload, dict):
                return response.status_code, payload
            raise ControlUnavailable(f"status {response.status_code}")
        raise AssertionError("unreachable")

    def start(self, request: dict, credential: str) -> dict:
        """The permit of cloud-start-replay-permit.json; a 409 start refusal raises ``Refused(reason)``."""
        status, body = self._call(START_PATH, request, credential)
        parsed = parse_start_response(body)
        if parsed is None or parsed[0] != ("permit" if status == 200 else "refused"):
            raise ControlUnavailable("malformed start response")
        if parsed[0] == "refused":
            raise Refused(parsed[1])
        return parsed[1]

    def bootstrap(self, request: dict, credential: str) -> tuple[str, datetime]:
        """The per-task ``(dlc_, expiresAt)`` of bootstrap-response.json; a server retry revokes the earlier
        ``dlc_``."""
        status, body = self._call(BOOTSTRAP_PATH, request, credential)
        parsed = parse_bootstrap_response(body) if status == 200 else None
        if parsed is None:
            raise ControlUnavailable("malformed bootstrap response")
        return parsed

    def report(self, body: dict, credential: str) -> None:
        """Acknowledged when the server answers ``recorded`` true or false (a replay). ``currentOccupant``
        false is a superseded assignment's own stop fact: acknowledged, nothing else is done."""
        status, response = self._call(REPORT_PATH, body, credential)
        parsed = parse_report_response(response) if status == 200 else None
        if parsed is None:
            raise ControlUnavailable("malformed report response")
        if not parsed[1]:
            logger.info("artifactbridge pooled report %s/%s acknowledged for a superseded assignment",
                        body["kind"], body["reportSequence"])


# A dlc_ this close to its expiresAt is re-bootstrapped before use.
_REFRESH_MARGIN = timedelta(seconds=60)
Renew = Callable[[], tuple[str, datetime]]


@dataclass
class _Bound:
    credential: str = field(repr=False)
    binding: dict
    expires_at: datetime
    renew: Renew = field(repr=False)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


class CredentialBindings:
    """In-memory ``dlc_`` per (platform, A2A contextId, A2A task id); a lookup never falls back.

    Refresh is serialized per task (``_Bound.lock``): ``lookup`` re-bootstraps a credential within
    ``_REFRESH_MARGIN`` of its expiresAt, and ``refresh(..., stale)`` re-bootstraps once after a
    ``credential_expired`` denial of ``stale``. Neither starts a turn; a dropped task's binding stays dropped."""

    def __init__(self, now: Callable[[], datetime] | None = None):
        self._lock = threading.Lock()
        self._bound: dict[tuple[str, str, str], _Bound] = {}
        self._now = now or _utcnow

    def bind(self, platform: str, context_id: str, task_id: str, credential: str, binding: dict,
             expires_at: datetime, renew: Renew) -> None:
        with self._lock:
            self._bound[(platform, context_id, task_id)] = _Bound(credential, binding, expires_at, renew)

    def lookup(self, platform: str, context_id: str, task_id: str) -> str | None:
        key = (platform, context_id, task_id)
        with self._lock:
            bound = self._bound.get(key)
        if bound is None:
            return None
        if self._now() + _REFRESH_MARGIN >= bound.expires_at:
            return self._renew(key, bound, bound.credential) or (bound.credential if self._now() < bound.expires_at
                                                                  else None)
        return bound.credential

    def refresh(self, platform: str, context_id: str, task_id: str, stale: str) -> str | None:
        """The credential after one re-bootstrap of ``stale`` (or the one a concurrent refresh already got)."""
        key = (platform, context_id, task_id)
        with self._lock:
            bound = self._bound.get(key)
        return self._renew(key, bound, stale) if bound else None

    def _renew(self, key: tuple[str, str, str], bound: _Bound, stale: str) -> str | None:
        with bound.lock:
            if bound.credential != stale:
                return bound.credential
            try:
                credential, expires_at = bound.renew()
            except (Refused, ControlUnavailable) as exc:
                logger.warning("artifactbridge pooled credential refresh failed: %s", type(exc).__name__)
                return None
            with self._lock:
                if self._bound.get(key) is not bound:  # the task ended meanwhile: the new dlc_ is discarded
                    return None
                bound.credential, bound.expires_at = credential, expires_at
            return credential

    def drop(self, platform: str, context_id: str, task_id: str) -> None:
        with self._lock:
            self._bound.pop((platform, context_id, task_id), None)


CREDENTIALS = CredentialBindings()


@dataclass(frozen=True)
class Decision:
    """``spawn``: run the turn as ``task_id`` on ``context_id``. ``working``/``submitted``: the dispatch
    already runs (or is about to) as ``task_id``. ``terminal``: answer ``outcome``/``text`` and run nothing;
    ``task_id`` is the journaled turn's id, "" when no turn was ever permitted."""
    kind: str
    context_id: str
    task_id: str = ""
    outcome: str = ""
    text: str = ""


def _key(envelope: dict) -> tuple[str, str]:
    return envelope["binding"]["assignmentId"], envelope["dispatchId"]


def _fingerprint(credential: str) -> str:
    return hashlib.sha256(credential.encode()).hexdigest()


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
        # startJournalId -> the assignment's agx_, held in memory only; a restart forgets it until the server's
        # next Send/Get/List/Cancel for that journal presents it again.
        self._grants: dict[str, str] = {}
        self._delivery = threading.Lock()

    # ---- intercept ----
    def admit(self, metadata: object, context_id: str, task_id: str, *, message_id: object, grant: Grant) -> Decision:
        """``context_id`` is the request's A2A contextId ("" when absent); ``task_id`` is the A2A task id a
        new dispatch is journaled with (a known dispatch keeps its own); ``message_id`` is the request's
        ``message.messageId``, which must be exactly the envelope's dispatchId; ``grant`` is the request's
        ``(agx_, startJournalId)``. It never waits on a running turn: a retry answers what the journal holds."""
        try:
            envelope = envelope_from_metadata(metadata)
            binding = envelope["binding"]
            if binding["executorId"] != self.executor_id or binding["executorConnectionRevision"] != self.connection_revision:
                raise Refused("executor_mismatch")
            if grant[1] != start_journal_id(binding["assignmentId"]):
                raise Refused("journal_mismatch")
            if message_id != envelope["dispatchId"]:  # missing or altered: no row, no control call, no turn
                raise Refused("message_id_mismatch")
            with self.journal.tx() as tx:
                row = self._journal_dispatch(tx, envelope, context_id, grant, task_id)
            self._hold(grant)
        except Refused as refused:
            logger.info("artifactbridge pooled dispatch refused: %s", refused.code)
            return Decision("terminal", context_id, outcome=REJECTED, text=refusal_text(refused.code))
        decision = self._advance(row)
        try:  # earlier reports only after this dispatch's own start path (startBy-safe); never alters its decision
            self.deliver_reports()
        except Exception as exc:
            logger.warning("artifactbridge pooled report replay failed: %s", type(exc).__name__)
        return decision

    def recovery_row(self, grant: Grant) -> dict | None:
        """The journaled row a Get/List/Cancel's start headers name, or None (an unknown journal or another
        grant). It never inserts a row or starts anything; a matching grant is held again so the row's
        pending reports can be delivered."""
        with self.journal.tx() as tx:
            rows = tx.where("start_journal_id", grant[1])
        row = next((r for r in rows if r["grant_fingerprint"] == _fingerprint(grant[0])), None)
        if row is not None:
            self._hold(grant)
            try:
                self.deliver_reports()
            except Exception as exc:
                logger.warning("artifactbridge pooled report replay failed: %s", type(exc).__name__)
        return row

    def _hold(self, grant: Grant) -> None:
        with self._lock:
            self._grants[grant[1]] = grant[0]

    def _grant(self, start_journal: str) -> str | None:
        with self._lock:
            return self._grants.get(start_journal)

    @staticmethod
    def _journal_dispatch(tx: Tx, envelope: dict, context_id: str, grant: Grant, task_id: str) -> dict:
        if any(row["envelope"] != envelope for row in tx.where("dispatch_id", envelope["dispatchId"])):
            raise Refused("tuple_mismatch")  # the first journaled envelope (startBy included) is immutable
        row = tx.get(_key(envelope))
        if row is not None:
            if row["grant_fingerprint"] != _fingerprint(grant[0]) or row["start_journal_id"] != grant[1]:
                raise Refused("grant_mismatch")  # one grant per assignment: never rotated in place
            if context_id and context_id != row["context_id"]:
                raise Refused("context_mismatch")
            if row["task_id"] is None:  # a row journaled before task ids were fixed at insert
                tx.update(_key(envelope), from_states=(row["state"],), task_id=task_id)
                row = tx.get(_key(envelope))
            return row
        delegation_id, generation = envelope["binding"]["delegationId"], envelope["binding"]["executionGeneration"]
        if context_id and any(r["delegation_id"] != delegation_id for r in tx.where("context_id", context_id)):
            raise Refused("context_bound_to_other_delegation")
        # Initial tasks only: every continuation (a later generation, or a delegation already journaled) is
        # refused before any control call, never resumed or started in a replacement session.
        if generation > 1 or tx.where("delegation_id", delegation_id):
            raise Refused("continuation_unsupported")
        return tx.insert(envelope, grant[1], context_id or "ctx-" + uuid.uuid4().hex[:16], _fingerprint(grant[0]),
                         task_id, envelope["dispatchId"])  # admit checked message.messageId == dispatchId

    def _advance(self, row: dict) -> Decision:
        task_id = row["task_id"]
        if row["state"] in TERMINAL_STATES:
            return self._terminal(row)
        if row["state"] in ("permitted", "spawned", "running"):  # never a second turn
            return Decision("working", row["context_id"], task_id=row["task_id"] or "")
        envelope, key, journal_id = row["envelope"], _key(row["envelope"]), row["start_journal_id"]
        credential = self._grant(journal_id)
        start_by = parse_start_by(envelope["startBy"])
        if self._now() >= start_by:
            return self._refuse(key, "start_deadline_passed")
        with self.journal.tx() as tx:
            tx.update(key, from_states=("journaled",), state="start_requested")
        try:
            permit = self.control.start(start_request(envelope, journal_id), credential)
        except Refused as refused:
            return self._refuse(key, refused.code)
        except ControlUnavailable as exc:
            logger.warning("artifactbridge pooled start unavailable: %s", exc)
            # Not journaled as an outcome: the row stays start_requested, so a retry asks again with the same
            # startJournalId and answers the same task id.
            return Decision("terminal", row["context_id"], task_id, FAILED, refusal_text("start_unavailable"))
        if permit != {**envelope, "startJournalId": journal_id}:
            return self._refuse(key, "permit_mismatch")
        if self._now() >= start_by:  # a replayed permit never starts a turn after startBy
            return self._refuse(key, "start_deadline_passed")
        with self.journal.tx() as tx:
            claimed = tx.update(key, from_states=("journaled", "start_requested"), state="permitted")
            current = tx.get(key)
        if not claimed:  # a concurrent delivery owns this permit
            return self._advance(current)
        with self._lock:
            self._live[task_id] = key
        bootstrap = bootstrap_request(envelope, journal_id)
        try:
            credential_dlc, expires_at = self.control.bootstrap(bootstrap, credential)
        except ControlUnavailable as exc:
            logger.warning("artifactbridge pooled bootstrap unavailable: %s", exc)
            self.finish(task_id, FAILED, refusal_text("bootstrap_unavailable"))
            return Decision("terminal", row["context_id"], outcome=FAILED, text=refusal_text("bootstrap_unavailable"))
        except Refused as refused:
            refusal = refused.code
        else:  # bootstrap can outlast startBy (rechecked right before the spawn-intent CAS); the dlc_ is discarded
            refusal = "start_deadline_passed" if self._now() >= start_by else None
        if refusal:
            self._end(task_id, FAILED, "", kind="stopped", refusal=refusal)
            with self.journal.tx() as tx:
                return self._terminal(tx.get(key))
        with self.journal.tx() as tx:
            spawned = tx.update(key, from_states=("permitted",), state="spawned")
            current = tx.get(key)
        if not spawned:
            return self._terminal(current)
        self.credentials.bind(POOL_PLATFORM, row["context_id"], task_id, credential_dlc, envelope["binding"],
                              expires_at, lambda: self.control.bootstrap(bootstrap, self._grant(journal_id)))
        return Decision("spawn", row["context_id"], task_id=task_id)

    def _refuse(self, key: tuple[str, str], code: str) -> Decision:
        with self.journal.tx() as tx:
            tx.update(key, from_states=("journaled", "start_requested"), state="refused", refusal=code)
            row = tx.get(key)
        return self._advance(row)

    @staticmethod
    def _terminal(row: dict) -> Decision:
        task_id = row["task_id"] or ""
        if row["state"] == "finished":
            return Decision("terminal", row["context_id"], task_id, row["result_state"], row["result_text"] or "")
        if row["state"] == "refused":
            return Decision("terminal", row["context_id"], task_id, REJECTED, refusal_text(row["refusal"]))
        return Decision("terminal", row["context_id"], task_id, FAILED, _UNCERTAIN_TEXT)

    def projection(self, row: dict) -> Decision:
        """What a journaled row answers to Get/List/Cancel: ``submitted`` (spawn intent, not yet admitted by
        the runtime), ``working`` (admitted), or its terminal outcome."""
        if row["state"] in TERMINAL_STATES:
            return self._terminal(row)
        kind = "working" if row["state"] == "running" else "submitted"
        return Decision(kind, row["context_id"], task_id=row["task_id"] or "")

    # ---- turn lifecycle ----
    def started(self, task_id: str) -> bool:
        """Hermes admitted this task's turn (``on_processing_start``): spawned -> running, reported once."""
        with self._lock:
            key = self._live.get(task_id)
        if key is None:
            return False
        with self.journal.tx() as tx:
            running = tx.update(key, from_states=("spawned",), state="running")
            if running:
                tx.enqueue_report(key, "running", None)
        if running:
            self.deliver_reports()
        return running

    def finish(self, task_id: str, outcome: str, text: str) -> None:
        """Record a turn's result on its OWN row only; a row already canceled (or replaced) stays as it is
        and gets no further report."""
        self._end(task_id, outcome, text, kind="stopped")

    def cancel(self, task_id: str) -> bool:
        """A confirmed stop of a live pooled turn: cancel_acknowledged with its own binding; drops its
        credential. Called only once the runtime confirmed the cancellation."""
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
                ended = tx.update(key, from_states=("permitted", "spawned", "running"), state="finished",
                                  result_state=outcome, result_text=text)
            if ended:
                tx.enqueue_report(key, kind, _REPORT_OUTCOME[outcome])
            row = tx.get(key)
        self.credentials.drop(POOL_PLATFORM, row["context_id"], task_id)
        with self._lock:
            self._live.pop(task_id, None)
        if ended:
            self.deliver_reports()
        return ended

    def deliver_reports(self) -> None:
        """Send journaled reports outside any transaction, each row's oldest sequence first, at most
        ``_REPORTS_PER_PASS``, each with its assignment's agx_. A row whose grant this process does not hold
        (after a restart) keeps its reports until a Send/Get/List/Cancel presents the grant again. The pass
        stops at the first undeliverable report, so a later sequence never overtakes it and an unreachable
        server costs one request. A named refusal (or 401) is no acknowledgment: the report stays journaled,
        the refusal is recorded on its row (apart from the row's execution state), and that row's reports are
        never retried or overtaken."""
        with self._delivery:  # one pass at a time in this process: sequences leave in order
            with self.journal.tx() as tx:
                queued = [(_key(row["envelope"]), credential, body) for row in tx.with_pending_reports()
                          if (credential := self._grant(row["start_journal_id"]))
                          for body in json.loads(row["pending_reports"])][:_REPORTS_PER_PASS]
            for key, credential, body in queued:
                refusal = None
                try:
                    self.control.report(body, credential)
                except ControlUnavailable as exc:
                    logger.warning("artifactbridge pooled report %s/%s pending: %s", body["kind"],
                                   body["reportSequence"], exc)
                    return
                except Refused as refused:
                    refusal = refused.code
                    logger.warning("artifactbridge pooled report %s/%s refused: %s", body["kind"],
                                   body["reportSequence"], refusal)
                with self.journal.tx() as tx:
                    tx.ack_report(key, body["reportSequence"], refusal)

    def recover(self) -> None:
        """On adapter start: a spawned or running row without a live turn here lost its in-memory Hermes turn.
        It becomes ``uncertain`` (never respawned) and is reported stopped/crashed once its grant returns. A
        permit claimed but never spawned goes back to start_requested so a retry re-asks with the same
        startJournalId."""
        with self._lock:
            live = set(self._live)
        with self.journal.tx() as tx:
            for row in tx.where("state", "permitted"):
                if row["task_id"] not in live:
                    tx.update(_key(row["envelope"]), from_states=("permitted",), state="start_requested")
            for row in [*tx.where("state", "spawned"), *tx.where("state", "running")]:
                key = _key(row["envelope"])
                if row["task_id"] not in live and tx.update(key, from_states=("spawned", "running"), state="uncertain"):
                    tx.enqueue_report(key, "stopped", "crashed")
        self.deliver_reports()  # this process's crash reports and any left pending by the previous one

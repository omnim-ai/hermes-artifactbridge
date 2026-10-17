"""Durable pooled-dispatch journal: one stdlib sqlite3 file (WAL) under the active Hermes home.

One row per dispatch identity (assignmentId, dispatchId) holding the full envelope as first received,
the locally generated stable startJournalId, the state, the report sequence, and the A2A task/context
ids. States: journaled -> start_requested -> permitted -> spawned -> finished | refused | uncertain.
No secrets: the agx_ control credential and the dlc_ task credential never reach this file. Rows are
never deleted (a refused or withdrawn row keeps its context known).

Reports are durable: ``pending_reports`` holds the row's unacknowledged, content-free report bodies in
sequence order, appended in the same transaction that allocates the sequence and makes the lifecycle
transition. ``report_refusal`` records a report the server refused for good; it is not an execution state.

Every mutation runs in its own connection under ``BEGIN IMMEDIATE``, so two deliveries of the same
dispatch (threads or processes) serialize on the database write lock.
"""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .pool_protocol import report_body

TERMINAL_STATES = frozenset({"finished", "refused", "uncertain"})

_SCHEMA = """
CREATE TABLE IF NOT EXISTS dispatches (
    assignment_id TEXT NOT NULL,
    dispatch_id TEXT NOT NULL,
    delegation_id TEXT NOT NULL,
    envelope TEXT NOT NULL,
    start_journal_id TEXT NOT NULL,
    state TEXT NOT NULL,
    context_id TEXT NOT NULL,
    task_id TEXT,
    report_sequence INTEGER NOT NULL DEFAULT 0,
    result_state TEXT,
    result_text TEXT,
    refusal TEXT,
    updated_at REAL NOT NULL,
    pending_reports TEXT,
    report_refusal TEXT,
    PRIMARY KEY (assignment_id, dispatch_id)
);
CREATE INDEX IF NOT EXISTS dispatches_dispatch ON dispatches (dispatch_id);
CREATE INDEX IF NOT EXISTS dispatches_delegation ON dispatches (delegation_id);
CREATE INDEX IF NOT EXISTS dispatches_context ON dispatches (context_id);
"""
# Columns added after 0.2.0.dev0's first schema; an existing journal gains them on open.
_ADDED_COLUMNS = ("pending_reports", "report_refusal")


def _row(raw: sqlite3.Row | None) -> dict | None:
    if raw is None:
        return None
    row = dict(raw)
    row["envelope"] = json.loads(row["envelope"])
    return row


class Tx:
    """Queries inside one ``BEGIN IMMEDIATE`` transaction."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def get(self, key: tuple[str, str]) -> dict | None:
        return _row(self._conn.execute("SELECT * FROM dispatches WHERE assignment_id = ? AND dispatch_id = ?",
                                       key).fetchone())

    def where(self, column: str, value: str) -> list[dict]:
        assert column in ("dispatch_id", "delegation_id", "context_id", "state")
        return [_row(r) for r in self._conn.execute(f"SELECT * FROM dispatches WHERE {column} = ?", (value,))]

    def insert(self, envelope: dict, start_journal_id: str, context_id: str) -> dict:
        binding = envelope["binding"]
        self._conn.execute(
            "INSERT INTO dispatches (assignment_id, dispatch_id, delegation_id, envelope, start_journal_id, state,"
            " context_id, updated_at) VALUES (?, ?, ?, ?, ?, 'journaled', ?, ?)",
            (binding["assignmentId"], envelope["dispatchId"], binding["delegationId"],
             json.dumps(envelope, sort_keys=True, separators=(",", ":")), start_journal_id, context_id, time.time()))
        return self.get((binding["assignmentId"], envelope["dispatchId"]))

    def update(self, key: tuple[str, str], *, from_states: tuple[str, ...], **fields: object) -> bool:
        """Compare-and-set: apply ``fields`` only while the row is in one of ``from_states``."""
        assignments = ", ".join(f"{name} = ?" for name in fields)
        marks = ", ".join("?" for _ in from_states)
        cursor = self._conn.execute(
            f"UPDATE dispatches SET {assignments}, updated_at = ? WHERE assignment_id = ? AND dispatch_id = ?"
            f" AND state IN ({marks})", (*fields.values(), time.time(), *key, *from_states))
        return cursor.rowcount == 1

    def enqueue_report(self, key: tuple[str, str], kind: str, outcome: str | None) -> None:
        """Allocate the row's next report sequence and append that report to its pending reports."""
        self._conn.execute("UPDATE dispatches SET report_sequence = report_sequence + 1 WHERE assignment_id = ?"
                           " AND dispatch_id = ?", key)
        row = self.get(key)
        body = report_body(row["envelope"]["binding"], row["report_sequence"], kind, outcome)
        self._set_pending(key, json.loads(row["pending_reports"] or "[]") + [body])

    def with_pending_reports(self) -> list[dict]:
        return [_row(r) for r in self._conn.execute(
            "SELECT * FROM dispatches WHERE pending_reports IS NOT NULL ORDER BY updated_at")]

    def ack_report(self, key: tuple[str, str], sequence: int, refusal: str | None = None) -> None:
        """Drop the acknowledged (or permanently refused) report ``sequence``; record a refusal apart."""
        row = self.get(key)
        bodies = json.loads(row["pending_reports"] or "[]")
        self._set_pending(key, [b for b in bodies if b["reportSequence"] != sequence])
        if refusal:
            self._conn.execute("UPDATE dispatches SET report_refusal = ? WHERE assignment_id = ? AND dispatch_id = ?",
                               (refusal, *key))

    def _set_pending(self, key: tuple[str, str], bodies: list[dict]) -> None:
        self._conn.execute("UPDATE dispatches SET pending_reports = ? WHERE assignment_id = ? AND dispatch_id = ?",
                           (json.dumps(bodies, separators=(",", ":")) if bodies else None, *key))


class Journal:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(_SCHEMA)
            conn.execute("BEGIN IMMEDIATE")  # two processes opening an old journal add each column once
            existing = {column[1] for column in conn.execute("PRAGMA table_info(dispatches)")}
            for column in _ADDED_COLUMNS:
                if column not in existing:
                    conn.execute(f"ALTER TABLE dispatches ADD COLUMN {column} TEXT")
            conn.execute("COMMIT")
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def tx(self) -> Iterator[Tx]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield Tx(conn)
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        finally:
            conn.close()

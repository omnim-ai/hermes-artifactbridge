"""A pooled task as an ArtifactBridge agent run in a factory container (AI-6656; agent run spec v4 §3, §4, §7.4).

With the task's ``dlc_`` the plugin reads the brief (``artifactbridge_read_room_context {}``) and ``tools/list`` at
the configured tools URL, writes request.json v1 (an owner-only 0600 file, deleted once ``start`` returns) and runs
``omp-run start --agent <serviceId> --request <file>``; ``omp-run wait --unit <unit>`` gives the outcome and
``omp-run stop <unit>`` ends the run on an A2A cancel. A task that cannot start fails closed: never a local Hermes
turn. Model and thinking are the factory's. The request content, the brief and the ``dlc_`` are never logged.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import tempfile
import threading
from dataclasses import dataclass

from .mcp_client import HttpFactory, call_scoped_tool, default_http_factory, list_scoped_tools
from .pool_core import CANCELED, COMPLETED, FAILED, QUESTION_MAX, clarification

logger = logging.getLogger(__name__)

DEFAULT_OMP_RUN = "omp-run"
ROOM_CONTEXT_TOOL = "artifactbridge_read_room_context"
_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")  # omp-run's refusal and error codes
_UNIT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")  # never an option, never a path
ROOM_MESSAGE_MAX = 20_000


@dataclass(frozen=True)
class Outcome:
    """A finished run: ``state`` is a pool_core outcome; ``text`` the result, error code or room message;
    ``terminal`` the cloudTerminal of a question; ``timed_out``: the run's stop is unknown."""
    state: str
    text: str
    terminal: dict | None = None
    timed_out: bool = False


def _failed(code: str, **kwargs) -> Outcome:
    return Outcome(FAILED, code, **kwargs)


def _answer(stdout: str | None) -> dict:
    """omp-run's one JSON line (the last line of stdout), or {}."""
    lines = (stdout or "").strip().splitlines()
    try:
        answer = json.loads(lines[-1]) if lines else None
    except ValueError:
        answer = None
    return answer if isinstance(answer, dict) else {}


def _code(value: object, default: str) -> str:
    return value if isinstance(value, str) and _CODE_RE.match(value) else default


def request_body(envelope: dict, brief: dict, tools: list) -> dict:
    """request.json v1 (spec §4.2): exactly these twelve keys; ``brief`` and ``tools`` unchanged."""
    binding = envelope["binding"]
    return {"omp_agent_request": 1, "kind": "room_task", "workspace_id": binding["workspaceId"],
            "agent_id": binding["serviceId"], "delegation_id": binding["delegationId"],
            "execution_generation": binding["executionGeneration"], "assignment_id": binding["assignmentId"],
            "dispatch_id": envelope["dispatchId"], "start_by": envelope["startBy"], "brief": brief, "tools": tools,
            "conversation": None}


def _question_text(question: object) -> str:
    """A string question unchanged; a structured one (AI-6423) by its non-empty ``prompt`` or ``question`` string,
    else as sorted JSON cut to the clarification limit; anything else is no question."""
    if isinstance(question, str):
        return question
    if not isinstance(question, dict):
        return ""
    for key in ("prompt", "question"):
        if isinstance(question.get(key), str) and question[key].strip():
            return question[key]
    return json.dumps(question, sort_keys=True)[:QUESTION_MAX]


def wait_outcome(answer: dict) -> Outcome:
    """The task outcome of an ``omp-run wait`` line (spec §4.4): ``completed`` with its result_text; a
    ``question`` fails with cloudTerminal clarification_unsupported (decision D4); everything else fails with
    its error code (``timeout`` when the wait gave up while the run was still active)."""
    outcome = answer.get("outcome")
    if outcome == "completed" and isinstance(answer.get("result_text"), str) and answer["result_text"]:
        return Outcome(COMPLETED, answer["result_text"])
    if outcome == "question":
        text, terminal = clarification(_question_text(answer.get("question")))
        return Outcome(FAILED, text, terminal)
    if outcome == "failed":
        return _failed(_code(answer.get("error_code"), "turn_failed"))
    if answer.get("timeout") is True:
        return _failed("timeout", timed_out=True)
    if outcome == "completed":  # no result text to report
        return _failed("omp_run_wait_failed")
    return _failed(_code(outcome or answer.get("result"), "omp_run_wait_failed"))


class AgentRuns:
    """``omp-run`` at ``binary`` (a name on PATH or a path) for pooled tasks; one instance per listener."""

    def __init__(self, *, tools_url: str, binary: str = DEFAULT_OMP_RUN,
                 http_factory: HttpFactory = default_http_factory):
        self.tools_url = tools_url
        self.binary = binary
        self._http_factory = http_factory
        self._lock = threading.Lock()
        self._waits: dict[str, subprocess.Popen] = {}  # unit -> its running `omp-run wait`
        self._stopped: set[str] = set()  # units whose `omp-run stop` succeeded

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run([self.binary, *args], stdin=subprocess.DEVNULL, capture_output=True, text=True,
                              check=False)

    async def _read(self, credential: str) -> tuple[dict | None, list | None]:
        headers = {"authorization": f"Bearer {credential}"}
        result = await call_scoped_tool(self.tools_url, headers, ROOM_CONTEXT_TOOL, {}, preserve=True,
                                        http_factory=self._http_factory)
        content = result.get("content")
        brief = None
        if result.get("isError") is False and isinstance(content, list) and len(content) == 1 \
                and content[0].get("type") == "text":
            try:
                brief = json.loads(content[0]["text"])
            except ValueError:
                pass
        if not isinstance(brief, dict):
            return None, None
        return brief, await list_scoped_tools(self.tools_url, headers, http_factory=self._http_factory)

    def start(self, envelope: dict, credential: str) -> str | Outcome:
        """The started unit (or the live unit of an ``already_running`` refusal), else the task's failed Outcome;
        a token-service refusal fails with its room message and starts nothing."""
        try:
            brief, tools = asyncio.run(self._read(credential))
        except Exception as exc:
            logger.warning("artifactbridge agent run: room context or tool list unavailable: %s", type(exc).__name__)
            return _failed("room_context_unavailable")
        if brief is None:
            logger.warning("artifactbridge agent run: the room context read was refused or malformed")
            return _failed("room_context_unavailable")
        request = request_body(envelope, brief, tools)
        fd, path = tempfile.mkstemp(prefix="artifactbridge-agent-request-", suffix=".json")
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(request, handle, ensure_ascii=False)
            done = self._run("start", "--agent", request["agent_id"], "--request", path)
        except OSError as exc:  # a missing or unrunnable binary: fail closed
            logger.warning("artifactbridge agent run: %s could not run: %s", self.binary, type(exc).__name__)
            return _failed("omp_run_unavailable")
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
        answer, unit = _answer(done.stdout), None
        if done.returncode == 0 and answer.get("ok") is True:
            unit = answer.get("unit")
        elif done.returncode == 2 and answer.get("code") == "already_running":
            unit = answer.get("unit")  # one live run per delegation: use it
        if isinstance(unit, str) and _UNIT_RE.match(unit):
            return unit
        message = answer.get("room_message")
        # runner_credential_refused: the token service refused; nothing was started.
        if done.returncode == 2 and answer.get("code") == "runner_credential_refused" and isinstance(message, str) \
                and 1 <= len(message) <= ROOM_MESSAGE_MAX:
            logger.warning("artifactbridge agent run refused by the token service: %s",
                           _code(answer.get("status"), "unknown"))
            return _failed(message)
        code = _code(answer.get("code"), "omp_run_start_failed")
        logger.warning("artifactbridge agent run not started: exit %s, %s", done.returncode, code)
        return _failed(code)

    def wait(self, unit: str) -> Outcome:
        """The run's outcome; ``CANCELED`` once ``stop`` stopped it."""
        with self._lock:
            if unit in self._stopped:
                self._stopped.discard(unit)
                return Outcome(CANCELED, "")
            try:
                waiting = self._waits[unit] = subprocess.Popen(
                    [self.binary, "wait", "--unit", unit], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL, text=True)
            except OSError as exc:
                logger.warning("artifactbridge agent run: %s could not run: %s", self.binary, type(exc).__name__)
                return _failed("omp_run_unavailable")
        stdout, _ = waiting.communicate()
        with self._lock:
            self._waits.pop(unit, None)
            if unit in self._stopped:
                self._stopped.discard(unit)
                return Outcome(CANCELED, "")
        return wait_outcome(_answer(stdout))

    def stop(self, unit: str) -> bool:
        """``omp-run stop <unit>``; True when it exited 0, which also ends this unit's wait as canceled."""
        try:
            stopped = self._run("stop", unit).returncode == 0
        except OSError as exc:
            logger.warning("artifactbridge agent run: %s could not run: %s", self.binary, type(exc).__name__)
            return False
        if stopped:
            with self._lock:
                self._stopped.add(unit)
                waiting = self._waits.get(unit)
            if waiting is not None:
                waiting.terminate()
        return stopped

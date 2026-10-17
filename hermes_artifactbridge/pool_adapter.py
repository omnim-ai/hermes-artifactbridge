"""The ``artifactbridge-pool`` listener: a thin subclass of Hermes' stock A2A adapter.

Its own request handler requires the remote bearer on every request (card included), serves only the
pooled JSON-RPC methods (SendMessage, GetTask, ListTasks, CancelTask and ``artifactbridge/pool/capability``)
and checks the start headers before anything else. Every Send goes through ``PoolCore.admit`` before any
MessageEvent exists; any refusal ends the A2A task immediately and the model is never invoked (no stock
fallthrough). Get/List/Cancel project the durable journal and never create a row or start a turn. The stock
``a2a`` platform is untouched. Hermes is imported lazily inside ``build_adapter_class`` so the plugin
imports without Hermes, and ``hermes_shape_supported`` refuses registration on an untested Hermes.
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
import threading
import time

from .pool_core import CANCELED, COMPLETED, FAILED, POOL_PLATFORM, REJECTED, Decision, clarification, refusal_text
from .pool_protocol import CAPABILITY_METHOD, METADATA_KEY, TERMINAL_KEY, Refused, capability_answer, pool_grant

logger = logging.getLogger(__name__)

TESTED_HERMES = ">=0.22.2,<0.23"
DEFAULT_PORT = 9901
# A Send waits this long for Hermes to admit the spawned turn; a turn not yet admitted answers Submitted.
_ADMISSION_WAIT = 5.0
# A CancelTask waits this long (after Hermes' own bounded session cancel) for the stop to be recorded.
_CANCEL_CONFIRM_WAIT = 10.0
# The stock A2AAdapter / BasePlatformAdapter members this shim overrides or calls, with the parameters it
# relies on, and the stock request handler members PoolRequestHandler reuses.
_STOCK_SHAPE = {
    "_end_task": ["self", "rec", "state", "text", "stored_reply"],
    "_finalize_task": ["self", "pending", "state", "reply"], "_await_reply": ["self", "pending", "keepalive"],
    "_record_outcome": ["self", "task_id", "context_id", "peer", "state", "reply", "started"],
    "send": ["self", "chat_id", "content", "reply_to", "metadata"], "connect": ["self", "_kwargs"],
    "_add_pending": ["self", "task_id", "context_id"], "_pop_pending": ["self", "task_id"],
    "_resolve_task": ["self", "task_id", "state", "text"], "_route_for_request": ["self", "raw_path", "params"],
    "_register_inline_push": ["self", "task_id", "params", "agent"], "_scope_for_agent": ["self", "agent"],
    "on_processing_start": ["self", "event"], "_event_session_key": ["self", "event"],
    "cancel_session_processing": ["self", "session_key", "release_guard", "discard_pending"],
    "_set_fatal_error": ["self", "code", "message", "retryable"],
}
_HANDLER_SHAPE = {"_json": ["self", "code", "payload"], "_error": ["self", "http_code", "req_id", "code", "message"],
                  "_client_ip": ["self"], "do_GET": ["self"]}


def hermes_shape_supported() -> bool:
    """True only on the Hermes range and A2A adapter shape this shim was tested against."""
    try:
        from hermes_cli.plugins_manifest import running_hermes_version, version_satisfies
        from plugins.platforms.a2a.adapter import _MAX_BODY, A2AAdapter, A2ARequestHandler, _err, _ok  # noqa: F401
        from plugins.platforms.a2a.protocol import TaskStore
        if not version_satisfies(TESTED_HERMES, running_hermes_version()):
            return False
        return (all(list(inspect.signature(getattr(A2AAdapter, name)).parameters) == params
                    for name, params in _STOCK_SHAPE.items())
                and all(list(inspect.signature(getattr(A2ARequestHandler, name)).parameters) == params
                        for name, params in _HANDLER_SHAPE.items())
                and "message_id" in inspect.signature(A2AAdapter.build_source).parameters
                and all(callable(getattr(TaskStore, m, None)) for m in ("create", "get", "complete", "set_state")))
    except Exception:
        return False


def build_adapter_class():
    from gateway.config import Platform
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent, MessageType
    from plugins.platforms.a2a import protocol, security
    from plugins.platforms.a2a.adapter import _MAX_BODY, A2AAdapter, A2ARequestHandler, _err, _ok

    a2a_state = {COMPLETED: protocol.STATE_COMPLETED, FAILED: protocol.STATE_FAILED, CANCELED: protocol.STATE_CANCELED,
                 REJECTED: protocol.STATE_REJECTED}
    core_outcome = {protocol.STATE_COMPLETED: COMPLETED, protocol.STATE_CANCELED: CANCELED}
    methods = {"SendMessage": "_pool_send", "GetTask": "_pool_get", "ListTasks": "_pool_list",
               "CancelTask": "_pool_cancel", CAPABILITY_METHOD: "_pool_capability"}

    class PoolRequestHandler(A2ARequestHandler):
        """The pooled listener's only handler: the bearer on every request (the card included), the pooled
        JSON-RPC methods only, and the start headers checked before any task, event or control call."""

        def _identity(self):
            authorization = self.headers.get_all("Authorization") or []  # exactly one; a repeat is refused
            if len(authorization) != 1:
                return None
            return self.adapter._security_context.authenticate(authorization[0], self._client_ip())

        def do_GET(self):
            if self._identity() is None:
                return self._error(401, None, protocol.ERR_UNAUTHORIZED, "unauthorized")
            return super().do_GET()

        def do_POST(self):
            adapter = self.adapter
            identity = self._identity()
            if identity is None:
                return self._error(401, None, protocol.ERR_UNAUTHORIZED, "unauthorized")
            lengths = self.headers.get_all("Content-Length") or ["0"]
            if len(lengths) != 1 or not lengths[0].isascii() or not lengths[0].isdigit():  # never a negative read
                return self._error(400, None, protocol.ERR_PARSE, "parse error")
            try:
                length = int(lengths[0])
                if length > _MAX_BODY:
                    return self._error(413, None, protocol.ERR_PARSE, "payload too large")
                req = json.loads((self.rfile.read(length) if length else b"{}").decode("utf-8"))
            except Exception:
                return self._error(400, None, protocol.ERR_PARSE, "parse error")
            if not isinstance(req, dict):
                return self._error(400, None, protocol.ERR_INVALID_PARAMS, "JSON-RPC request must be an object")
            req_id, method = req.get("id"), str(req.get("method", ""))
            params = req["params"] if req.get("params") is not None else {}
            version = (self.headers.get("A2A-Version") or "").strip()
            route = adapter._route_for_request(self.path, params) if isinstance(params, dict) else {}
            handler_name = methods.get(method, "")
            checks = (  # the stock order; the pooled method set; then the start headers
                (lambda: not isinstance(params, dict), 200, protocol.ERR_INVALID_PARAMS, "params must be an object"),
                (lambda: version and version not in {"1.0", "1.0.0"}, 200, protocol.ERR_INVALID_PARAMS,
                 f"unsupported A2A-Version: {version}"),
                (lambda: route.get("error") or route["agent"].get("slug"), 400, protocol.ERR_INVALID_PARAMS,
                 "unsupported route"),
                (lambda: not adapter._rate_limiter.allow(identity), 429, protocol.ERR_RATE_LIMITED, "rate limit exceeded"),
                (lambda: not adapter._security_context.is_trusted_peer(identity), 403, protocol.ERR_UNTRUSTED_PEER,
                 f"peer '{identity}' not trusted"),
                (lambda: not handler_name, 200, protocol.ERR_METHOD_NOT_FOUND, "Method not found"),
            )
            for failed, http, code, msg in checks:
                if failed():
                    return self._error(http, req_id, code, msg)
            if handler_name == "_pool_capability":
                return self._json(200, adapter._pool_capability(req_id, params))
            try:
                grant = pool_grant(self.headers.get_all)
            except Refused as refused:
                return self._error(200, req_id, protocol.ERR_INVALID_PARAMS, refusal_text(refused.code))
            self._json(200, getattr(adapter, handler_name)(req_id, params, identity, grant))

    class PoolAdapter(A2AAdapter):
        def __init__(self, config, *, core, port: int):
            super().__init__(config)
            self.platform = Platform(POOL_PLATFORM)
            self.port = port
            self._core = core
            self._events: dict[str, object] = {}  # A2A task id -> its MessageEvent, for a session cancel
            self._admitted: dict[str, threading.Event] = {}  # set once Hermes admitted the task's turn
            self._finalizers: dict[str, threading.Thread] = {}  # waits on the task's reply, records its outcome
            self._cancel_requested: set[str] = set()

        @property
        def name(self) -> str:
            return "ArtifactBridge pool"

        # The stock connect() builds its ThreadingHTTPServer with the stock handler; the class is swapped here,
        # on assignment, before serve_forever starts, so no request ever reaches the stock handler.
        @property
        def _httpd(self):
            return self.__dict__.get("_pool_httpd")

        @_httpd.setter
        def _httpd(self, server):
            if server is not None:
                server.RequestHandlerClass = PoolRequestHandler
            self.__dict__["_pool_httpd"] = server

        async def connect(self, **kwargs) -> bool:
            if self._security_context.localhost_only():  # pooled cloud requires the remote bearer
                logger.error("artifactbridge-pool: A2A_BEARER_TOKEN or A2A_PEER_TOKENS is required; not serving")
                self._set_fatal_error("bearer_required", "artifactbridge-pool requires a remote bearer",
                                      retryable=False)
                return False
            await asyncio.to_thread(self._core.recover)  # turns lost to a restart become uncertain
            return await super().connect(**kwargs)

        # ---- JSON-RPC methods (PoolRequestHandler) ----
        def _pool_capability(self, req_id, params):
            from . import VERSION
            try:
                return _ok(req_id, capability_answer(params, executor_id=self._core.executor_id,
                                                     connection_revision=self._core.connection_revision,
                                                     runtime_version=VERSION))
            except Refused as refused:
                return _err(req_id, protocol.ERR_INVALID_PARAMS, refusal_text(refused.code))

        def _pool_send(self, req_id, params, peer, grant):
            task = self._prepare_pool_task(params, peer, grant)
            return _ok(req_id, protocol.send_message_response(task))

        def _pool_get(self, req_id, params, peer, grant):
            task_id = str(params.get("id") or "")
            row = self._core.recovery_row(grant)
            if row is None or row["task_id"] != task_id:
                return _err(req_id, protocol.ERR_TASK_NOT_FOUND, f"task not found: {task_id}")
            return _ok(req_id, self._project(row))

        def _pool_list(self, req_id, params, peer, grant):
            row = self._core.recovery_row(grant)
            tasks = [self._project(row)] if row and row["task_id"] and row["context_id"] == params.get("contextId") else []
            page_size = params.get("pageSize") if type(params.get("pageSize")) is int else 50
            return _ok(req_id, {"tasks": tasks, "nextPageToken": "", "pageSize": max(1, min(page_size, 100)),
                                "totalSize": len(tasks)})

        def _pool_cancel(self, req_id, params, peer, grant):
            """Canceled only on a confirmed stop of this task's own turn; otherwise the task stays live with
            ``artifactbridge.cancel: pending`` and keeps its occupancy."""
            task_id = str(params.get("id") or "")
            row = self._core.recovery_row(grant)
            if row is None or row["task_id"] != task_id:
                return _err(req_id, protocol.ERR_TASK_NOT_FOUND, f"task not found: {task_id}")
            view = self._core.projection(row)
            if view.kind == "terminal" and view.outcome != CANCELED:
                return _err(req_id, protocol.ERR_TASK_NOT_CANCELABLE, f"task {task_id} already {a2a_state[view.outcome]}")
            event = self._events.get(task_id)
            if view.kind == "working" and event is not None and self._loop is not None:
                self._cancel_requested.add(task_id)
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.cancel_session_processing(self._event_session_key(event)), self._loop
                    ).result(_CANCEL_CONFIRM_WAIT)
                except Exception as exc:
                    logger.warning("artifactbridge pooled cancel did not complete: %s", type(exc).__name__)
                finalizer = self._finalizers.get(task_id)
                if finalizer is not None:  # a confirmed stop resolves the future; its finalizer records it
                    finalizer.join(_CANCEL_CONFIRM_WAIT)
                row = self._core.recovery_row(grant) or row
            task = self._project(row)
            if task["status"]["state"] != protocol.STATE_CANCELED:
                task["metadata"]["artifactbridge.cancel"] = "pending"
            return _ok(req_id, task)

        # ---- dispatch ----
        def _project(self, row) -> dict:
            view = self._core.projection(row)
            state = {"submitted": protocol.STATE_SUBMITTED, "working": protocol.STATE_WORKING}.get(
                view.kind) or a2a_state[view.outcome]
            task = self._with_assignment(protocol.build_task(row["task_id"], row["context_id"], state, view.text), row)
            if view.terminal:
                task["metadata"][TERMINAL_KEY] = view.terminal
            return task

        @staticmethod
        def _with_assignment(task: dict, row) -> dict:
            """The nonsecret ``artifactbridge.assignment`` task metadata the server matches tasks by, and the
            (possibly empty) artifacts list cloud-send-response.json carries."""
            task.setdefault("artifacts", [])
            task["metadata"] = {METADATA_KEY: {**row["envelope"], "startJournalId": row["start_journal_id"]}}
            return task

        def _prepare_pool_task(self, params, peer, grant) -> dict:
            context_id = protocol.extract_context_id(params)
            task_id = protocol.new_task_id()
            if not protocol.extract_text(params):
                decision = Decision("terminal", context_id, outcome=REJECTED, text=refusal_text("empty_task"))
            else:
                try:
                    message = params.get("message")
                    decision = self._core.admit(params.get("metadata"), context_id, task_id, grant=grant,
                                                message_id=message.get("messageId") if isinstance(message, dict)
                                                else None)
                except Exception as exc:  # fail closed: never the stock path
                    logger.warning("artifactbridge pooled intercept failed: %s", type(exc).__name__)
                    decision = Decision("terminal", context_id, outcome=FAILED, text=refusal_text("intercept_failed"))
            if decision.kind == "spawn":
                self._dispatch(params, peer, decision.context_id, decision.task_id)
            row = self._core.recovery_row(grant) if decision.task_id else None
            if row is not None and row["task_id"] == decision.task_id:
                if decision.kind != "terminal" or self._core.projection(row).kind == "terminal":
                    return self._project(row)
                task_id = decision.task_id  # an unanswered start: this answer only; the row stays retryable
            if decision.kind != "terminal":  # a live row the grant no longer names: never a guessed task
                decision = Decision("terminal", context_id, outcome=FAILED, text=refusal_text("intercept_failed"))
            rec = self.tasks.create(task_id, decision.context_id or protocol.new_context_id(), peer,
                                    *self._scope_for_agent(None))
            task = self._end_task(rec, a2a_state[decision.outcome], decision.text, stored_reply=decision.text)[0]
            if row is not None and row["task_id"] == task_id:
                return self._with_assignment(task, row)
            if decision.terminal:  # a continuation: rejected before any row, control call or turn
                task["metadata"] = {TERMINAL_KEY: decision.terminal}
            return task

        def _dispatch(self, params, peer, context_id, task_id) -> None:
            """The stock local dispatch (anti-loop, framing, audit, persistence, push) under the permitted ids.
            The task is Submitted until Hermes admits the turn (``on_processing_start``); this waits up to
            ``_ADMISSION_WAIT`` for that, and a finalizer thread records the outcome."""
            text = protocol.extract_text(params)
            rec = self.tasks.create(task_id, context_id, peer, *self._scope_for_agent(None))
            turn, max_turns = self._turns.track(context_id), protocol.max_pingpong_turns()
            if turn > max_turns:  # no stock metric increments: rebinding a Hermes module attribute fails validate
                return self._fail(rec, f"Anti-loop protection: context {context_id} exceeded {max_turns} turns.")
            if self._loop is None or self._message_handler is None:
                return self._fail(rec, "Agent gateway not ready to accept A2A tasks.")
            framed = security.wrap_inbound(peer, text)
            security.audit("inbound", peer, task_id, text)
            protocol.persist_message(context_id, "user", text, task_id)
            self._register_inline_push(task_id, params, agent=None)
            fut = self._add_pending(task_id, context_id)
            event = MessageEvent(text=framed, message_type=MessageType.TEXT, message_id=task_id,
                                 source=self.build_source(chat_id=context_id, chat_name=f"a2a:{peer}", chat_type="dm",
                                                          user_id=peer, user_name=peer, message_id=task_id))
            admitted = self._admitted[task_id] = threading.Event()
            self._events[task_id] = event
            try:
                asyncio.run_coroutine_threadsafe(self.handle_message(event), self._loop)
            except Exception as e:
                self._pop_pending(task_id)
                return self._fail(rec, security.redact_outbound(f"Dispatch failed: {e}"))
            pending = {"task_id": task_id, "context_id": context_id, "peer": peer, "future": fut,
                       "created_iso": rec["created_iso"], "started": time.time()}
            finalizer = self._finalizers[task_id] = threading.Thread(
                target=lambda: self._finalize_task(pending, *self._await_reply(pending)),
                name=f"artifactbridge-pool-{task_id}", daemon=True)
            finalizer.start()
            admitted.wait(_ADMISSION_WAIT)

        def _fail(self, rec, text: str) -> None:
            self._core.finish(rec["task_id"], FAILED, text)
            self._end_task(rec, protocol.STATE_FAILED, text, stored_reply=text)

        async def on_processing_start(self, event) -> None:
            task_id = str(getattr(event, "message_id", "") or "")
            if await asyncio.to_thread(self._core.started, task_id):
                self.tasks.set_state(task_id, protocol.STATE_WORKING)
            admitted = self._admitted.get(task_id)
            if admitted is not None:
                admitted.set()
            await super().on_processing_start(event)

        def _finalize_task(self, pending, state, reply):
            """The journal first: the row's result, cloudTerminal and report commit in one transaction before
            the stock finalizer's volatile TaskStore, persistence, audit and push, so Get/List after a crash or
            a lost response answer from the journal. A clarification cannot be continued (AI-4105): the task
            fails with ``artifactbridge.cloudTerminal`` and the server, not this runner, posts the question."""
            task_id = pending["task_id"]
            timed_out = not pending["future"].done()  # the stock wait gave up: the turn's stop is unknown
            reply = security.redact_outbound(reply or "")
            stripped, terminal = reply.lstrip(), None
            if state == protocol.STATE_COMPLETED and stripped.upper().startswith(protocol.INPUT_REQUIRED_MARKER):
                state = protocol.STATE_FAILED
                reply, terminal = clarification(stripped[len(protocol.INPUT_REQUIRED_MARKER):])
            try:  # only this task's own row; a replaced row is left untouched and unreported
                if state == protocol.STATE_CANCELED and task_id in self._cancel_requested:
                    self._core.cancel(task_id)
                else:
                    self._core.finish(task_id, core_outcome.get(state, FAILED), reply, terminal=terminal,
                                      timed_out=timed_out)
            finally:
                state, reply = super()._finalize_task(pending, state, reply)
                self._cancel_requested.discard(task_id)
                for live in (self._events, self._admitted, self._finalizers):
                    live.pop(task_id, None)
            return state, reply

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            """The final reply resolves exactly the task it answers (``reply_to`` is the A2A task id), so a
            canceled turn's late reply never completes a newer task queued on the same context."""
            rec = self.tasks.get(str(reply_to or ""))
            if (metadata or {}).get("notify") and rec and rec["context_id"] == chat_id:
                self._resolve_task(rec["task_id"], protocol.STATE_COMPLETED, content or "")
                return SendResult(success=True, message_id=str(int(time.time() * 1000)))
            return await super().send(chat_id, content, reply_to=reply_to, metadata=metadata)

    return PoolAdapter

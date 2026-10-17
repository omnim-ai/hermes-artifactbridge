"""The ``artifactbridge-pool`` listener: a thin subclass of Hermes' stock A2A adapter.

Every request on THIS listener goes through ``PoolCore.admit`` before any MessageEvent exists; any
refusal ends the A2A task immediately and the model is never invoked (no stock fallthrough). The stock
``a2a`` platform is untouched. Hermes is imported lazily inside ``build_adapter_class`` so the plugin
imports without Hermes, and ``hermes_shape_supported`` refuses registration on an untested Hermes.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import time

from .pool_core import CANCELED, COMPLETED, FAILED, INPUT_REQUIRED, POOL_PLATFORM, REJECTED, Decision, refusal_text

logger = logging.getLogger(__name__)

TESTED_HERMES = ">=0.22.2,<0.23"
DEFAULT_PORT = 9901
# The stock A2AAdapter members this shim overrides or calls, with the parameters it relies on.
_STOCK_SHAPE = {
    "_prepare_task": ["self", "params", "peer", "agent"], "_end_task": ["self", "rec", "state", "text", "stored_reply"],
    "_finalize_task": ["self", "pending", "state", "reply"], "_rpc_tasks_cancel": ["self", "req_id", "params", "agent"],
    "_record_outcome": ["self", "task_id", "context_id", "peer", "state", "reply", "started"],
    "send": ["self", "chat_id", "content", "reply_to", "metadata"], "connect": ["self", "_kwargs"],
    "_find_task": ["self", "req_id", "params", "agent"], "_add_pending": ["self", "task_id", "context_id"],
    "_pop_pending": ["self", "task_id"], "_resolve_task": ["self", "task_id", "state", "text"],
    "_register_inline_push": ["self", "task_id", "params", "agent"], "_scope_for_agent": ["self", "agent"],
}


def hermes_shape_supported() -> bool:
    """True only on the Hermes range and A2A adapter shape this shim was tested against."""
    try:
        from hermes_cli.plugins_manifest import running_hermes_version, version_satisfies
        from plugins.platforms.a2a.adapter import A2AAdapter
        from plugins.platforms.a2a.protocol import TaskStore
        if not version_satisfies(TESTED_HERMES, running_hermes_version()):
            return False
        return (all(list(inspect.signature(getattr(A2AAdapter, name)).parameters) == params
                    for name, params in _STOCK_SHAPE.items())
                and "message_id" in inspect.signature(A2AAdapter.build_source).parameters
                and all(callable(getattr(TaskStore, m, None)) for m in ("create", "get", "complete", "set_state", "to_task")))
    except Exception:
        return False


def build_adapter_class():
    from gateway.config import Platform
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent, MessageType
    from plugins.platforms.a2a import protocol, security
    from plugins.platforms.a2a.adapter import A2AAdapter, _reply_timeout

    a2a_state = {COMPLETED: protocol.STATE_COMPLETED, INPUT_REQUIRED: protocol.STATE_INPUT_REQUIRED,
                 FAILED: protocol.STATE_FAILED, CANCELED: protocol.STATE_CANCELED, REJECTED: protocol.STATE_REJECTED}
    core_outcome = {protocol.STATE_COMPLETED: COMPLETED, protocol.STATE_INPUT_REQUIRED: INPUT_REQUIRED,
                    protocol.STATE_CANCELED: CANCELED}

    class PoolAdapter(A2AAdapter):
        def __init__(self, config, *, core, port: int):
            super().__init__(config)
            self.platform = Platform(POOL_PLATFORM)
            self.port = port
            self._core = core

        @property
        def name(self) -> str:
            return "ArtifactBridge pool"

        async def connect(self, **kwargs) -> bool:
            await asyncio.to_thread(self._core.recover)  # turns lost to a restart become uncertain
            return await super().connect(**kwargs)

        def _prepare_task(self, params, peer, agent=None):
            agent = agent or self._agents[""]
            context_id = protocol.extract_context_id(params)
            task_id = protocol.new_task_id()
            if agent.get("slug"):  # no profile forwarding on the pooled listener
                decision = Decision("terminal", context_id, outcome=REJECTED, text=refusal_text("unsupported_route"))
            elif not protocol.extract_text(params):
                decision = Decision("terminal", context_id, outcome=REJECTED, text=refusal_text("empty_task"))
            else:
                try:
                    decision = self._core.admit(params.get("metadata"), context_id, task_id, wait=_reply_timeout())
                except Exception as exc:  # fail closed: never the stock path
                    logger.warning("artifactbridge pooled intercept failed: %s", type(exc).__name__)
                    decision = Decision("terminal", context_id, outcome=FAILED, text=refusal_text("intercept_failed"))
            if decision.kind == "spawn":
                return self._dispatch(params, peer, agent, decision.context_id, task_id)
            if decision.kind == "working":
                rec = self.tasks.get(decision.task_id)
                return (protocol.TaskStore.to_task(rec) if rec else
                        protocol.build_task(decision.task_id, decision.context_id, protocol.STATE_WORKING)), None
            rec = self.tasks.create(task_id, decision.context_id or protocol.new_context_id(), peer,
                                    *self._scope_for_agent(agent))
            return self._end_task(rec, a2a_state[decision.outcome], decision.text, stored_reply=decision.text)

        def _dispatch(self, params, peer, agent, context_id, task_id):
            """The stock local dispatch (framing, audit, persistence, push), under the permitted ids."""
            text = protocol.extract_text(params)
            rec = self.tasks.create(task_id, context_id, peer, *self._scope_for_agent(agent))
            if self._loop is None or self._message_handler is None:
                self._core.finish(task_id, FAILED, "")
                return self._end_task(rec, protocol.STATE_FAILED, "Agent gateway not ready to accept A2A tasks.")
            framed = security.wrap_inbound(peer, text)
            security.audit("inbound", peer, task_id, text)
            protocol.persist_message(context_id, "user", text, task_id)
            # No stock inbound_total increment: rebinding a Hermes module attribute fails `hermes plugins validate`.
            self._register_inline_push(task_id, params, agent=agent)
            fut = self._add_pending(task_id, context_id)
            event = MessageEvent(text=framed, message_type=MessageType.TEXT, message_id=task_id,
                                 source=self.build_source(chat_id=context_id, chat_name=f"a2a:{peer}", chat_type="dm",
                                                          user_id=peer, user_name=peer, message_id=task_id))
            try:
                asyncio.run_coroutine_threadsafe(self.handle_message(event), self._loop)
            except Exception as e:
                msg = security.redact_outbound(f"Dispatch failed: {e}")
                self._core.finish(task_id, FAILED, msg)
                try:
                    return self._end_task(rec, protocol.STATE_FAILED, msg, stored_reply=msg)
                finally:
                    self._pop_pending(task_id)
            self.tasks.set_state(task_id, protocol.STATE_WORKING)
            return None, {"task_id": task_id, "context_id": context_id, "peer": peer, "future": fut,
                          "created_iso": rec["created_iso"], "started": time.time()}

        def _finalize_task(self, pending, state, reply):
            state, reply = super()._finalize_task(pending, state, reply)
            # Only this task's own row; a canceled or replaced row is left untouched and unreported.
            self._core.finish(pending["task_id"], core_outcome.get(state, FAILED), reply)
            return state, reply

        def _rpc_tasks_cancel(self, req_id, params, agent=None):
            task_id, rec, _error = self._find_task(req_id, params, agent)
            if rec and rec["state"] not in protocol.TERMINAL_STATES:
                self._core.cancel(task_id)  # before the stock cancel wakes the waiting request
            return super()._rpc_tasks_cancel(req_id, params, agent=agent)

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            """The final reply resolves exactly the task it answers (``reply_to`` is the A2A task id), so a
            canceled turn's late reply never completes a newer task queued on the same context."""
            rec = self.tasks.get(str(reply_to or ""))
            if (metadata or {}).get("notify") and rec and rec["context_id"] == chat_id:
                self._resolve_task(rec["task_id"], protocol.STATE_COMPLETED, content or "")
                return SendResult(success=True, message_id=str(int(time.time() * 1000)))
            return await super().send(chat_id, content, reply_to=reply_to, metadata=metadata)

    return PoolAdapter

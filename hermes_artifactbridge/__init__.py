"""hermes-artifactbridge: delegation-scoped ArtifactBridge tools for Hermes Agent.

``register(ctx)`` is the Hermes plugin entry point. It registers the static
scoped tools and the namespaced skill. Each tool takes the opaque delegation
token from the task brief and forwards it, with the service credential, to the
configured ArtifactBridge tools URL only. No Hermes hooks are registered.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from . import pool_adapter
from .coordinator import (COORDINATOR_TOOL_SCHEMAS, COORDINATOR_TOOLSET, CREDENTIAL_PREFIX, SEARCH_TOOL,
                          CoordinatorReads, confined_search_handler, confined_search_schema)
from .endpoint import is_trusted_url
from .pool_core import CREDENTIALS, POOL_PLATFORM, PoolControl, PoolCore
from .pool_journal import Journal
from .pool_protocol import is_bigint_string, is_uuid
from .schemas import POOL_TOOL_SCHEMAS, POOL_TOOLSET, TOOL_SCHEMAS, TOOLSET
from .tools import PooledTools, ScopedTools

logger = logging.getLogger(__name__)

VERSION = "0.2.0.dev0"
POOL_ENABLED_ENV = "ARTIFACTBRIDGE_POOL_ENABLED"
POOL_EXECUTOR_ID_ENV = "ARTIFACTBRIDGE_POOL_EXECUTOR_ID"
POOL_REVISION_ENV = "ARTIFACTBRIDGE_POOL_CONNECTION_REVISION"
POOL_PORT_ENV = "ARTIFACTBRIDGE_POOL_PORT"
POOL_REQUIRED_ENV = (POOL_ENABLED_ENV, POOL_EXECUTOR_ID_ENV, POOL_REVISION_ENV)
SERVICE_CREDENTIAL_ENV = "ARTIFACTBRIDGE_SERVICE_CREDENTIAL"
TOOLS_URL_ENV = "ARTIFACTBRIDGE_TOOLS_URL"
COORDINATOR_CREDENTIAL_ENV = "ARTIFACTBRIDGE_COORDINATOR_READ_CREDENTIAL"
ROLE_SETTING = "role"  # plugins.entries.hermes-artifactbridge.settings.role: delegation (default) | coordinator
DEFAULT_TOOLS_URL = "https://app.artifactbridge.com/mcp/agent-gateway"
SKILL_PATH = Path(__file__).resolve().parent.parent / "skills" / "delegation" / "SKILL.md"

PLUGIN: ScopedTools | None = None


def _env(name: str, default: str | None = None) -> str | None:
    """Read a setting from the ACTIVE Hermes profile.

    Under a multiplexed gateway (one process serving several profiles) ``os.environ`` holds the
    default profile's ``.env``, not the serving profile's. Hermes' ``get_secret`` resolves the
    per-turn profile scope; outside a scope it fails closed while multiplexing, which reads as
    "not configured" here. Plain ``os.environ`` remains the fallback for Hermes builds without it.
    """
    try:
        from agent.secret_scope import get_secret
    except ImportError:
        return os.environ.get(name, default)
    try:
        return get_secret(name, default)
    except Exception:  # UnscopedSecretError: no profile scope while multiplexing -> fail closed
        return None


def _service_credential() -> str:
    value = _env(SERVICE_CREDENTIAL_ENV, "") or ""
    if not value:
        raise RuntimeError(f"{SERVICE_CREDENTIAL_ENV} is not set")
    return value


def _tools_url() -> str:
    """Default only when unset; an explicit empty or invalid override must fail closed."""
    value = _env(TOOLS_URL_ENV, DEFAULT_TOOLS_URL)
    return DEFAULT_TOOLS_URL if value is None else value


def _configured() -> bool:
    return bool(_env(SERVICE_CREDENTIAL_ENV)) and is_trusted_url(_tools_url())


def build_plugin(*, tools_url: str, **kwargs) -> ScopedTools:
    return ScopedTools(tools_url=tools_url, service_credential=_service_credential, **kwargs)


def _unconfigured_handler(name: str):
    async def handler(args: dict, **kwargs) -> str:
        return json.dumps({"error": "not_configured", "tool": name,
                           "detail": f"{TOOLS_URL_ENV} must be an https URL (or http on loopback)"})
    handler.__name__ = name
    return handler


def _pool_setting(name: str) -> str:
    return (_env(name, "") or "").strip()


def _pool_configured() -> bool:
    """Pooled execution is opt-in and off by default: its server routes are not merged or deployed yet (README).
    The listener also needs the stock remote bearer (``A2A_BEARER_TOKEN`` / ``A2A_PEER_TOKENS``); it refuses to
    serve without one. Each assignment's ``agx_`` arrives with its own requests: there is no executor-wide one."""
    return (_pool_setting(POOL_ENABLED_ENV) == "1"
            and is_uuid(_pool_setting(POOL_EXECUTOR_ID_ENV)) and is_bigint_string(_pool_setting(POOL_REVISION_ENV))
            and is_trusted_url(_tools_url()))


def _journal_path() -> Path:
    try:
        from hermes_constants import get_hermes_home
    except ImportError:
        raise RuntimeError("pooled mode needs the Hermes home for its journal") from None
    return Path(get_hermes_home()) / "artifactbridge" / "pool-journal.sqlite3"


def _pool_port(config) -> int:
    value = _pool_setting(POOL_PORT_ENV) or str((getattr(config, "extra", None) or {}).get("port") or "")
    return int(value) if value else pool_adapter.DEFAULT_PORT


def _pool_adapter_factory(config):
    """Runs inside the serving profile's scope: settings and the journal come from that profile."""
    core = PoolCore(journal=Journal(_journal_path()),
                    control=PoolControl(tools_url=_tools_url()),
                    executor_id=_pool_setting(POOL_EXECUTOR_ID_ENV), connection_revision=_pool_setting(POOL_REVISION_ENV))
    return pool_adapter.build_adapter_class()(config, core=core, port=_pool_port(config))


def _register_pool(ctx) -> None:
    if not pool_adapter.hermes_shape_supported():
        logger.warning("artifactbridge pooled mode is configured but this Hermes is outside the tested range %s; "
                       "the %s platform is not registered", pool_adapter.TESTED_HERMES, POOL_PLATFORM)
        return
    from plugins.platforms.a2a import _PLATFORM_HINT
    ctx.register_platform(
        name=POOL_PLATFORM, label="ArtifactBridge pool", adapter_factory=_pool_adapter_factory,
        check_fn=pool_adapter.hermes_shape_supported, validate_config=lambda config: _pool_configured(),
        is_connected=lambda config: _pool_configured(), required_env=list(POOL_REQUIRED_ENV),
        install_hint="Set ARTIFACTBRIDGE_POOL_ENABLED=1 and the pooled executor settings",
        allow_update_command=False, platform_hint=_PLATFORM_HINT,
    )
    pooled = PooledTools(tools_url=_tools_url(), credentials=CREDENTIALS)
    for schema in POOL_TOOL_SCHEMAS:
        ctx.register_tool(
            name=schema["name"], toolset=POOL_TOOLSET, schema=schema, handler=pooled.handler_for(schema["name"]),
            is_async=True, description=schema["description"], check_fn=_pool_configured,
            requires_env=list(POOL_REQUIRED_ENV),
        )


def _coordinator_credential() -> str:
    return _env(COORDINATOR_CREDENTIAL_ENV, "") or ""


def _coordinator_configured() -> bool:
    return _coordinator_credential().startswith(CREDENTIAL_PREFIX) and is_trusted_url(_tools_url())


def _stock_session_search():
    """Hermes' own process-global ``session_search`` entry (public ``tools.registry``), or None."""
    try:
        from tools.registry import registry
    except ImportError:
        return None
    entry = registry.snapshot_registration(SEARCH_TOOL)
    return entry if entry is not None and entry.toolset == SEARCH_TOOL else None


def _register_coordinator(ctx) -> None:
    """Exactly the confined ``session_search`` override and the three reads: no delegation, pool or skill.
    Without the operator's ``tools.override`` grant, or without Hermes' stock search to confine, nothing
    registers (the stock search would stay unconfined beside the reads)."""
    has_capability = getattr(ctx, "has_capability", None)
    if not (has_capability and has_capability("tools.override")):
        logger.error("artifactbridge coordinator role needs the tools.override capability to confine "
                     "session_search; nothing registered")
        return
    stock = _stock_session_search()
    if stock is None:
        logger.error("artifactbridge coordinator role found no stock session_search to confine; nothing registered")
        return
    schema = confined_search_schema(stock.schema)
    ctx.register_tool(name=SEARCH_TOOL, toolset=stock.toolset, schema=schema,
                      handler=confined_search_handler(stock.handler), check_fn=stock.check_fn, is_async=False,
                      description=schema["description"], emoji=stock.emoji, override=True)
    tools_url = _tools_url()
    reads = CoordinatorReads(tools_url=tools_url, credential=_coordinator_credential) if is_trusted_url(tools_url) else None
    for schema in COORDINATOR_TOOL_SCHEMAS:
        name = schema["name"]
        ctx.register_tool(
            name=name, toolset=COORDINATOR_TOOLSET, schema=schema,
            handler=reads.handler_for(name) if reads else _unconfigured_handler(name),
            is_async=True, description=schema["description"], check_fn=_coordinator_configured,
            requires_env=[COORDINATOR_CREDENTIAL_ENV],
        )
    logger.info("artifactbridge coordinator role: registered confined %s and %s (%d tools)",
                SEARCH_TOOL, COORDINATOR_TOOLSET, len(COORDINATOR_TOOL_SCHEMAS))


def register(ctx) -> None:
    """Register the eleven static tools unconditionally (the stock ``requires_env``/``check_fn`` gate hides
    them until a credential and trusted tools URL are available) and the skill. With an untrusted override the
    handlers refuse with ``not_configured`` and no client is built. Only when pooled mode is configured, also
    the ``artifactbridge-pool`` listener and its token-free tools.

    A profile whose plugin setting ``role`` is ``coordinator`` gets the coordinator surface instead (see
    ``_register_coordinator``); any other value registers nothing."""
    global PLUGIN
    get_config = getattr(ctx, "get_config", None)  # absent on Hermes builds without plugin settings
    role = get_config(ROLE_SETTING, "delegation") if get_config else "delegation"
    if role == "coordinator":
        _register_coordinator(ctx)
        return
    if role != "delegation":
        logger.error("artifactbridge: unknown role %r; nothing registered", role)
        return
    tools_url = _tools_url()
    PLUGIN = build_plugin(tools_url=tools_url) if is_trusted_url(tools_url) else None
    for schema in TOOL_SCHEMAS:
        name = schema["name"]
        ctx.register_tool(
            name=name, toolset=TOOLSET, schema=schema,
            handler=PLUGIN.handler_for(name) if PLUGIN else _unconfigured_handler(name),
            is_async=True, description=schema["description"], check_fn=_configured,
            requires_env=[SERVICE_CREDENTIAL_ENV],
        )
    ctx.register_skill("delegation", SKILL_PATH,
                       description="How to execute an ArtifactBridge delegation with the scoped tools.")
    if _pool_configured():
        _register_pool(ctx)

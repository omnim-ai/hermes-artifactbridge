"""hermes-artifactbridge: delegation-scoped ArtifactBridge tools for Hermes Agent.

``register(ctx)`` is the Hermes plugin entry point. It registers the static
scoped tools and the namespaced skill. Each tool takes the opaque delegation
token from the task brief and forwards it, with the service credential, to the
configured ArtifactBridge tools URL only. No Hermes hooks are registered.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from .endpoint import is_trusted_url
from .schemas import TOOL_SCHEMAS, TOOLSET
from .tools import ScopedTools

SERVICE_CREDENTIAL_ENV = "ARTIFACTBRIDGE_SERVICE_CREDENTIAL"
TOOLS_URL_ENV = "ARTIFACTBRIDGE_TOOLS_URL"
SKILL_PATH = Path(__file__).resolve().parent.parent / "skills" / "artifactbridge-delegation" / "SKILL.md"

PLUGIN: ScopedTools | None = None


def _service_credential() -> str:
    value = os.environ.get(SERVICE_CREDENTIAL_ENV, "")
    if not value:
        raise RuntimeError(f"{SERVICE_CREDENTIAL_ENV} is not set")
    return value


def _configured() -> bool:
    return bool(os.environ.get(SERVICE_CREDENTIAL_ENV)) and is_trusted_url(os.environ.get(TOOLS_URL_ENV, ""))


def build_plugin(*, tools_url: str, **kwargs) -> ScopedTools:
    return ScopedTools(tools_url=tools_url, service_credential=_service_credential, **kwargs)


def _unconfigured_handler(name: str):
    async def handler(args: dict, **kwargs) -> str:
        return json.dumps({"error": "not_configured", "tool": name,
                           "detail": f"{TOOLS_URL_ENV} must be an https URL (or http on loopback)"})
    handler.__name__ = name
    return handler


def register(ctx) -> None:
    """Register the nine static tools unconditionally (the stock ``requires_env``/``check_fn`` gate hides
    them until both variables are set) and the skill. With an untrusted or missing tools URL the
    handlers refuse with ``not_configured`` and no client is built."""
    global PLUGIN
    tools_url = os.environ.get(TOOLS_URL_ENV, "")
    PLUGIN = build_plugin(tools_url=tools_url) if is_trusted_url(tools_url) else None
    for schema in TOOL_SCHEMAS:
        name = schema["name"]
        ctx.register_tool(
            name=name, toolset=TOOLSET, schema=schema,
            handler=PLUGIN.handler_for(name) if PLUGIN else _unconfigured_handler(name),
            is_async=True, description=schema["description"], check_fn=_configured,
            requires_env=[SERVICE_CREDENTIAL_ENV, TOOLS_URL_ENV],
        )
    ctx.register_skill("delegation", SKILL_PATH,
                       description="How to execute an ArtifactBridge delegation with the scoped tools.")

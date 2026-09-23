"""The one ArtifactBridge endpoint this runtime talks to.

The tools URL comes from the production default or operator configuration, never from
task text. Trust rule (the one ArtifactBridge applies to runtime endpoints, see
THIRD_PARTY.md): https anywhere, http only on loopback, no userinfo, no fragment. The renewal URL is derived from that same origin.
"""
from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

RENEW_PATH = "/api/agent-gateway/delegation-token/renew"
_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def is_trusted_url(value: str) -> bool:
    try:
        url = urlsplit(value)
    except ValueError:
        return False
    if url.username or url.password or url.fragment or not url.hostname:
        return False
    if url.scheme == "https":
        return True
    return url.scheme == "http" and url.hostname in _LOOPBACK_HOSTS


def renew_url_for(tools_url: str) -> str:
    """Same scheme and host as the configured tools URL, fixed renewal path."""
    url = urlsplit(tools_url)
    return urlunsplit((url.scheme, url.netloc, RENEW_PATH, "", ""))

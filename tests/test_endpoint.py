import pytest

from hermes_artifactbridge.endpoint import RENEW_PATH, is_trusted_url, renew_url_for


@pytest.mark.parametrize("url", [
    "https://app.artifactbridge.com/mcp/agent-gateway", "https://ab.test:8443/mcp/agent-gateway",
    "http://127.0.0.1:3000/mcp/agent-gateway", "http://localhost/mcp/agent-gateway", "http://[::1]:3000/mcp",
])
def test_trusted_urls(url):
    assert is_trusted_url(url)


@pytest.mark.parametrize("url", [
    "", "http://app.artifactbridge.com/mcp/agent-gateway", "https://user:pw@ab.test/mcp", "https://ab.test/mcp#frag",
    "ftp://ab.test/mcp", "https:///mcp", "not a url", "http://[::1", "http://127.0.0.1.evil.test/mcp",
])
def test_untrusted_urls(url):
    assert not is_trusted_url(url)


def test_renew_url_is_derived_from_the_configured_origin_only():
    assert renew_url_for("https://ab.test:8443/mcp/agent-gateway?x=1") == "https://ab.test:8443" + RENEW_PATH
    assert renew_url_for("http://127.0.0.1:3000/anything") == "http://127.0.0.1:3000" + RENEW_PATH

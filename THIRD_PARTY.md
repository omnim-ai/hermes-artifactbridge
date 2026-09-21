# Third-party notices

This plugin is original work by Omnim, licensed under MIT (see LICENSE).

## ArtifactBridge

ArtifactBridge is Omnim's proprietary product; its source is not open and
nothing in this repository licenses it. Omnim has authorized the publication,
under this repository's MIT license, of exactly two kinds of ArtifactBridge
material:

1. **The Agent Gateway scoped-tool interface as this plugin uses it:** the nine
   `artifactbridge_delegation_*` tool names, their argument names and bounds,
   the `dlt_` delegation-token shape, the `Authorization` and
   `X-ArtifactBridge-Delegation-Token` headers, the token renewal path, the
   boundary error-code vocabulary, and the trusted-endpoint URL rule.
2. **Descriptions of that interface and of the delegation flow** as written in
   this repository (tool descriptions, the skill, the README). They are this
   repository's own wording.

`tests/fake_ab.py` is an original stand-in that reproduces the observable
behaviour of that interface for the tests here. No ArtifactBridge source code,
test fixture, image, document or internal report is included, and this
authorization covers nothing beyond the items above.

## Dependencies

- `mcp` (MIT) and `httpx2` (BSD-3-Clause), from PyPI, installed by Hermes
  into its own environment.
- Hermes Agent is neither vendored nor modified; the plugin uses its documented
  plugin API only.

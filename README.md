# hermes-artifactbridge

ArtifactBridge delegation plugin for [Hermes Agent](https://hermes-agent.nousresearch.com).
During an ArtifactBridge A2A delegation, your existing Hermes agent gets
delegation-scoped ArtifactBridge tools. The task brief carries an opaque
delegation token; the model passes it to the tools, and the runtime forwards it
together with the service credential to ArtifactBridge, which decides every
call. No personal ArtifactBridge login; contributions are attributed to the
shared service; the final A2A answer is the one Room result.

Status: proof of concept, not released. Tested with Hermes Agent 0.21.3 (a
source checkout at that version), mcp 2.0.0 and httpx2 2.7.0, against a pinned
ArtifactBridge backend on a loopback journey (dispatch, input-required resume,
cancel, reply timeout). No other Hermes version has been tested. The public CI
runs only the self-contained tests below.

## What it does

- Registers nine static tools in toolset `artifactbridge-delegation`, mirroring
  ArtifactBridge's scoped surface: `artifactbridge_delegation_read_brief`,
  `_read_room`, `_read_document`, `_list_documents`, `_search_documents`,
  `_contribute`, `_upload_image`, `_create_document`, `_propose_change`.
  Every tool requires `delegation_token` (`dlt_` + 43 base64url characters).
  `delegation_id` and `room_id` are never model arguments: the token binds the
  delegation and ArtifactBridge defaults the Room from it.
- Sends each call as one short-lived streamable-HTTP MCP request to exactly the
  configured tools URL with `Authorization: Bearer <service credential>` and
  `X-ArtifactBridge-Delegation-Token: <token>`. No process-wide header, no
  personal MCP connection, no redirects, no fallback when scoped access is refused.
- On `403 credential_expired` from the boundary it POSTs the renewal endpoint
  on the same origin (`/api/agent-gateway/delegation-token/renew`, same headers,
  no body) and retries the call once. The boundary refuses before any tool
  runs, so that retry cannot duplicate a write. Every other 401/403 is final for
  the token and is returned as `{"error": code, "status": n}`.
- Writes that ArtifactBridge deduplicates (`contribute`, `create_document`,
  `propose_change`) take an optional model-supplied `idempotency_key`, scoped
  server-side to the delegation and generation. The plugin never retries an
  ambiguous write and never derives a key from content, so intentionally
  repeated operations stay distinct.
- Ships the namespaced skill `hermes-artifactbridge:delegation`.
- Registers no Hermes hooks and patches nothing in Hermes.

## Operator setup

1. **Serve delegations from a profile with no personal ArtifactBridge access.**
   Do not configure the personal `artifact-bridge` MCP server in that profile.
   This proof of concept does not exclude personal tools per execution; keeping
   them out of the serving profile is the operator's precondition.
2. **Enrol the Hermes agent as a service in ArtifactBridge** (Settings > Agent
   Sharing). Save the service credential in the serving profile's environment:

   ```
   ARTIFACTBRIDGE_SERVICE_CREDENTIAL=agw_...          # runtime-only; never in chat or logs
   ```

   Put it in the serving profile's own `.env` (`<HERMES_HOME>/profiles/<name>/.env`).
   Under a multiplexed gateway (one Hermes process serving several profiles) the
   plugin reads the credential and `ARTIFACTBRIDGE_TOOLS_URL` from that profile's
   secret scope for each turn, never from the default profile's environment; a
   profile without its own credential shows the tools as not configured.

   The **service credential** is held by the runtime only: it never enters
   tool arguments, results, logs or exceptions. The **delegation token**
   (`dlt_...`) is different: ArtifactBridge puts it in the task text of each
   delegation, so the model sees it, and it lands in the Hermes session store
   and A2A audit files. On its own it carries no authority (ArtifactBridge also
   requires the enrolled service's credential and checks delegation state,
   generation and scope), but anyone holding both can use that delegation's
   scope until it is revoked, expires or the delegation moves on.
3. **Production URL by default.** When `ARTIFACTBRIDGE_TOOLS_URL` is unset, the
   plugin uses `https://app.artifactbridge.com/mcp/agent-gateway`. Set it explicitly
   for a different deployment. The override must be https (plain `http://` only for
   `127.0.0.1`, `localhost` and `::1`, for self-hosted development), without
   userinfo or fragment. The token is only ever sent to this origin; redirects
   are never followed; any URL found in task text is ignored. An explicit empty
   or untrusted override hides the tools through `check_fn`; a direct call
   refuses with `not_configured` and never falls back to production. The service
   credential remains required through Hermes' `requires_env` gate.
4. **Expose the agent over A2A** with Hermes' bundled A2A platform, and set
   `A2A_REPLY_TIMEOUT` for the longest delegation you intend to serve (see
   Known limitations). The plugin never changes this setting.
5. Optional: with Hermes Tool Search at its default, plugin tools are offered
   behind `tool_search`/`tool_call`. Set `tools.tool_search.enabled: "off"` in
   the serving profile to offer the nine tools directly.

Removing the plugin does not revoke enrollment: rotate or disable the service
in ArtifactBridge separately.

## Installation

Hermes installs directory plugins from a Git URL or `owner/repo` shorthand
(`hermes plugins install --help`). Pin to a full 40-character commit SHA from
this repository:

```
hermes plugins install omnim-ai/hermes-artifactbridge --ref <40-character commit SHA> --no-enable
hermes plugins list
hermes plugins enable hermes-artifactbridge     # prompts for the service credential, saves it to .env
hermes plugins doctor hermes-artifactbridge --ci
```

A local checkout placed at `<HERMES_HOME>/plugins/hermes-artifactbridge` is
discovered the same way (`hermes plugins list`, then `enable`).

- **Dependencies.** `pyproject.toml` beside `plugin.yaml` declares `mcp>=2.0,<3`
  and `httpx2>=2.7,<3`; Hermes installs them into its own venv on install or
  enable, under Hermes' own pins, and re-applies them after `hermes update`.
  Exercised with mcp 2.0.0 and httpx2 2.7.0. `--no-deps` skips this.
- **Version gate.** `requires_hermes: ">=0.21.3"` is a load gate only: that is
  the version this plugin was exercised against, not a validated release.
- **Update.** For an existing unpinned install, run
  `hermes plugins update hermes-artifactbridge`. If disabled, also run
  `hermes plugins enable hermes-artifactbridge`. A `--ref` install is pinned;
  `hermes plugins update` refuses to move it. Move the pin with
  `hermes plugins install <source> --force --ref <new 40-character SHA>`.
- **Uninstall.** `hermes plugins remove hermes-artifactbridge` (alias
  `uninstall`) deletes the install tree. The plugin writes nothing under
  `HERMES_HOME` of its own; any saved env vars stay in `.env` until you remove them.

## Troubleshooting

- **Already installed.** Hermes' installer refuses an existing install before
  this plugin runs, so the plugin cannot prevent that error. Use
  `hermes plugins update hermes-artifactbridge` for an unpinned install instead
  of installing again, then `hermes plugins enable hermes-artifactbridge` if
  disabled. For a pinned install, use the explicit `--force --ref` command above.
- **Tools hidden or `not_configured`.** Check that the service credential is
  set. If `ARTIFACTBRIDGE_TOOLS_URL` is present but empty or invalid, remove the
  variable to use production, or replace it with a trusted deployment URL.
  An empty override does not mean “use the default.” Restart the serving Hermes
  process after changing its environment.

## Known limitations

- **A2A reply timeout is Hermes', not the plugin's.** Stock Hermes serves
  `message/send` as a blocking call and answers FAILED after `A2A_REPLY_TIMEOUT`
  (default 300 s) while the agent turn keeps running. ArtifactBridge then marks
  the delegation failed and revokes its token, so the still-running turn's next
  scoped call is refused with `credential_revoked`. **Token renewal does not
  extend this timeout**: renewal only extends the ArtifactBridge access window
  of a token whose delegation is still active. The operator sets
  `A2A_REPLY_TIMEOUT` on the serving profile; the plugin changes no settings and
  gives no guarantee of unlimited execution.
- **Cancel does not stop the agent.** A cancel is confirmed through Hermes'
  `CancelTask`, and ArtifactBridge revokes the token at the request; the Hermes
  turn may run on, but every further scoped call is refused.
- **Hermes restart loses in-flight tasks.** Hermes' A2A task store is in
  memory; a restart mid-delegation leaves ArtifactBridge to reconcile by
  context and fail or time out the delegation. Not exercised.
- **Resume creates a new Hermes task.** After an `[INPUT_REQUIRED]` answer,
  ArtifactBridge resumes in a new generation with a new token; Hermes assigns a
  fresh task id in the same context. Handled by ArtifactBridge; noted here.
- **Token visibility** as described under Operator setup; Hermes' egress
  redaction does not cover `dlt_` tokens, and nothing strips a token the model
  echoes into a contribution. The skill forbids echoing it.
- The plugin does not prevent an unrestricted operator profile from reading
  host secrets or making direct network calls. ArtifactBridge remains
  authoritative: scope, live delegation state, human approval of proposals and
  the one final Room result are enforced server-side.

## Development

```
uv sync --group dev
.venv/bin/python -m pytest
```

The suite runs against an in-process fake ArtifactBridge
(`tests/fake_ab.py`) that reproduces the service-bearer-plus-token boundary,
its denial codes and the renewal route.

The plugin was also exercised end to end against ArtifactBridge's own backend
and a stock Hermes gateway on a loopback journey (dispatch to completion, an
input-required question answered in the Room and resumed in a new generation,
a cancel while the turn runs, and the reply-timeout behaviour above). Those
integration suites depend on private ArtifactBridge test material and are not
part of this repository.

Stock checks that run without private material:

```
hermes plugins doctor /path/to/hermes-artifactbridge --ci
hermes plugins validate /path/to/hermes-artifactbridge
```

## License

MIT. THIRD_PARTY.md lists exactly what in this repository derives from
ArtifactBridge and what still needs Omnim's publication decision.

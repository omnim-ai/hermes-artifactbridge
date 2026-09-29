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
cancel, reply timeout). HTML support also passes plugin doctor/validate on
Hermes 0.21.5+4533.g39faafb; no A2A journey was run on that version. The public
CI runs only the self-contained tests below.

## What it does

- Registers nine static tools in toolset `artifactbridge-delegation`, mirroring
  ArtifactBridge's scoped surface: `artifactbridge_delegation_read_brief`,
  `_read_room`, `_read_document`, `_list_documents`, `_search_documents`,
  `_contribute`, `_upload_image`, `_create_document`, `_propose_change`.
  Every tool requires `delegation_token` (`dlt_` + 43 base64url characters).
  `delegation_id` is never a model argument. Only `create_document` accepts
  `room_id`, to opt into an HTML Room artifact. The backend checks that it is
  the delegation's Room; the plugin strips `room_id` from all other tools.
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

## HTML Room artifacts and Claude Code handoff

`artifactbridge_delegation_create_document` accepts:

- `title` and `content_md` (required). Despite its name, `content_md` carries
  the complete HTML source when `format` is `html`.
- `format`: `markdown` (default) or `html`.
- `room_id`: the Room UUID returned by `read_brief`. With `format: "html"`,
  this creates unfiled Room context and attaches its exact version. Omit it
  for a Library document in the authorized destination folder, for either format.
- `idempotency_key`: a stable key for this one logical write. Keep the key and
  payload unchanged when retrying; use a new key for an intentional new artifact.

HTML may contain at most 10,000,000 UTF-8 bytes. Markdown retains its
200,000-character limit. HTML must be nonempty, well-formed UTF-8 text without
NUL bytes. The plugin preserves content as supplied, including CRLF, Unicode,
inline styles and scripts. It does not sanitize, trim, wrap or normalize HTML.

The schema follows the backend's flat schema: content limits are described,
not expressed as one `maxLength` or provider-specific conditional schemas.
`maxLength` cannot enforce a UTF-8 byte cap. The backend applies format-specific
validation and same-Room authorization; errors such as `validation_error`,
`room_out_of_scope` and `destination_not_allowed` pass through unchanged.
An invalid request is never retried as Markdown or as a Library document.

### Claude writes; Hermes publishes

1. Hermes reads the delegation brief with the scoped plugin. Keep the token in
   Hermes; do not put it in Claude's prompt or generated file.
2. Ask Claude Code to create one self-contained UTF-8 HTML file, with inline
   CSS/JS/assets, and return its absolute path. The path must be accessible to
   Hermes, not only inside Claude's container or remote machine. Arrange an
   authorized file transfer first if they do not share a filesystem.
3. Hermes reads the exact file contents. For a byte-preserving Python read,
   use `Path(path).read_bytes().decode("utf-8")`; the default text-mode read
   can translate CRLF. Check `len(raw_bytes) <= 10_000_000`. Do not publish
   line-number prefixes, truncated tool output, a filename, or Markdown fences.
4. Hermes calls `artifactbridge_delegation_create_document` with that content,
   `format: "html"`, the brief's `room_id`, its `delegation_token`, and a stable
   `idempotency_key` such as `design-first-draft`. The plugin supplies the
   serving profile's service credential. Claude Code does not implicitly
   inherit Hermes tools, profile scope or credentials. No personal login is needed.
5. Inspect the receipt. Return the document/version IDs and Room attachment
   status in the final A2A answer. Read back the document and Room with scoped
   tools before claiming delivery. Creating an artifact does not complete A2A.

A receipt can contain `room_attach_failed` even though the document was created.
Preserve and report its document/version IDs and failure details. A same-key
retry replays that receipt; it does not repair the attachment. This plugin has
no scoped attach-repair tool. Do not follow the receipt's member-tool retry
hint through personal credentials, or create another document to hide the failure.

### Large-file handoff remains a separate design choice

This release accepts inline content only; it adds no `content_path` argument,
file reader, subprocess bridge or new tool. Schema support does not make a
multi-megabyte file practical to send through a model's context/output window.
Hermes' documented `execute_code` tool bridge exposes a fixed built-in tool
allowlist, not arbitrary plugin tools. Do not assume it can call this plugin
or recover profile credentials in a child process.

For small files, use a complete, untruncated read and an inline scoped call.
For multi-megabyte files, stop if the available runtime cannot transfer the
exact contents to the plugin without model transcription. A future path-based
helper needs an explicit design decision: host versus sandbox path resolution,
authorized roots, symlink handling, strict UTF-8 reads, byte limits, and dispatch
under the active profile's scope. Do not work around this by exposing credentials
to Claude, importing the plugin in a shell, or using personal MCP access.

The transport tests cover a 10,000,000-byte body against an in-process fake.
They do not prove a Claude-to-Hermes multi-megabyte handoff or deployed Worker
capacity. Large escaped HTML can still exceed backend Worker memory; this
plugin change does not resolve that acceptance risk.

Contract: [merged backend PR #3504](https://github.com/omnim-ai/artifact-bridge/pull/3504)
(commit `cf9c3fda86cc1cda08e0d620888cc23ecbe1b078`). Hermes references:
[code execution](https://hermes-agent.nousresearch.com/docs/user-guide/features/code-execution)
and [plugin tools](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins).

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
its denial codes and the renewal route. Registered-tool HTML tests exercise
schema admission, exact file-to-MCP content, authority stripping, backend error
and receipt passthrough, and safe retry behavior. Their scripted backend replies
do not independently verify backend authorization, byte validation or SQL
idempotency. Local checks used Python 3.14.7, mcp 2.2.0 and httpx2 2.13.1.

`skills-ref validate skills/artifactbridge-delegation` reports a pre-existing
name/directory mismatch (`delegation` versus `artifactbridge-delegation`). The
skill metadata is preserved because Hermes registers it as
`hermes-artifactbridge:delegation`; plugin doctor and validate accept it.

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

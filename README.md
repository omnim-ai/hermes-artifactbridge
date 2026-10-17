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

- Registers eleven static tools in toolset `artifactbridge-delegation`, mirroring
  ArtifactBridge's scoped surface: `artifactbridge_delegation_read_brief`,
  `_read_room`, `_read_document`, `_list_documents`, `_search_documents`,
  `_contribute`, `_upload_image`, `_create_document`, `_propose_change`, plus
  the read-only Space tools `artifactbridge_read_space_context` and
  `artifactbridge_read_skill`.
  Every tool requires `delegation_token` (`dlt_` + 43 base64url characters).
  Only the two Space tools accept an explicit `delegation_id`; it is optional,
  defaults server-side to the token's own delegation, and any other is
  refused. The plugin strips `delegation_id` from the other nine tools. Only
  `create_document` accepts `room_id`, to opt into an HTML Room artifact. The
  backend checks that it is the delegation's Room; the plugin strips `room_id`
  from all other tools.
- The Space tools forward `space_id`, `cursor`, `limit`, `include_archived`,
  `slug` and `version_policy` unchanged and only when given; ArtifactBridge
  owns their defaults. They return ArtifactBridge's MCP result unchanged as
  `{"content": [...], "isError": bool}`, including every refusal such as
  `space_unavailable`, `invalid_cursor` or `skill_not_found`. A tool the
  server does not offer returns its JSON-RPC error as `{"error": {"code",
  "message"}}`. The plugin never flattens, pages, installs or retries these
  results; the other nine tools keep their flattened JSON results.
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
   the serving profile to offer the eleven tools directly.

Removing the plugin does not revoke enrollment: rotate or disable the service
in ArtifactBridge separately.

## Pooled execution (protocol v1, cloud-contract-r2, not yet usable against a deployed server)

Pooled (assignment-bound) execution lets ArtifactBridge dispatch one exact
assignment to this executor instead of putting a `dlt_` token in the task
text. It is **off by default** and changes nothing until you opt in:

```
ARTIFACTBRIDGE_POOL_ENABLED=1
ARTIFACTBRIDGE_POOL_EXECUTOR_ID=<executor uuid>
ARTIFACTBRIDGE_POOL_CONNECTION_REVISION=<revision>
ARTIFACTBRIDGE_POOL_PORT=9901                       # optional; else platform extra.port, else 9901
A2A_BEARER_TOKEN=<remote bearer>                    # or A2A_PEER_TOKENS; required, see below
```

The three settings must be present and well formed (and the tools URL trusted);
otherwise `register()` registers exactly the legacy surface above. They are read
through the same profile-scoped reader as the service credential. There is no
executor-wide control credential: each assignment's `agx_` arrives with the
server's requests (see Start headers).

- **Separate listener.** Pooled mode registers its own Hermes platform,
  `artifactbridge-pool`, a thin subclass of the stock A2A adapter on its own
  port. The stock `a2a` platform and the legacy `dlt_` flow are unchanged. The
  pooled listener keeps the stock platform hint, inbound framing, audit,
  history and per-context anti-loop turn counter; it does not forward to other
  profiles.
- **Remote bearer.** The listener uses the stock A2A bearer
  (`A2A_BEARER_TOKEN` or `A2A_PEER_TOKENS`, stock `A2A_HOST`/trusted-peer rules)
  and refuses to serve without one (`bearer_required`, not retryable). Every
  request needs it, the Agent Card GET included: a missing, repeated or wrong
  `Authorization` header is HTTP 401 before anything is read. A
  `Content-Length` that is repeated or not plain decimal digits is HTTP 400
  before the body is read. Request headers are never logged, audited or
  journaled.
- **Methods.** Only `SendMessage`, `GetTask`, `ListTasks`, `CancelTask` and
  `artifactbridge/pool/capability` (JSON-RPC, A2A 1.0) are served on the root
  route; any other method is `-32601 Method not found` and a served-agent
  route is refused. No request reaches the stock handlers.
- **Capability.** `artifactbridge/pool/capability` echoes the server's nonce,
  executor id, connection revision, protocol version and the six capabilities
  this plugin implements with this plugin's version, only when they match this
  executor's configuration and every required capability is one of those six;
  otherwise it answers a JSON-RPC error and no result. The runner takes initial
  tasks only and does not claim `same_session_continuation`: a probe for the six
  succeeds, and a probe that requires `same_session_continuation` (the current
  server probe asks for all seven) is refused `capability_mismatch`. Aligning
  the server's eligibility check to the six is owned server-side, outside this
  plugin. It needs no start headers and never claims open-pool consent.
- **Start headers.** Send, Get, List and Cancel must carry exactly one
  `X-AB-Pool-Protocol-Version: 1`, one `X-AB-Control-Credential: agx_...` and
  one `X-AB-Start-Journal-Id` (printable ASCII, at most 128). Anything missing,
  repeated or malformed is a JSON-RPC `-32602` (`invalid_pool_headers`) before
  any task, event, journal row or control call. The journal id must be the one
  the server derives for the envelope's assignment (`agj_` plus the dashless
  assignment id), else `journal_mismatch`. The plugin never mints a journal id:
  it starts the assignment the server already started (`/start` replays the
  server's journal) and authenticates `/start`, `/bootstrap` and `/report` with
  that assignment's own `agx_`. The `agx_` is held in memory per journal id
  only; the journal stores its SHA-256 fingerprint, and a later request with
  another `agx_` for the same assignment is refused `grant_mismatch`.
- **Intercept before the model.** Every Send must carry
  `params.metadata["artifactbridge.assignment"]` (protocol v1, strict;
  `commandSequence` is the integer 1, in the envelope and in the start permit)
  and a `params.message.messageId` exactly equal to the envelope's
  `dispatchId` (else `message_id_mismatch`, also for a repeated Send with an
  altered messageId). A missing,
  malformed or unknown-version envelope, another executor or connection
  revision, a tuple that differs from the journaled one, a refused or
  mismatched start permit, or a passed `startBy` ends the A2A task as
  rejected/failed before any message reaches Hermes. Only after a valid permit
  does the plugin bootstrap a per-task `dlc_` credential, held in memory for
  that (platform, contextId, task id) only. `startBy` is checked again after
  bootstrap: if it has passed, the credential is discarded, the dispatch is
  refused `start_deadline_passed` and reported `stopped`/`failed`.
- **Send answers at once.** Send never waits for the reply. It waits up to 5 s
  for Hermes to admit the turn and answers `TASK_STATE_WORKING` once admitted
  (`running` is reported then, not at spawn), else `TASK_STATE_SUBMITTED`. The
  task's `metadata["artifactbridge.assignment"]` is the envelope plus
  `startJournalId`, which the server matches tasks by. The A2A task id is
  journaled with the dispatch before the first answer, so a repeated Send for
  the same journal answers the same task id (its refusal, result, or running
  turn) and never starts a second turn. If `/start` gets no answer, the Send
  answers that task id as failed `start_unavailable` while the row stays
  retryable: the next Send asks again with the same startJournalId and runs as
  the same task id. A request refused before journaling (bad envelope,
  another executor, a foreign grant) has no row and gets a fresh rejected task.
- **Get, List, Cancel.** They read the journal row the start headers name
  (journal id and `agx_` fingerprint) and never create a row or start a turn.
  An unknown or foreign journal is `-32001 task not found` (List: an empty
  page). Cancel stops the turn through Hermes' own session cancel; the task is
  `TASK_STATE_CANCELED` (and `cancel_acknowledged` is reported with its own
  binding) only once the runtime confirmed the stop. Otherwise it stays live
  with `metadata["artifactbridge.cancel"] = "pending"` and later records its
  real outcome. A finished task is `-32002 not cancelable`.
- **Pooled tools.** Toolset `artifactbridge-pool` holds token-free
  `artifactbridge_pool_*` variants of the eleven tools (same server tools on the
  wire). They present only the current task's `dlc_` as `Authorization: Bearer`,
  with no delegation-token header and no service credential. Without a binding
  they return `pooled_credential_unavailable`. A `dlc_` within 60 s of its
  `expiresAt` is re-bootstrapped before use, and a `credential_expired` denial
  is re-bootstrapped and retried exactly once (one refresh per task at a time;
  a refresh never starts a turn). Legacy tools refuse with
  `pooled_session_requires_pooled_tools` inside a pooled session.
- **Clarification.** A pooled runner cannot continue a conversation. When a
  turn's reply starts with `[INPUT_REQUIRED]`, the plugin posts the question
  to the Room as a `question` event through the pooled contribute tool, under
  the task's own `dlc_`, before anything terminal is recorded. The task then
  ends `TASK_STATE_FAILED` with `clarification_unsupported`, telling the
  sender to send the task again as a new request with the answer included, and
  quoting the question. It is reported `stopped`/`failed`, never an input wait;
  Get and a repeated Send answer the same failure.
- **Sessions.** Continuations are not supported (native support is tracked as
  AI-4105). Every continuation (a higher execution generation, or another
  assignment of a delegation already journaled) is rejected
  `continuation_unsupported`, with the same resend guidance, before any control
  call: no start, no bootstrap, no turn, no Hermes session and no journal row.
  A context bound to another delegation is refused
  `context_bound_to_other_delegation`.
- **Journal on disk.** With pooled mode on, the plugin writes a SQLite (WAL)
  journal at `<HERMES_HOME>/artifactbridge/pool-journal.sqlite3`: one row per
  (assignmentId, dispatchId) with the envelope, the server's startJournalId,
  the `agx_` fingerprint, state, A2A ids (contextId, task id and the Send's
  messageId), report sequence and the recorded result. It never holds
  an `agx_` or `dlc_` value. Rows are never deleted;
  there is no cleanup sweep.
- **Retries and restart.** A repeated delivery of a dispatch reads its row: a
  running turn is not started twice, a finished or refused dispatch gets its
  recorded answer, and an unanswered start is resent with the same
  startJournalId. When the adapter starts, a dispatch that was running when the
  process stopped is marked `uncertain` and reported `stopped`/`crashed`; it is
  never re-run, and a later retry answers failed/uncertain.
- **Durable reports.** Each `running`, `stopped` and `cancel_acknowledged`
  report is written to the journal row (`pending_reports`) in the same
  transaction that allocates its `reportSequence` and makes the state change,
  then sent outside it, each row's oldest sequence first, with that row's
  `agx_`. After a restart the process holds no `agx_`, so a row's reports wait
  until the server's next Send/Get/List/Cancel presents it again. A report
  leaves the journal only when the server answers `{recorded, currentOccupant}`,
  including `recorded: false` (a replay of a recorded sequence).
  `scheduler_busy` is retried twice inline (0.05 s, 0.2 s); any other failure
  leaves the report pending, and the next admit (after its own start path),
  finish, cancel, recovery read or adapter start sends it, at most 8 per pass
  and stopping at the first failure. A named refusal is not an
  acknowledgment: the report stays in `pending_reports`, the refusal is
  recorded in `report_refusal` (apart from the row's execution state), and
  that row's reports are never retried or overtaken by a later sequence.

**Server contract.** Routes, headers and shapes match artifact-forge
`a26eb977c476d7b2efd1c636deaa7e74f31c50c2` (cloud-contract-r2) and its generated
fixtures in `tests/fixtures/cloud_r2` (see `SOURCE.md`). That server code is not
merged or deployed, and its CI database/runtime is not certified. Calls go to
`POST /api/agent-gateway/pool/start`, `/bootstrap` and `/report` on the tools
URL's origin with `Authorization: Bearer agx_...`; redirects are never
followed. Status mapping: a contract 409 refusal (`start_owned`,
`start_deadline_passed`, `start_not_authorized`, `withdrawn`, any named error
code) and 401/403/400 are terminal refusals, never retried; `scheduler_busy` is
retried; any other status or a body that is not exactly the contract shape is
"unavailable". Bootstrap sends `{protocolVersion, binding, startJournalId}` and
accepts only `{credential: dlc_..., expiresAt}`.

Contract ambiguities, as implemented:

- The server mints `dispatchId` as `'agd_'` plus a dashless random UUID
  (`supabase/migrations/20261017071910_ai4046_oldest-eligible-scheduler.sql:657`),
  so it is treated as unique within the journal: a second assignment reusing an
  existing `dispatchId` with a different envelope is refused `tuple_mismatch`
  without a start request.
- After a crash between permit and spawn, the row returns to `start_requested`
  and a retry sends the same startJournalId and bootstraps again; the server
  revokes the earlier `dlc_`.
- A header refusal is a JSON-RPC error at HTTP 200, and the Agent Card GET
  requires the bearer.
- The `send` override that resolves only the task named by `reply_to` is
  tested against the stock adapter over HTTP but not proven in a live gateway
  turn.
- Pooled requests do not increment the stock A2A `inbound_total` or
  `anti_loop_triggers` metrics: rebinding a Hermes module attribute fails
  `hermes plugins validate`. Validation passes, but it warns about private core
  imports (`plugins.platforms.a2a.*`, `gateway.session_context`,
  `hermes_cli.plugins_manifest`). They are inherent to subclassing the stock
  adapter and its request handler, which is why the runtime shape and version
  gate exist.

**Not proven.** No pooled journey has run against a real ArtifactBridge server
or a live Hermes gateway. The tests drive the listener's real socket with the
generated fixture requests, the stock Hermes 0.22.2 A2A adapter and message
pipeline in-process, a fake control server and a fake MCP boundary; only the
model is a stand-in. The pooled platform refuses to register on any Hermes
outside `>=0.22.2,<0.23` or with a different A2A adapter or handler shape.

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
idempotency. Space tool tests likewise script Space map and skill replies and
refusals as contract fixtures; they prove forwarding, header handling, renewal
and exact result passthrough, not backend ACL or SQL behavior.
Local checks used Python 3.14.7, mcp 2.2.0 and httpx2 2.13.1.

Pooled-mode core tests (`tests/test_pool_core.py`, `tests/test_pool_tools.py`)
use a fake control server (`tests/fake_pool.py`) and run without Hermes. The
adapter tests (`tests/test_pool_adapter.py`) skip unless Hermes' stock A2A
adapter is importable; run them from a scratch venv on Hermes' Python whose
`.pth` file points at Hermes' site-packages and source tree, with only pytest,
pytest-asyncio, starlette and jsonschema installed into the scratch venv.

Validate the bundled skill with `skills-ref validate skills/delegation`.

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

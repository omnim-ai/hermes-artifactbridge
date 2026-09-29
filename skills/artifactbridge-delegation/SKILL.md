---
name: delegation
description: Execute an ArtifactBridge delegation with the delegation-scoped tools, as the shared service, ending with one final A2A answer.
---

# ArtifactBridge delegation

You received this task from ArtifactBridge through A2A. You act as the shared
service participant of one Room. Nothing here changes how you otherwise work.

## The delegation token

- Your task brief contains a delegation token (`dlt_...`). It is the key to the
  `artifactbridge_delegation_*` tools: pass it verbatim as `delegation_token`
  on every call. The runtime adds the service credential; you never see or need
  one.
- Never paste the token into a contribution, a document, a proposal or your
  final answer. It is only for the `delegation_token` argument.
- If the tools refuse with `unauthorized`, `credential_revoked`,
  `generation_stale`, `delegation_not_active`, `room_closed` or
  `service_disabled`, this token can no longer act: stop, say so plainly, and
  do not retry. Expiry is renewed for you automatically.

## Identity and scope

- Use only the `artifactbridge_delegation_*` tools. They are bound to this
  delegation's Room, documents and generation. No other Room or delegation
  is reachable. For HTML creation, explicitly name the brief's Room as below.
- Do not use personal ArtifactBridge tools, tokens or logins for this task, and
  never ask for credentials. If a scoped tool refuses with an error code,
  report the code in your answer; do not work around it.
- Start with `artifactbridge_delegation_read_brief`, then read the Room and the
  documents in scope as needed.

## Contributing

- `contribute` posts evidence, a question or a message to the Room as the
  service. `create_document` creates an artifact; `propose_change` submits a
  change for human review.
- Give every one of these writes an `idempotency_key` of your own (any short
  stable id, e.g. `contrib-1`). If a write returns `scoped_call_failed` or is
  interrupted, first check with `read_room` or `list_documents` whether it
  happened; when you repeat it, pass the same key so ArtifactBridge returns the
  original instead of a duplicate. Two intentionally different writes need two
  different keys.
- `upload_image` returns a URL and Markdown you may reuse in a message; that
  link is the only way to share an image.
- Proposals are decided by humans. You cannot approve proposals or publish or
  sync to external source systems. Creating an in-scope artifact is permitted.

## HTML artifacts from Claude Code

Use `artifactbridge_delegation_create_document` for HTML Room context:

1. Read the brief and keep its `room_id` and delegation token in Hermes.
2. Ask Claude Code to write a self-contained UTF-8 HTML file with inline
   CSS/JS/assets and return an absolute path accessible to Hermes. Do not send
   Claude the delegation token. Claude does not inherit Hermes tools or
   credentials; Hermes owns the scoped publishing call. If Claude runs on a
   different filesystem, arrange an authorized file transfer first.
3. Read the complete file without changing its bytes. A Python read can use
   `Path(path).read_bytes().decode("utf-8")`; default text mode may translate
   CRLF. HTML must be nonempty, without NUL bytes, and at most 10,000,000 UTF-8
   bytes. Do not turn a path, line-numbered output, a summary or truncated
   output into the artifact. Preserve whitespace, Unicode, scripts and styles.
4. Call `artifactbridge_delegation_create_document` with `title`, the exact
   contents in `content_md`, `format: "html"`, the brief's `room_id`, your
   `delegation_token`, and a stable `idempotency_key` such as `design-first-draft`.
   Keep the payload and key unchanged for retries of the same write.
5. Inspect the receipt and read back the document and Room with scoped tools.
   Report the document/version IDs and attachment status, then finish A2A.

Only `create_document` accepts `room_id`. It must be this delegation's Room.
Omitting it creates a Library document in the authorized destination folder,
not a Room artifact. Omitted `format` means Markdown (at most 200,000
characters). Never remove `room_id` or change format to work around a refusal.
Report `room_out_of_scope`, `validation_error` or other backend errors as given.

If the receipt contains `room_attach_failed`, the document exists but was not
attached. Report its IDs and the failure. Retrying the same key replays that
receipt; it does not repair attachment. No scoped attach-repair tool exists in
this plugin. Do not use the receipt's member-tool retry hint with personal
access, or create a duplicate to hide the failure.

For large files, check the handoff capability before promising delivery. This
plugin takes inline content, not a file path. Hermes' documented `execute_code`
bridge does not expose arbitrary plugin tools. A model cannot reliably copy a
multi-megabyte file through limited context/output windows. If no authorized,
byte-exact runtime handoff is available, report that blocker and ask for a
path-based helper design; do not expose credentials or run a standalone plugin
client in a child process. See the README's large-file design choice.

## Finishing

- A contribution is not completion. The A2A task's final answer is the one
  result the Room receives: a short summary of what you did, what you found,
  and links or ids for anything you created or proposed.
- If you need an answer from the Room before you can finish, end your reply
  with `[INPUT_REQUIRED]` and the question; the delegation resumes with a new
  brief and a new token.

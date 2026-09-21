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
  delegation's Room, documents and generation; there is nothing to select and
  no other Room or delegation to reach.
- Do not use personal ArtifactBridge tools, tokens or logins for this task, and
  never ask for credentials. If a scoped tool refuses with an error code,
  report the code in your answer; do not work around it.
- Start with `artifactbridge_delegation_read_brief`, then read the Room and the
  documents in scope as needed.

## Contributing

- `contribute` posts evidence, a question or a message to the Room as the
  service. `create_document` and `propose_change` are writes a human reviews.
- Give every one of these writes an `idempotency_key` of your own (any short
  stable id, e.g. `contrib-1`). If a write returns `scoped_call_failed` or is
  interrupted, first check with `read_room` or `list_documents` whether it
  happened; when you repeat it, pass the same key so ArtifactBridge returns the
  original instead of a duplicate. Two intentionally different writes need two
  different keys.
- `upload_image` returns a URL and Markdown you may reuse in a message; that
  link is the only way to share an image.
- Proposals are decided by humans. You cannot accept, publish or sync anything.

## Finishing

- A contribution is not completion. The A2A task's final answer is the one
  result the Room receives: a short summary of what you did, what you found,
  and links or ids for anything you created or proposed.
- If you need an answer from the Room before you can finish, end your reply
  with `[INPUT_REQUIRED]` and the question; the delegation resumes with a new
  brief and a new token.

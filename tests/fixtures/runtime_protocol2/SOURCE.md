# Runtime capability protocol 2 fixtures (provenance)

- `plugin-fixtures.json`: server-generated pure fixtures, copied byte-for-byte
  from the AI-4128 s3-protocol-r1 delivery. Server: `omnim-ai/artifact-forge`
  branch `wt/t_ee5f18c4`, head `04154d73ad61cddcaf8706428a9b9dabe4e54985` (a
  local, unpushed worktree), `src/agent-gateway/runtime-protocol.ts`, copied
  from `test/ai4179-runtime-protocol.test.ts`. Pure fixtures, NOT a
  paired-runtime proof. Its `exactSets.tray` sets are the tray's and are not
  used by this plugin.
- `capability-v2-request.json`: plugin-generated, not server-generated. The
  `cloud-capability-request.json` wire shape of
  `tests/fixtures/cloud_r2_terminal_clarification_1` with `params` built from
  `cloudV2Challenge` (protocolVersion, executorId, executorConnectionRevision,
  nonce) and `exactSets.cloud["2"]` as `requiredCapabilities`.
- `capability-v2-result.json`: plugin-generated, not server-generated. This
  plugin's answer to that request, with `runtimeVersion` `0.0.0-fixture` as in
  `cloud-capability-result.json`.

Caveats:

- The server's protocol 2 recording and matching are not merged or deployed;
  the cloud probe still sends protocol 1. No real paired handshake has been run.
- `cloudV2Answer.runtimeVersion` (`hermes-2.0.0`) is the server test's value,
  not this plugin's version.

| File | sha256 |
| --- | --- |
| `capability-v2-request.json` | `7ac2457002b05062d411c36993c98026625584b57c73cc1bf6c97856c774a4d8` |
| `capability-v2-result.json` | `58acc4cd099721614d51a3c527dcc455e36e03d5f5452e8601165e47c632736a` |
| `plugin-fixtures.json` | `bd0449d9331d9d9528f58df51e28027030fab2491ab36fabdf53464115c1c009` |

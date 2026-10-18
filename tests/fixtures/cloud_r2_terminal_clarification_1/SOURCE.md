# Cloud contract r2 terminal-clarification-1 fixtures (provenance)

- Revision: `cloud-contract-r2-terminal-clarification-1`; pooled `protocolVersion` 1.
- Server: `omnim-ai/artifact-forge` branch `wt/t_23ccc323`, head
  `e041a62ccf3e5df6f34ec226b3b85c9d5ceb3c82` (a local, unpushed worktree).
- Generated from that worktree by
  `bun run scripts/ai-4047-pooled-fixtures.ts <out-dir>` (exit 0; regeneration
  byte-identical): 64 fixtures plus `manifest.json`.
- `manifest.json` sha256 `3187662d748077ec086b890657031dc6d086b0ee2e6b9b35539fa1ede08c2d57`.
- `final-fixtures.SHA256SUMS` (kept outside this directory) sha256
  `fe91ea728a43f43404ea956e064ca20f013c00a236e09c92580fc30828be095b`; all 65
  files below were copied byte-for-byte and pass `sha256sum -c` against it.
- `tests/fixtures/cloud_r2/` is the preserved historical archive (revision
  `cloud-contract-r2`); its old seven-capability `cloud-capability-request.json`
  is used only as a historical negative input.

Caveats:

- `*-row.json` and `cloud-prepare-receipt-row.json` are SQL schema examples,
  not runtime receipts.
- Credential values (`agx_`, `dlc_`, bearer) are shape-only placeholders.
- The eight `cloud-continuation-*` files are historical generation-2 negative
  inputs, never an allocation or a start capability.
- `cloud-capability-verdict-*.json` are the server's verdicts on a runtime's
  capability answer, not requests.
- Nothing here came from production or the current remote.
- The server head is source only: not merged, deployed, or certified against a
  CI database or runtime.
- Any later fixture delta requires re-running the plugin suite against it.

| File | sha256 |
| --- | --- |
| `bootstrap-request.json` | `bfd8aff984f1c8ac89f11d61f6bdd6a1dd75f787994281ae7ba62fba25c2a55b` |
| `bootstrap-response.json` | `c41381c4f495ffd13d39055a29f85dc7033f3afcb024b721a31443c1c3a89153` |
| `cancel-ack.json` | `5e0bc39c74495d4d95b1c4d55eebd8e29f144f576457bbea02a78929816c0457` |
| `capability-challenge.json` | `e23b48294b9384e882e6606646e7783fc2808eafba6de55ec257057f1516a10d` |
| `capability-probe-required-response.json` | `6beb8aed87eb35ba5031c2f145439b14f02f1a2fd8e12c997d585824766dd1c1` |
| `capability-proof.json` | `a0c90f3112c66a879979eec74981d04a1ebb8990de8c2eac7f2136ee1535450b` |
| `capability-refusal-response.json` | `8e0f0e1e46e735501e19df976c5666ae5177c6b38a4d2eaa2defa59094b1775b` |
| `capability-response.json` | `8b51e5acfe40138c9cc0e442f6618c8f8892929fee4ec49fe94c700e14e5e2ad` |
| `cloud-bootstrap-request.json` | `cfc9d0436deae11b12870b0978e4e06a565ee41790be028e28c54e6256084d5d` |
| `cloud-cancel-request.json` | `e6486e682dc015c59b5235b52a16fea5fb00e25c74bbd32570e8e4a7b6c1324c` |
| `cloud-capability-error-method-not-found.json` | `363f108b766778f6d0ce980162991cf91a488906276693efd54aeaa869aad497` |
| `cloud-capability-proof-record.json` | `042e2c8a7e4179b10a62a52d6fc38bad9d92a6a2fe2864c6f75f088c9d2a7066` |
| `cloud-capability-request.json` | `f8959dc2bd042fdc6dd7649ce98b83df83091280ac8cfc13ce5c033160038739` |
| `cloud-capability-result.json` | `0f049563dbe98700d0d18054f82c1bd27bee4c0884f76ee9228060e379f0c487` |
| `cloud-capability-verdict-duplicate-capability.json` | `2d4750f2acc7aa62aced93a4dc9ec406e6fada7c1b218fed2b3d48194eb96d45` |
| `cloud-capability-verdict-missing-capability.json` | `2d4750f2acc7aa62aced93a4dc9ec406e6fada7c1b218fed2b3d48194eb96d45` |
| `cloud-capability-verdict-old-seven.json` | `2d4750f2acc7aa62aced93a4dc9ec406e6fada7c1b218fed2b3d48194eb96d45` |
| `cloud-capability-verdict-unknown-capability.json` | `2d4750f2acc7aa62aced93a4dc9ec406e6fada7c1b218fed2b3d48194eb96d45` |
| `cloud-capability-verdict-wrong-nonce.json` | `266532dad6aef9665f16c4cddefd5c6ff9274c95f167b74614ac510854fbac4a` |
| `cloud-capability-verdict-wrong-protocol.json` | `2d4750f2acc7aa62aced93a4dc9ec406e6fada7c1b218fed2b3d48194eb96d45` |
| `cloud-card-request.json` | `732142df6b61814826498c01d35c1ac41460f0eaf2873628ade282212a0bfa6f` |
| `cloud-connection-response.json` | `83a4ff66e3945b596a81cc249212dad448e561395a20062f85041197571774bf` |
| `cloud-connections-due-check-row.json` | `5b07a9b58e8ed87f0b8af94afa03dcf26c63d1ea3e1ca503f67b639dbb31350d` |
| `cloud-continuation-bootstrap-request.json` | `fdf122ae73ef6b616ba57aceb107cbc7f94499c899ab016422f7784d77c967da` |
| `cloud-continuation-envelope.json` | `3a79e8af6896da15e3a242560af7c25372d58bdfe5b7ec581d6d28f9f4162568` |
| `cloud-continuation-request.json` | `007fd16f1c22050482cdb98668d2c8b18b4583335c8be99c144d00d2353d8be5` |
| `cloud-continuation-response.json` | `1f3840c71a9773992c388b09c1fe776c70a9a4d6d915d8e7944098653c0562a2` |
| `cloud-continuation-start-permit.json` | `dd644205eeede0c43f613558737169b48ab213698dceaf19aac3d991c01e4e4e` |
| `cloud-continuation-start-replay-permit.json` | `dd644205eeede0c43f613558737169b48ab213698dceaf19aac3d991c01e4e4e` |
| `cloud-continuation-start-request.json` | `4f2bd3309a2b2006792d9397ca409224c562d4d6f7dbe80af38372667aa8c698` |
| `cloud-continuation-transport-row.json` | `4df553daff5ecfada0fecab6082145edd89d3bfa2350a5eff3ea806d69719ff2` |
| `cloud-get-task-request.json` | `78eea79d067f663b95e7d00f572ae290b91700e2a567a3f0c173264ab16b0490` |
| `cloud-list-tasks-request.json` | `12d94fddcccc1fa9aef6403bfaaeea71b23d4331bc53d13b2ed104bc65a25e21` |
| `cloud-prepare-receipt-row.json` | `577acb4e1ac019ab536bcb7f0a8ce0a50774d992f6148a7358bd6890db45f28b` |
| `cloud-probe-failed-response.json` | `6bbd8b98c35dc8933474398a7e435ad4c4b031ecd03376d851d9c464ac3cc67b` |
| `cloud-send-request.json` | `20874415b8a05094ea9299454fe41f822e99b24ec9d144fd53202af94bbc9313` |
| `cloud-send-response.json` | `b28e726511a96d5d8934106b5fe99dd101ffdb5af84ff6f3e51ac1071a3ce6db` |
| `cloud-start-not-authorized-refusal.json` | `d25e609e4535cf3108e45960abcb905118be69fee8926ff2229b79380f63f84f` |
| `cloud-start-owned-refusal.json` | `e3f467d2078aeb595b7d736618355f67cff7443d4a54c2c031412fdc3abe4825` |
| `cloud-start-replay-permit.json` | `17c19381e12105cf76d1b7d4ac044afb7a60a455be23eb0684aa5e2f27dde8b2` |
| `cloud-start-replay-request.json` | `fa01516b2f00a58aacfbf68adda2920d48bcebe1290b7437d434c492f675cf86` |
| `cloud-terminal-clarification-null-question-task.json` | `6c9730018f3b34fd94a591f248a0c0b13a5155576c6814c16f9e763d8038bf81` |
| `cloud-terminal-clarification-task.json` | `9b208e8a702ec9f7fc1e5a92f78a51de0349f71db430560b1cfba307dc51605d` |
| `cloud-terminal-continuation-task.json` | `14c3980c9b85f841b28653e776ca7a5afd6127c324f5489477a3383e2e0c84cb` |
| `cloud-transport-row.json` | `df7276fd190f2ad40c15487cc3359949321c3ece6d84b59dafaba5d9f3775498` |
| `configuration-revision-envelope.json` | `ac94d614ecfeb6376a1fca33c119b04c4c20e3bcec9dd89fb93413eee238f86b` |
| `connection-changed-response.json` | `3d1a726048ecd89241bd05df5974cd43c3c969bfe3d78934635c034e31df0c5d` |
| `decline-request.json` | `9e997c8e1ea4cd660bf73c4168d64a8e12df88025159439cb30c3ecc6c563fb1` |
| `decline-response.json` | `2bbcb6ca2ba20f8df4c2d633ea13844e98a5facaf1fee62c02692ab0141ad0a5` |
| `dispatch-metadata.json` | `16c9184540f9aa3bb95ed23bf9818d26ecdcb675d7c5593586f1448c553453d5` |
| `join-capability-probe-required-response.json` | `0cbfd5cb9d6db09a8ab310403afe19c168db9d53ad5286881adac2a9a8950232` |
| `mailbox-assignment-command.json` | `9c046fb7cf9e6ebe7f5c66b2e13d44e832a61a71a475043fb6065fad7a8379f0` |
| `mailbox-withdrawn-tombstone.json` | `1f72432af605f63543b65ea4e97d0d2770c1c477fa92d558545bd2a1718d42b6` |
| `manifest.json` | `3187662d748077ec086b890657031dc6d086b0ee2e6b9b35539fa1ede08c2d57` |
| `refusal-response.json` | `a4bb868913f0c00efc5e816a6e7b848d5e9045cebb1dcaa11dc042dec39f76b2` |
| `report-replay-response.json` | `0d192045a42fa7aff289cced109522cb0cd52a6240371a97bbe437e74f818a01` |
| `report-response.json` | `e502bd2c0f55015041e92e999e93be6cf153d5144db19af18445030c86dfd9a0` |
| `report.json` | `8a049cddf3fc6f27138600131ad4dda14c27d254507cb05fde748ad5e46b1fb4` |
| `scheduler-busy-response.json` | `6333de8d10ad48474239593b71dfe0df6b3e64ccebab60b8da20018a83353efb` |
| `start-deadline-refusal.json` | `be6f3cf6ede3e322c79594815a126292f7d8920da05dd18529e4f1b7b923f6e6` |
| `start-owned-refusal.json` | `e3f467d2078aeb595b7d736618355f67cff7443d4a54c2c031412fdc3abe4825` |
| `start-permit-with-control-credential.json` | `ab4492bbd8521e625a71b6717839efb627ac4ecf6da2da136d8b886ca43d7725` |
| `start-permit.json` | `28535deb5aacf14bd6071ad82d3254f99f7aaad964a419aa80b6df7a3bc2689d` |
| `start-request.json` | `65d86c62d07ea15a814aa9817ba9bf52692a0f305b5e2bd05d58b5c4044624e6` |
| `unauthorized-response.json` | `12be6be5476d4e572316b28e9d8ac98433ef4ac1a18d62cd53d1dc5b83afb51c` |

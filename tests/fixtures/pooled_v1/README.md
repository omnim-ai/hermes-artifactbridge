# AI-4047 U3 interoperability fixtures

Generated in the repository by
`bun run scripts/ai-4047-pooled-fixtures.ts <this-dir>`. Each file is parsed
through its schema, or built by the exported decision, before it is written.
Source: `omnim-ai/artifact-forge` branch `wt/t_7b561db9` (uncommitted at generation).

| File | Schema export | Endpoint / RPC |
| --- | --- | --- |
| `dispatch-metadata.json` | `buildPooledDispatchMetadata` (strict, module-private schema; `src/agent-gateway/pooled-cloud-dispatch.ts`) | Pooled cloud A2A `SendMessageRequest.metadata` (key `artifactbridge.assignment`). Never carries a `dlt_` token. |
| `mailbox-assignment-command.json` | `pooledMailboxCommandSchema` kind `assignment` (`pooled-mailbox-envelope.ts`) | Tray mailbox claim payload for pooled work (protocol v1). |
| `mailbox-withdrawn-tombstone.json` | `pooledMailboxCommandSchema` kind `withdrawn` | Tray mailbox claim payload; terminal for its dispatch id. |
| `start-request.json` | `agentGatewayStartRequestSchema` | `POST /api/workspaces/:workspaceId/agent-gateway/tray/pool/start` (tray); cloud start endpoint is U2/U4. Port `readPooledStartFacts` over U2 authorize-start. |
| `start-permit.json` | `agentGatewayStartResponseSchema` outcome `permit` (wraps `agentGatewayStartPermitSchema`, `assignment-binding.ts`) | Start response; same journal replay returns the same permit. |
| `start-owned-refusal.json` | `agentGatewayStartResponseSchema` reason `start_owned` | Start response for a different journal. |
| `start-deadline-refusal.json` | `agentGatewayStartResponseSchema` reason `start_deadline_passed` | Start response after `startBy` when unspawned, replay included. |
| `report.json` | `pooledAssignmentReportSchema` (`src/agent-gateway/pooled-assignment-credential.ts`); port `PooledAssignmentRepository.recordPooledAssignmentReport` | RPC `agent_gateway_pooled_report` (kind `stopped`). |
| `cancel-ack.json` | `pooledAssignmentReportSchema` | RPC `agent_gateway_pooled_report` (kind `cancel_acknowledged`). |
| `capability-challenge.json` | `pooledCapabilityChallengeSchema` | Enrollment/conformance challenge (separate from open-pool consent). |
| `capability-proof.json` | `pooledCapabilityProofSchema` | Runtime answer; `verifyPooledCapabilityProof` records the capability revision. |


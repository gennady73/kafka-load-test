# Two-tenant adapter — offline-only package 0.2

This is additive to the existing PF/continuity code. It implements the minimal C2 tenant fixture reviewed by C3 and C6 on November 25, 2026: separate A/B producer/consumer credentials, four topics, literal groups, 16 positive 1,000-byte records, eight negative cases, and at most20,000 attempted logical value bytes within one proposed 300-second envelope. It supplies **an in-memory simulation driver, not a live k6 entrypoint or runnable Job**. All contract/plan deployment, execution, readiness and tests flags stay false. Every result is tagged OFFLINE_STUB_ONLY; runtime and production acceptance stay NOT_ASSESSED.

## Files and quick checks

- `policy.mjs`, `contract-keys.mjs`: source-locked contract/compiler and strict serialized-plan rehydration. Exact binding, ACL, group, caller, control, native-setting, record and reserve relationships are recompiled before credential/factory access can be accessed. Compiled plans are deeply frozen.
- `engine.mjs`: dependency-injected tenant lifecycle and outcome state machine; no native Kafka, k6, filesystem or HTTP import. External dependency exceptions never become trusted internal reasons. A private WeakMap brands internal failures; exported reason/severity/category fields are allowlisted.
- `audit.mjs`: separate envelope/chain/seal, record-set/outcome, control/denial, receipt/commit, timing and lifecycle checker. It does not trust the adapter's summary or invent producer ACK offsets.
- `offline-fakes.mjs`: in-memory credentials, broker/groups, clock, typed failures and durable-ACK simulation. These synthetic mappings prove orchestration rules only.
- `offline.mjs`: file-based explicit-input driver that selects only those fakes. It cannot choose a live endpoint, native transport, credential file loader or execute/arm flag. Filesystem reads cover the explicitly provided contract/review documents and this package’s own source fingerprints; credential/CA callbacks are in-memory fakes.
- `tests/`: portable source-derived contract fixture and adapter, checker-mutation and CLI tests. The fixture is the reviewed disabled 0.1 contract; the compiler also accepts additive 0.2 review metadata. No real credentials are present.

From the utility repository:

```sh
node preflight/tenant/tests/adapter.test.mjs
node preflight/tenant/tests/audit.test.mjs
node preflight/tenant/tests/cli.test.mjs
```

For a new immutable **offline** evidence directory, explicitly provide the disabled project contract and review files:

```sh
node preflight/tenant/offline.mjs \
  --contract ../config/preflight-r2/c2-two-tenant-client-contract.json \
  --c3-review ../docs/testing/c3-r2-contract-review-2026-11-25.md \
  --c6-review ../docs/security/r2-two-tenant-contract-review-2026-11-25.md \
  --output /tmp/new-tenant-offline-fixture \
  --run-id offline-tenant-check --seed 2511
```

The driver refuses an existing output directory, a non-offline run ID, armed flags and unknown transport/execute arguments. It writes a disabled plan, events, seal, adapter report, separate-code-path audit, actual driver source fingerprints and a SHA-256 manifest. Review hashes identify supplied documents; hashing alone does not establish their approval or runtime truth. An OFFLINE_COMPLETE result means the simulated minimal matrix and transcript match; it never authorizes or claims live execution.

## Locked identities and group semantics

The selected source is resource commit3211df93ae40d7d6f47c6334966ae6d1f658fee7. A/B each have an events/audit topic, producer and consumer principal, and literal events-group/audit-group. The compiler verifies source ACLs: each producer has its own two Topic Write/Describe ACLs plus Cluster IdempotentWrite; each consumer has Read/Describe on both own topics and both own groups. Either own group may read either own topic; the default pairing is fixture scheduling, not an extra ACL rule. No run suffix, group reset/delete, wildcard permission or smoke identity is used.

Every factory receives one immutable tenant/role/principal/path tuple. A consumer-write diagnostic uses the consumer credential in a producer API path; it never substitutes its owner's producer password. Positive/negative producer options share an explicit safe settings signature. Missing/empty credentials/CA, swapped/duplicate/smoke bindings, principal overrides or unknown debug/credential fields fail closed. Debug/manual-commit safety gates run before injected credential loading. No Secret API, environment credential fallback or real credential reader exists in the provided driver. Co-locating four credentials in a trusted harness does not qualify tenant-Pod credential isolation.

Literal groups require exclusive disposable fixture ownership, observed zero competing members and initial committed/log-end evidence. This minimal fixture refuses unread baseline records. Its conservative single-partition ordering requires each next fixture receipt at the expected next offset; this is a test restriction, not a claim that all Kafka application offsets are contiguous. Real offset gaps, transactions or existing traffic require a separately reviewed expected-offset plan, not an automatic seek or skip/commit workaround.

Positive consumers are closed before any group is reused by a negative probe. Negative factories require manual-only/no-close-commit capability; the adapter never calls commitOffsets in any negative path. Before/after snapshots compare observed committed offsets and membership, including stop paths where available. An unavailable final snapshot is explicit incomplete evidence. The JSON no-group-creation flag is intent, not server enforcement; an unexpectedly permitted join can have side effects even with no commits. Unexpected ACK, record or broker-confirmed forbidden membership stops later probes and is retained as failure, with no destructive cleanup.

## Denial controls and native confounds

Each negative names controls in the same 16 positive record set: source own access, destination owner access, and peer group use as applicable. There is no extra control traffic. Only the exact expected typed topic/group authorization category at the allowed operation/stage satisfies a simulated denial. Empty polling, generic messages, wrapped/unknown codes, wrong stage, SASL/TLS/network/timeouts and unreviewed mapping IDs do not pass.

Consumer-write probes are explicitly marked confounded: consumer ACLs lack Cluster IdempotentWrite, so a real diagnostic producer can fail before Topic Write is tested. A typed Cluster denial is INCONCLUSIVE for that topic case. The offline known-good fixture demonstrates the hypothetical exact Topic-stage outcome under a fake typed map and equivalent authorized write options. It does **not** establish that the current binary exposes non-idempotent diagnostics or a reliable structured native error stage. Actual error conversion/idempotence settings remain unverified and block a live negative-suite promotion. No library message substring matching, broker ACL widening or native-map fabrication is implemented.

## Outcome and durable evidence ordering

All 20 potential logical send IDs are frozen before calls. The adapter reserves an ID/byte budget once, persists attempt intent before native invocation, then rechecks remaining time. A persisted intent is not atomic proof of a Kafka call: a pre-call stop emits not_sent; a missing outcome after interruption remains uncertain. One application attempt per ID is allowed; native wire retries/bytes remain separate and unmeasured.

Reports distinguish ACKed, definitive-not-accepted and unresolved values; late ACKs have a separate bucket and do not count toward the16,000 positive-byte cutoff. A later receipt may establish presence but does not manufacture a producer ACK. Negative attempted/accepted bytes remain outside the positive denominator. Unexpected negative success remains failed even if its record is delivered. No exactly-once or storage durability claim is made.

For positive records, the adapter verifies ID/topic/group/bytes/checksum/partition/expected-offset and rejects duplicates, foreign data or gaps. It verifies the sink's exact durable receipt acknowledgement, persists the correlation, records commit intent, rechecks time, then calls manual commit and records its returned result. A lost commit response is uncertain, not a fabricated successful commit. Evidence/commit is not a Kafka+sink transaction. Matching duplicate sink ACKs are idempotent; wrong ID/hash, refusal or missing durable ACK prevents commit. Native ACK partition/offset remains unavailable.

The audit consumes raw exact envelopes plus a seal, checks identities, contiguous event sequence/hash chain, exact 16 positive/eight-negative denominator, controls, each outcome and receipt/commit link, all initialized client closures, cutoff/elapsed evidence and the export hash/count. Removing or corrupting required events, changing records/groups/digests, reordering commits, false late buckets, unexpected outcomes, truncating a seal/export or missing terminal evidence makes the transcript nonpassing. An offline seal is only a fake collection assertion; it is not an attestation of real collector persistence/custody or Kafka delivery.

The existing live collector accepts only armed sandbox_continuity registrations at its current endpoint and is not automatically compatible with a tenant plan. This package does not change that service or its registration guard. Any later collector/protocol binding is separate owner work and must retain durable-before-ACK and complete evidence semantics.

## Time, stop and redaction limits

One monotonic/global budget includes startup. Phase caps are 45/90/90/45/30 seconds, totaling 300; they are caps, not enforced waits or measured schedulability. Before every new operation, evidence and cleanup allowances must fit both the phase and total deadline. A slow sink triggers a second check before send/commit. Phase transitions and returned native/evidence calls cannot silently borrow later phase time; last-operation overruns are nonpassing. No producer traffic is scheduled during drain. External stop, integrity/evidence failure or unexpected access skips remaining work. Every initialized client receives at most one bounded cleanup offer; skipped, failed or overlong cleanup remains incomplete and does not overwrite the original failure.

Fake time tests demonstrate decisions only. They cannot interrupt a blocked native commit/close or prove real 300-second cleanup. A future live binding needs an actual external Job/startup/deadline observation, source/build pins, approved budgets and measured native bounds. A terminal evidence write overrun is captured by the independent final elapsed/seal observation, not hidden behind a pre-write timestamp.

Adapter evidence is constructed from allowlisted metadata, IDs, lengths, digests, normalized categories/stages and fixed reasons. No raw exception, dependency error property, options/password/CA, environment or arbitrary foreign record key/body is logged. Secret-canary tests cover read/factory/consume/sink/close and adapter-like/getter/nested-cause attacks. Hashes of foreign keys/payloads allow safe correlation; they do not prove authorization was denied. Logging by a future native extension/binder remains a separate execution gate.

## Review and deployment boundary

C3-R2-01–06 and C6-R2-01–08 are mapped to concrete test groups in the project implementation receipt. C6's in-progress findings I01/I02 are corrected: private error identity plus fixed reason/severity exports, and full recompilation validation of serialized plans before credential/factory access. Original reproducer output is retained separately from the fixed-probe receipt. Completed-package owner review remains separate from C2's own tests; no independent human review is claimed.

Before future live work: actual R1/current health, tenant credential/CA delivery and approved group state, native error/commit/timeout mapping, trusted debug-disabled image, advertised network/trust, resource/evidence/custody and collector compatibility, concrete owner/window and explicit execution scope. No current file, flag flip, fake result or review hash resolves those. Full namespace isolation, malicious tenant credential access, other C6 negative cases, registry serialization, quotas/fairness, recovery/no-loss/3h and full-day/TB capacity remain unqualified. Existing continuity and generic code/frozen copies stay unchanged; no deployment or publication occurs in this package.

# KaaS pre-flight adapter (experimental, revision 0.3)

## Continuity extension, 2026-11-24

[Continuity preparation](CONTINUITY.md) adds a separate sandbox-only planner mode and eight-client runner for 15m/1.04MB, 60m/4.168MB and 24h/100MB stages. All supplied/generated plans are disabled and validated offline only. No native Kafka/k6/HTTP execution was performed for this extension. The original PF path below retains its existing volume gates and limitations. The prior API assumptions and build statements below are historical; use the selected runtime evidence in the coordinator workspace and continuity documentation for the new path.

Publication note: the continuity guide documents the sibling-checkout layout and published requirements-contract snapshot needed by its planner commands. This source publication does not enable or launch a profile.

This additive implementation targets KaaS SRS 0.8 PF-01–PF-08 and profile contract 1.0. It leaves the existing framework and deployment manifests unchanged. It has passed offline checks; it has not produced or consumed Kafka traffic. Kafka API/configuration behavior, image provenance, effective settings and evidence collection still require a selected-build short validation. No topic/schema creation, GitOps sync, deployment, fault injection or recurring job is included.

## Files and local verification

- `plan.py`: standard-library Python planner, compact deterministic per-worker budgets and consumer sessions.
- `core.mjs`: deterministic PRNG, byte-safe ASCII payload generation, feasible size selection and weighted timing.
- `run.js`: k6/x/kafka adapter, explicit acks, per-attempt pacing, stable retry IDs, manual verified-batch commits and separate drain sessions.
- `reconcile.py`: disk-backed SQLite ledger for per-ID acknowledgement/receipt evidence, duplicates, checksum differences, missing full-run group receipts and unknown outcomes.
- `examples/offline-fixture.json`: tiny synthetic example, deliberately not executable against Kafka. Its acks, compression, topic, budgets and schedule are test choices, not production decisions.

From the cloned repository root:

```sh
python3 -m unittest discover -s preflight/tests -v
node preflight/tests/core.test.mjs
node preflight/tests/adapter.test.mjs
python3 preflight/plan.py --contract ../config/preflight-profiles.json \
  --scenario preflight/examples/offline-fixture.json --offline --output /tmp/pf-new-offline-plan
```

Choose a fresh output path; the planner refuses to replace an existing plan directory. It emits `worker-N.json` and their SHA-256 manifest. Offline plans carry `execution_ready=false` and the adapter refuses them. Tests use only in-memory/stub data and no Kafka clients. `tests/k6-core-smoke.js` can run in the supplied binary with usage reporting disabled; it imports only the pure core and sends no network data. It is not a Kafka test.

## Scenario overlay

Keep the shared contract as an input contract. Copy the offline example to a new scenario file, select WP-A or WP-B, and supply actual synthetic scenario choices. For an execution candidate set `fixture=false`. Set `short_validation=true` for an explicitly separate <=1-hour short check with a reduced budget; otherwise duration must be 86,400 seconds and target must match the selected 2e12/2e13-byte profile. A short result cannot claim a daily-profile pass.

Required fields are exemplified in the offline fixture. Topic budgets must sum to the single selected target. Each topic supplies explicit `record_count`, min/max decimal sizes, compression, and distributions. The current adapter supports `raw_utf8` serialization, `record_id` keys and either `seeded_ascii` or `repeated_ascii` content. No schema-registry serialization or claim check is implemented here. This restricted synthetic path does not validate a selected application's registry/governance/serialization path. Existing framework schema code remains separate.

`uniform_feasible_fixed_count` chooses each size uniformly within the interval that leaves a feasible remainder for the declared remaining record count. It is conditional sampling, not an independent uniform distribution over the original bounds. Tail sizes are constrained, and a topic budget at its minimum/maximum forces fixed sizes. Record the selected count and compare observed distributions; do not call these payloads production-representative. ASCII value length equals original value bytes. Metadata resides in key/headers and is excluded from the target. Kafka framing, compression, serialized/wire/storage bytes remain separate and need runtime observation; no unsupported `extraConfig` size override is passed.

The planner derives streams using SHA-256 of `pf-xorshift32-v1:<seed>:<label>` (first four bytes, big-endian, zero replaced with one), then xorshift32 with shifts 13/17/5 and division by 2^32. Size/content seeds depend on topic and worker; consumer seeds depend on group. Adding/changing consumers preserves producer streams. Changing worker count preserves total/topic budgets, but intentionally changes record streams and IDs. No automatic process retry/resume is implemented: never restart the same worker plan silently. A crash requires reconciliation and an explicitly versioned follow-up plan; otherwise IDs would repeat.

Producer windows have non-overlapping `[start_seconds,end_seconds)` bounds, positive weights and explicit `max_value_bytes_per_second_per_worker`. Weights assign relative cumulative byte fractions across windows. Gaps are idle. The planner checks aggregate scheduled bytes/s per worker against the window ceiling; at runtime streams share that ceiling proportionally to their byte budgets. Sends and application retries consume spacing. One record can exceed a one-second byte budget: this is record-granular start spacing, not a broker-level sliding-window quota. Hash/payload generation and backpressure can cause visible shortfall. There is no unlimited catch-up or generation during drain. Overshooting calls/acks are timed and reported separately. The absolute producer deadline is not extended.

Consumer groups explicitly choose subscriptions, reset behavior, required full-run consumption, and bounded randomized start/active/idle/concurrency. Group IDs persist between sessions; no UUID suffixes are added. New independent fan-out groups need separate definitions. `earliest`/`latest` are reset policies; they do not override existing committed offsets. Existing-group starting offsets must be captured. Sessions use manual commits only after local payload verification and event emission. Receipt evidence and offset commits are separate; flushing the external evidence sink before a commit is not currently enforced. An integrity failure aborts the local k6 worker, with other workers stopped by the run owner/existing stop controls. Drain sessions reuse each group's identity and run only within the separately recorded drain bound.

## Execution gate and selected build

To compile without `--offline`, resolve the environment contract fields checked by `plan.py` and provide `runtime_evidence` references:

- `effective_topic_settings_reference`: read-only evidence for RF=2, 6h retention, cleanup/size limits, and approved existing topics.
- `binary_ack_semantics_reference`: selected binary/image version/digest and observed synchronous acknowledgement/timeout semantics.
- `worker_budget_reference`: actual worker allocations/scope and planned session/VU count.
- `evidence_collection_reference`: incremental raw/JSON console-event archival, retention/capacity, loss detection and existing telemetry/lag/resource collection.
- `stop_policy_reference`: actual operational health/capacity/evidence stop bounds and run owner.
- `security_adapter_reference`: reviewed selected-extension SASL/TLS configuration and Secret injection.
- `initial_group_offsets_reference`: initial group state, offset/reset/commit policy and expected read set.

These are required reference strings, not automatic evidence verification. Runtime validation and C3 review must establish their contents. Populate a run-specific contract copy with actual environment references; do not fill the shared template with invented defaults. Full application production schedules are not needed for this synthetic test.

The code is adapted to the inspected local xk6-kafka/v2 compatibility API: `Producer.produce` throws on failure, explicit `requiredAcks` -1 or 1, raw string key/value input, headers, `Consumer.consume`, `commitOffsets`, `groupTopics` and string `maxWait`. The supplied binary reports k6 1.7.1 / extension 2.1.0; it has not been proven to contain the inspected modified source. No extension build/image was created or selected implicitly. Before execution, verify byte/header conversion, API mappings, manual commits, acks and close/deadline behavior on the actual build. `acks=0` is rejected because it cannot provide acknowledged-ingestion evidence; choosing -1 versus 1 remains an explicit scenario policy and is not a no-loss guarantee.

The runner additionally requires `PREFLIGHT_PLAN`, its exact `PREFLIGHT_PLAN_SHA256`, explicit `PREFLIGHT_SECURITY_JSON` (only `sasl` and `tls`; `{}` allowed for a separately reviewed plaintext listener) supplied via the existing credential injection mechanism, and a common `PREFLIGHT_START_UNIX_MS` 1 second to 5 minutes in the future. Credentials must not be placed in scenario files or command lines. Start time is shared across worker processes; synchronize clocks and record uncertainty. Late starts/restarts are rejected. Use an image already containing the verified extension; no runtime remote JS imports or dependency downloads occur.

A run owner must launch each worker plan exactly once, capture logs using k6 `--log-format raw` or `--log-format json`, disable usage reporting for the air-gapped run, and configure the existing metrics output. Default escaped text logs are not supported by the reconciler. Store the complete `preflight/` module directory and corresponding plan together in the image/mount. There is deliberately no apply/launch script that picks a cluster, creates topics or chooses credentials. Disable orchestrator worker retries and freeze code/image/config hashes before a run.

## Evidence and limits

`PF_EVENT` records carry run/worker/plan hashes and logical record IDs. Original bytes are counted once per logical record; attempts/retries are separate. Every produce exception is conservatively unresolved because the adapter cannot reliably distinguish rejection from lost acknowledgement. A later acknowledgement preserves the acknowledged state. A receipt can establish presence without manufacturing a producer acknowledgement. Producer metrics distinguish acknowledgements observed by t1 and after t1. No IDs appear in metric tags. Produce latency measures the call; receipt latency uses producer attempt time and includes clock uncertainty/retry effects, not a guaranteed application end-to-end latency.

Export logs incrementally during generation; six-hour Kafka retention cannot support a full-day final reread. Reconcile archived chunks into SQLite after or during collection, keeping the original immutable source logs. The CLI currently expects a complete log set in a fresh database, not repeated appending of chunks into an existing database. It verifies plan manifests, separates group fan-out/replay duplicates, reports unresolved and missing/unexpected/corrupt records, and never certifies production readiness. SQLite bounds process memory but disk/index/CPU/I/O and per-record console overhead have not been benchmarked at 2/20 TB scale. Evidence collector completeness/durable flush is not attested by the adapter; even balanced logs remain INCONCLUSIVE for exhaustive integrity. A scalable durable evidence transport is still a C3-reviewed acceptance gap.

```sh
python3 preflight/reconcile.py --plans /path/to/frozen-plan-directory \
  --events /path/to/worker-0.raw /path/to/worker-1.raw \
  --database /path/to/new-ledger.sqlite
```

Do not pipe actual application payloads/secrets into these tools. Per-record evidence contains IDs, lengths and digests, not payload bodies or raw exception text. The report's `observed_target` is arithmetic over observed acknowledgement logs; `evidence_markers_complete`, accounting, integrity and configuration coverage must be assessed separately. Groups with `require_full_run=false` have no fabricated expected ID set; their completeness remains INCONCLUSIVE. Missing records are not automatically classified as retention expiry or broker loss. External lag/resource/expiry/stop evidence remains necessary.

Producer clients close in their owning VU, not a separate teardown runtime. The adapter uses 1-second configured call timeouts and short consumer waits as candidate bounds; actual mapping/cancellation/close behavior requires runtime validation. A worker killed during a call may omit terminal evidence. Scenario startup, connection, serialization, hashing, evidence writes and worker resource saturation all consume the test budget. No throughput, duration, durability, restart recovery or production acceptance result is claimed by this implementation.

# Sandbox continuity adapter — offline preparation

Revision 0.1, 2026-11-24. These files implement preparation for the user's low-budget continuity stages. **No live scenario has been executed or authorized by this preparation.** The existing PF runner and 2 TB/20 TB budget guards are preserved. This is a separate planner mode and separate k6 entrypoint, not a fixture/short-validation bypass.

| Stage | Measured seconds | Records | Original value bytes | Per topic records | Maximum total Job envelope |
| --- | ---: | ---: | ---: | ---: | ---: |
| 15m | 900 | 1,040 | 1,040,000 | 260 | 1,380 s |
| 60m | 3,600 | 4,168 | 4,168,000 | 1,042 | 4,080 s |
| 24h | 86,400 | 100,000 | 100,000,000 | 25,000 | 86,880 s |

Each value is exactly 1,000 ASCII bytes. Targets exclude reads, replication, keys, native retries, TLS/protocol and evidence overhead. Setup <=120s, drain <=300s and close/shutdown reserve <=60s are outside the measured interval. No automatic replay/application retry; a failed/ambiguous produce outcome stops the run. The library's native retries remain within its configured delivery timeout, not an asserted attempt count. The 2 MiB boundary cases remain a separate already completed smoke, not this profile's small-value distribution.

## Files and ownership

- `plan.py` dispatches explicit mode `sandbox_continuity` to `continuity_plan.py`. Allowed stage, volume, four sandbox topics, RF2/6h and sandbox-only endpoints/build references are validated exactly. Parent WP-A/WP-B rules are unchanged.
- `continuity-core.mjs`: pure shape checks, centered interleaved schedule, deterministic value/hash/ID verification and durable-ACK validation.
- `continuity-engine.mjs`: dependency-injected producer/consumer lifecycle. Offline checks supply fake clocks, in-memory producer/consumer classes and fake collector responses. It imports no Kafka or HTTP library.
- `continuity.js`: actual k6 entrypoint, with native Kafka/HTTP imports. This file is **not invoked during offline validation**.
- `continuity-entrypoint.sh`: future container entrypoint template, hashes the runtime binary before k6. It does not create a Job, arm a plan or invent credentials. Do not execute it for offline checks.
- `continuity_reconcile.py`: separate SQLite auditor for collector exports. It does not connect to Kafka, read cluster credentials or launch clients. Its `ACCOUNTING_COMPLETE` classification is narrower than a live continuity PASS.
- `tests/test_continuity_plan.py`, `tests/continuity.test.mjs`, `tests/check_continuity_export.py`: offline-only validation, including full logical-day schedule/lifecycle simulation and independent cross-language audit mutations.

Canonical utility source is the [kafka-load-test preflight directory](https://github.com/gennady73/kafka-load-test/tree/master/preflight). The coordinating workspace is not itself a Git repository; the planner/runner and demo configurations are published through their separate repositories. `kaas-demo/tests/continuity/` holds the scenario overlays, generated **disabled** plans and a hash-checked frozen runtime-source bundle for review/transfer. The frozen bundle is copied from these files; edit the canonical source and regenerate the bundle/plans together rather than maintaining two independent implementations.

## Offline preparation commands

Use sibling `kafka-load-test/` and `kaas-demo/` checkouts under a common directory (the coordinator uses `/home/gunger/Projects/kaas-solution`). The exact requirements baseline is published as `kaas-demo/tests/continuity/requirements-contract.json`; it is a non-runnable contract snapshot, not a new large-volume execution profile. The existing offline regression fixtures additionally expect that same snapshot at `config/preflight-profiles.json` in the common parent. No regression checks are executed merely by publishing it. From that common directory, choose a new output directory; the planner refuses to overwrite an existing plan:

```sh
python3 kafka-load-test/preflight/plan.py --contract kaas-demo/tests/continuity/requirements-contract.json --scenario kaas-demo/tests/continuity/scenarios/15m.json --offline --output /tmp/continuity-review-15m
python3 kafka-load-test/preflight/plan.py --contract kaas-demo/tests/continuity/requirements-contract.json --scenario kaas-demo/tests/continuity/scenarios/60m.json --offline --output /tmp/continuity-review-60m
python3 kafka-load-test/preflight/plan.py --contract kaas-demo/tests/continuity/requirements-contract.json --scenario kaas-demo/tests/continuity/scenarios/24h.json --offline --output /tmp/continuity-review-24h
```

All generated plans have `execution_ready=false` and `execution_enabled=false`, even if a scenario enables execution while using `--offline`. The supplied scenario files are disabled, use offline-prefixed IDs and have no collector, owner, start time or evidence references. Do not alter execution flags in generated JSON: the runner also validates the plan, hashes, fresh identity and runtime evidence fields.

Offline code checks (no actual k6 execution, native clients, HTTP or real-time waits):

```sh
python3 -m unittest discover -s kafka-load-test/preflight/tests -v
node kafka-load-test/preflight/tests/core.test.mjs
node kafka-load-test/preflight/tests/adapter.test.mjs
CONTINUITY_OFFLINE_FIXTURE=/tmp/continuity-fake-export.json node kafka-load-test/preflight/tests/continuity.test.mjs
python3 kafka-load-test/preflight/tests/check_continuity_export.py /tmp/continuity-fake-export.json
node --input-type=module --check < kafka-load-test/preflight/continuity.js
sh -n kafka-load-test/preflight/continuity-entrypoint.sh
```

Fake exports use `.invalid` collector names and in-memory callback responses: no DNS/HTTP access is performed. Fake-clock results do not measure native client timings, CPU/memory, network, broker durability or collector storage. A syntax/VM check of the live entrypoint is not a native-runtime check.

## Pacing and client lifecycle

One worker runs eight k6 scenario VUs: four producers, four consumers, one long-lived client per VU. They use fixed topic-specific identities and fresh run groups. This separation avoids serial empty consumer polls (the inspected compatibility API may spend ~1.5s per empty `consume`) delaying producer sends. A worker resource estimate from the previous single-VU smoke is a starting budget, not evidence that eight VUs fit; measure during the separately authorized 15m stage.

For stream ordinal t=0..3 and sequence j, due milliseconds are `(4*j+t+0.5)*duration_ms/total_records`. Producers retain absolute due times and stop if start lateness exceeds 5s; no unlimited catch-up or sending after generation end. Producers wait through the entire measured phase after their last record. Consumers poll continuously through setup, generation and bounded drain, verify each body against an independently regenerated expected value, and reject duplicate/current-run malformed IDs, wrong sizes/checksums or non-increasing partition offsets.

Before first send, all eight clients must appear in the collector's readiness barrier. Consumers only announce readiness after a usable partition-0 position is observed. Transient pre-assignment position queries are retried within the same fixed setup deadline without recreating the client. This method requires later selected-build runtime validation; offline stubs do not prove real assignment behavior. Both wall timestamps and a k6 monotonic elapsed clock are checked: wall/monotonic divergence above 2s aborts, rather than silently extending the day after a pause/clock jump. The reviewed UTC start time must match the externally supplied future epoch, which fits within the 120s setup envelope.

Each client sends a heartbeat every 30s. Producer ACK gaps, group receipt gaps and receive latency must stay <=60s during generation; the auditor also checks full-phase heartbeat coverage and end boundaries. Global stop uses k6 abort after failure/cleanup; missing terminal evidence remains incomplete. No process retry/resume is implemented. An unexpected interruption requires reconciliation and a new reviewed run ID.

## Durable collector contract — not yet deployed or verified

A reviewed **HTTPS** service must implement these interfaces before live execution. Existing generic console logs/metrics are not automatically this service. No collector URL or deployment is supplied by these preparations.

`POST <base>/events` body:

```json
{"event_json":"<exact compact JSON string of the event>","event_sha256":"<SHA-256 of those exact UTF-8 bytes>"}
```

Events have run/plan/client IDs, per-client contiguous sequence in event_id, topic/group, timestamp and measurement epoch. The sink must hash-check, persist durably, deduplicate by event_id (reject conflicting bodies), and **only then** return HTTP200/201:

```json
{"event_id":"<run>/<client>/<sequence>","event_sha256":"<matching digest>","durable":true}
```

The runner validates all three fields and fails on timeout, redirect, malformed response, mismatching hash or negative durability. No response is treated as persistence merely because a request was sent. Request timeout is 2s; no collector retry or silent queue is implemented. When a receipt's persistence ACK has been checked, the consumer calls `commitOffsets`, then persists the commit confirmation linked to that receipt's event_id. If receipt persistence fails, **no offset commit is issued for it**. A commit failure stops the run; native receipt already preserved does not imply the commit succeeded.

`GET <base>/runs/<run_id>/readiness?plan_sha256=<digest>` returns HTTP200 with:

```json
{"run_id":"<run>","plan_sha256":"<digest>","durable":true,"ready_clients":["producer_0","producer_1","producer_2","producer_3","consumer_0","consumer_1","consumer_2","consumer_3"]}
```

Return only durably recorded client_ready events for that run/plan, not observed sockets. Distinct client event sequences can be interleaved in exports. Collector export is NDJSON containing the exact POST envelopes. A separate seal must attest run_id, plan_sha256, complete=true, total event_count and client_event_counts for all eight clients. The independent auditor checks those counts, contiguous event IDs, exact record sets/hashes, progress/heartbeat gaps, commits after durable receipt evidence, close/end markers and timing. Export duplicates/conflicts are invalid; server retries should be idempotently deduplicated at storage.

A seal remains an **external storage/coverage assertion**. Verify its ownership/storage, raw export, Job exit/deadline evidence, runtime fingerprints, restart deltas and monitoring before declaring live PASS. A balanced console tail is insufficient. Without a matching seal the auditor cannot return ACCOUNTING_COMPLETE; even that classification does not prove production durability or capacity. There are four record events per ID (attempt, ACK, receipt, commit), plus readiness/heartbeats/lifecycle; budget evidence capacity using measured S2 event/export/SQLite sizes rather than payload volume alone.

Optional collector bearer authentication is read from `CONTINUITY_EVIDENCE_TOKEN_FILE`; no token belongs in JSON, arguments or repository. The current Kafka credential/CA mounts use `/credentials/password` and `/credentials/ca.crt`, with TLS hostname verification and SCRAM. Disable HTTP/native debug and never print connection settings. The corrected image's allowlisted native size/acks output remains safe; its specific digest alone does not carry these new JavaScript modules. Mount the frozen source bundle plus the reviewed plan and `kafka-client-limits.txt=3145728` explicitly in any later reviewed Job.

## Execution gates and remaining native limitations

No execution commands or runnable Jobs are generated here. Later arming needs a new scenario copy, unique non-offline run ID, seed, owner, valid UTC planned start, HTTPS collector, stop controls, and evidence references for effective settings/security, initial offsets, resources, collector durability, native API semantics and external Job deadline. `plan.py` without `--offline` checks explicit execution_enabled=true and the required fields; those are **presence/shape gates, not automated verification that evidence is true**.

The live entrypoint independently rejects disabled plans and requires CONTINUITY_EXECUTE=true, exact CONTINUITY_PLAN_SHA256, the actual startup binary fingerprint, every mounted JavaScript source hash, future epoch and matching declared external Job deadline. Configure the actual Job `activeDeadlineSeconds` as the plan's maximum_total_job_seconds, `backoffLimit=0`, `restartPolicy=Never`, a bounded termination grace and no ArgoCD sync hook/CronJob. An environment declaration alone does not prove a Job deadline; retain the API-observed Job spec/result. Scheduler termination grace is not a hard real-time guarantee: actual elapsed time/clean close must be checked, and forced expiry is incomplete.

Inspected selected extension source exposes position and commitOffsets and disables native auto-commit. **CommitOffsets checks context before synchronous native Commit, and Close is synchronous; neither supplies a cancellable per-call deadline to this JS adapter.** The runner detects returned overlong closes and uses a scenario/Job envelope, but cannot promise prompt interruption of a native call that never returns. Do not treat fake-clock close/commit tests as native deadline proof. Collector compatibility/durability, actual source mounts, position/commit/close behavior, eight-VU resources and live metrics remain OPEN for separately authorized short-stage validation. No source/image rebuild or live test was done in this adaptation.

Full-scale WP-A/WP-B configs remain deferred until the 100MB/24h stage succeeds. Passing continuity at low rate is not permission or capacity evidence for TB traffic on the three-broker/60Gi sandbox.

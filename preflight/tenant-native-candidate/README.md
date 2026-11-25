# Supplemental R2 native-boundary preparation

This directory is an **offline candidate and source-test aid**, separate from the accepted `preflight/tenant` adapter and C2's in-progress `kaas-demo/tests/tenant-r2-native` implementation. It is not an executable Kafka runner. There are no credential reads, Kafka imports, network calls or deployment switches in its JavaScript.

`native-contract.mjs` validates the accepted plan before compiling native option references for four positive streams and eight negative operations. It covers the position/byte conversion boundary, safe numeric offsets, exact topic/group/principal references, nanosecond timeout units and a fixed producer/evidence/cleanup reservation. Password and CA fields remain file references, not values or directly usable xk6 options. Typed authorization decoding accepts only the proposed native record schema/code/operation combination; arbitrary strings, fields and getters cannot prove a denial. `requireCurrentBuildReady` always rejects live use.

`extension/r2_error_boundary.go` and its test are additive helper candidates tested in an isolated copy of the corrected custom extension archive. They unwrap actual confluent-kafka-go errors and export only fixed numeric codes/stages. They are **not exported, wired into the old JS API, included in its image, or a replacement for C2's new module**. The existing API's raw exception logging and commit/close calls still need their own reviewed disposition.

Offline checks from the utility root:

```sh
node preflight/tenant-native-candidate/tests/native.test.mjs
```

The source-test helper requires the exact source archive `a0c283d9025ab416350856ee2e1c393101d04c51d03cb4db341d0bc1e2067212`, Go1.26.4, cached dependencies and CGO. Copy both Go files into an isolated archive extraction's `pkg/kafka`, then run only `go test -mod=readonly ./pkg/kafka -run '^TestR2' -count=1`. Tests build configuration maps and error objects; they construct no Kafka clients. Do not alter the historical build source or original custom checkout.

Authoritative preparation evidence and known gaps are indexed in the shared workspace at `docs/coordination/r2-native-preparation-2026-11-25/README.md`. No runtime qualification or publication is claimed.

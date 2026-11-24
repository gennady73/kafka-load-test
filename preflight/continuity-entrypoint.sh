#!/bin/sh
# Template entrypoint only. All supplied plans are offline and will be refused.
set -eu
: "${CONTINUITY_PLAN:?reviewed plan path required}"
: "${CONTINUITY_PLAN_SHA256:?exact reviewed plan hash required}"
: "${CONTINUITY_JOB_DEADLINE_SECONDS:?external Job active deadline required}"
: "${CONTINUITY_EXECUTE:?explicit later execution authorization required}"
[ "$CONTINUITY_EXECUTE" = true ] || exit 2
continuity_binary_hash=$(sha256sum /usr/local/bin/k6)
CONTINUITY_OBSERVED_BINARY_SHA256=${continuity_binary_hash%% *}
export CONTINUITY_OBSERVED_BINARY_SHA256
exec /usr/local/bin/k6 run --log-format=raw --no-usage-report --no-color --quiet /script/preflight/continuity.js

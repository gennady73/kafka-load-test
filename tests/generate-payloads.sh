#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="./payloads"
mkdir -p "${OUTPUT_DIR}"

generate_json_payload() {
    local label=$1
    local target_bytes=$2
    local output_file="${OUTPUT_DIR}/message-${label}.json"

    # 1. Define the static JSON pieces
    local prefix="{\"specVersion\":\"1.0\",\"type\":\"perf.test.event\",\"payloadSize\":\"${label}\",\"payload\":\""
    local suffix="\"}"

    # 2. Calculate exact lengths to hit the target byte size perfectly
    local prefix_len=${#prefix}
    local suffix_len=${#suffix}
    local wrapper_size=$((prefix_len + suffix_len))
    
    local padding_bytes=$((target_bytes - wrapper_size))
    
    if [ ${padding_bytes} -lt 1 ]; then
        padding_bytes=1
    fi

    echo "Generating ${output_file} (Target: ${target_bytes} bytes)..."

    # 3. Write prefix (using printf to prevent unwanted newlines)
    printf "%s" "${prefix}" > "${output_file}"
    
    # 4. Stream random characters directly to disk
    # FIX: Added '|| true' to safely catch the expected SIGPIPE when 'head' closes the pipe.
    tr -dc 'a-zA-Z0-9' < /dev/urandom | head -c "${padding_bytes}" >> "${output_file}" || true
    
    # 5. Append suffix
    printf "%s" "${suffix}" >> "${output_file}"

    # 6. Verify final size
    local actual_size
    actual_size=$(wc -c < "${output_file}" | tr -d ' ')
    echo " Finished  ${output_file} | Actual size: ${actual_size} bytes"
}

# --- Execute Payload Generations ---
generate_json_payload "256b" 256
generate_json_payload "1k"   1024
generate_json_payload "1m"   1048576
generate_json_payload "5m"   5242880

echo ""
echo "All test payloads generated successfully in ${OUTPUT_DIR}:"
ls -lh "${OUTPUT_DIR}"/*.json

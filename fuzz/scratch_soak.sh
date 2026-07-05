#!/usr/bin/env bash
# Sequential soak of all canonical targets. Override SOAK_SECONDS for longer
# runs.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
mkdir -p logs

SOAK_SECONDS="${SOAK_SECONDS:-600}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"

# LeakSanitizer cannot inspect /proc in some container sandboxes. Override
# ASAN_OPTIONS if leak detection is available in your environment.
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"

run_target() {
    local target="$1"
    local log="logs/soak-${target}-${RUN_ID}.log"

    echo "=== ${target} soak: ${SOAK_SECONDS}s -> ${log} ==="
    cargo +nightly fuzz run "${target}" -- -max_total_time="${SOAK_SECONDS}" >"${log}" 2>&1
    tail -n 3 "${log}"
}

run_target storage_snapshot_decode
run_target node_message_sequences
run_target cluster_schedules
run_target codec_decode

echo "=== ALL SOAKS DONE: run_id=${RUN_ID} seconds_per_target=${SOAK_SECONDS} ==="

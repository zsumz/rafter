#!/usr/bin/env bash
# C5 comparison harness: builds bench-compare/ (a standalone package outside
# the frozen root workspace), runs the rafter, raft-rs, and openraft
# benchmark binaries, merges their JSON reports into
# bench-compare/results/latest.json, and prints a comparison table.
#
# Usage:
#   scripts/bench-compare.sh
#
# Environment:
#   CARGO   Cargo executable. Defaults to cargo.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CARGO="${CARGO:-cargo}"
BENCH_DIR="$ROOT/bench-compare"
RESULTS_DIR="$BENCH_DIR/results"
OUT="$RESULTS_DIR/latest.json"

mkdir -p "$RESULTS_DIR"

echo "==> building bench-compare (release, --locked)"
(cd "$BENCH_DIR" && "$CARGO" build --release --locked)

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

for bin in bench-rafter bench-raft-rs bench-openraft; do
  echo "==> running $bin"
  "$BENCH_DIR/target/release/$bin" > "$TMP_DIR/$bin.json"
done

MACHINE="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"
OS_VERSION="$(sw_vers -productVersion 2>/dev/null || uname -sr)"
RUSTC_VERSION="$(rustc --version)"

MACHINE="$MACHINE" OS_VERSION="$OS_VERSION" RUSTC_VERSION="$RUSTC_VERSION" \
  OUT="$OUT" TMP_DIR="$TMP_DIR" python3 - <<'PY'
import json
import os

tmp = os.environ["TMP_DIR"]
results = []
for name in ("bench-rafter", "bench-raft-rs", "bench-openraft"):
    with open(os.path.join(tmp, name + ".json")) as f:
        results.append(json.load(f))

merged = {
    "harness": "bench-compare",
    "machine": os.environ["MACHINE"],
    "os": "macOS " + os.environ["OS_VERSION"],
    "rustc": os.environ["RUSTC_VERSION"],
    "results": results,
}
out = os.environ["OUT"]
with open(out, "w") as f:
    json.dump(merged, f, indent=2)
    f.write("\n")

print(f"\nwrote {out}\n")

header = (
    f'{"library":<10} {"workload":<10} {"proposals":>9} {"wall_ms":>10} '
    f'{"props/s":>10} {"p50_us":>10} {"p99_us":>10}'
)
print(header)
print("-" * len(header))
for report in results:
    for workload in report["workloads"]:
        latency = workload["commit_latency_us"]
        print(
            f'{report["library"]:<10} {workload["name"]:<10} '
            f'{workload["proposals"]:>9} {workload["elapsed_ms"]:>10.1f} '
            f'{workload["proposals_per_s"]:>10.0f} '
            f'{latency["p50"]:>10.1f} {latency["p99"]:>10.1f}'
        )
PY

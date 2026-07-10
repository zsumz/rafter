#!/usr/bin/env bash
# C5 comparison harness: builds bench-compare/ (a standalone package outside
# the frozen root workspace), runs the rafter, raft-rs, and openraft
# benchmark binaries, merges their JSON reports into a results file, and
# prints a comparison table.
#
# Usage:
#   scripts/bench-compare.sh                         # full comparison
#   BENCH_COMPARE_MODE=rafter-only scripts/bench-compare.sh
#
# Environment:
#   CARGO                Cargo executable. Defaults to cargo.
#   BENCH_COMPARE_MODE   `full` (default) or `rafter-only`.
#   BENCH_COMPARE_RUNS   Number of isolated runs per benchmark binary. Defaults to 5.
#   OUT                  Output JSON path. Defaults by mode.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CARGO="${CARGO:-cargo}"
MODE="${BENCH_COMPARE_MODE:-full}"
RUNS="${BENCH_COMPARE_RUNS:-5}"
BENCH_DIR="$ROOT/bench-compare"
RESULTS_DIR="$BENCH_DIR/results"

if ! [[ "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
  echo "BENCH_COMPARE_RUNS must be a positive integer, got: $RUNS" >&2
  exit 2
fi

configure_protoc() {
  if [[ "$MODE" != "full" || -n "${PROTOC:-}" ]] || command -v protoc >/dev/null 2>&1; then
    return
  fi

  local protoc_name
  case "$(uname -s)-$(uname -m)" in
    Linux-aarch64|Linux-arm64)
      protoc_name="protoc-linux-aarch_64"
      ;;
    Linux-x86_64|Linux-amd64)
      protoc_name="protoc-linux-x86_64"
      ;;
    Linux-i386|Linux-i686)
      protoc_name="protoc-linux-x86_32"
      ;;
    Darwin-x86_64)
      protoc_name="protoc-osx-x86_64"
      ;;
    *)
      return
      ;;
  esac

  # raft-rs' prost-codec path needs protoc at build-script time. If the
  # optional comparison dependencies are already fetched, protobuf-build
  # carries small pinned protoc binaries that are good enough for the harness.
  local cargo_home="${CARGO_HOME:-$HOME/.cargo}"
  local registry_src="$cargo_home/registry/src"
  if [[ ! -d "$registry_src" ]]; then
    return
  fi

  local candidate
  candidate="$(
    find "$registry_src" -path "*/protobuf-build-*/bin/$protoc_name" -type f 2>/dev/null \
      | sort \
      | tail -n 1
  )"
  if [[ -n "$candidate" && -x "$candidate" ]]; then
    export PROTOC="$candidate"
    echo "==> using cached protobuf-build protoc: $PROTOC"
  fi
}

case "$MODE" in
  full)
    OUT="${OUT:-$RESULTS_DIR/latest.json}"
    BUILD_ARGS=(build --release --locked)
    BINS=(bench-rafter bench-raft-rs bench-openraft)
    ;;
  rafter-only)
    OUT="${OUT:-$RESULTS_DIR/rafter-only.json}"
    BUILD_ARGS=(build --release --locked --no-default-features --bin bench-rafter --bin bench-rafter-service --bin bench-rafter-codec --bin bench-rafter-multiraft)
    BINS=(bench-rafter bench-rafter-service bench-rafter-codec bench-rafter-multiraft)
    ;;
  *)
    echo "unknown BENCH_COMPARE_MODE: $MODE" >&2
    exit 2
    ;;
esac

mkdir -p "$RESULTS_DIR"
configure_protoc

echo "==> building bench-compare ($MODE, release, --locked)"
(cd "$BENCH_DIR" && "$CARGO" "${BUILD_ARGS[@]}")

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

for run in $(seq 1 "$RUNS"); do
  bin_count="${#BINS[@]}"
  for offset in "${!BINS[@]}"; do
    bin="${BINS[$(((run - 1 + offset) % bin_count))]}"
    echo "==> running $bin ($run/$RUNS)"
    if [[ "$MODE" == "rafter-only" && "$bin" == "bench-rafter" ]]; then
      BENCH_RAFTER_EXTRA_WORKLOADS=1 "$BENCH_DIR/target/release/$bin" > "$TMP_DIR/run-$run-$bin.json"
    else
      "$BENCH_DIR/target/release/$bin" > "$TMP_DIR/run-$run-$bin.json"
    fi
  done
done

MACHINE="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"
if command -v sw_vers >/dev/null 2>&1; then
  OS_VERSION="macOS $(sw_vers -productVersion)"
else
  OS_VERSION="$(uname -sr)"
fi
RUSTC_VERSION="$(rustc --version)"

MACHINE="$MACHINE" OS_VERSION="$OS_VERSION" RUSTC_VERSION="$RUSTC_VERSION" \
  OUT="$OUT" TMP_DIR="$TMP_DIR" BINS="${BINS[*]}" RUNS="$RUNS" python3 - <<'PY'
import json
import os
from statistics import median

tmp = os.environ["TMP_DIR"]
run_count = int(os.environ["RUNS"])
bins = os.environ["BINS"].split()


def execution_order(run):
    return [bins[(run - 1 + offset) % len(bins)] for offset in range(len(bins))]


def load_run(run):
    results = []
    for name in bins:
        with open(os.path.join(tmp, f"run-{run}-{name}.json")) as f:
            results.append(json.load(f))
    return results


raw_runs = [
    {
        "run": run,
        "execution_order": execution_order(run),
        "results": load_run(run),
    }
    for run in range(1, run_count + 1)
]


def median_number(values):
    value = median(values)
    if all(isinstance(item, int) for item in values) and float(value).is_integer():
        return int(value)
    return round(float(value), 6)


def require_equal(values, field):
    first = values[0]
    if any(value != first for value in values[1:]):
        raise SystemExit(f"cannot aggregate benchmark reports with mismatched {field}: {values}")
    return first


MEDIAN_NUMERIC_PATHS = {
    ("commit_latency_us", "p50"),
    ("commit_latency_us", "p99"),
    ("read_shape", "read_latency_us", "p50"),
    ("read_shape", "read_latency_us", "p99"),
    ("codec_shape", "encoded_mb_per_s"),
}


def aggregate_object(samples, path=()):
    keys = sorted(samples[0].keys())
    for sample in samples[1:]:
        if sorted(sample.keys()) != keys:
            raise SystemExit("cannot aggregate benchmark reports with mismatched object keys")
    aggregated = {}
    for key in keys:
        field_path = path + (key,)
        values = [sample[key] for sample in samples]
        if all(isinstance(value, dict) for value in values):
            aggregated[key] = aggregate_object(values, field_path)
        elif all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
            if field_path in MEDIAN_NUMERIC_PATHS:
                aggregated[key] = median_number(values)
            else:
                aggregated[key] = require_equal(values, ".".join(field_path))
        else:
            aggregated[key] = require_equal(values, ".".join(field_path))
    return aggregated


def aggregate_run_summary(workloads):
    elapsed = [workload["elapsed_ms"] for workload in workloads]
    throughput = [workload["proposals_per_s"] for workload in workloads]
    return {
        "elapsed_ms": {
            "min": min(elapsed),
            "median": median_number(elapsed),
            "max": max(elapsed),
        },
        "proposals_per_s": {
            "min": min(throughput),
            "median": median_number(throughput),
            "max": max(throughput),
        },
    }


def aggregate_workload(samples):
    workload = {
        "name": require_equal([sample["name"] for sample in samples], "workload name"),
        "proposals": require_equal([sample["proposals"] for sample in samples], "proposal count"),
        "payload_bytes": require_equal([sample["payload_bytes"] for sample in samples], "payload bytes"),
        "max_in_flight": require_equal([sample["max_in_flight"] for sample in samples], "max in flight"),
        "elapsed_ms": median_number([sample["elapsed_ms"] for sample in samples]),
        "proposals_per_s": median_number([sample["proposals_per_s"] for sample in samples]),
        "commit_latency_us": aggregate_object(
            [sample["commit_latency_us"] for sample in samples],
            ("commit_latency_us",),
        ),
        "run_summary": aggregate_run_summary(samples),
    }
    optional_fields = [
        "shape",
        "service_shape",
        "read_shape",
        "codec_shape",
        "multiraft_shape",
        "failover_shape",
    ]
    for field in optional_fields:
        present = [field in sample for sample in samples]
        if any(present) and not all(present):
            raise SystemExit(f"cannot aggregate benchmark reports with inconsistent {field}")
        if all(present):
            workload[field] = aggregate_object([sample[field] for sample in samples], (field,))
    return workload


def aggregate_report(samples):
    report = {
        "harness": require_equal([sample["harness"] for sample in samples], "harness"),
        "library": require_equal([sample["library"] for sample in samples], "library"),
        "version": require_equal([sample["version"] for sample in samples], "version"),
        "commit_latency_definition": require_equal(
            [sample["commit_latency_definition"] for sample in samples],
            "commit latency definition",
        ),
    }
    workload_count = require_equal(
        [len(sample["workloads"]) for sample in samples],
        "workload count",
    )
    report["workloads"] = [
        aggregate_workload([sample["workloads"][index] for sample in samples])
        for index in range(workload_count)
    ]
    return report


results = [
    aggregate_report([run["results"][index] for run in raw_runs])
    for index in range(len(bins))
]

merged = {
    "harness": "bench-compare",
    "machine": os.environ["MACHINE"],
    "os": os.environ["OS_VERSION"],
    "rustc": os.environ["RUSTC_VERSION"],
    "run_count": run_count,
    "aggregation": {
        "kind": "single_run" if run_count == 1 else "median_of_runs",
        "runs": run_count,
        "unit": "one process invocation per benchmark binary",
        "summary": "results contains median values; runs contains raw per-run reports",
    },
    "results": results,
    "runs": raw_runs,
}
out = os.environ["OUT"]
with open(out, "w") as f:
    json.dump(merged, f, indent=2)
    f.write("\n")

print(f"\nwrote {out}")
print(f"aggregation: {merged['aggregation']['kind']} ({run_count} run{'s' if run_count != 1 else ''})\n")

header = (
    f'{"library":<15} {"workload":<13} {"proposals":>9} {"wall_ms":>10} '
    f'{"props/s":>10} {"p50_us":>10} {"p99_us":>10}'
)
print(header)
print("-" * len(header))
shape_rows = []
service_shape_rows = []
read_shape_rows = []
codec_shape_rows = []
multiraft_shape_rows = []
failover_shape_rows = []
for report in results:
    for workload in report["workloads"]:
        latency = workload["commit_latency_us"]
        print(
            f'{report["library"]:<15} {workload["name"]:<13} '
            f'{workload["proposals"]:>9} {workload["elapsed_ms"]:>10.1f} '
            f'{workload["proposals_per_s"]:>10.0f} '
            f'{latency["p50"]:>10.1f} {latency["p99"]:>10.1f}'
        )
        shape = workload.get("shape")
        if shape:
            shape_rows.append((report["library"], workload["name"], shape))
        service_shape = workload.get("service_shape")
        if service_shape:
            service_shape_rows.append((report["library"], workload["name"], service_shape))
        read_shape = workload.get("read_shape")
        if read_shape:
            read_shape_rows.append((report["library"], workload["name"], read_shape))
        codec_shape = workload.get("codec_shape")
        if codec_shape:
            codec_shape_rows.append((report["library"], workload["name"], codec_shape))
        multiraft_shape = workload.get("multiraft_shape")
        if multiraft_shape:
            multiraft_shape_rows.append((report["library"], workload["name"], multiraft_shape))
        failover_shape = workload.get("failover_shape")
        if failover_shape:
            failover_shape_rows.append((report["library"], workload["name"], failover_shape))

if shape_rows:
    print("\nshape counters")
    shape_header = (
        f'{"library":<15} {"workload":<13} {"app/proposal":>13} '
        f'{"entries/app":>12} {"mat/prop":>10} {"evals/entry":>13} '
        f'{"rounds/batch":>13} {"outputs/prop":>13}'
    )
    print(shape_header)
    print("-" * len(shape_header))
    for library, workload, shape in shape_rows:
        print(
            f'{library:<15} {workload:<13} '
            f'{shape["append_messages_per_proposal"]:>13.6f} '
            f'{shape["append_entries_per_append_message"]:>12.3f} '
            f'{shape["log_entry_materializations_per_proposal"]:>10.3f} '
            f'{shape["commit_evaluations_per_committed_entry"]:>13.6f} '
            f'{shape["leader_broadcast_rounds_per_proposal_batch"]:>13.3f} '
            f'{shape["outputs_per_proposal"]:>13.3f}'
        )

if service_shape_rows:
    print("\nservice shape counters")
    service_header = (
        f'{"library":<15} {"workload":<13} {"runtime/write":>13} '
        f'{"tracked/runtime":>15} {"applied/tracked":>15}'
    )
    print(service_header)
    print("-" * len(service_header))
    for library, workload, shape in service_shape_rows:
        print(
            f'{library:<15} {workload:<13} '
            f'{shape["runtime_batches_per_write_batch"]:>13.3f} '
            f'{shape["tracked_proposals_per_runtime_batch"]:>15.3f} '
            f'{shape["applied_writes_per_tracked_proposal"]:>15.3f}'
        )

if read_shape_rows:
    print("\nread-index shape counters")
    read_header = (
        f'{"library":<15} {"workload":<15} {"grants/read":>11} '
        f'{"rounds/read":>12} {"read_p50_us":>12} {"read_p99_us":>12}'
    )
    print(read_header)
    print("-" * len(read_header))
    for library, workload, shape in read_shape_rows:
        latency = shape["read_latency_us"]
        print(
            f'{library:<15} {workload:<15} '
            f'{shape["read_grants_per_request"]:>11.3f} '
            f'{shape["confirmation_rounds_per_request"]:>12.3f} '
            f'{latency["p50"]:>12.1f} {latency["p99"]:>12.1f}'
        )

if codec_shape_rows:
    print("\ncodec shape counters")
    codec_header = (
        f'{"library":<15} {"workload":<15} {"entries/frame":>13} '
        f'{"bytes/frame":>12} {"bytes/entry":>12} {"MB/s":>10} '
        f'{"alloc/frame":>12}'
    )
    print(codec_header)
    print("-" * len(codec_header))
    for library, workload, shape in codec_shape_rows:
        print(
            f'{library:<15} {workload:<15} '
            f'{shape["entries_per_frame"]:>13.3f} '
            f'{shape["encoded_bytes_per_frame"]:>12.1f} '
            f'{shape["encoded_bytes_per_entry"]:>12.1f} '
            f'{shape["encoded_mb_per_s"]:>10.1f} '
            f'{shape["allocations_per_frame"]:>12.3f}'
        )

if multiraft_shape_rows:
    print("\nmultiraft shape counters")
    multiraft_header = (
        f'{"library":<18} {"workload":<20} {"runtime/group":>14} '
        f'{"tracked/runtime":>15} {"applied/tracked":>15}'
    )
    print(multiraft_header)
    print("-" * len(multiraft_header))
    for library, workload, shape in multiraft_shape_rows:
        print(
            f'{library:<18} {workload:<20} '
            f'{shape["runtime_batches_per_group_batch"]:>14.3f} '
            f'{shape["tracked_proposals_per_runtime_batch"]:>15.3f} '
            f'{shape["applied_proposals_per_tracked_proposal"]:>15.3f}'
        )

if failover_shape_rows:
    print("\nfailover shape counters")
    failover_header = (
        f'{"library":<15} {"workload":<22} {"queued/failover":>15} '
        f'{"prefail/app":>12} {"successor/queued":>16} {"ticks/failover":>15}'
    )
    print(failover_header)
    print("-" * len(failover_header))
    for library, workload, shape in failover_shape_rows:
        print(
            f'{library:<15} {workload:<22} '
            f'{shape["queued_proposals_per_failover"]:>15.3f} '
            f'{shape["prefailover_append_messages_per_failover"]:>12.3f} '
            f'{shape["successor_applies_per_queued_proposal"]:>16.3f} '
            f'{shape["election_ticks_per_failover"]:>15.3f}'
        )
PY

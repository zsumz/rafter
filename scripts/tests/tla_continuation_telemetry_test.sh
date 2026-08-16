#!/usr/bin/env bash
# Adversarial checks for the TLA+ continuation sampler.
#
# The sampler observes a job that runs several TLC processes into one shared
# capture directory, and it runs nohup'd behind a multi-hour step where nothing
# notices if it dies. Both of those are checked here against stubbed process
# and filesystem tools: no TLC, no JVM, seconds rather than hours.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
sampler="$repo_root/scripts/tla-continuation-telemetry"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/rafter-continuation-telemetry-test.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT

CONTINUATION_CONFIG="RaftNightly.cfg"
failures=0

# --------------------------------------------------------------------------
# stubbed process table
#
# The stubs read a table of "pid pgid rss cpu argv" lines, so a scenario states
# which TLC processes exist simply by writing that file. `ps` answers per-pid
# queries against the table, and a process vanishes from the sampler
# perspective exactly the way a real one does when its row is removed.
# --------------------------------------------------------------------------
stub_bin="$scratch/bin"
mkdir -p "$stub_bin"
process_table="$scratch/process-table"
: >"$process_table"

cat >"$stub_bin/ps" <<'STUB'
#!/usr/bin/env bash
# ps -o FIELD= -p PID
field="" pid=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -o) field="${2%=}"; shift 2 ;;
        -p) pid="$2"; shift 2 ;;
        *) shift ;;
    esac
done
if [[ -n "${RAFTER_TEST_PS_FAILS:-}" && -f "$RAFTER_TEST_PS_FAILS" ]]; then
    echo "ps: stubbed transient failure" >&2
    exit 1
fi
awk -v pid="$pid" -v field="$field" '
    $1 == pid {
        if (field == "pgid") { print "  " $2 }
        else if (field == "rss") { print " " $3 }
        else if (field == "%cpu") { print " " $4 }
        else { $1 = ""; $2 = ""; $3 = ""; $4 = ""; sub(/^ +/, ""); print }
        found = 1
    }
    END { if (!found) exit 1 }
' "$RAFTER_TEST_PROCESS_TABLE"
STUB

# GNU du exits non-zero when a file it is walking vanishes underneath it, which
# is routine while TLC rotates a multi-GiB checkpoint generation.
cat >"$stub_bin/du" <<'STUB'
#!/usr/bin/env bash
if [[ -n "${RAFTER_TEST_DU_FAILS:-}" && -f "$RAFTER_TEST_DU_FAILS" ]]; then
    echo "du: cannot access: No such file or directory" >&2
    exit 1
fi
exec /usr/bin/env -u PATH PATH="$RAFTER_TEST_REAL_PATH" du "$@"
STUB

chmod +x "$stub_bin/ps" "$stub_bin/du"

export RAFTER_TEST_PROCESS_TABLE="$process_table"
export RAFTER_TEST_REAL_PATH="$PATH"
export PATH="$stub_bin:$PATH"

# --------------------------------------------------------------------------
# capture fixtures
#
# Named the way the invariant runner names them: PRODUCER_PID-SEQUENCE, with a
# .pgid receipt beside each .stdout, written the way the target-group launcher
# writes it: the first line is the PID the launcher publishes before exec --
# the same PID the target keeps through exec -- and `ready` lands after the
# runner anchors that process into a process group owned by a separate anchor
# process. The receipt therefore names a PID whose process group is never its
# own pid, and the process tables below keep that true: every target row
# carries an anchor group id distinct from its pid, exactly the shape a PGID
# comparison against the receipt can never bind.
# --------------------------------------------------------------------------
write_capture() {
    local directory="$1" prefix="$2" target_pid="$3" body="$4"
    mkdir -p "$directory"
    printf '%s\nready\n' "$target_pid" >"$directory/$prefix.pgid"
    printf '%s' "$body" >"$directory/$prefix.stdout"
}

progress_line() {
    printf '%s states generated, %s distinct states found, %s states left on queue\n' "$1" "$2" "$3"
}

# A drained proof obligation: small model, exhausted, TLC says so.
obligation_capture() {
    local text=""
    local index
    # Deliberately the largest capture in the directory. The sampler used to
    # pick by size, and an obligation that ran for half an hour before the
    # continuation started is exactly what that picked.
    for index in $(seq 1 400); do
        text+="$(progress_line "$((index * 1000))" "$((index * 100))" "$((400 - index))")"
    done
    text+="Model checking completed. No error has been found.
"
    printf '%s' "$text"
}

# The negative detector, whose entire purpose is to report a violation.
detector_capture() {
    printf '%s' "$(progress_line 500 100 0)
Error: Invariant RafterStateMachineSafety is violated.
"
}

# The continuation, at the moment it starts reporting progress.
continuation_capture() {
    progress_line 10000000 2000000 900000
}

# Appends progress lines the way TLC does, so the sampler observes a moving
# frontier rather than a static file. The frontier grows: this is the
# trajectory the report has to describe, and it is the one no auxiliary TLC in
# the job is on.
grow_continuation_from() {
    local file="$1" start="$2" ticks="$3" tick
    for tick in $(seq "$start" "$((start + ticks - 1))"); do
        sleep 1
        progress_line \
            "$((10000000 + tick * 10000000))" \
            "$((2000000 + tick * 2000000))" \
            "$((900000 + tick * 1000000))" >>"$file"
    done
}

grow_continuation() {
    grow_continuation_from "$1" 1 "$2"
}

run_scenario() {
    local label="$1" duration="$2"
    local telemetry="$scratch/$label/telemetry"
    local output="$scratch/$label/samples.jsonl"
    mkdir -p "$scratch/$label" "$scratch/$label/checkpoint"
    printf 'checkpoint payload\n' >"$scratch/$label/checkpoint/00000000.chkpt"

    "$sampler" sample \
        --config "$CONTINUATION_CONFIG" \
        --checkpoint "$scratch/$label/checkpoint" \
        --output "$output" \
        --interval 1 \
        --telemetry-dir "$telemetry" \
        >"$scratch/$label/sampler.log" 2>&1 &
    sampler_pid=$!
    sleep "$duration"
}

stop_sampler() {
    kill "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
}

summarize_status() {
    "$sampler" summarize --input "$1" --label test \
        | sed -n 's/^continuation status (test): \([a-z-]*\) .*/\1/p'
}

check() {
    local label="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        printf 'ok   %s\n' "$label"
    else
        printf 'FAIL %s: expected %s, got %s\n' "$label" "$expected" "$actual" >&2
        failures=$((failures + 1))
    fi
}

# --------------------------------------------------------------------------
# Scenario 1: an obligation drained before the continuation started.
#
# Its capture is exhausted, complete, and the largest file in the directory.
# The continuation is still expanding. The report must describe the
# continuation.
# --------------------------------------------------------------------------
scenario_obligation_precedes_continuation() {
    local label="obligation-first"
    local telemetry="$scratch/$label/telemetry"
    write_capture "$telemetry" "9000-0" 4101 "$(obligation_capture)"

    # Only the obligation is alive at first, and it is not the continuation.
    # Its /usr/bin/time resource wrapper runs beside it, in a group of its own,
    # with the entire java command visible in its arguments -- a command-line
    # scan matches the wrapper before the JVM, and the wrapper's pid is in no
    # receipt. The continuation's launcher has published its pid but not yet
    # reached `ready`: a mid-handshake receipt names a perl process that has
    # not exec'd the JVM.
    {
        printf '4100 4100 1500 0.0 /usr/bin/time -v java -cp tla2tools.jar tlc2.TLC -config RaftCoreObligationDeep.cfg Raft.tla\n'
        printf '4101 6000 900000 180 java -cp tla2tools.jar tlc2.TLC -config RaftCoreObligationDeep.cfg Raft.tla\n'
        printf '4107 6001 800 0.0 perl -e launcher\n'
    } >"$process_table"
    printf '4107\n' >"$telemetry/9000-1.pgid"
    : >"$telemetry/9000-1.stdout"
    run_scenario "$label" 2

    # The continuation is released and execs into the JVM: the receipt reaches
    # `ready`, the pid it named is now TLC, and its time wrapper -- lower pid,
    # same java command in its arguments -- sits beside it as bait.
    write_capture "$telemetry" "9000-1" 4107 "$(continuation_capture)"
    {
        printf '4106 4106 1500 0.0 /usr/bin/time -v java -cp tla2tools.jar tlc2.TLC -config %s Raft.tla\n' \
            "$CONTINUATION_CONFIG"
        printf '4107 6001 4000000 390 java -cp tla2tools.jar tlc2.TLC -config %s Raft.tla\n' \
            "$CONTINUATION_CONFIG"
    } >"$process_table"
    grow_continuation "$telemetry/9000-1.stdout" 8
    stop_sampler

    local output="$scratch/$label/samples.jsonl"
    check "$label classifies the continuation" \
        "incomplete-expanding" "$(summarize_status "$output")"
    if grep -q '"tlc_capture":"[^"]*9000-0.stdout"' "$output"; then
        printf 'FAIL %s: sampler read the obligation capture\n' "$label" >&2
        failures=$((failures + 1))
    else
        printf 'ok   %s never read the obligation capture\n' "$label"
    fi
    if grep -q '"tlc_capture":"[^"]*9000-1.stdout"' "$output"; then
        printf 'ok   %s bound the continuation capture\n' "$label"
    else
        printf 'FAIL %s: never bound the continuation capture\n' "$label" >&2
        failures=$((failures + 1))
    fi
    if grep -q '"tlc_pid":4107' "$output"; then
        printf 'ok   %s reported the TLC process the receipt names\n' "$label"
    else
        printf 'FAIL %s: never reported the receipt target pid\n' "$label" >&2
        failures=$((failures + 1))
    fi
    if grep -q '"tlc_pid":410[06]' "$output"; then
        printf 'FAIL %s: reported a resource wrapper as the continuation\n' "$label" >&2
        failures=$((failures + 1))
    else
        printf 'ok   %s never reported a resource wrapper\n' "$label"
    fi
}

# --------------------------------------------------------------------------
# Scenario 2: the negative detector runs alongside the continuation.
#
# The detector prints a violation by design. It must never become the
# continuation verdict.
# --------------------------------------------------------------------------
scenario_detector_violation_alongside_continuation() {
    local label="detector-negative"
    local telemetry="$scratch/$label/telemetry"
    write_capture "$telemetry" "9100-0" 4201 "$(detector_capture)"
    write_capture "$telemetry" "9100-1" 4207 "$(continuation_capture)"

    {
        printf '4201 6002 200000 90 java -cp tla2tools.jar tlc2.TLC -config RafterInvariantDetectorNegative.cfg RafterInvariantDetectorNegative.tla\n'
        printf '4206 4206 1500 0.0 /usr/bin/time -v java -cp tla2tools.jar tlc2.TLC -config %s Raft.tla\n' \
            "$CONTINUATION_CONFIG"
        printf '4207 6003 4000000 390 java -cp tla2tools.jar tlc2.TLC -config %s Raft.tla\n' \
            "$CONTINUATION_CONFIG"
    } >"$process_table"

    run_scenario "$label" 1
    grow_continuation "$telemetry/9100-1.stdout" 8
    stop_sampler

    local output="$scratch/$label/samples.jsonl"
    check "$label does not adopt the detector verdict" \
        "incomplete-expanding" "$(summarize_status "$output")"
    if grep -q '"tlc_verdict":"violated"' "$output"; then
        printf 'FAIL %s: a detector violation was recorded as the continuation verdict\n' "$label" >&2
        failures=$((failures + 1))
    else
        printf 'ok   %s recorded no violation\n' "$label"
    fi
}

# --------------------------------------------------------------------------
# Scenario 3: a sub-command fails mid-run.
#
# `ps` against an auxiliary TLC that just exited, and `du` against a checkpoint
# generation being retired, both exit non-zero. Under `set -euo pipefail` an
# unguarded substitution ends the sampler, and nothing notices for hours.
# --------------------------------------------------------------------------
scenario_transient_subcommand_failure() {
    local label="transient-failure"
    local telemetry="$scratch/$label/telemetry"
    write_capture "$telemetry" "9200-0" 4307 "$(continuation_capture)"
    {
        printf '4306 4306 1500 0.0 /usr/bin/time -v java -cp tla2tools.jar tlc2.TLC -config %s Raft.tla\n' \
            "$CONTINUATION_CONFIG"
        printf '4307 6004 4000000 390 java -cp tla2tools.jar tlc2.TLC -config %s Raft.tla\n' \
            "$CONTINUATION_CONFIG"
    } >"$process_table"

    export RAFTER_TEST_PS_FAILS="$scratch/$label/ps-fails"
    export RAFTER_TEST_DU_FAILS="$scratch/$label/du-fails"
    run_scenario "$label" 1
    grow_continuation "$telemetry/9200-0.stdout" 3

    local output="$scratch/$label/samples.jsonl"
    local before
    before="$(wc -l <"$output" | tr -d ' ')"

    touch "$RAFTER_TEST_PS_FAILS" "$RAFTER_TEST_DU_FAILS"
    sleep 2
    rm -f "$RAFTER_TEST_PS_FAILS" "$RAFTER_TEST_DU_FAILS"
    grow_continuation_from "$telemetry/9200-0.stdout" 5 4

    local alive="no"
    kill -0 "$sampler_pid" 2>/dev/null && alive="yes"
    stop_sampler

    check "$label leaves the sampler running" "yes" "$alive"

    local after
    after="$(wc -l <"$output" | tr -d ' ')"
    if ((after > before)); then
        printf 'ok   %s kept sampling after the failure\n' "$label"
    else
        printf 'FAIL %s: no samples after the failure (%s then %s)\n' \
            "$label" "$before" "$after" >&2
        failures=$((failures + 1))
    fi

    # Every line stays a closed JSON object; a degraded sample nulls a field
    # rather than truncating the record.
    if [[ "$(grep -cv '^{.*}$' "$output" || true)" == "0" ]]; then
        printf 'ok   %s wrote only closed records\n' "$label"
    else
        printf 'FAIL %s: wrote a malformed record\n' "$label" >&2
        failures=$((failures + 1))
    fi

    check "$label still classifies from the continuation" \
        "incomplete-expanding" "$(summarize_status "$output")"

    unset RAFTER_TEST_PS_FAILS RAFTER_TEST_DU_FAILS
}

# --------------------------------------------------------------------------
# Scenario 4: the continuation itself exhausts.
#
# Attribution must not cost a real verdict: TLC states it once and exits, and
# the ticks after that see no live process.
# --------------------------------------------------------------------------
scenario_continuation_exhausts() {
    local label="continuation-exhausted"
    local telemetry="$scratch/$label/telemetry"
    write_capture "$telemetry" "9300-0" 4407 "$(continuation_capture)"
    {
        printf '4406 4406 1500 0.0 /usr/bin/time -v java -cp tla2tools.jar tlc2.TLC -config %s Raft.tla\n' \
            "$CONTINUATION_CONFIG"
        printf '4407 6005 4000000 390 java -cp tla2tools.jar tlc2.TLC -config %s Raft.tla\n' \
            "$CONTINUATION_CONFIG"
    } >"$process_table"
    run_scenario "$label" 2

    # The runner always passes -tool, where TLC never prints "Model checking
    # completed": a finished search states its depth instead, and states it
    # only after exploring everything.
    {
        continuation_capture
        progress_line 40000000 8000000 0
        printf 'The depth of the complete state graph search is 97.\n'
    } >"$telemetry/9300-0.stdout"
    sleep 2
    : >"$process_table"
    sleep 2
    stop_sampler

    local output="$scratch/$label/samples.jsonl"
    check "$label reports the exhaustion TLC stated" \
        "exhausted" "$(summarize_status "$output")"
    if grep -q '"tlc_verdict":"exhausted"' "$output"; then
        printf 'ok   %s parsed the -tool completion signature\n' "$label"
    else
        printf 'FAIL %s: the -tool completion signature was not parsed\n' "$label" >&2
        failures=$((failures + 1))
    fi
}

# --------------------------------------------------------------------------
# Startup misconfiguration stays loud.
# --------------------------------------------------------------------------
scenario_missing_config_is_fatal() {
    local status=0
    "$sampler" sample \
        --checkpoint "$scratch/missing-config" \
        --output "$scratch/missing-config.jsonl" \
        >/dev/null 2>&1 || status=$?
    check "missing --config exits non-zero" "2" "$status"
}

scenario_obligation_precedes_continuation
scenario_detector_violation_alongside_continuation
scenario_transient_subcommand_failure
scenario_continuation_exhausts
scenario_missing_config_is_fatal

if ((failures > 0)); then
    printf '\n%s continuation telemetry check(s) failed\n' "$failures" >&2
    exit 1
fi
printf '\nall continuation telemetry checks passed\n'

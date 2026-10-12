#!/usr/bin/env python3
"""Verify counter workload accounting between caller-supplied quorum observations."""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path

from load import parse_json

MAX_RECORD = 16 * 1024
MAX_METADATA = 64 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def natural(value):
    return type(value) is int and value >= 0


def counter_value(value):
    return natural(value) and value < 2**64


def regular(path):
    require(path.is_file() and not path.is_symlink(), f"not a regular evidence file: {path}")


def metadata(path):
    regular(path)
    with path.open("rb") as stream:
        raw = stream.read(MAX_METADATA + 1)
    require(len(raw) <= MAX_METADATA, f"oversized metadata: {path}")
    value = parse_json(raw)
    require(isinstance(value, dict), f"metadata must be an object: {path}")
    return value, hashlib.sha256(raw).hexdigest()


def inspect(directory, baseline, final, run_id, expected_client, acknowledged, minimum_seconds):
    invocation, invocation_hash = metadata(directory / "invocation.json")
    summary, summary_hash = metadata(directory / "summary.json")
    metrics, metrics_hash = metadata(directory / "initial-metrics.json")
    require(invocation.get("run_id") == summary.get("run_id") == run_id, "capture run identity mismatch")
    node = invocation.get("node")
    require(type(node) is int and node in (1, 2, 3), "invalid node identity")
    require(type(invocation.get("schema")) is int and type(summary.get("schema")) is int
            and invocation["schema"] == summary["schema"] == 1, "unknown capture schema")
    require(type(summary.get("node")) is int and summary["node"] == node, "summary node mismatch")
    require(type(metrics.get("node")) is int and metrics["node"] == node and metrics.get("ready") is True,
            "initial ready-node observation mismatch")
    require(invocation.get("client_sha256") == expected_client, "client source hash mismatch")
    require(isinstance(invocation.get("host"), str) and invocation["host"], "missing host label")
    require(summary.get("status") == "completed" and summary.get("error") is None
            and summary.get("qualification") == "not_evaluated", "capture is incomplete or has an unknown verdict")
    read_every = invocation.get("read_every")
    require(type(read_every) is int and read_every >= 2, "invalid operation mix")
    cap = invocation.get("max_operations")
    require(cap is None or (natural(cap) and cap > 0), "invalid operation cap")
    require(type(summary.get("elapsed_seconds")) in (int, float)
            and math.isfinite(summary["elapsed_seconds"]) and summary["elapsed_seconds"] > 0,
            "invalid elapsed time")

    counts = Counter()
    number = total_latency = length = 0
    minimum = maximum = first_invocation = None
    previous_end = 0
    previous_value = baseline
    pending = None
    checksum = hashlib.sha256()
    path = directory / "history.jsonl"
    regular(path)
    with path.open("rb") as history:
        while raw := history.readline(MAX_RECORD + 1):
            require(len(raw) <= MAX_RECORD and raw.endswith(b"\n"), "oversized or truncated history record")
            checksum.update(raw)
            length += len(raw)
            event = parse_json(raw)
            require(isinstance(event, dict) and type(event.get("node")) is int and event["node"] == node,
                    "history node mismatch")
            require(event.get("run_id") == run_id, "history run identity mismatch")
            require(type(event.get("id")) is int and event["id"] == number + 1, "missing, duplicate or reordered operation")
            if pending is None:
                require(event.get("event") == "invoke", "completion without an invocation")
                writing = event["id"] % read_every != 0
                require(event.get("kind") == ("write" if writing else "read"), "operation mix mismatch")
                require((type(event.get("increment")) is int and event["increment"] == 1) if writing
                        else event.get("increment") is None, "unexpected increment")
                moment = event.get("monotonic_ns")
                require(natural(moment) and moment >= previous_end, "overlapping or regressing client invocation")
                first_invocation = moment if first_invocation is None else first_invocation
                pending = event
                continue

            require(event.get("event") == "complete", "invocation missing its completion")
            started, latency = event.get("started_monotonic_ns"), event.get("latency_ns")
            require(natural(started) and started >= pending["monotonic_ns"] and natural(latency),
                    "invalid completion timing")
            previous_end = started + latency
            outcome = event.get("outcome")
            allowed = ({"write_acknowledged", "write_refused", "write_uncertain"} if writing
                       else {"read_succeeded", "read_refused", "read_failed"})
            require(outcome in allowed, "outcome does not match operation kind")
            code = event.get("status")
            require(code is None or (type(code) is int and 100 <= code <= 599), "invalid HTTP status")
            if outcome in ("write_acknowledged", "read_succeeded"):
                value = event.get("value")
                require(code == 200 and "error" not in event and counter_value(value), "malformed successful observation")
                require(baseline <= value <= final, "observation lies outside supplied quorum boundaries")
                require(value > previous_value if writing else value >= previous_value,
                        "counter observation regressed within a client history")
                if writing:
                    require(value not in acknowledged, "duplicate acknowledged increment value")
                    acknowledged.add(value)
                previous_value = value
            else:
                require("value" not in event, "unsuccessful operation carries an acknowledged value")
                if outcome.endswith("refused"):
                    require(code in ((307, 503) if writing else (307,)), "unsupported refusal status")
                else:
                    require(isinstance(event.get("error"), str), "missing failure observation")
            counts[outcome] += 1
            number += 1
            total_latency += latency
            minimum = latency if minimum is None else min(minimum, latency)
            maximum = latency if maximum is None else max(maximum, latency)
            pending = None

    require(pending is None and number > 0, "incomplete or empty operation history")
    require(cap is None or number <= cap, "operation cap exceeded")
    require(summary.get("history_sha256") == checksum.hexdigest()
            and type(summary.get("history_bytes")) is int and summary["history_bytes"] == length,
            "history byte hash or length mismatch")
    for key in ("invoked", "completed"):
        require(type(summary.get(key)) is int and summary[key] == number, f"{key} count mismatch")
    require(type(summary.get("unresolved_invocations")) is int and summary["unresolved_invocations"] == 0,
            "unresolved capture cannot pass completed-history checks")
    require(summary.get("outcomes") == dict(counts)
            and all(type(value) is int for value in summary["outcomes"].values()), "outcome count mismatch")
    latencies = {"sum": total_latency, "min": minimum, "max": maximum}
    require(summary.get("latency_ns") == latencies
            and all(type(value) is int for value in summary["latency_ns"].values()), "latency summary mismatch")
    span = (previous_end - first_invocation) / 1e9
    require(span >= minimum_seconds, "recorded workload span is below the required duration")
    require(summary["elapsed_seconds"] >= span, "history span exceeds captured elapsed time")
    return {"node": node, "host_label": invocation["host"], "operations": number,
            "outcomes": dict(counts), "history_span_seconds": span,
            "input_sha256": {"invocation.json": invocation_hash, "summary.json": summary_hash,
                             "initial-metrics.json": metrics_hash, "history.jsonl": checksum.hexdigest()}}


def verify(histories, baseline, final, run_id, minimum_seconds=0):
    require(isinstance(run_id, str) and run_id, "missing expected run identity")
    require(counter_value(baseline) and counter_value(final) and final >= baseline, "invalid quorum boundaries")
    require(math.isfinite(minimum_seconds) and minimum_seconds >= 0, "invalid minimum duration")
    require(len(histories) == 3, "supply exactly three node histories")
    client_hash = hashlib.sha256(Path(__file__).with_name("load.py").read_bytes()).hexdigest()
    acknowledged = set()
    results = [inspect(Path(path), baseline, final, run_id, client_hash, acknowledged, minimum_seconds) for path in histories]
    require({result["node"] for result in results} == {1, 2, 3}, "histories must cover distinct nodes 1, 2, 3")
    counts = Counter()
    for result in results:
        counts.update(result["outcomes"])
    require(counts["write_acknowledged"] > 0 and counts["read_succeeded"] > 0, "no successful write/read progress")
    lower = baseline + counts["write_acknowledged"]
    upper = lower + counts["write_uncertain"]
    require(lower <= final <= upper, "final counter loses an acknowledgement or has unexplained increments")
    return {"schema": 1, "run_id": run_id, "status": "passed", "scope": "counter accounting and per-client observations",
            "baseline": baseline, "final": final, "allowed_final_range": [lower, upper],
            "outcomes": dict(counts), "client_sha256": client_hash,
            "minimum_history_span_seconds": minimum_seconds, "nodes": results,
            "limits": "Boundary values must be quorum reads taken before and after all clients. Requires no other writers. Does not establish cross-host real-time ordering, complete linearizability, server identity, durability after future crashes, or resource bounds."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--baseline", type=int, required=True)
    parser.add_argument("--final", type=int, required=True)
    parser.add_argument("--minimum-seconds", type=float, default=0)
    parser.add_argument("histories", type=Path, nargs=3)
    args = parser.parse_args()
    result = verify(args.histories, args.baseline, args.final, args.run_id, args.minimum_seconds)
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

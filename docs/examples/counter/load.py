#!/usr/bin/env python3
"""Capture a bounded, serial workload against this host's counter replica."""
import argparse
import asyncio
from collections import Counter
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import sys
import time

MAX_HEADER = 16 * 1024
MAX_BODY = 16 * 1024


async def exchange(port, method, path, timeout):
    """One loopback request with a whole-exchange deadline; never follow redirects."""
    async def once():
        reader, writer = await asyncio.open_connection("127.0.0.1", port, limit=MAX_HEADER)
        try:
            writer.write((f"{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                          "Connection: close\r\nContent-Length: 0\r\n\r\n").encode("ascii"))
            await writer.drain()
            header = await reader.readuntil(b"\r\n\r\n")
            if len(header) > MAX_HEADER:
                raise ValueError("response header exceeds limit")
            lines = header[:-4].decode("ascii").split("\r\n")
            version, status, *_ = lines.pop(0).split(" ", 2)
            if version not in ("HTTP/1.0", "HTTP/1.1") or not status.isdigit():
                raise ValueError("invalid HTTP status")
            fields = {}
            for line in lines:
                key, value = line.split(":", 1)
                key = key.lower()
                if key in fields:
                    raise ValueError("duplicate response header")
                fields[key] = value.strip()
            # The counter sends fixed-size JSON responses. Refuse ambiguous framing.
            if "transfer-encoding" in fields or "content-length" not in fields:
                raise ValueError("expected a fixed-length counter response")
            length = int(fields["content-length"])
            if not 0 <= length <= MAX_BODY:
                raise ValueError("response body exceeds limit")
            return int(status), await reader.readexactly(length)
        finally:
            writer.close()

    return await asyncio.wait_for(once(), timeout)


def parse_json(body):
    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON field")
            value[key] = item
        return value
    return json.loads(body, object_pairs_hook=unique_object)


def classify(writing, status, body):
    fallback = "write_uncertain" if writing else "read_failed"
    if status == 307:
        return {"outcome": "write_refused" if writing else "read_refused"}
    try:
        value = parse_json(body)
        if not isinstance(value, dict):
            raise ValueError("response is not an object")
        if status == 200:
            count = value.get("value")
            if type(count) is not int or not 0 <= count < 2**64:
                raise ValueError("response does not contain a counter value")
            return {"outcome": "write_acknowledged" if writing else "read_succeeded",
                    "value": count}
        if writing and status == 503 and value.get("write_may_have_committed") is False:
            return {"outcome": "write_refused", "error": str(value.get("error", ""))[:512]}
        return {"outcome": fallback, "error": str(value.get("error", "unexpected response"))[:512]}
    except (ValueError, UnicodeError) as error:
        return {"outcome": fallback, "error": str(error)[:512]}


async def operation(port, writing, timeout):
    started = time.monotonic_ns()
    try:
        status, body = await exchange(port, "POST" if writing else "GET",
                                      "/add/1" if writing else "/value", timeout)
        result = dict(classify(writing, status, body), status=status)
    except (OSError, ValueError, asyncio.TimeoutError, asyncio.IncompleteReadError,
            asyncio.LimitOverrunError) as error:
        result = {"outcome": "write_uncertain" if writing else "read_failed",
                  "error": f"{type(error).__name__}: {error}"[:512]}
    return dict(result, started_monotonic_ns=started, latency_ns=time.monotonic_ns() - started)


def save(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


async def run(args):
    args.output.mkdir()  # Existing results are never replaced, even after failure.
    port = args.http_base + args.node
    save(args.output / "invocation.json", {
        "schema": 1, "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "host": platform.node(), "platform": platform.platform(), "python": sys.version,
        "client_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "node": args.node, "endpoint": f"127.0.0.1:{port}", "seconds": args.seconds,
        "max_operations": args.max_operations, "timeout_seconds": args.timeout,
        "pause_ms": args.pause_ms, "read_every": args.read_every,
        "scope": "One local HTTP client; Raft peers use configured TLS endpoints. No capacity or global safety verdict.",
    })
    counts = Counter()
    invoked = completed = total_latency = 0
    minimum = maximum = None
    started = time.monotonic()
    status = "incomplete"
    failure = None
    history_hash = hashlib.sha256()
    history_bytes = 0
    with (args.output / "history.jsonl").open("xb") as history:
        def emit(event):
            nonlocal history_bytes
            data = (json.dumps(event, allow_nan=False) + "\n").encode()
            history.write(data)
            history.flush()  # Retain invocation before sending; no implicit retry.
            history_hash.update(data)
            history_bytes += len(data)

        try:
            code, body = await exchange(port, "GET", "/metrics", args.timeout)
            metrics = parse_json(body)
            if code != 200 or not isinstance(metrics, dict) or type(metrics.get("node")) is not int \
                    or metrics["node"] != args.node or metrics.get("ready") is not True:
                raise ValueError("local endpoint is not the requested ready counter node")
            save(args.output / "initial-metrics.json", metrics)
            started = time.monotonic()
            while time.monotonic() - started < args.seconds:
                if args.max_operations is not None and invoked >= args.max_operations:
                    break
                invoked += 1
                writing = invoked % args.read_every != 0
                emit({"event": "invoke", "id": invoked, "node": args.node,
                      "kind": "write" if writing else "read", "increment": 1 if writing else None,
                      "monotonic_ns": time.monotonic_ns()})
                result = await operation(port, writing, args.timeout)
                emit(dict(result, event="complete", id=invoked, node=args.node))
                completed += 1
                counts[result["outcome"]] += 1
                latency = result["latency_ns"]
                total_latency += latency
                minimum = latency if minimum is None else min(minimum, latency)
                maximum = latency if maximum is None else max(maximum, latency)
                remaining = args.seconds - (time.monotonic() - started)
                if remaining > 0 and args.pause_ms:
                    await asyncio.sleep(min(args.pause_ms / 1000, remaining))
            status = "completed"
        except BaseException as error:
            failure = f"{type(error).__name__}: {error}"[:512]
            raise
        finally:
            history.flush()
            os.fsync(history.fileno())
            save(args.output / "summary.json", {
                "schema": 1, "status": status, "qualification": "not_evaluated", "error": failure,
                "node": args.node, "elapsed_seconds": time.monotonic() - started,
                "invoked": invoked, "completed": completed, "unresolved_invocations": invoked - completed,
                "outcomes": dict(counts), "latency_ns": {"sum": total_latency, "min": minimum, "max": maximum},
                "history_sha256": history_hash.hexdigest(), "history_bytes": history_bytes,
            })
    return dict(counts)


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", type=int, choices=(1, 2, 3), required=True)
    parser.add_argument("--output", type=Path, required=True, help="new directory beneath an existing parent")
    parser.add_argument("--http-base", type=int, default=8000)
    parser.add_argument("--seconds", type=float, default=300)
    parser.add_argument("--max-operations", type=int)
    parser.add_argument("--timeout", type=float, default=6, help="whole HTTP exchange deadline, in seconds")
    parser.add_argument("--pause-ms", type=float, default=10, help="pause after each completion; no catch-up bursts")
    parser.add_argument("--read-every", type=int, default=5, help="every Nth operation is a quorum read")
    args = parser.parse_args(argv)
    if not 1 <= args.http_base <= 65532 or args.read_every < 2:
        parser.error("http-base must be 1..65532 and read-every must be at least 2")
    if any(not math.isfinite(value) or value <= 0 for value in (args.seconds, args.timeout)):
        parser.error("seconds and timeout must be positive and finite")
    if not math.isfinite(args.pause_ms) or args.pause_ms < 0:
        parser.error("pause-ms must be nonnegative and finite")
    if args.max_operations is not None and args.max_operations < 1:
        parser.error("max-operations must be positive")
    return args


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run(arguments())), sort_keys=True))

#!/usr/bin/env python3
"""Retain a local three-process TLS workload, leader-loss, and cold-restart trace."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time

from check_load import verify
from load import save
from smoke import Cluster

PROJECT = Path(__file__).resolve().parent


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def preserve_state(source, destination):
    if any(path.is_symlink() for path in source.rglob("*")):
        raise ValueError("refusing symlinks in owned counter state")
    shutil.copytree(source, destination)
    return {str(path.relative_to(destination)): {"bytes": path.stat().st_size, "sha256": digest(path)}
            for path in sorted(destination.rglob("*")) if path.is_file()}


def reap(processes):
    """Attempt every owned child even when another child cannot be reaped."""
    errors = []
    for child in processes:
        try:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=15)
        except (OSError, subprocess.TimeoutExpired) as error:
            errors.append(f"pid {child.pid}: {error}")
    return errors


def acknowledged_above(directory, value):
    with (directory / "history.jsonl").open("rb") as stream:
        for line in stream:
            if not line.endswith(b"\n"):
                break  # A live writer may not have flushed its last record yet.
            event = json.loads(line)
            if event.get("event") == "complete" and event.get("outcome") == "write_acknowledged" \
                    and event["value"] > value:
                return event
    return None


def run(args):
    if os.name != "posix":
        raise ValueError("this process-loss fixture requires POSIX SIGKILL semantics")
    args.output.mkdir()
    cluster_root = args.output / "cluster"
    cluster_root.mkdir()
    binary = args.output / "counter-binary"
    shutil.copy2(args.binary, binary)
    binary_hash = digest(binary)
    if binary_hash != digest(args.binary):
        raise ValueError("counter binary changed while copying")
    inputs = [PROJECT / name for name in ("soak.py", "smoke.py", "load.py", "check_load.py", "certs.sh")]
    inputs.extend(sorted((PROJECT / "src").rglob("*.rs")))
    inputs.extend(PROJECT / name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml"))
    before = {str(path.relative_to(PROJECT)): digest(path) for path in inputs}
    run_id = secrets.token_hex(16)
    save(args.output / "invocation.json", {
        "schema": 1, "run_id": run_id, "seconds": args.seconds,
        "binary_sha256": binary_hash, "example_source_sha256": before,
        "binary_provenance": "Executed bytes are copied and hashed; compiler and library-source provenance require the accompanying build receipt.",
        "scope": "Three local processes and direct loopback TLS; no multi-host, physical power-loss, peak-throughput or whole-host-bound claim.",
    })
    cluster = Cluster(binary.resolve(), cluster_root.resolve(), explicit_peers=True)
    clients = {}
    logs = []
    events = []
    failure = None
    complete = False
    baseline = final = recovered = None

    def record(event, **fields):
        value = dict(fields, event=event, run_id=run_id, monotonic_ns=time.monotonic_ns())
        events.append(value)
        with (args.output / "events.jsonl").open("a") as stream:
            stream.write(json.dumps(value) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        print(event, flush=True)

    def clients_live():
        for node, child in clients.items():
            if child.poll() is not None:
                raise RuntimeError(f"client {node} exited before the fault exercise: {child.returncode}")

    def wait_until(deadline):
        while time.monotonic() < deadline:
            clients_live()
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))

    try:
        with (args.output / "certificates.log").open("w") as log:
            subprocess.run(["sh", str(PROJECT / "certs.sh")], cwd=cluster_root,
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=60)
        initialized = cluster.command("init")
        (args.output / "initialization.stdout").write_text(initialized.stdout)
        (args.output / "initialization.stderr").write_text(initialized.stderr)
        initialized.check_returncode()
        for node in (1, 2, 3):
            cluster.start(node)
        leader = cluster.leader()
        baseline = cluster.request(leader, "/value")["value"]
        record("baseline_quorum_read", node=leader, value=baseline,
               processes={node: child.pid for node, child in cluster.children.items()},
               http_base=cluster.http_base, peer_addresses=cluster.env["COUNTER_PEER_ADDRS"])
        histories = [args.output / f"client-{node}" for node in (1, 2, 3)]
        for node, directory in enumerate(histories, 1):
            log = (args.output / f"client-{node}.log").open("w")
            logs.append(log)
            clients[node] = subprocess.Popen([
                sys.executable, str(PROJECT / "load.py"), "--run-id", run_id,
                "--node", str(node), "--http-base", str(cluster.http_base),
                "--seconds", str(args.seconds), "--pause-ms", "20", "--timeout", "6",
                "--output", str(directory.resolve())], stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 10
        while not all((path / "initial-metrics.json").exists() for path in histories):
            clients_live()
            if time.monotonic() > deadline:
                raise TimeoutError("all local clients did not become ready")
            time.sleep(0.05)
        started = time.monotonic()
        record("all_clients_ready", processes={node: child.pid for node, child in clients.items()})
        wait_until(started + args.seconds / 3)
        leader = cluster.leader()
        before_fault = cluster.request(leader, "/value")["value"]
        victim = cluster.children[leader]
        record("leader_kill_requested", node=leader, pid=victim.pid, value_before_fault=before_fault)
        cluster.stop(leader)
        if victim.returncode != -9:
            raise RuntimeError(f"leader did not exit by SIGKILL: {victim.returncode}")
        fault_started = time.monotonic()
        record("leader_killed", node=leader, pid=victim.pid, returncode=victim.returncode)
        preserved = preserve_state(cluster_root / f"data/node-{leader}", args.output / "leader-before-restart")
        save(args.output / "leader-before-restart.json", preserved)
        replacement = cluster.leader()
        if replacement == leader:
            raise RuntimeError("killed leader remained the observed leader")
        held_value = cluster.request(replacement, "/value")["value"]
        record("healthy_majority_quorum_read", node=replacement, value=held_value)
        ack = None
        deadline = time.monotonic() + 10
        while ack is None:
            clients_live()
            for node in cluster.children:
                ack = acknowledged_above(histories[node - 1], held_value)
                if ack is not None:
                    break
            if time.monotonic() > deadline:
                raise TimeoutError("healthy majority did not acknowledge fresh client work")
            time.sleep(0.05)
        record("healthy_majority_acknowledged", acknowledgement=ack)
        wait_until(fault_started + 2)
        cluster.start(leader)
        record("leader_restarted", node=leader, pid=cluster.children[leader].pid,
               pre_recovery_files=len(preserved))
        for node, child in clients.items():
            child.wait(timeout=max(1, started + args.seconds + 15 - time.monotonic()))
            if child.returncode != 0:
                raise RuntimeError(f"client {node} failed: {child.returncode}")
        record("clients_completed", returncodes={node: child.returncode for node, child in clients.items()})
        leader = cluster.leader()
        final = cluster.request(leader, "/value")["value"]
        cluster.converged(final)
        record("final_quorum_read", node=leader, value=final)
        report = verify(histories, baseline, final, run_id, minimum_seconds=args.seconds - 1)
        save(args.output / "history-check.json", report)
        stopped = dict(cluster.children)
        for node in list(cluster.children):
            cluster.stop(node, graceful=True)
        record("all_servers_stopped", kind="graceful backend release before cold restart",
               processes={node: {"pid": child.pid, "returncode": child.returncode} for node, child in stopped.items()})
        state = preserve_state(cluster_root / "data", args.output / "before-cold-restart")
        save(args.output / "before-cold-restart.json", state)
        for node in (1, 2, 3):
            cluster.start(node)
        leader = cluster.leader()
        recovered = cluster.request(leader, "/value")["value"]
        if recovered != final:
            raise RuntimeError(f"cold recovery changed acknowledged state: {final} -> {recovered}")
        cluster.converged(recovered)
        record("cold_recovery_quorum_read", node=leader, value=recovered,
               processes={node: child.pid for node, child in cluster.children.items()})
        fresh = cluster.request(leader, "/add/1", "POST")["value"]
        if fresh != recovered + 1:
            raise RuntimeError("new admission after cold recovery returned an unexpected value")
        cluster.converged(fresh)
        record("post_recovery_increment", node=leader, value=fresh)
        stopped = dict(cluster.children)
        for node in list(cluster.children):
            cluster.stop(node, graceful=True)
        record("final_servers_stopped", processes={node: {"pid": child.pid, "returncode": child.returncode}
                                                   for node, child in stopped.items()})
        complete = True
    except BaseException as error:
        failure = f"{type(error).__name__}: {error}"[:4096]
        raise
    finally:
        errors = reap(clients.values()) + reap(cluster.children.values())
        for log in logs + cluster.logs:
            log.close()
        preserved = digest(binary) == binary_hash and all(digest(path) == before[str(path.relative_to(PROJECT))] for path in inputs)
        passed = complete and not errors and preserved
        save(args.output / "receipt.json", {
            "schema": 1, "run_id": run_id, "status": "passed" if passed else "failed",
            "error": failure, "cleanup_errors": errors, "source_and_binary_preserved": preserved,
            "baseline": baseline, "final": final, "cold_recovered": recovered,
            "events_sha256": digest(args.output / "events.jsonl") if events else None,
            "scope": "Local three-process TLS leader loss and graceful cold restart. History accounting is partial; this is not a multi-host or power-loss qualification.",
        })
        if complete and not passed:
            raise RuntimeError("cleanup or source preservation failed")


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=lambda value: Path(value).resolve(), required=True)
    parser.add_argument("--output", type=lambda value: Path(value).resolve(), required=True)
    parser.add_argument("--seconds", type=float, default=60)
    args = parser.parse_args(argv)
    if not math.isfinite(args.seconds) or args.seconds < 30:
        parser.error("seconds must be finite and at least 30")
    return args


if __name__ == "__main__":
    run(arguments())

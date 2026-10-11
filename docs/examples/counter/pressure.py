#!/usr/bin/env python3
"""Bounded client admission while a real application's publication I/O is blocked."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.error

from smoke import Cluster


def refused(cluster, leader, path, method="POST"):
    try:
        cluster.request(leader, path, method, timeout=8)
    except urllib.error.HTTPError as error:
        assert error.code == 503, error
        return json.load(error)
    raise AssertionError(f"operation unexpectedly succeeded: {path}")


def exercise(binary, root):
    cluster = Cluster(binary, root)
    reader = None
    try:
        with open(root / "certificates.log", "w") as log:
            subprocess.run(["sh", str(Path(__file__).with_name("certs.sh"))], cwd=root,
                           stdout=log, stderr=log, check=True, timeout=60)
        initialized = cluster.command("init")
        assert initialized.returncode == 0, initialized.stderr
        for node in (1, 2, 3):
            cluster.start(node)
        leader = cluster.leader()
        peers = [node for node in cluster.children if node != leader]
        baseline = cluster.request(leader, "/metrics")
        obstacle = root / f"data/node-{leader}/counter.tmp"
        saved_counter = obstacle.with_name("counter").read_bytes()
        held_fifo = root / "blocked-publication.fifo"
        # Opening a FIFO for writing blocks the application worker until a
        # reader appears. No production hook or Raft thread is paused.
        os.mkfifo(obstacle, 0o600)
        for count in range(1, 9):
            error = refused(cluster, leader, "/add/1")
            assert error["write_may_have_committed"]
            assert "timed out" in error["error"], error
            metrics = cluster.request(leader, "/metrics")
            assert metrics["pending_writes"] == count, metrics
            assert metrics["applied"] == baseline["applied"], metrics
            assert metrics["role"] == "Leader" and metrics["term"] == baseline["term"], metrics
            cluster.wait("healthy followers apply committed work", lambda:
                         all(cluster.request(peer, "/status")["local_value"] == count
                             for peer in peers))
            print(f"PASS: {count}/8 canceled callers retain admission; Raft and metrics remain live", flush=True)

        started = time.monotonic()
        error = refused(cluster, leader, "/add/1")
        assert time.monotonic() - started < 2, "full queue blocked instead of refusing"
        assert not error["write_may_have_committed"], error
        metrics = cluster.request(leader, "/metrics")
        assert metrics["pending_writes"] == 8 and metrics["role"] == "Leader", metrics
        error = refused(cluster, leader, "/value", "GET")
        assert "timed out" in error["error"], error
        metrics = cluster.request(leader, "/metrics")
        assert metrics["reserved_reads"] == 1 and metrics["applied"] == baseline["applied"], metrics
        print("PASS: full writes refuse promptly; quorum read remains behind application durability", flush=True)

        with ThreadPoolExecutor(max_workers=1) as executor:
            stopping = executor.submit(cluster.request, leader, "/stop", "POST", 8)
            cluster.wait("owner begins draining", lambda: any(
                "draining durable work" in path.read_text()
                for path in root.glob(f"node-{leader}-*.log")), timeout=3)
            assert not stopping.done(), "shutdown finished before the blocked worker was released"
            assert cluster.children[leader].poll() is None
            # Some systems accept FIFO fsync. Force the later atomic rename to
            # fail as well, without changing the old counter record: a directory
            # cannot replace that regular file. The blocked open retains its FIFO.
            obstacle.rename(held_fifo)
            obstacle.mkdir()
            reader = os.open(held_fifo, os.O_RDONLY | os.O_NONBLOCK)
            assert stopping.result(timeout=10) == {"stopping": leader}
        child = cluster.children[leader]
        child.wait(timeout=15)
        assert child.returncode != 0, "publication failure was hidden"
        del cluster.children[leader]
        assert obstacle.with_name("counter").read_bytes() == saved_counter
        os.close(reader)
        reader = None
        obstacle.rmdir()
        held_fifo.unlink()
        print("PASS: shutdown waits for blocked publication and failure releases the backend", flush=True)

        cluster.start(leader)
        cluster.converged(8)
        active = cluster.leader()
        assert cluster.request(active, "/add/1", "POST") == {"value": 9}
        cluster.converged(9)
        for node in cluster.children:
            assert cluster.request(node, "/metrics")["pending_writes"] == 0
        for node in list(cluster.children):
            cluster.stop(node, graceful=True)
        print("PASS: recovery replays all eight committed writes and new admission succeeds", flush=True)
    except BaseException:
        for path in sorted(root.glob("node-*.log")):
            print(f"{path.name}:\n{path.read_text()[-3000:]}", file=sys.stderr)
        raise
    finally:
        if reader is not None:
            os.close(reader)
        cluster.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python3 pressure.py /path/to/rafter-counter (POSIX FIFO required)")
    with tempfile.TemporaryDirectory(prefix="rafter-counter-pressure-") as directory:
        exercise(Path(sys.argv[1]).resolve(), Path(directory))

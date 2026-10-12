#!/usr/bin/env python3
"""Exercise the standalone binary with real processes, TLS, and crash recovery."""
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zlib


def free_base(exclude=None):
    for _ in range(100):
        base = 20000 + secrets.randbelow(20000)
        if exclude is not None and abs(base - exclude) < 4:
            continue
        held = []
        try:
            for node in (1, 2, 3):
                sock = socket.socket()
                held.append(sock)
                sock.bind(("127.0.0.1", base + node))
            return base
        except OSError:
            pass
        finally:
            for sock in held:
                sock.close()
    raise RuntimeError("no free local port range")


class Cluster:
    def __init__(self, binary, root, explicit_peers=False):
        self.binary, self.root = binary, root
        self.http_base = free_base()
        self.peer_base = free_base(self.http_base)
        self.env = dict(os.environ, COUNTER_HTTP_BASE=str(self.http_base),
                        COUNTER_PEER_BASE=str(self.peer_base))
        # Qualification must never inherit a caller's remote peer destinations.
        self.env.pop("COUNTER_PEER_ADDRS", None)
        self.peer_ports = {node: self.peer_base + node for node in (1, 2, 3)}
        if explicit_peers:
            # Reverse placement to detect any remaining node-ID/base-port assumption.
            self.peer_ports = {node: self.peer_base + 4 - node for node in (1, 2, 3)}
            self.env["COUNTER_PEER_ADDRS"] = ",".join(
                f"127.0.0.1:{self.peer_ports[node]}" for node in (1, 2, 3))
            self.env["COUNTER_PEER_BASE"] = "unused-with-explicit-addresses"
        self.children, self.logs = {}, []

    def command(self, *args):
        return subprocess.run([str(self.binary), *args], cwd=self.root,
                              env=self.env, capture_output=True, text=True, timeout=15)

    def start(self, node):
        log = open(self.root / f"node-{node}-{len(self.logs)}.log", "w")
        self.logs.append(log)
        self.children[node] = subprocess.Popen(
            [str(self.binary), "node", str(node)], cwd=self.root,
            env=self.env, stdout=log, stderr=log)

    def stop(self, node, graceful=False):
        child = self.children[node]
        if graceful:
            assert self.request(node, "/stop", "POST")["stopping"] == node
        else:
            child.kill()
        child.wait(timeout=15)
        if graceful:
            assert child.returncode == 0, f"node {node} did not shut down cleanly"
        # Keep cleanup ownership if the HTTP request or bounded wait fails.
        del self.children[node]

    def request(self, node, path, method="GET", timeout=2):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.http_base + node}{path}", method=method)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)

    def statuses(self):
        return {node: self.request(node, "/status") for node in self.children}

    def wait(self, description, predicate, timeout=30):
        deadline, last = time.monotonic() + timeout, None
        while time.monotonic() < deadline:
            for node, child in self.children.items():
                if child.poll() is not None:
                    raise AssertionError(f"node {node} exited with {child.returncode}")
            try:
                last = predicate()
                if last:
                    return last
            except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
                last = str(error)
            time.sleep(0.05)
        raise AssertionError(f"timed out: {description}; last={last}")

    def leader(self):
        def ready_leader():
            states = self.statuses()
            leaders = [node for node, state in states.items() if state["role"] == "Leader"]
            if len(leaders) == 1 and all(s["ready"] and s["tls_connections"] > 0
                                         for s in states.values()):
                # Election status can precede the leader's usable read barrier.
                # Retry this read in wait(); never retry an uncertain increment.
                self.request(leaders[0], "/value")
                return leaders[0]
        return self.wait("one leader and authenticated connections", ready_leader)

    def converged(self, value):
        return self.wait(f"all saved counters reach {value}", lambda:
                         all(s["local_value"] == value and s["applied"] > 0
                             for s in self.statuses().values()))

    def refuses_without_client_certificate(self, node):
        context = ssl.create_default_context(cafile=str(self.root / "certs/ca.pem"))
        context.verify_flags |= ssl.VERIFY_X509_STRICT
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.set_alpn_protocols(["rafter/1"])
        try:
            with socket.create_connection(("127.0.0.1", self.peer_ports[node]), timeout=3) as raw:
                with context.wrap_socket(raw, server_hostname=f"node-{node}") as secure:
                    secure.sendall(b"no client certificate")
                    secure.recv(1)
        except ssl.SSLError as error:
            assert any(reason in str(error) for reason in
                       ("CERTIFICATE_REQUIRED", "HANDSHAKE_FAILURE", "BAD_CERTIFICATE")), error
            return
        raise AssertionError("peer endpoint did not require a client certificate")

    def reject_missing_or_corrupt(self, node, name):
        path = self.root / f"data/node-{node}" / name
        original = path.read_bytes()
        try:
            path.unlink()
            missing = self.command("node", str(node))
            assert missing.returncode != 0, f"accepted missing {name}"
            path.write_bytes(b"corrupt record\n")
            corrupt = self.command("node", str(node))
            assert corrupt.returncode != 0, f"accepted corrupt {name}"
        finally:
            path.write_bytes(original)

    def reject_checkpoint_policy(self, node):
        path = self.root / f"data/node-{node}/checkpoint"
        original = path.read_bytes()
        saved = json.loads(original[8:])
        variants = [
            dict(saved, high_water=None, current=None),
            dict(saved, high_water=2),
            dict(saved, high_water=4),
            dict(saved, current=[0, [1, 2]]),
            dict(saved, current=[0, [1, 2, 3, 3]]),
            dict(saved, current=[2**64 - 1, [1, 2, 3]]),
            dict(saved, contradicted=0),
        ]
        try:
            for record in variants:
                payload = json.dumps(record).encode()
                path.write_bytes(b"RCT1" + zlib.crc32(payload).to_bytes(4, "big") + payload)
                rejected = self.command("node", str(node))
                assert rejected.returncode != 0, f"accepted unsafe checkpoint: {record}"
                assert "checkpoint" in rejected.stderr, rejected.stderr
        finally:
            path.write_bytes(original)

    def fail_application_save(self, node):
        obstacle = self.root / f"data/node-{node}/counter.tmp"
        obstacle.mkdir()
        try:
            try:
                self.request(node, "/add/1", "POST", timeout=8)
                raise AssertionError("acknowledged a failed application save")
            except urllib.error.HTTPError as error:
                assert error.code == 503
                assert json.load(error)["write_may_have_committed"]
            child = self.children[node]
            child.wait(timeout=15)
            assert child.returncode != 0, "failed application owner kept serving"
            del self.children[node]
        finally:
            obstacle.rmdir()

    def close(self):
        for child in self.children.values():
            if child.poll() is None:
                child.kill()
            child.wait(timeout=15)
        for log in self.logs:
            log.close()


def exercise(binary, root, explicit_peers=False):
    cluster = Cluster(binary, root, explicit_peers)
    try:
        shutil.copyfile(Path(__file__).with_name("certs.sh"), root / "certs.sh")
        with open(root / "certificates.log", "w") as log:
            subprocess.run(["sh", "certs.sh"], cwd=root, stdout=log, stderr=log,
                           check=True, timeout=60)
        initialized = cluster.command("init")
        assert initialized.returncode == 0, initialized.stderr
        assert cluster.command("init").returncode != 0, "init replaced an existing cluster"
        for node in (1, 2, 3):
            cluster.start(node)
        leader = cluster.leader()
        duplicate = cluster.command("node", str(leader))
        assert duplicate.returncode != 0 and "AlreadyOpen" in duplicate.stderr, duplicate.stderr
        assert cluster.request(leader, "/add/5", "POST") == {"value": 5}
        cluster.converged(5)
        follower = next(node for node in cluster.children if node != leader)
        assert cluster.request(follower, "/value") == {"value": 5}
        cluster.refuses_without_client_certificate(leader)
        print("PASS: three processes, acknowledged writes, redirected reads, mutual TLS", flush=True)

        cluster.stop(leader)
        replacement = cluster.leader()
        assert replacement != leader
        assert cluster.request(replacement, "/add/3", "POST") == {"value": 8}
        cluster.converged(8)
        cluster.start(leader)
        cluster.converged(8)
        print("PASS: leader loss, continued writes, restarted follower catch-up", flush=True)

        victim = next(node for node in cluster.children if node != cluster.leader())
        cluster.stop(victim, graceful=True)
        for artifact in ("counter", "checkpoint", "sessions", "identity"):
            cluster.reject_missing_or_corrupt(victim, artifact)
        cluster.reject_checkpoint_policy(victim)
        cluster.start(victim)
        cluster.converged(8)
        print("PASS: missing, corrupt, or unsafe membership state refuses startup", flush=True)

        for node in list(cluster.children):
            cluster.stop(node)
        for node in (1, 2, 3):
            cluster.start(node)
        leader = cluster.leader()
        assert cluster.request(leader, "/value") == {"value": 8}
        assert cluster.request(leader, "/add/2", "POST") == {"value": 10}
        cluster.converged(10)
        print("PASS: all-process crash and recovery preserve acknowledged writes", flush=True)
        peers = [node for node in cluster.children if node != leader]
        for node in peers:
            cluster.stop(node)
        try:
            cluster.request(leader, "/add/1", "POST", timeout=8)
            raise AssertionError("acknowledged a write without a quorum")
        except urllib.error.HTTPError as error:
            assert error.code == 503
            assert json.load(error)["write_may_have_committed"]
        assert cluster.request(leader, "/status")["node"] == leader
        for node in peers:
            cluster.start(node)
        leader = cluster.leader()
        # A local leader status can precede a usable read barrier after an election.
        # Retrying this read is safe; never retry the uncertain increment above.
        value = cluster.wait("linearizable read after quorum recovery", lambda:
                             cluster.request(leader, "/value"))["value"]
        assert value in (10, 11), value
        cluster.converged(value)
        print("PASS: quorum loss returns an error without claiming a write was undone", flush=True)
        leader = cluster.leader()
        cluster.fail_application_save(leader)
        cluster.start(leader)
        cluster.converged(value + 1)
        print("PASS: failed application save closes clients; restart replays the committed write", flush=True)
        for node in list(cluster.children):
            cluster.stop(node, graceful=True)
        print("PASS: clean shutdown; standalone counter smoke passed", flush=True)
    except BaseException:
        for path in sorted(root.glob("node-*.log")):
            print(f"{path.name}:\n{path.read_text()[-3000:]}", file=sys.stderr)
        raise
    finally:
        cluster.close()


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3) or (len(sys.argv) == 3 and sys.argv[2] != "--explicit-peers"):
        raise SystemExit("usage: python3 smoke.py /path/to/rafter-counter [--explicit-peers]")
    with tempfile.TemporaryDirectory(prefix="rafter-counter-") as directory:
        exercise(Path(sys.argv[1]).resolve(), Path(directory), len(sys.argv) == 3)

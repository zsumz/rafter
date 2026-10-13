#!/usr/bin/env python3
"""Verify periodic pruning, offline TLS snapshot catch-up, and cold recovery."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

from smoke import Cluster


def exercise(binary, root):
    cluster = Cluster(binary, root)
    cluster.env["COUNTER_SNAPSHOT_ENTRIES"] = "4"
    try:
        shutil.copyfile(Path(__file__).with_name("certs.sh"), root / "certs.sh")
        with (root / "certificates.log").open("w") as log:
            subprocess.run(["sh", "certs.sh"], cwd=root, stdout=log, stderr=log,
                           check=True, timeout=60)
        initialized = cluster.command("init")
        assert initialized.returncode == 0, initialized.stderr
        for invalid in ("0", "-1", "not-a-number", "18446744073709551616"):
            cluster.env["COUNTER_SNAPSHOT_ENTRIES"] = invalid
            result = cluster.command("node", "1")
            assert result.returncode != 0, f"accepted invalid snapshot interval {invalid}"
        cluster.env["COUNTER_SNAPSHOT_ENTRIES"] = "4"
        for node in (1, 2, 3):
            cluster.start(node)
        leader = cluster.leader()
        for value in (1, 2):
            assert cluster.request(leader, "/add/1", "POST") == {"value": value}
        cluster.converged(2)
        victim = next(node for node in cluster.children if node != leader)
        before = cluster.request(victim, "/metrics")
        cluster.stop(victim, graceful=True)
        for value in range(3, 19):
            assert cluster.request(leader, "/add/1", "POST") == {"value": value}
        cluster.converged(18)

        def compacted():
            metrics = {node: cluster.request(node, "/metrics") for node in cluster.children}
            if not all(row["snapshot"] >= before["applied"] + 8 for row in metrics.values()):
                return None
            for node in metrics:
                files = list((root / f"data/node-{node}/raft/snapshots").glob("snapshot-*.rfsn"))
                if len(files) != 1:
                    return None
            return metrics

        majority = cluster.wait("majority compacts and retains only its current snapshot", compacted)
        # Disable this replica's local compaction while it catches up. Its new
        # snapshot floor must therefore come from the existing TLS transfer path.
        cluster.env["COUNTER_SNAPSHOT_ENTRIES"] = "1000000"
        cluster.start(victim)
        cluster.env["COUNTER_SNAPSHOT_ENTRIES"] = "4"
        cluster.converged(18)
        caught_up = cluster.request(victim, "/metrics")
        assert caught_up["snapshot"] >= before["applied"] + 8, (before, caught_up)
        assert caught_up["tls_connections"] > 0, caught_up
        for node in list(cluster.children):
            cluster.stop(node, graceful=True)
        for node in (1, 2, 3):
            cluster.start(node)
        leader = cluster.leader()
        assert cluster.request(leader, "/value") == {"value": 18}
        assert cluster.request(leader, "/add/1", "POST") == {"value": 19}
        cluster.converged(19)
        recovered = {node: cluster.request(node, "/metrics") for node in cluster.children}
        assert all(row["snapshot"] > 0 for row in recovered.values()), recovered
        for node in list(cluster.children):
            cluster.stop(node, graceful=True)
        receipt = {"status": "passed", "writes": 19, "victim": victim,
                   "before": before, "majority_compacted": majority,
                   "received_snapshot": caught_up, "cold_recovered": recovered}
        (root / "compaction.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print("PASS: repeated compaction, one retained snapshot, offline TLS catch-up and cold recovery", flush=True)
    except BaseException:
        for path in sorted(root.glob("node-*.log")):
            print(f"{path.name}:\n{path.read_text()[-3000:]}", flush=True)
        raise
    finally:
        cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--output", type=Path, help="retain evidence in a new directory")
    args = parser.parse_args()
    if args.output:
        args.output.mkdir()
        exercise(args.binary.resolve(), args.output.resolve())
    else:
        with tempfile.TemporaryDirectory(prefix="rafter-counter-compaction-") as temporary:
            exercise(args.binary.resolve(), Path(temporary))

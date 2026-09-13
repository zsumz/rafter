"""WAL reclamation evidence rejects identity, accounting, and bound drift."""

from copy import deepcopy
from pathlib import Path
import runpy
import unittest


CHECK = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "check-wal-reclamation")
)["check"]
SHA = "0123456789abcdef0123456789abcdef01234567"


def latency(count, maximum=30):
    return {"count": count, "p50_ns": 10, "p99_ns": 20, "max_ns": maximum}


def inventory(generation, size=1000):
    return {
        "file_count": 3,
        "bytes": size,
        "names": [
            f"raft-wal-checkpoint-{generation:020d}.rfwc",
            "raft-wal-current",
            f"raft-wal-segment-{generation:020d}.rfwb",
        ],
    }


def fixture():
    rounds = 2
    batches = 3
    batch_size = 4
    appended = batches * batch_size
    phase = lambda name, total: {
        "name": name,
        "calls": rounds,
        "total_ns": total,
        "max_ns": total // 2,
        "buckets": [0, rounds],
    }
    return {
        "schema": 1,
        "layer": "durable_replication",
        "comparison_kind": "rafter_component_qualification",
        "source_sha": SHA,
        "completion_boundary": "durable receipt and cleanup fence",
        "config": {
            "retained_entries": 100,
            "rounds": rounds,
            "batches_per_round": batches,
            "batch_size": batch_size,
            "payload_bytes": 64,
        },
        "rounds": [
            {
                "round": number,
                "appended_entries": appended,
                "compacted_through": number * appended,
                "snapshot_ns": 5,
                "reclamation_pause_ns": 100,
                "write_latency": latency(batches),
                "managed_wal": inventory(number),
            }
            for number in range(1, rounds + 1)
        ],
        "aggregate": {
            "write_latency": latency(rounds * batches),
            "reclamation_pause": latency(rounds),
            "reopen_ns": 5,
            "managed_wal_bytes": 1000,
            "managed_wal_files": 3,
        },
        "phase_metrics": [
            phase("wal_reclamation", 180),
            phase("wal_checkpoint_prepare", 60),
            phase("wal_manifest_publish", 50),
            phase("wal_cleanup", 40),
        ],
        "final": {
            "hard_commit": 100 + rounds * appended,
            "compacted_through": rounds * appended,
            "retained_entries": 100,
            "next_index": 101 + rounds * appended,
            "recovery_verified": True,
            "physical_bound_verified": True,
            "managed_wal": inventory(rounds),
        },
    }


class WalReclamationEvidence(unittest.TestCase):
    def test_complete_receipt_qualifies(self):
        summary = CHECK(fixture(), SHA)
        self.assertTrue(summary["qualified"])
        self.assertEqual(summary["retained_entries"], 100)

    def test_identity_and_comparison_layer_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "identity"):
            CHECK(fixture(), "f" * 40)
        changed = fixture()
        changed["comparison_kind"] = "competitor_comparison"
        with self.assertRaisesRegex(ValueError, "mislabeled"):
            CHECK(changed, SHA)

    def test_missing_phase_and_histogram_drift_fail_closed(self):
        changed = fixture()
        changed["phase_metrics"].pop()
        with self.assertRaisesRegex(ValueError, "phase inventory"):
            CHECK(changed, SHA)
        changed = fixture()
        changed["phase_metrics"][0]["buckets"] = [0, 1]
        with self.assertRaisesRegex(ValueError, "histogram"):
            CHECK(changed, SHA)

    def test_stale_files_and_byte_growth_fail_closed(self):
        changed = fixture()
        changed["rounds"][0]["managed_wal"]["names"].append("hard-state")
        changed["rounds"][0]["managed_wal"]["file_count"] = 4
        with self.assertRaisesRegex(ValueError, "three managed"):
            CHECK(changed, SHA)
        changed = fixture()
        changed["rounds"][1]["managed_wal"]["bytes"] += 1
        with self.assertRaisesRegex(ValueError, "bytes disagree"):
            CHECK(changed, SHA)

    def test_recovery_and_boundary_drift_fail_closed(self):
        changed = fixture()
        changed["final"]["recovery_verified"] = False
        with self.assertRaisesRegex(ValueError, "not verified"):
            CHECK(changed, SHA)
        changed = fixture()
        changed["rounds"][1]["compacted_through"] -= 1
        with self.assertRaisesRegex(ValueError, "compaction boundary"):
            CHECK(changed, SHA)


if __name__ == "__main__":
    unittest.main()

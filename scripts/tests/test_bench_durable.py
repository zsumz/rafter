"""Evidence validation rejects incomplete, mismatched, and non-finite reports."""

import copy
from pathlib import Path
import runpy
import unittest

BENCH = runpy.run_path(str(Path(__file__).resolve().parents[1] / "bench-durable"))


class DurableEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.report = {"harness": "rafter-bench-cluster", "workloads": [{
            "name": "proposals", "batch_size": 32, "proposals": 32000,
            "payload_bytes": 256, "proposal_batches": 1000, "elapsed_ms": 5000.0,
            "proposals_per_s": 6400.0,
            "commit_latency_ms": {"samples": 32000, "p50": 2.0, "p99": 5.0, "max": 7.0},
            "batch_completion_latency_ms": {"samples": 1000, "p50": 4.0, "p99": 6.0, "max": 8.0},
        }]}

    def validate(self, report):
        return BENCH["validate"](report, "batch-32", 1000, 1024)

    def test_rejects_missing_or_extra_workloads(self):
        for workloads in [[], self.report["workloads"] * 2]:
            with self.assertRaises(ValueError):
                self.validate({"harness": "rafter-bench-cluster", "workloads": workloads})

    def test_rejects_wrong_workload_and_correlated_sample_counts(self):
        for key in ["proposals", "proposal_batches", "batch_size", "payload_bytes"]:
            report = copy.deepcopy(self.report)
            report["workloads"][0][key] += 1
            with self.assertRaises(ValueError):
                self.validate(report)
        report = copy.deepcopy(self.report)
        report["workloads"][0]["batch_completion_latency_ms"]["samples"] = 32000
        with self.assertRaises(ValueError):
            self.validate(report)

    def test_rejects_invalid_timing(self):
        for invalid in [-1.0, float("nan"), float("inf"), 0.0]:
            report = copy.deepcopy(self.report)
            report["workloads"][0]["elapsed_ms"] = invalid
            with self.assertRaises(ValueError):
                self.validate(report)

    def test_requires_the_explicitly_selected_backend(self):
        for backend in [None, "replace"]:
            self.report["hard_state"] = backend
            with self.assertRaises(ValueError):
                BENCH["validate"](self.report, "batch-32", 1000, 1024, "journal")
        self.report["hard_state"] = "journal"
        BENCH["validate"](self.report, "batch-32", 1000, 1024, "journal")

    def test_aggregates_run_percentiles_without_pooling_samples(self):
        values = []
        for rate, p99 in [(6000.0, 10.0), (9000.0, 2.0), (7000.0, 4.0)]:
            report = copy.deepcopy(self.report)
            report["workloads"][0]["proposals_per_s"] = rate
            report["workloads"][0]["commit_latency_ms"]["p99"] = p99
            values.append(self.validate(report))
        result = BENCH["aggregate"](values)
        self.assertEqual(result["proposals_per_s"], 7000.0)
        self.assertEqual(result["commit_latency_ms"]["p99"], 4.0)
        self.assertEqual(result["commit_latency_ms"]["samples"], 32000)


if __name__ == "__main__":
    unittest.main()

"""Validate workload reports and preserve evidence across execution failures."""

import copy
import errno
import json
import os
from pathlib import Path
import runpy
import subprocess
import tempfile
from types import SimpleNamespace
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
            "phase_ms": {"leader_steps": 2000.0, "message_pumps": 2900.0},
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

    def test_rejects_non_numeric_and_missing_metrics(self):
        for field in ["elapsed_ms", "proposals_per_s"]:
            for invalid in [None, True, False, "6400", [], {}, float("nan"), float("inf"), -1, 0]:
                with self.subTest(field=field, value=invalid):
                    report = copy.deepcopy(self.report)
                    report["workloads"][0][field] = invalid
                    with self.assertRaises(ValueError):
                        self.validate(report)
            report = copy.deepcopy(self.report)
            del report["workloads"][0][field]
            with self.assertRaises(ValueError):
                self.validate(report)

    def test_rejects_non_integer_counts_and_malformed_report_shapes(self):
        for field in ["proposals", "proposal_batches", "batch_size", "payload_bytes"]:
            report = copy.deepcopy(self.report)
            report["workloads"][0][field] = float(report["workloads"][0][field])
            with self.assertRaises(ValueError):
                self.validate(report)
        for report in [None, [], {"harness": "rafter-bench-cluster", "workloads": {"0": {}}},
                       {"harness": "rafter-bench-cluster", "workloads": [None]}]:
            with self.assertRaises(ValueError):
                self.validate(report)

    def test_rejects_invalid_latency_summaries(self):
        for field in ["commit_latency_ms", "batch_completion_latency_ms"]:
            for patch in [{"p99": 1.0}, {"max": 3.0}, {"max": 5001.0}, {"p50": "2"},
                          {"p99": float("nan")}, {"samples": 1000.0}, {"p50": -1.0}]:
                with self.subTest(field=field, patch=patch):
                    report = copy.deepcopy(self.report)
                    report["workloads"][0][field].update(patch)
                    with self.assertRaises(ValueError):
                        self.validate(report)
            for invalid in [None, [], {}]:
                report = copy.deepcopy(self.report)
                report["workloads"][0][field] = invalid
                with self.assertRaises(ValueError):
                    self.validate(report)

    def test_rejects_inconsistent_throughput_and_phase_timings(self):
        for rate in [1.0, 6399.0, 6401.0, 640000000.0]:
            report = copy.deepcopy(self.report)
            report["workloads"][0]["proposals_per_s"] = rate
            with self.assertRaises(ValueError):
                self.validate(report)
        for phases in [None, {}, {"leader_steps": 2500.0, "message_pumps": 2500.01},
                       {"leader_steps": "2000", "message_pumps": 2900.0}]:
            report = copy.deepcopy(self.report)
            report["workloads"][0]["phase_ms"] = phases
            with self.assertRaises(ValueError):
                self.validate(report)

    def test_accepts_reporter_rounding_and_zero_duration_subphases(self):
        for elapsed in [0.00149, 1.000499, 4999.99951, 5000.00049, 123456.78951]:
            report = copy.deepcopy(self.report)
            workload = report["workloads"][0]
            workload["elapsed_ms"] = round(elapsed, 3)
            workload["proposals_per_s"] = round(32000 * 1000 / elapsed, 3)
            for field in ["commit_latency_ms", "batch_completion_latency_ms"]:
                workload[field].update(p50=0.0, p99=0.0, max=0.0)
            workload["phase_ms"] = {"leader_steps": 0.0, "message_pumps": 0.0}
            self.validate(report)
        workload["phase_ms"] = {"leader_steps": workload["elapsed_ms"], "message_pumps": 0.001}
        self.validate(report)

    def test_validates_snapshot_transfer_rate_without_including_preparation(self):
        report = {"harness": "rafter-bench-cluster", "workloads": [{
            "name": "snapshot_transfer", "payload_bytes": 128 * 1024 * 1024,
            "elapsed_ms": 2000.0, "throughput_mib_per_s": 64.0,
            "preparation_ms": 10000.0,
        }]}
        def validate(value):
            return BENCH["validate"](value, "snapshot", 1000, 128 * 1024 * 1024)
        validate(report)
        for field, invalid in [("throughput_mib_per_s", 640.0), ("throughput_mib_per_s", "64"),
                               ("preparation_ms", -1), ("preparation_ms", True)]:
            altered = copy.deepcopy(report)
            altered["workloads"][0][field] = invalid
            with self.assertRaises(ValueError):
                validate(altered)
        for field in ["preparation_ms", "throughput_mib_per_s"]:
            altered = copy.deepcopy(report)
            del altered["workloads"][0][field]
            with self.assertRaises(ValueError):
                validate(altered)

    def test_accepts_a_tiny_snapshot_rate_rounded_to_zero(self):
        report = {"harness": "rafter-bench-cluster", "workloads": [{
            "name": "snapshot_transfer", "payload_bytes": 1, "elapsed_ms": 1000.0,
            "throughput_mib_per_s": 0.0, "preparation_ms": 0.0,
        }]}
        BENCH["validate"](report, "snapshot", 1000, 1)

    def test_rejects_invalid_extra_measurements(self):
        for value in [float("nan"), float("inf"), -1, 10 ** 1000]:
            for extra in [{"nested": value}, [value]]:
                report = copy.deepcopy(self.report)
                report["workloads"][0]["additional_metrics"] = extra
                with self.assertRaises(ValueError):
                    self.validate(report)

    def test_aggregates_run_percentiles_without_pooling_samples(self):
        values = []
        for rate, p99 in [(6000.0, 10.0), (9000.0, 2.0), (7000.0, 4.0)]:
            report = copy.deepcopy(self.report)
            workload = report["workloads"][0]
            workload["proposals_per_s"] = rate
            workload["elapsed_ms"] = round(32000 * 1000 / rate, 3)
            workload["phase_ms"] = {"leader_steps": 1000.0, "message_pumps": 2000.0}
            workload["commit_latency_ms"]["p99"] = p99
            workload["commit_latency_ms"]["max"] = max(7.0, p99)
            values.append(self.validate(report))
        result = BENCH["aggregate"](values)
        self.assertEqual(result["proposals_per_s"], 7000.0)
        self.assertEqual(result["commit_latency_ms"]["p99"], 4.0)
        self.assertEqual(result["commit_latency_ms"]["samples"], 32000)

    @unittest.skipUnless(os.name == "posix", "executable permission and format controls require POSIX")
    def test_failed_launch_retains_outputs_and_execution_receipt(self):
        for kind, expected_errno in [("missing", errno.ENOENT), ("denied", errno.EACCES),
                                     ("invalid", errno.ENOEXEC)]:
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binary = root / "benchmark"
                if kind != "missing":
                    binary.write_text("not an executable format\n")
                    binary.chmod(0o600 if kind == "denied" else 0o700)
                args = SimpleNamespace(directory=root, batches=1000, snapshot_bytes=1024, timeout=1)
                with self.assertRaises(OSError) as raised:
                    BENCH["run_sample"](binary, "batch-32", args, root, "trial")
                self.assertEqual(raised.exception.errno, expected_errno)
                receipt = json.loads((root / "trial.execution.json").read_text())
                self.assertEqual(receipt["command"][0], str(binary))
                self.assertIsNone(receipt["exit_code"])
                self.assertFalse(receipt["timed_out"])
                self.assertEqual(receipt["launch_error"]["errno"], expected_errno)
                self.assertEqual(receipt["launch_error"]["type"], type(raised.exception).__name__)
                self.assertEqual(receipt["launch_error"]["message"], str(raised.exception))
                self.assertEqual((root / "trial.json").read_bytes(), b"")
                self.assertEqual((root / "trial.stderr").read_bytes(), b"")

    @unittest.skipUnless(os.name == "posix", "executable fixture requires a POSIX shebang")
    def test_started_children_keep_success_failure_and_timeout_receipts(self):
        for outcome in ["success", "failure", "timeout"]:
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binary = root / "benchmark"
                body = "#!/usr/bin/env python3\nimport sys, time\n"
                body += "print('trial diagnostic', file=sys.stderr, flush=True)\n"
                if outcome == "success":
                    body += f"print({json.dumps(self.report)!r})\n"
                elif outcome == "failure":
                    body += "raise SystemExit(7)\n"
                else:
                    body += "time.sleep(60)\n"
                binary.write_text(body)
                binary.chmod(0o700)
                args = SimpleNamespace(directory=root, batches=1000, snapshot_bytes=1024, timeout=1)
                if outcome == "success":
                    result = BENCH["run_sample"](binary, "batch-32", args, root, "trial")
                    self.assertEqual(result, self.report["workloads"][0])
                else:
                    exception = (subprocess.CalledProcessError if outcome == "failure"
                                 else subprocess.TimeoutExpired)
                    with self.assertRaises(exception):
                        BENCH["run_sample"](binary, "batch-32", args, root, "trial")
                receipt = json.loads((root / "trial.execution.json").read_text())
                expected_exit = {"success": 0, "failure": 7, "timeout": None}[outcome]
                self.assertEqual(receipt["exit_code"], expected_exit)
                self.assertEqual(receipt["timed_out"], outcome == "timeout")
                self.assertNotIn("launch_error", receipt)
                self.assertTrue((root / "trial.json").is_file())
                self.assertTrue((root / "trial.stderr").is_file())
                if outcome != "timeout":
                    self.assertEqual((root / "trial.stderr").read_text(), "trial diagnostic\n")


if __name__ == "__main__":
    unittest.main()

"""Rehashed counterexamples must fail the workload accounting checks."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import check_load


class WorkloadAccounting(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.histories = [self.root / str(node) for node in (1, 2, 3)]
        client_hash = hashlib.sha256(Path(check_load.__file__).with_name("load.py").read_bytes()).hexdigest()
        for node, directory in enumerate(self.histories, 1):
            directory.mkdir()
            self.write(directory / "invocation.json", {
                "schema": 1, "run_id": "run-one", "node": node, "host": f"host-{node}",
                "client_sha256": client_hash, "read_every": 2, "max_operations": 2,
            })
            self.write(directory / "initial-metrics.json", {"node": node, "ready": True})
            events = []
            for number in (1, 2):
                writing = number == 1
                events.append({"event": "invoke", "run_id": "run-one", "id": number, "node": node,
                               "kind": "write" if writing else "read", "increment": 1 if writing else None,
                               "monotonic_ns": number * 100})
                result = {"event": "complete", "run_id": "run-one", "id": number, "node": node,
                          "started_monotonic_ns": number * 100 + 10, "latency_ns": 10}
                if node == 1:
                    result.update(outcome="write_acknowledged" if writing else "read_succeeded",
                                  status=200, value=1)
                else:
                    result.update(outcome="write_refused" if writing else "read_refused", status=307)
                events.append(result)
            self.seal(node, events)

    @staticmethod
    def write(path, value):
        path.write_text(json.dumps(value) + "\n")

    def events(self, node):
        return [json.loads(line) for line in (self.histories[node - 1] / "history.jsonl").read_text().splitlines()]

    def seal(self, node, events):
        directory = self.histories[node - 1]
        raw = b"".join((json.dumps(event) + "\n").encode() for event in events)
        (directory / "history.jsonl").write_bytes(raw)
        complete = [event for event in events if event["event"] == "complete"]
        latencies = [event["latency_ns"] for event in complete]
        invoked = len([event for event in events if event["event"] == "invoke"])
        self.write(directory / "summary.json", {
            "schema": 1, "run_id": "run-one", "node": node, "status": "completed", "qualification": "not_evaluated", "error": None,
            "invoked": invoked, "completed": len(complete), "unresolved_invocations": invoked - len(complete),
            "outcomes": dict(Counter(event["outcome"] for event in complete)), "elapsed_seconds": 1,
            "latency_ns": {"sum": sum(latencies), "min": min(latencies) if latencies else None,
                           "max": max(latencies) if latencies else None},
            "history_sha256": hashlib.sha256(raw).hexdigest(), "history_bytes": len(raw),
        })

    def change_json(self, node, name, key, value):
        path = self.histories[node - 1] / name
        data = json.loads(path.read_text())
        data[key] = value
        self.write(path, data)

    def verify(self, final=1, **kwargs):
        return check_load.verify(self.histories, 0, final, "run-one", **kwargs)

    def test_valid_leader_and_follower_histories(self):
        report = self.verify()
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["allowed_final_range"], [1, 1])
        self.assertEqual(report["outcomes"], {"write_acknowledged": 1, "read_succeeded": 1,
                                            "write_refused": 2, "read_refused": 2})
        self.assertEqual({node["node"] for node in report["nodes"]}, {1, 2, 3})

    def test_latency_populations_remain_separate_and_pool_samples_not_percentiles(self):
        for node, count in ((1, 200), (2, 2), (3, 4)):
            events = []
            self.change_json(node, "invocation.json", "max_operations", count)
            for number in range(1, count + 1):
                writing = number % 2 == 1
                events.append({"event": "invoke", "run_id": "run-one", "id": number, "node": node,
                               "kind": "write" if writing else "read", "increment": 1 if writing else None,
                               "monotonic_ns": number * 1_000_000})
                result = {"event": "complete", "run_id": "run-one", "id": number, "node": node,
                          "started_monotonic_ns": number * 1_000_000 + 10}
                if node in (1, 2):
                    value = (number + 1) // 2 if node == 1 else 101
                    latency = (value if writing else 20_000) if node == 1 else (10_000 if writing else 30_000)
                    result.update(outcome="write_acknowledged" if writing else "read_succeeded",
                                  status=200, value=value, latency_ns=latency)
                elif number <= 2:
                    result.update(outcome="write_uncertain" if writing else "read_failed", status=None,
                                  error="deadline", latency_ns=500_000 if writing else 600_000)
                else:
                    result.update(outcome="write_refused" if writing else "read_refused", status=307,
                                  latency_ns=0 if writing else 7)
                events.append(result)
            self.seal(node, events)
        report = self.verify(final=101)
        pooled = report["latency_ns_by_outcome"]
        self.assertEqual(pooled["write_acknowledged"],
                         {"count": 101, "sum": 15_050, "min": 1, "max": 10_000,
                          "p50": 51, "p95": 96, "p99": 100})
        self.assertEqual(pooled["read_succeeded"],
                         {"count": 101, "sum": 2_030_000, "min": 20_000, "max": 30_000,
                          "p50": 20_000, "p95": 20_000, "p99": 20_000})
        for outcome, duration in (("write_uncertain", 500_000), ("read_failed", 600_000),
                                  ("write_refused", 0), ("read_refused", 7)):
            self.assertEqual(pooled[outcome], {"count": 1, "sum": duration, "min": duration,
                             "max": duration, "p50": duration, "p95": duration, "p99": duration})
        nodes = {node["node"]: node["latency_ns_by_outcome"] for node in report["nodes"]}
        self.assertEqual(nodes[1]["write_acknowledged"]["p95"], 95)
        self.assertEqual(nodes[2]["write_acknowledged"]["p95"], 10_000)
        self.assertEqual(nodes[1]["write_uncertain"],
                         {"count": 0, "sum": 0, "min": None, "max": None,
                          "p50": None, "p95": None, "p99": None})

    def test_uncertain_increment_may_commit_without_becoming_an_acknowledgement(self):
        events = self.events(2)
        events[1].update(outcome="write_uncertain", status=503, error="timeout")
        events[3].update(outcome="read_succeeded", status=200, value=2)
        self.seal(2, events)
        report = self.verify(final=2)
        self.assertEqual(report["allowed_final_range"], [1, 2])
        self.assertEqual(report["outcomes"]["write_acknowledged"], 1)
        self.assertEqual(report["outcomes"]["write_uncertain"], 1)

    def test_lost_acknowledged_value_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "quorum boundaries"):
            self.verify(final=0)

    def test_unexplained_extra_increment_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unexplained increments"):
            self.verify(final=2)

    def test_duplicate_acknowledged_values_are_rejected_even_after_rehashing(self):
        events = self.events(2)
        events[1].update(outcome="write_acknowledged", status=200, value=1)
        self.seal(2, events)
        with self.assertRaisesRegex(ValueError, "duplicate acknowledged"):
            self.verify(final=2)

    def test_regressing_read_is_rejected_even_after_rehashing(self):
        events = self.events(1)
        events[3]["value"] = 0
        self.seal(1, events)
        with self.assertRaisesRegex(ValueError, "observation regressed"):
            self.verify()

    def test_missing_completion_is_rejected_even_after_rehashing(self):
        self.seal(3, self.events(3)[:-1])
        with self.assertRaisesRegex(ValueError, "incomplete or empty"):
            self.verify()

    def test_reordered_operation_is_rejected_even_after_rehashing(self):
        events = self.events(3)
        events[2]["id"] = events[3]["id"] = 3
        self.seal(3, events)
        with self.assertRaisesRegex(ValueError, "reordered operation"):
            self.verify()

    def test_changed_history_bytes_are_rejected(self):
        path = self.histories[2] / "history.jsonl"
        path.write_bytes(b" " + path.read_bytes())
        with self.assertRaisesRegex(ValueError, "hash or length mismatch"):
            self.verify()

    def test_summary_counts_are_recomputed(self):
        self.change_json(3, "summary.json", "outcomes", {"write_refused": 100, "read_refused": 1})
        with self.assertRaisesRegex(ValueError, "outcome count mismatch"):
            self.verify()

    def test_histories_from_different_runs_cannot_be_combined(self):
        for node in (1, 2, 3):
            self.change_json(node, "invocation.json", "run_id", "run-one" if node != 3 else "another-run")
        with self.assertRaisesRegex(ValueError, "run identity"):
            self.verify()

    def test_event_run_identity_is_checked_after_rehashing(self):
        events = self.events(3)
        events[0]["run_id"] = "another-run"
        self.seal(3, events)
        with self.assertRaisesRegex(ValueError, "history run identity"):
            self.verify()

    def test_foreign_client_source_is_rejected(self):
        self.change_json(3, "invocation.json", "client_sha256", "0" * 64)
        with self.assertRaisesRegex(ValueError, "client source hash mismatch"):
            self.verify()

    def test_boolean_cannot_replace_a_schema_or_counter_integer(self):
        self.change_json(3, "invocation.json", "schema", True)
        with self.assertRaisesRegex(ValueError, "capture schema"):
            self.verify()
        self.change_json(3, "invocation.json", "schema", 1)
        events = self.events(1)
        events[1]["value"] = True
        self.seal(1, events)
        with self.assertRaisesRegex(ValueError, "malformed successful"):
            self.verify()

    def test_client_overlap_and_insufficient_duration_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "below the required duration"):
            self.verify(minimum_seconds=1)
        events = self.events(3)
        events[2]["monotonic_ns"] = 110
        self.seal(3, events)
        with self.assertRaisesRegex(ValueError, "overlapping or regressing"):
            self.verify()

    def test_all_three_nodes_and_actual_progress_are_required(self):
        with self.assertRaisesRegex(ValueError, "exactly three"):
            check_load.verify(self.histories[:2], 0, 1, "run-one")
        events = self.events(1)
        for event in events[1::2]:
            event.pop("value")
            event.update(outcome="write_refused" if event["id"] == 1 else "read_refused", status=307)
        self.seal(1, events)
        with self.assertRaisesRegex(ValueError, "no successful write/read progress"):
            self.verify(final=0)


    def test_shared_clock_rejects_cross_client_stale_read(self):
        events = self.events(2)
        events[3].update(outcome="read_succeeded", status=200, value=0)
        self.seal(2, events)
        self.assertEqual(self.verify()["status"], "passed")
        with self.assertRaisesRegex(ValueError, "no shared-clock linearization"):
            self.verify(shared_clock=True)

    def test_uncertain_write_can_linearize_after_its_client_timeout(self):
        events = self.events(2)
        events[1].update(outcome="write_uncertain", status=503, error="timeout")
        events[3].update(outcome="read_succeeded", status=200, value=2)
        self.seal(2, events)
        report = self.verify(final=2, shared_clock=True)["shared_clock_linearizability"]
        selected = [step for step in report["witness"] if step.get("uncertain")]
        self.assertEqual(len(selected), 1)
        self.assertGreater(selected[0]["monotonic_ns"], 120)

    def test_future_uncertain_invocation_cannot_explain_an_earlier_read(self):
        events = self.events(2)
        events[1].update(outcome="write_uncertain", status=503, error="timeout")
        for event in events:
            key = "monotonic_ns" if event["event"] == "invoke" else "started_monotonic_ns"
            event[key] += 200
        self.seal(2, events)
        events = self.events(3)
        events[3].update(outcome="read_succeeded", status=200, value=2)
        self.seal(3, events)
        self.assertEqual(self.verify(final=2)["status"], "passed")
        with self.assertRaisesRegex(ValueError, "no shared-clock linearization"):
            self.verify(final=2, shared_clock=True)

    def test_shared_clock_rejects_acknowledgements_in_impossible_real_time_order(self):
        events = self.events(1)
        for event in events:
            key = "monotonic_ns" if event["event"] == "invoke" else "started_monotonic_ns"
            event[key] += 200
        self.seal(1, events)
        events = self.events(2)
        events[1].update(outcome="write_acknowledged", status=200, value=2)
        events[3].update(outcome="read_succeeded", status=200, value=2)
        self.seal(2, events)
        self.assertEqual(self.verify(final=2)["status"], "passed")
        with self.assertRaisesRegex(ValueError, "no shared-clock linearization"):
            self.verify(final=2, shared_clock=True)


class SharedClockOracle(unittest.TestCase):
    @staticmethod
    def exhaustive(operations, baseline, final):
        # Deliberately independent: enumerate pending subsets and every sequential
        # order, replay the counter, then fit that order into operation intervals.
        import itertools
        required = [op for op in operations if op["outcome"] in ("write_acknowledged", "read_succeeded")]
        pending = [op for op in operations if op["outcome"] == "write_uncertain"]
        needed = final - baseline - sum(op["outcome"] == "write_acknowledged" for op in required)
        if not 0 <= needed <= len(pending):
            return False
        for chosen in itertools.combinations(pending, needed):
            for order in itertools.permutations(required + list(chosen)):
                value, moment = baseline, 0
                for op in order:
                    moment = max(moment, op["start"])
                    if op["outcome"] != "write_uncertain" and moment > op["end"]:
                        break
                    if op["outcome"].startswith("write_"):
                        value += 1
                    if op["outcome"] != "write_uncertain" and value != op["value"]:
                        break
                else:
                    if value == final:
                        return True
        return False

    def replay(self, operations, baseline, final, report):
        source = {(op["node"], op["id"]): op for op in operations}
        seen, value, moment = set(), baseline, 0
        for step in report["witness"]:
            identity = step["node"], step["id"]
            self.assertNotIn(identity, seen)
            seen.add(identity)
            op = source[identity]
            self.assertGreaterEqual(step["monotonic_ns"], max(moment, op["start"]))
            moment = step["monotonic_ns"]
            if op["outcome"] != "write_uncertain":
                self.assertLessEqual(moment, op["end"])
            if step["kind"] == "write":
                self.assertIn(op["outcome"], ("write_acknowledged", "write_uncertain"))
                self.assertEqual(step["uncertain"], op["outcome"] == "write_uncertain")
                value += 1
            else:
                self.assertEqual(op["outcome"], "read_succeeded")
            self.assertEqual(step["value"], value)
            if op["outcome"] != "write_uncertain":
                self.assertEqual(op["value"], value)
        self.assertEqual(value, final)
        required = {key for key, op in source.items() if op["outcome"] in ("write_acknowledged", "read_succeeded")}
        self.assertTrue(required <= seen)

    def test_rank_solver_matches_exhaustive_orders_and_replayable_witnesses(self):
        import random
        rng = random.Random(0x52414654)
        passes = failures = 0
        for case in range(3000):
            baseline = rng.randrange(4)
            final = baseline + rng.randrange(5)
            operations = []
            for index in range(rng.randrange(1, 7)):
                start = rng.randrange(12)
                operations.append({"node": index % 3 + 1, "id": index + 1,
                    "outcome": rng.choice(("write_acknowledged", "read_succeeded", "write_uncertain")),
                    "start": start, "end": start + rng.randrange(5),
                    "value": baseline + rng.randrange(6)})
            expected = self.exhaustive(operations, baseline, final)
            try:
                report = check_load.linearize(operations, baseline, final)
            except ValueError:
                actual = False
                failures += 1
            else:
                actual = True
                passes += 1
                self.replay(operations, baseline, final, report)
            self.assertEqual(actual, expected, (case, baseline, final, operations))
        self.assertGreater(passes, 100)
        self.assertGreater(failures, 100)


if __name__ == "__main__":
    unittest.main()

import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "model_check_profile_parser.py"
SPEC = importlib.util.spec_from_file_location("model_check_profile_parser", MODULE_PATH)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


def exhaustive_event(**updates):
    event = {
        "event": "exhaustive-check",
        "check_id": "raft-election",
        "status": "pass",
        "classification": None,
        "completion": "frontier_exhausted",
        "configured_depth": 7,
        "reached_depth": 7,
        "unique_states": 12,
        "unique_protocol_states": 10,
        "unique_verifier_states": 12,
        "explored_states": 15,
        "explored_actions": 14,
        "duration_ms": 3,
    }
    event.update(updates)
    return event


class ModelCheckProfileReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def output(self, *events, profile="fast"):
        path = self.root / "run.stdout"
        lines = [
            f'model-check profile={profile} expected_runtime=per-commit '
            'exhaustive_target_protocol_states=none bounds="depth=7" '
            "schedule_classes=proposal,deliver",
            *[f"{REPORT.EVENT_PREFIX}{REPORT.json.dumps(event)}" for event in events],
        ]
        path.write_text("\n".join(lines) + "\n")
        return path

    def test_structured_exhaustive_event_is_accepted(self):
        checks = REPORT.parse_checks(self.output(exhaustive_event()))

        self.assertEqual(checks[0]["unique_protocol_states"], 10)
        self.assertEqual(checks[0]["unique_verifier_states"], 12)

    def test_human_summary_without_event_is_rejected(self):
        path = self.root / "run.stdout"
        path.write_text(
            "model-check profile=fast expected_runtime=per-commit "
            'exhaustive_target_protocol_states=none bounds="depth=7" '
            "schedule_classes=proposal,deliver\n"
            "model-check raft-election: unique_states=12\n"
        )

        with self.assertRaisesRegex(REPORT.ReportError, "no RAFTER_EVENT"):
            REPORT.parse_checks(path)

    def test_legacy_event_without_independent_state_counts_is_rejected(self):
        event = exhaustive_event()
        del event["unique_protocol_states"]

        with self.assertRaisesRegex(REPORT.ReportError, "unique_protocol_states"):
            REPORT.parse_checks(self.output(event))

    def test_incomplete_exploration_is_rejected(self):
        event = exhaustive_event(
            status="incomplete",
            classification="coverage-not-reached",
            completion="budget_exhausted",
        )

        with self.assertRaisesRegex(REPORT.ReportError, "did not pass cleanly"):
            REPORT.parse_checks(self.output(event))

    def test_duplicate_check_ids_are_rejected(self):
        event = exhaustive_event()

        with self.assertRaisesRegex(REPORT.ReportError, "duplicate exhaustive check_id"):
            REPORT.parse_checks(self.output(event, event))

    def test_profile_header_must_match_requested_profile(self):
        with self.assertRaisesRegex(REPORT.ReportError, "!= requested raft-deep"):
            REPORT.parse_checks(self.output(exhaustive_event()), "raft-deep")

    def test_impossible_state_count_relationship_is_rejected(self):
        event = exhaustive_event(unique_protocol_states=13)

        with self.assertRaisesRegex(REPORT.ReportError, "impossible state counts"):
            REPORT.parse_checks(self.output(event))

    def test_soak_checks_are_keyed_by_seed(self):
        soak = {
            "event": "soak-check",
            "check_id": "raft-soak",
            "status": "pass",
            "classification": None,
            "seed": 1,
        }
        second = {**soak, "seed": 2}

        parsed = REPORT.parse_output(self.output(exhaustive_event(), soak, second))

        self.assertEqual([event["seed"] for event in parsed["soak_checks"]], [1, 2])

    def test_missing_peak_rss_is_rejected(self):
        path = self.root / "run.time"
        path.write_text("0.25 real 0.20 user 0.01 sys\n")

        with self.assertRaisesRegex(REPORT.ReportError, "incomplete wall-time or peak-RSS"):
            REPORT.parse_time(path, "bsd")

    def test_gnu_timing_telemetry_is_parsed(self):
        path = self.root / "run.time"
        path.write_text(
            "Elapsed (wall clock) time (h:mm:ss or m:ss): 0:01.25\n"
            "Maximum resident set size (kbytes): 2048\n"
        )

        self.assertEqual(
            REPORT.parse_time(path, "gnu"),
            {"wall_time_ms": 1250, "peak_rss_bytes": 2 * 1024 * 1024},
        )

    def test_execution_sequence_must_balance_labels_and_profiles(self):
        rows = [
            {"run": "1", "profile": "fast", "label": "base"},
            {"run": "1", "profile": "fast", "label": "current"},
            {"run": "1", "profile": "deep", "label": "base"},
            {"run": "1", "profile": "deep", "label": "current"},
            {"run": "2", "profile": "deep", "label": "current"},
            {"run": "2", "profile": "deep", "label": "base"},
            {"run": "2", "profile": "fast", "label": "current"},
            {"run": "2", "profile": "fast", "label": "base"},
        ]
        REPORT._validate_execution_order(rows)

        rows[-1]["label"] = "current"
        with self.assertRaisesRegex(REPORT.ReportError, "execution sequence"):
            REPORT._validate_execution_order(rows)

    def test_deterministic_shape_drift_between_samples_is_rejected(self):
        first = {
            "run": 1,
            "label": "base",
            "profile": "fast",
            "wall_time_ms": 10,
            "peak_rss_bytes": 100,
            "profile_contract": {"profile": "fast"},
            "checks": REPORT.parse_checks(self.output(exhaustive_event())),
            "totals": {},
        }
        second = {
            **first,
            "run": 2,
            "checks": REPORT.parse_checks(
                self.output(exhaustive_event(unique_protocol_states=9))
            ),
        }

        with self.assertRaisesRegex(REPORT.ReportError, "state shape changed"):
            REPORT.summarize_samples([first, second], 2)


if __name__ == "__main__":
    unittest.main()

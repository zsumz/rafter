import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).parents[1] / "model_check_profile_parser.py"
SPEC = importlib.util.spec_from_file_location("model_check_profile_parser", MODULE_PATH)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)
sys.modules["model_check_profile_parser"] = REPORT

MIGRATION_MODULE_PATH = Path(__file__).parents[1] / "model_check_contract_migration.py"
MIGRATION_SPEC = importlib.util.spec_from_file_location(
    "model_check_contract_migration", MIGRATION_MODULE_PATH
)
MIGRATION = importlib.util.module_from_spec(MIGRATION_SPEC)
MIGRATION_SPEC.loader.exec_module(MIGRATION)


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

    def migration_samples(self, *, current_events=None, current_contract=None):
        base_event = exhaustive_event()
        current_events = current_events or [
            exhaustive_event(
                configured_depth=8,
                reached_depth=8,
                unique_states=22,
                unique_protocol_states=20,
                unique_verifier_states=22,
                explored_states=25,
                explored_actions=24,
            )
        ]
        base = {
            "run": 1,
            "label": "base",
            "profile": "fast",
            "wall_time_ms": 10,
            "peak_rss_bytes": 100,
            "profile_contract": {
                "profile": "fast",
                "expected_runtime": "per-commit",
                "bounds": "depth=7",
                "schedule_classes": ["proposal", "deliver"],
            },
            "checks": [base_event],
            "totals": {},
        }
        current = {
            **base,
            "label": "current",
            "wall_time_ms": 20,
            "peak_rss_bytes": 150,
            "profile_contract": current_contract
            or {**base["profile_contract"], "bounds": "depth=8"},
            "checks": current_events,
        }
        return [base, current]

    def migration_manifest(self, delta=None):
        delta = delta or REPORT.compare_contract_migration(
            self.migration_samples(), 1
        )
        snapshots = {
            snapshot["label"]: snapshot
            for snapshot in delta["contract_snapshots"]
        }
        return {
            "schema_version": 1,
            "migrations": [
                {
                    "id": "test-migration",
                    "pivot_commit": "a" * 40,
                    "pivot_parent": "b" * 40,
                    "changed_paths": ["one", "two"],
                    "profiles": {
                        "fast": {
                            "from_contract_sha256": snapshots["base"]["sha256"],
                            "to_contract_sha256": snapshots["current"]["sha256"],
                            "configured_depth_changes": [
                                {
                                    "check_id": "raft-election",
                                    "from_depth": 7,
                                    "to_depth": 8,
                                }
                            ],
                        }
                    },
                }
            ],
        }

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

    def test_comparison_separates_protocol_and_verifier_shape(self):
        checks = REPORT.parse_checks(self.output(exhaustive_event()))
        base = {
            "run": 1,
            "label": "base",
            "profile": "fast",
            "wall_time_ms": 10,
            "peak_rss_bytes": 100,
            "profile_contract": {"profile": "fast"},
            "checks": checks,
            "totals": {},
        }
        current_checks = [
            {
                **checks[0],
                "unique_states": 14,
                "unique_verifier_states": 14,
                "explored_states": 17,
            }
        ]
        current = {
            **base,
            "label": "current",
            "wall_time_ms": 20,
            "peak_rss_bytes": 150,
            "checks": current_checks,
        }

        comparison = REPORT.compare_revisions([base, current], 1)[0]

        self.assertTrue(comparison["like_for_like_protocol_state_shape"])
        self.assertFalse(comparison["like_for_like_verifier_state_shape"])
        self.assertEqual(
            REPORT.evaluate_comparisons([comparison], 2.0, 1.5)["status"], "pass"
        )

    def test_performance_thresholds_fail_closed(self):
        comparison = {
            "profile": "fast",
            "like_for_like_protocol_state_shape": False,
            "paired_current_over_base_wall_time": {"median": 2.1},
            "paired_current_over_base_peak_rss": {"median": 1.6},
        }

        verdict = REPORT.evaluate_comparisons([comparison], 2.0, 1.5)

        self.assertEqual(verdict["status"], "fail")
        self.assertEqual(len(verdict["failures"]), 3)

    def test_performance_thresholds_reject_values_below_one(self):
        with self.assertRaisesRegex(REPORT.ReportError, "at least 1.0"):
            REPORT.evaluate_comparisons([], 0.9, 1.0)

    def test_contract_migration_reports_exact_monotone_delta(self):
        delta = REPORT.compare_contract_migration(self.migration_samples(), 1)

        self.assertEqual(
            delta["configured_depth_changes"],
            [
                {
                    "profile": "fast",
                    "check_id": "raft-election",
                    "from_depth": 7,
                    "to_depth": 8,
                }
            ],
        )
        self.assertEqual(delta["coverage_delta"][0]["protocol_states"], 10)

    def test_contract_migration_rejects_decrease_and_check_set_drift(self):
        decreased = exhaustive_event(configured_depth=6, reached_depth=6)
        with self.assertRaisesRegex(REPORT.ReportError, "configured depth decreased"):
            REPORT.compare_contract_migration(
                self.migration_samples(current_events=[decreased]), 1
            )

        added = [
            exhaustive_event(configured_depth=8, reached_depth=8),
            exhaustive_event(check_id="new-check", configured_depth=1, reached_depth=1),
        ]
        with self.assertRaisesRegex(REPORT.ReportError, "added or removed checks"):
            REPORT.compare_contract_migration(
                self.migration_samples(current_events=added), 1
            )

    def test_contract_migration_requires_real_monotone_coverage_growth(self):
        shallow = exhaustive_event(
            configured_depth=8,
            reached_depth=7,
            unique_states=22,
            unique_protocol_states=20,
            unique_verifier_states=22,
            explored_states=25,
            explored_actions=30,
        )
        with self.assertRaisesRegex(REPORT.ReportError, "deeper frontier"):
            REPORT.compare_contract_migration(
                self.migration_samples(current_events=[shallow]), 1
            )

        regressed = exhaustive_event(
            configured_depth=8,
            reached_depth=8,
            unique_states=11,
            unique_protocol_states=9,
            unique_verifier_states=11,
            explored_states=14,
            explored_actions=19,
        )
        with self.assertRaisesRegex(REPORT.ReportError, "coverage counters decreased"):
            REPORT.compare_contract_migration(
                self.migration_samples(current_events=[regressed]), 1
            )

    def test_contract_migration_rejects_non_bound_semantic_change(self):
        contract = {
            "profile": "fast",
            "expected_runtime": "nightly",
            "bounds": "depth=8",
            "schedule_classes": ["proposal", "deliver"],
        }
        with self.assertRaisesRegex(REPORT.ReportError, "expected_runtime changed"):
            REPORT.compare_contract_migration(
                self.migration_samples(current_contract=contract), 1
            )

    def test_manifest_rejects_non_monotone_or_unexpected_schema(self):
        manifest = self.migration_manifest()
        change = manifest["migrations"][0]["profiles"]["fast"][
            "configured_depth_changes"
        ][0]
        change["to_depth"] = change["from_depth"]
        with self.assertRaisesRegex(MIGRATION.MigrationError, "not monotone"):
            MIGRATION.validate_manifest(manifest)

        manifest = self.migration_manifest()
        manifest["unreviewed"] = True
        with self.assertRaisesRegex(MIGRATION.MigrationError, "keys"):
            MIGRATION.validate_manifest(manifest)

    def test_repository_migration_pins_parent_and_exact_paths(self):
        migration = self.migration_manifest()["migrations"][0]

        def valid_git(_repo, *args, **_kwargs):
            if args[0] == "rev-parse":
                return migration["pivot_commit"]
            if args[0] == "show":
                return migration["pivot_parent"]
            if args[0] == "diff-tree":
                return "two\none"
            self.fail(f"unexpected git invocation: {args}")

        with mock.patch.object(MIGRATION, "_git", side_effect=valid_git):
            MIGRATION.validate_repository_migration(self.root, migration)

        with mock.patch.object(
            MIGRATION,
            "_git",
            return_value="c" * 40,
        ):
            with self.assertRaisesRegex(MIGRATION.MigrationError, "resolve exactly"):
                MIGRATION.validate_repository_migration(self.root, migration)

        with mock.patch.object(
            MIGRATION,
            "_git",
            side_effect=lambda _repo, *args, **_kwargs: (
                migration["pivot_commit"]
                if args[0] == "rev-parse"
                else "c" * 40
                if args[0] == "show"
                else "one\ntwo"
            ),
        ):
            with self.assertRaisesRegex(MIGRATION.MigrationError, "pivot parents"):
                MIGRATION.validate_repository_migration(self.root, migration)

        with mock.patch.object(
            MIGRATION,
            "_git",
            side_effect=lambda _repo, *args, **_kwargs: (
                migration["pivot_commit"]
                if args[0] == "rev-parse"
                else migration["pivot_parent"]
                if args[0] == "show"
                else "one\nwrong"
            ),
        ):
            with self.assertRaisesRegex(MIGRATION.MigrationError, "changed paths"):
                MIGRATION.validate_repository_migration(self.root, migration)

    def test_planner_rejects_non_ancestor_baseline(self):
        manifest_path = self.root / "manifest.json"
        manifest_path.write_text(json.dumps(self.migration_manifest()))

        with mock.patch.object(
            MIGRATION,
            "_git",
            side_effect=["c" * 40, "d" * 40],
        ), mock.patch.object(MIGRATION, "_is_ancestor", return_value=False):
            with self.assertRaisesRegex(MIGRATION.MigrationError, "not an ancestor"):
                MIGRATION.build_plan(
                    self.root, manifest_path, "base", "current", ["fast"]
                )

    def test_delta_validation_rejects_wrong_contract_digest(self):
        samples = self.migration_samples()
        manifest = self.migration_manifest()
        manifest["migrations"][0]["profiles"]["fast"][
            "to_contract_sha256"
        ] = "f" * 64
        report = {
            "methodology": {"run_count_per_revision_and_profile": 1},
            "samples": samples,
        }

        with self.assertRaisesRegex(MIGRATION.MigrationError, "digest mismatch"):
            MIGRATION._validate_delta(report, manifest["migrations"][0])

        manifest = self.migration_manifest()
        manifest["migrations"][0]["profiles"]["fast"][
            "configured_depth_changes"
        ][0]["check_id"] = "wrong-check"
        with self.assertRaisesRegex(MIGRATION.MigrationError, "do not match"):
            MIGRATION._validate_delta(report, manifest["migrations"][0])

    def test_aggregate_marks_missing_or_failed_segments_red(self):
        plan = {
            "schema_version": 1,
            "status": "pass",
            "comparison_mode": "like-for-like",
            "base_commit": "a" * 40,
            "current_commit": "b" * 40,
            "segments": [
                {
                    "name": "like-for-like",
                    "required": True,
                    "base_commit": "a" * 40,
                    "current_commit": "b" * 40,
                    "profiles": ["fast"],
                }
            ],
        }
        aggregate = MIGRATION.build_aggregate(plan, self.root, 2.25, 1.75)
        self.assertEqual(aggregate["acceptance"]["status"], "fail")
        self.assertIn("required report is missing", aggregate["acceptance"]["failures"][0])

        report_dir = self.root / "segments" / "like-for-like"
        report_dir.mkdir(parents=True)
        (report_dir / "compare.json").write_text(
            json.dumps(
                {
                    "schema_version": 3,
                    "comparison_mode": "like-for-like",
                    "requested_base_commit": "a" * 40,
                    "sources": {"current": {"commit": "b" * 40}},
                    "acceptance": {
                        "status": "fail",
                        "max_median_wall_ratio": 2.25,
                        "max_median_peak_rss_ratio": 1.75,
                        "failures": ["fast: regression"],
                    },
                }
            )
        )
        aggregate = MIGRATION.build_aggregate(plan, self.root, 2.25, 1.75)
        self.assertEqual(aggregate["acceptance"]["status"], "fail")
        self.assertTrue(
            any(
                "fast: regression" in failure
                for failure in aggregate["acceptance"]["failures"]
            )
        )


if __name__ == "__main__":
    unittest.main()

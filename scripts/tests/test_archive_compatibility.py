"""Adversarial boundaries of the real-archive compatibility runners."""

import copy
import io
import json
from pathlib import Path
import runpy
import sys
import tarfile
import tempfile
import tomllib
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from archive_inputs import (  # noqa: E402
    audit_graph, baseline_archive, baseline_lock, candidate_archive,
    sha256, tree_inventory, unpack,
)


PACKAGES = {"rafter", "rafter-crc32", "rafter-storage"}


class ArchiveBoundary(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.archive = self.root / "rafter-1.0.0.crate"

    def archive_with(self, entries):
        with tarfile.open(self.archive, "w:gz") as archive:
            for path, body, kind in entries:
                entry = tarfile.TarInfo(path)
                entry.type = kind
                entry.size = len(body) if kind == tarfile.REGTYPE else 0
                entry.linkname = "../outside"
                archive.addfile(entry, io.BytesIO(body))

    def test_regular_archive_and_identity(self):
        self.archive_with([("rafter-1.0.0/Cargo.toml",
                            b'[package]\nname="rafter"\nversion="1.0.0"\n', tarfile.REGTYPE)])
        root, version = unpack(self.archive, self.root / "unpacked", "rafter")
        self.assertEqual(version, "1.0.0")
        self.assertTrue((root / "Cargo.toml").is_file())
        with self.assertRaisesRegex(ValueError, "identity disagree"):
            unpack(self.archive, self.root / "wrong-name", "rafter-storage")

    def test_traversal_absolute_paths_links_and_special_files_are_refused(self):
        cases = [("../outside", tarfile.REGTYPE), ("/outside", tarfile.REGTYPE),
                 ("rafter-1.0.0/../../outside", tarfile.REGTYPE),
                 ("different-root/file", tarfile.REGTYPE),
                 ("rafter-1.0.0/link", tarfile.SYMTYPE),
                 ("rafter-1.0.0/hardlink", tarfile.LNKTYPE),
                 ("rafter-1.0.0/device", tarfile.CHRTYPE)]
        for index, (path, kind) in enumerate(cases):
            with self.subTest(path=path):
                self.archive_with([(path, b"untrusted", kind)])
                with self.assertRaises(ValueError):
                    unpack(self.archive, self.root / f"invalid-{index}", "rafter")
        self.assertFalse((self.root / "outside").exists())

    def test_duplicate_normalized_names_are_refused(self):
        self.archive_with([("rafter-1.0.0/file", b"first", tarfile.REGTYPE),
                           ("rafter-1.0.0/./file", b"second", tarfile.REGTYPE)])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            unpack(self.archive, self.root / "duplicates", "rafter")

    def test_extraction_file_count_and_byte_budgets_are_enforced(self):
        self.archive_with([("rafter-1.0.0/first", b"1234", tarfile.REGTYPE),
                           ("rafter-1.0.0/second", b"5678", tarfile.REGTYPE)])
        for index, (limit, value) in enumerate([("MAX_ENTRIES", 1), ("MAX_UNPACKED_BYTES", 7),
                                                ("MAX_ARCHIVE_BYTES", 1)]):
            with self.subTest(limit=limit), patch(f"archive_inputs.{limit}", value):
                with self.assertRaisesRegex(ValueError, "budget"):
                    unpack(self.archive, self.root / f"limit-{index}", "rafter")

    def test_archive_selection_does_not_confuse_sibling_names_or_stale_versions(self):
        self.archive.write_bytes(b"archive")
        (self.root / "rafter-storage-1.0.0.crate").write_bytes(b"sibling")
        self.assertEqual(candidate_archive(self.root, "rafter"), self.archive)
        (self.root / "rafter-2.0.0.crate").write_bytes(b"stale")
        with self.assertRaisesRegex(ValueError, "exactly one"):
            candidate_archive(self.root, "rafter")

    def test_offline_missing_and_corrupt_baselines_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "missing"):
            baseline_archive(self.root, "rafter", "1.0.0", "0" * 64, True)
        self.archive.write_bytes(b"archive")
        self.assertEqual(baseline_archive(self.root, "rafter", "1.0.0", sha256(self.archive), True),
                         self.archive)
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            baseline_archive(self.root, "rafter", "1.0.0", "0" * 64, True)

    def test_inventory_detects_modified_bytes_added_files_and_empty_directories(self):
        state = self.root / "state"
        state.mkdir()
        (state / "hard-state").write_bytes(b"first")
        before = tree_inventory(state)
        (state / "hard-state").write_bytes(b"other")
        self.assertNotEqual(before, tree_inventory(state))
        (state / "hard-state").write_bytes(b"first")
        (state / "new-directory").mkdir()
        self.assertNotEqual(before, tree_inventory(state))
        (state / "new-directory").rmdir()
        (state / "new-file").touch()
        self.assertNotEqual(before, tree_inventory(state))

    def test_baseline_lock_is_complete(self):
        root = Path(__file__).resolve().parents[2]
        self.assertEqual(set(baseline_lock(root / "verification/storage-upgrade-baseline.json", PACKAGES)["packages"]),
                         PACKAGES)

    def test_baseline_from_another_matrix_cannot_substitute_a_dependency(self):
        root = Path(__file__).resolve().parents[2]
        wire_packages = {"rafter", "rafter-codec", "rafter-crc32"}
        path = root / "verification/wire-compatibility-baseline.json"
        self.assertEqual(set(baseline_lock(path, wire_packages)["packages"]), wire_packages)
        with self.assertRaisesRegex(ValueError, "exactly"):
            baseline_lock(path, PACKAGES)


class DependencyBoundary(unittest.TestCase):
    def setUp(self):
        self.checkout = Path("/checkout")
        self.consumer = Path("/outside/probe")
        self.roots = {name: Path("/outside") / name for name in PACKAGES}
        self.metadata = {
            "workspace_members": ["probe"],
            "packages": [{"id": "probe", "name": "rafter-storage-upgrade-probe", "source": None,
                          "manifest_path": str(self.consumer / "Cargo.toml")}]
                        + [{"id": name, "name": name, "source": None,
                            "manifest_path": str(root / "Cargo.toml")}
                           for name, root in self.roots.items()],
            "resolve": {"nodes": [{"features": []}]},
        }

    def audit(self, metadata):
        audit_graph(metadata, self.checkout, self.roots, self.consumer, "rafter-storage-upgrade-probe")

    def test_exact_archive_graph_is_accepted(self):
        self.audit(self.metadata)

    def test_checkout_registry_private_and_unpublished_substitutions_are_refused(self):
        for path, name, source in [
            ("/checkout/crates/rafter/Cargo.toml", "rafter", None),
            ("/registry/rafter/Cargo.toml", "rafter", "registry+https://github.com/rust-lang/crates.io-index"),
            ("/outside/private/Cargo.toml", "rafter-sim", None),
            ("/outside/unpublished/Cargo.toml", "unpublished", None),
        ]:
            with self.subTest(path=path):
                metadata = copy.deepcopy(self.metadata)
                metadata["packages"].append({"id": "substituted", "name": name,
                                             "source": source, "manifest_path": path})
                with self.assertRaises(ValueError):
                    self.audit(metadata)

    def test_missing_member_package_and_internal_hooks_are_refused(self):
        for field in ["workspace", "package", "features"]:
            metadata = copy.deepcopy(self.metadata)
            if field == "workspace":
                metadata["workspace_members"] = []
            elif field == "package":
                metadata["packages"].pop()
            else:
                metadata["resolve"]["nodes"][0]["features"] = ["internal-test-hooks"]
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.audit(metadata)


class RunnerFailure(unittest.TestCase):
    def test_changed_source_invalidates_the_qualification(self):
        from archive_compatibility import ArchiveRun
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "main.rs"
            source.write_text("fn main() {}")
            run = ArchiveRun(root, "unused", True)
            run.fixture = root
            run.script = "storage-upgrade-check"
            run.receipt.update(run.fingerprints())
            run.verify_inputs()
            source.write_text("fn main() { panic!(); }")
            with self.assertRaisesRegex(ValueError, "inputs changed"):
                run.verify_inputs()

    def test_failed_child_cannot_be_counted_as_a_pass_and_retains_diagnostics(self):
        runner = runpy.run_path(str(Path(__file__).resolve().parents[1] / "storage-upgrade-check"))["Run"]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = runner(root, "unused", True)
            with self.assertRaisesRegex(RuntimeError, "failed \\(9\\)"):
                run.command("failed-case", [sys.executable, "-c",
                            "import sys; print('failure detail', file=sys.stderr); sys.exit(9)"], root)
            self.assertNotEqual(run.receipt["status"], "pass")
            self.assertEqual(run.receipt["commands"][0]["exit_code"], 9)
            self.assertIn("failure detail", (root / "failed-case.stderr").read_text())


class RollingBoundary(unittest.TestCase):
    def setUp(self):
        self.runner = runpy.run_path(str(Path(__file__).resolve().parents[1] / "rolling-upgrade-check"))
        self.lines = [f'UPGRADE_OBSERVATION stage={stage} node={node} '
                      + json.dumps({"node": node, "ready": True})
                      for stage in self.runner["STAGES"] for node in (1, 2, 3)]
        self.lines += ["UPGRADE_REPLACEMENTS mode=graceful",
                       "UPGRADE_MEMBERSHIP mixed-versions learner-promoted-and-retired",
                       "UPGRADE_LOST_REPLY node=1",
                       "UPGRADE_RETRY attempt=1 command=fixture outcome=unknown",
                       "UPGRADE_HISTORY searched=25 discharged=0 events=[]",
                       "UPGRADE_COMPLETE published-to-candidate tls snapshots-both-directions membership durable-restarts",
                       "test result: ok. 1 passed; 0 failed;"]

    def test_process_consumer_keeps_identical_sources_without_unrelated_candidate_features(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = root / "fixture"
            fixture.mkdir()
            manifest = '[workspace]\nmembers = ["fenced-lock", "harness", "ledger", "sharded-counter"]\nresolver = "2"\n'
            (fixture / "Cargo.toml").write_text(manifest)
            for name in (*self.runner["MEMBERS"], "ledger"):
                member = fixture / name
                (member / "src").mkdir(parents=True)
                (member / "src/main.rs").write_bytes(b"unchanged process source\n")
                (member / "Cargo.toml").write_text('[package]\nname = "' + name + '"\n')
            (fixture / "ledger/Cargo.toml").write_text('rafter-service = { features = ["future-api"] }\n')
            for role in ("baseline", "candidate"):
                copied = root / role
                self.runner["copy_consumer"](fixture, copied)
                self.assertEqual(tomllib.loads((copied / "Cargo.toml").read_text())["workspace"],
                                 {"members": list(self.runner["MEMBERS"]), "resolver": "2"})
                self.assertFalse((copied / "ledger").exists())
                for name in self.runner["MEMBERS"]:
                    for relative in ("src/main.rs", "Cargo.toml"):
                        self.assertEqual((copied / name / relative).read_bytes(),
                                         (fixture / name / relative).read_bytes())
            (fixture / "Cargo.toml").write_text('[workspace]\nmembers = ["harness"]\n')
            with self.assertRaisesRegex(ValueError, "missing a required member"):
                self.runner["copy_consumer"](fixture, root / "incomplete")

    def test_complete_rolling_transcript_and_baseline_set(self):
        self.assertEqual(len(self.runner["observations"]("\n".join(self.lines))), 18)
        root = Path(__file__).resolve().parents[2]
        baseline_lock(root / "verification/rolling-upgrade-baseline.json", self.runner["PACKAGES"])

    def test_empty_missing_duplicate_unready_and_zero_test_evidence_is_refused(self):
        cases = [[], self.lines[1:], self.lines + self.lines[:1], self.lines[:-1],
                 [line for line in self.lines if not line.startswith("UPGRADE_HISTORY")],
                 [line.replace("searched=25", "searched=0") for line in self.lines],
                 [self.lines[0].replace('"ready": true', '"ready": false'), *self.lines[1:]],
                 [self.lines[0].replace('"node": 1', '"node": 9'), *self.lines[1:]]]
        for lines in cases:
            with self.subTest(lines=lines[:1]), self.assertRaises(ValueError):
                self.runner["observations"]("\n".join(lines))

    def test_lost_reply_and_every_bounded_retry_must_remain_in_the_evidence(self):
        cases = [
            [line for line in self.lines if not line.startswith("UPGRADE_LOST_REPLY")],
            self.lines + ["UPGRADE_LOST_REPLY node=2"],
            [line for line in self.lines if not line.startswith("UPGRADE_RETRY")],
            [line.replace("attempt=1 ", "attempt=2 ") for line in self.lines],
            self.lines + ["UPGRADE_RETRY attempt=1 command=fixture outcome=unknown"],
            [line.replace("searched=25", "searched=24") for line in self.lines],
        ]
        for lines in cases:
            with self.subTest(lines=lines[-5:]), self.assertRaises(ValueError):
                self.runner["observations"]("\n".join(lines))
        full = [line.replace("searched=25", "searched=32") for line in self.lines]
        full += [f"UPGRADE_RETRY attempt={number} command=fixture outcome=unknown"
                 for number in range(2, 9)]
        self.assertEqual(len(self.runner["observations"]("\n".join(full))), 18)
        full += ["UPGRADE_RETRY attempt=9 command=fixture outcome=unknown"]
        with self.assertRaises(ValueError):
            self.runner["observations"]("\n".join(full))

    def test_crash_replacements_require_exact_mode_order_and_signal(self):
        lines = [line.replace("mode=graceful", "mode=sigkill") for line in self.lines]
        markers = [f"UPGRADE_CRASH stage={stage} node={node} pid={100 + index} signal=9"
                   for index, (stage, node) in enumerate(self.runner["CRASHES"])]
        transcript = "\n".join(lines + markers)
        self.assertEqual(len(self.runner["observations"](transcript, "sigkill")), 18)
        self.assertEqual(len(self.runner["crash_records"](transcript, "sigkill")), 6)
        cases = [
            "\n".join(lines + markers[:-1]),
            "\n".join(lines + markers + markers[:1]),
            "\n".join(lines + list(reversed(markers))),
            transcript.replace("signal=9", "signal=15", 1),
            transcript.replace("pid=100", "pid=0", 1),
            transcript.replace("mode=sigkill", "mode=graceful"),
            transcript.replace("UPGRADE_REPLACEMENTS mode=sigkill", ""),
            transcript + "\nUPGRADE_REPLACEMENTS mode=sigkill",
            transcript + "\nUPGRADE_CRASH malformed-record",
        ]
        for case in cases:
            with self.subTest(case=case[-150:]), self.assertRaises(ValueError):
                self.runner["observations"](case, "sigkill")
        with self.assertRaises(ValueError):
            self.runner["observations"](transcript, "graceful")
        with self.assertRaises(ValueError):
            self.runner["observations"]("\n".join(self.lines + markers))

    def test_every_crashed_replica_retains_recovery_inputs_before_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for stage, node in self.runner["CRASHES"]:
                case = root / f"{stage}-node-{node}"
                for name in ("replica-identity", "control-plane", "transport-replay",
                             "raft/hard-state", "raft/log", "app/lock-state.0"):
                    path = case / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(b"retained bytes")
            inventory = self.runner["crash_inventory"](root)
            self.assertEqual(sum(item["kind"] == "file" for item in inventory.values()), 36)
            removed = root / "published-to-candidate-node-3/raft/log"
            saved = removed.read_bytes()
            removed.unlink()
            with self.assertRaisesRegex(ValueError, "pre-recovery input missing"):
                self.runner["crash_inventory"](root)
            removed.write_bytes(b"")
            compacted = self.runner["crash_inventory"](root)
            self.assertEqual(compacted["published-to-candidate-node-3/raft/log"]["bytes"], 0)
            removed.write_bytes(saved)
            hard_state = root / "candidate-restart-node-1/raft/hard-state"
            hard_state.write_bytes(b"")
            with self.assertRaisesRegex(ValueError, "pre-recovery input missing"):
                self.runner["crash_inventory"](root)
            hard_state.write_bytes(saved)
            application = root / "candidate-restart-node-3/app/lock-state.0"
            application.unlink()
            with self.assertRaisesRegex(ValueError, "application state missing"):
                self.runner["crash_inventory"](root)
            application.write_bytes(saved)
            (root / "unexpected-case").mkdir()
            with self.assertRaisesRegex(ValueError, "incomplete or unexpected"):
                self.runner["crash_inventory"](root)

    def test_multi_consumer_workspace_cannot_hide_an_extra_private_member(self):
        case = DependencyBoundary()
        case.setUp()
        workspace = {"rafter-storage-upgrade-probe": case.consumer / "Cargo.toml",
                     "rafter-reference-harness": case.consumer / "harness/Cargo.toml"}
        metadata = case.metadata
        metadata["packages"].append({"id": "harness", "name": "rafter-reference-harness", "source": None,
                                      "manifest_path": str(workspace["rafter-reference-harness"])})
        metadata["workspace_members"].append("harness")
        audit_graph(metadata, case.checkout, case.roots, case.consumer, "", workspace=workspace)
        metadata["packages"].append({"id": "private", "name": "rafter-sim", "source": None,
                                      "manifest_path": str(case.consumer / "private/Cargo.toml")})
        metadata["workspace_members"].append("private")
        with self.assertRaisesRegex(ValueError, "expected workspace"):
            audit_graph(metadata, case.checkout, case.roots, case.consumer, "", workspace=workspace)


if __name__ == "__main__":
    unittest.main()

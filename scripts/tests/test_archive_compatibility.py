"""Adversarial boundaries of the real-archive compatibility runners."""

import copy
import io
from pathlib import Path
import runpy
import sys
import tarfile
import tempfile
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


if __name__ == "__main__":
    unittest.main()

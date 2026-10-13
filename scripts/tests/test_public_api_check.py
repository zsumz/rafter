"""Controls for source identity and complete, unsuppressed public API checks."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from public_api_check import (
    BASELINE, comparison_command, inputs, validate_metadata, validate_report, verify_snapshot,
)


def package(name):
    return {"version": BASELINE, "name": name, "id": name, "source": None,
            "publish": None, "metadata": None, "dependencies": [], "features": {},
            "manifest_path": f"/snapshot/crates/{name}/Cargo.toml"}


def report(names):
    return "".join(
        f"    Checking {name} v{BASELINE} -> v{BASELINE} (assume patch change)\n"
        "     Checked [   0.012s] 229 checks: 229 pass, 31 skip\n"
        "     Summary no semver update required\n"
        f"    Finished [   1.011s] {name}\n" for name in names)


class Reports(unittest.TestCase):
    def setUp(self):
        self.packages = {name: package(name) for name in ("rafter", "rafter-service")}
        self.output = report(self.packages)

    def test_full_report_passes(self):
        validate_report(self.output, 0, self.packages)

    def test_tool_failure_is_not_api_success(self):
        for code in (100, 101, -9):
            with self.subTest(code=code), self.assertRaisesRegex(ValueError, "exit"):
                validate_report(self.output, code, self.packages)

    def test_empty_or_partial_report_is_refused(self):
        for text in ("", report(["rafter"]), self.output.split("    Finished")[0]):
            with self.subTest(text=text), self.assertRaises(ValueError):
                validate_report(text, 0, self.packages)

    def test_duplicate_and_unexpected_crates_are_refused(self):
        for name in ("rafter", "rafter-sim"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                validate_report(self.output + report([name]), 0, self.packages)

    def test_wrong_baseline_current_version_and_release_type_are_refused(self):
        for old, new in ((f"v{BASELINE} ->", "v0.0.1 ->"),
                         (f"-> v{BASELINE}", "-> v9.0.0"), ("patch change", "major change")):
            with self.subTest(new=new), self.assertRaises(ValueError):
                validate_report(self.output.replace(old, new), 0, self.packages)

    def test_warning_with_success_exit_is_refused(self):
        for warning in ("--- warning partial_ord_enum_variants_reordered: changed ---\n",
                        "warning: skipped package\n", "error: failed to parse\n"):
            with self.subTest(warning=warning), self.assertRaises(ValueError):
                validate_report(self.output + warning, 0, self.packages)

    def test_narrowed_lint_counts_are_refused(self):
        for text in ("228 checks: 228 pass, 32 skip", "0 checks: 0 pass, 260 skip",
                     "229 checks: 228 pass, 0 fail, 1 warn, 31 skip"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                validate_report(self.output.replace("229 checks: 229 pass, 31 skip", text),
                                0, self.packages)

    def test_missing_duplicate_or_unclean_summary_is_refused(self):
        for text in ("", "Summary no semver update required\nSummary no semver update required",
                     "Summary semver requires new major version"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                validate_report(self.output.replace("Summary no semver update required", text),
                                0, self.packages)

    def test_missing_or_duplicate_completion_is_refused(self):
        for old, new in (("Finished [   1.011s] rafter-service", "Finished [   1.011s] rafter"),
                         ("Finished [   1.011s] rafter-service", "")):
            with self.subTest(new=new), self.assertRaises(ValueError):
                validate_report(self.output.replace(old, new), 0, self.packages)

    def test_modes_select_every_public_package_explicitly(self):
        for mode in ("default-features", "all-features"):
            command = comparison_command(Path("/snapshot"), self.packages, mode, "example-host")
            self.assertIn(f"--{mode}", command)
            self.assertEqual(command[command.index("--baseline-version") + 1], BASELINE)
            self.assertEqual(command[command.index("--release-type") + 1], "patch")
            self.assertEqual(command[command.index("--target") + 1], "example-host")
            self.assertEqual([command[i + 1] for i, arg in enumerate(command) if arg == "-p"],
                             sorted(self.packages))
        with self.assertRaises(ValueError):
            comparison_command(Path("/snapshot"), self.packages, "only-explicit-features", "host")


class SourceIdentity(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.parent = Path(self.temporary.name).resolve()
        self.root = self.parent / "repository"
        self.root.mkdir()
        subprocess.run(["git", "init", "--quiet", str(self.root)], check=True)
        for name in ("Cargo.toml", "Cargo.lock", "RELEASE.md", "deleted.rs"):
            (self.root / name).write_text("original\n")
        (self.root / ".gitignore").write_text("/target\n")
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        self.snapshot = self.parent / "snapshot"
        self.snapshot.mkdir()

    def test_snapshot_checks_working_additions_edits_and_deletions(self):
        (self.root / "new.rs").write_text("new\n")
        (self.root / "Cargo.toml").write_text("edited\n")
        (self.root / "deleted.rs").unlink()
        (self.root / "target").mkdir()
        (self.root / "target/ignored").write_text("not a source input\n")
        records = inputs(self.root, self.snapshot)
        self.assertEqual((self.snapshot / "new.rs").read_text(), "new\n")
        self.assertEqual((self.snapshot / "Cargo.toml").read_text(), "edited\n")
        self.assertIsNone(records["deleted.rs"])
        self.assertFalse((self.snapshot / "target").exists())
        verify_snapshot(self.snapshot, records)
        (self.root / "new.rs").write_text("changed during run\n")
        self.assertNotEqual(inputs(self.root), records)

    def test_source_symlinks_are_refused(self):
        (self.root / "outside").symlink_to(self.parent)
        with self.assertRaisesRegex(ValueError, "symlinks"):
            inputs(self.root, self.snapshot)

    def test_symlinked_parent_of_tracked_file_is_refused(self):
        (self.root / "child").mkdir()
        (self.root / "child/source.rs").write_text("original\n")
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        (self.root / "child").rename(self.parent / "outside")
        (self.root / "child").symlink_to(self.parent / "outside")
        with self.assertRaisesRegex(ValueError, "symlinks"):
            inputs(self.root, self.snapshot)

    def test_checker_lock_rewrite_is_refused(self):
        records = inputs(self.root, self.snapshot)
        (self.snapshot / "Cargo.lock").write_text("re-resolved\n")
        with self.assertRaisesRegex(ValueError, "changed a source input"):
            verify_snapshot(self.snapshot, records)

    def test_checker_added_or_deleted_source_is_refused(self):
        records = inputs(self.root, self.snapshot)
        (self.snapshot / "extra.rs").write_text("extra\n")
        with self.assertRaisesRegex(ValueError, "inventory"):
            verify_snapshot(self.snapshot, records)
        (self.snapshot / "extra.rs").unlink()
        (self.snapshot / "Cargo.toml").unlink()
        with self.assertRaisesRegex(ValueError, "inventory"):
            verify_snapshot(self.snapshot, records)

    def test_missing_required_input_is_refused(self):
        (self.root / "Cargo.lock").unlink()
        with self.assertRaisesRegex(ValueError, "required workspace inputs"):
            inputs(self.root, self.snapshot)


class MetadataCoverage(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.names = ["rafter", "rafter-service", "rafter-transport-tls"]
        self.metadata = {"workspace_members": self.names, "metadata": None,
                         "packages": [package(name) for name in self.names]}
        for p in self.metadata["packages"]:
            p["manifest_path"] = str(self.root / "crates" / p["name"] / "Cargo.toml")
        (self.root / "RELEASE.md").write_text(
            "Publish these crates:\n\n```text\n" + "\n".join(self.names) + "\n```\n")

    def test_complete_inventory_passes(self):
        self.assertEqual(set(validate_metadata(self.metadata, self.root)), set(self.names))

    def test_manifest_suppressions_are_refused(self):
        for target in (self.metadata, self.metadata["packages"][0]):
            target["metadata"] = {"cargo-semver-checks": {"lints": {"function_missing": "allow"}}}
            with self.assertRaisesRegex(ValueError, "overrides"):
                validate_metadata(self.metadata, self.root)
            target["metadata"] = None

    def test_omitted_public_package_is_refused(self):
        self.metadata["packages"].pop()
        with self.assertRaises(ValueError):
            validate_metadata(self.metadata, self.root)

    def test_path_escape_is_refused(self):
        for path in ("/outside", str(self.root / ".." / "outside")):
            self.metadata["packages"][0]["dependencies"] = [{"path": path}]
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "escapes"):
                validate_metadata(self.metadata, self.root)


if __name__ == "__main__":
    unittest.main()

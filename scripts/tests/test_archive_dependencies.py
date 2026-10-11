"""Adversarial controls for the exact-archive public dependency projection."""

import copy
import hashlib
import json
from pathlib import Path
import sys
import subprocess
import tempfile
import tomllib
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from archive_dependencies import audit, derive, prepare, public_closure  # noqa: E402
from published_dependency_audit import AUDIT_VERSION, REGISTRY, audit_policy  # noqa: E402


class ArchiveClosure(unittest.TestCase):
    def setUp(self):
        self.root = "/isolated/unpacked"
        self.versions = {name: "1.0.0" for name in
                         ("rafter", "rafter-service", "rafter-transport-tls")}
        packages = [dict(id=name, name=name, version=version, source=None,
                         manifest_path=f"{self.root}/{name}-{version}/Cargo.toml",
                         features={}) for name, version in self.versions.items()]
        packages[1]["features"] = {"default": [], "durable": []}
        packages += [dict(id="consumer", name="consumer", version="0.0.0", source=None,
                          manifest_path="/isolated/consumer/Cargo.toml"),
                     dict(id="tls", name="tls", version="1.2.3", source=REGISTRY)]
        self.graph = {"packages": packages, "workspace_members": ["consumer"],
                      "resolve": {"nodes": [
                          dict(id=p["id"], features=list(p.get("features", {})),
                               deps=[self.edge("tls")] if p["name"] in self.versions else [])
                          for p in packages]}}
        self.lock = 'version = 4\n\n'
        for p in packages:
            self.lock += ('[[package]]\n' + f'name = "{p["name"]}"\n'
                          + f'version = "{p["version"]}"\n')
            if p["source"] is not None:
                self.lock += f'source = "{p["source"]}"\nchecksum = "' + "a" * 64 + '"\n'
            if p["name"] in self.versions:
                self.lock += 'dependencies = ["tls"]\n'
            self.lock += "\n"

    @staticmethod
    def edge(package, kind=None):
        return {"pkg": package, "dep_kinds": [{"kind": kind, "target": None}]}

    def derive(self):
        return public_closure(self.graph, self.lock, self.versions, self.root)

    def test_projection_preserves_package_records_and_every_public_feature(self):
        output, receipt = self.derive()
        original = tomllib.loads(self.lock)["package"]
        expected = [p for p in original if p["name"] != "consumer"]
        self.assertEqual(tomllib.loads(output)["package"], expected)
        self.assertEqual(receipt["package_count"], 4)
        self.assertEqual(receipt["registry_count"], 1)
        self.assertEqual(receipt["public_features"]["rafter-service"], ["default", "durable"])

    def test_missing_archive_and_omitted_features_are_refused(self):
        self.versions["rafter-new"] = "1.0.0"
        with self.assertRaisesRegex(ValueError, "exactly one resolved public archive"):
            self.derive()
        del self.versions["rafter-new"]
        self.graph["resolve"]["nodes"][1]["features"] = ["default"]
        with self.assertRaisesRegex(ValueError, "omitted public features"):
            self.derive()

    def test_public_inventory_cannot_hide_a_resolved_archive(self):
        del self.versions["rafter-service"]
        with self.assertRaisesRegex(ValueError, "resolved archive set"):
            self.derive()

    def test_checkout_substitution_wrong_version_and_workspace_root_are_refused(self):
        package = self.graph["packages"][0]
        package["manifest_path"] = "/checkout/crates/rafter/Cargo.toml"
        with self.assertRaisesRegex(ValueError, "expected archive"):
            self.derive()
        package["manifest_path"] = f"{self.root}/rafter-1.0.0/Cargo.toml"
        self.versions["rafter"] = "0.9.0"
        with self.assertRaisesRegex(ValueError, "expected archive"):
            self.derive()
        self.versions["rafter"] = "1.0.0"
        self.graph["workspace_members"].append("rafter")
        with self.assertRaisesRegex(ValueError, "isolated consumers"):
            self.derive()

    def test_dev_edges_are_excluded_but_private_build_edges_are_refused(self):
        dependencies = self.graph["resolve"]["nodes"][0]["deps"]
        dependencies.append(self.edge("consumer", "dev"))
        self.assertEqual(self.derive()[1]["package_count"], 4)
        dependencies[-1] = self.edge("consumer", "build")
        with self.assertRaisesRegex(ValueError, "private path"):
            self.derive()

    def test_missing_nodes_duplicate_packages_and_changed_lock_are_refused(self):
        original = copy.deepcopy(self.graph)
        self.graph["resolve"]["nodes"].pop()
        with self.assertRaisesRegex(ValueError, "complete and unique"):
            self.derive()
        self.graph = copy.deepcopy(original)
        self.graph["packages"].append(dict(self.graph["packages"][0]))
        with self.assertRaisesRegex(ValueError, "complete and unique"):
            self.derive()
        self.graph = original
        self.lock = self.lock.replace('version = "1.2.3"', 'version = "1.2.4"')
        with self.assertRaisesRegex(ValueError, "identities differ"):
            self.derive()

    def test_unknown_dependency_kind_and_internal_hooks_are_refused(self):
        node = self.graph["resolve"]["nodes"][0]
        node["deps"][0]["dep_kinds"] = [{"kind": "unknown"}]
        with self.assertRaisesRegex(ValueError, "dependency kind"):
            self.derive()
        node["deps"] = [self.edge("tls")]
        node["features"] = ["internal-test-hooks"]
        with self.assertRaisesRegex(ValueError, "internal test hooks"):
            self.derive()

    def test_lock_edges_cannot_be_silently_dropped_or_invented(self):
        self.graph["resolve"]["nodes"][0]["deps"] = []
        with self.assertRaisesRegex(ValueError, "edges disagree"):
            self.derive()
        self.graph["resolve"]["nodes"][0]["deps"] = [self.edge("missing")]
        with self.assertRaisesRegex(ValueError, "unknown package"):
            self.derive()

    def test_ambiguous_lock_edges_require_original_version_disambiguation(self):
        self.graph["packages"].append(dict(id="tls2", name="tls", version="2.0.0", source=REGISTRY))
        self.graph["resolve"]["nodes"].append(dict(id="tls2", features=[], deps=[]))
        self.lock += ('[[package]]\nname = "tls"\nversion = "2.0.0"\n'
                      + f'source = "{REGISTRY}"\nchecksum = "' + "b" * 64 + '"\n')
        with self.assertRaisesRegex(ValueError, "ambiguous or missing"):
            self.derive()
        self.lock = self.lock.replace('dependencies = ["tls"]', 'dependencies = ["tls 1.2.3"]')
        output, _ = self.derive()
        self.assertNotIn('version = "2.0.0"', output)
        self.assertIn('dependencies = ["tls 1.2.3"]', output)

    def test_unreviewed_sources_missing_checksums_and_trailing_data_are_refused(self):
        original = self.lock
        self.lock = self.lock.replace("a" * 64, "invalid")
        with self.assertRaisesRegex(ValueError, "checksum missing"):
            self.derive()
        self.lock = original + '\n[[patch.unused]]\nname = "unused"\nversion = "1.0.0"\n'
        with self.assertRaisesRegex(ValueError, "unexpected trailing lock data"):
            self.derive()
        self.lock = original.replace(REGISTRY, "git+https://example.invalid/repo")
        self.graph["packages"][-1]["source"] = "git+https://example.invalid/repo"
        with self.assertRaisesRegex(ValueError, "unreviewed dependency source"):
            self.derive()


class RetainedArtifactBoundary(unittest.TestCase):
    def setUp(self):
        fixture = ArchiveClosure()
        fixture.setUp()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir()
        self.release = self.root / "RELEASE.md"
        self.release.write_text("Publish these crates:\n```text\n"
                                + "\n".join(fixture.versions) + "\n```\n")
        self.write("Cargo.resolved.lock", fixture.lock)
        self.write("consumer-metadata.json", json.dumps(fixture.graph))
        hashes = []
        for name in ("Cargo.resolved.lock", "consumer-metadata.json"):
            checksum = hashlib.sha256((self.artifacts / name).read_bytes()).hexdigest()
            hashes.append(f"{checksum}  {name}\n")
        self.write("resolved-inputs.sha256", "".join(hashes))
        self.write("crate-versions.txt", "".join(f"{name} {version}\n"
                                                for name, version in fixture.versions.items()))
        self.write("package-sha256.txt", "".join("a" * 64 + f"  {name}-{version}.crate\n"
                                                for name, version in fixture.versions.items()))
        self.write("metadata-boundary.txt", "checkout-paths=none\nforbidden-path-dependencies=none\n"
                   "unpublished-crates=none\ninternal-test-hooks=none\n"
                   f"rafter-source-root={fixture.root}\n")

    def write(self, name, text):
        (self.artifacts / name).write_text(text)

    def prepare(self):
        return prepare(self.artifacts, self.release)

    def test_retained_inputs_work_after_the_original_archive_directory_is_gone(self):
        destination, receipt = self.prepare()
        self.assertEqual(receipt["package_count"], 4)
        self.assertEqual(len(receipt["source_sha256"]), 6)
        self.assertEqual(receipt["projected_lock_sha256"],
                         hashlib.sha256((destination / "Cargo.lock").read_bytes()).hexdigest())
        self.assertEqual(json.loads((destination / "receipt.json").read_text()), receipt)

    def test_modified_final_lock_or_metadata_is_refused_before_emitting_evidence(self):
        path = self.artifacts / "Cargo.resolved.lock"
        path.write_text(path.read_text() + "\n# changed\n")
        with self.assertRaisesRegex(ValueError, "resolution digest mismatch"):
            self.prepare()
        self.assertFalse((self.artifacts / "public-dependencies").exists())

    def test_digest_manifest_cannot_omit_duplicate_or_redirect_an_input(self):
        path = self.artifacts / "resolved-inputs.sha256"
        original = path.read_text()
        bad = [original.splitlines()[0] + "\n", original + original.splitlines()[0] + "\n",
               original.replace("consumer-metadata.json", "../../outside.json")]
        for text in bad:
            with self.subTest(text=text):
                path.write_text(text)
                with self.assertRaises(ValueError):
                    self.prepare()

    def test_symlinked_evidence_is_refused(self):
        path = self.artifacts / "consumer-metadata.json"
        saved = self.root / "saved.json"
        path.rename(saved)
        path.symlink_to(saved)
        with self.assertRaisesRegex(ValueError, "regular artifact missing"):
            self.prepare()

    def test_missing_archive_digest_or_version_is_refused(self):
        hashes = (self.artifacts / "package-sha256.txt").read_text()
        self.write("package-sha256.txt", "\n".join(hashes.splitlines()[1:]) + "\n")
        with self.assertRaisesRegex(ValueError, "digest inventory differs"):
            self.prepare()
        self.write("package-sha256.txt", hashes)
        self.write("crate-versions.txt", "rafter 1.0.0\n")
        with self.assertRaisesRegex(ValueError, "version inventory omits"):
            self.prepare()

    def test_failed_duplicate_or_missing_boundary_verdict_is_refused(self):
        path = self.artifacts / "metadata-boundary.txt"
        original = path.read_text()
        for text in (original.replace("checkout-paths=none", "checkout-paths=present"),
                     original + "checkout-paths=none\n", ""):
            with self.subTest(text=text):
                path.write_text(text)
                with self.assertRaises(ValueError):
                    self.prepare()

    def test_duplicate_release_or_version_inventory_is_refused(self):
        original = self.release.read_text()
        self.release.write_text(original.replace("rafter\n", "rafter\nrafter\n"))
        with self.assertRaisesRegex(ValueError, "public release inventory"):
            self.prepare()
        self.release.write_text(original)
        path = self.artifacts / "crate-versions.txt"
        path.write_text(path.read_text() + "rafter 9.0.0\n")
        with self.assertRaisesRegex(ValueError, "duplicate crate version"):
            self.prepare()

    def test_previous_projection_is_never_overwritten(self):
        destination, _ = self.prepare()
        receipt = (destination / "receipt.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.prepare()
        self.assertEqual((destination / "receipt.json").read_bytes(), receipt)


class ArchiveAudit(unittest.TestCase):
    def setUp(self):
        self.fixture = RetainedArtifactBoundary()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.artifacts = self.fixture.artifacts
        self.release = self.fixture.release
        self.projection, _ = self.fixture.prepare()
        self.destination = self.projection / "audit"
        self.version = AUDIT_VERSION
        self.returncode = 0
        self.effect = lambda: None
        self.report = {
            "database": {"advisory-count": 1000, "last-commit": "a" * 40,
                         "last-updated": "2026-10-01T00:00:00Z"},
            "lockfile": {"dependency-count": 4},
            "settings": {"target_arch": [], "target_os": [], "severity": None, "ignore": [],
                         "informational_warnings": ["unmaintained", "unsound", "notice"]},
            "vulnerabilities": {"found": False, "count": 0, "list": []}, "warnings": {},
        }

    def command(self, command, **kwargs):
        versions = {("cargo", "--version"): "cargo 1.88.0",
                    ("rustc", "--version", "--verbose"): "rustc 1.88.0",
                    ("cargo", "audit", "--version"): f"cargo-audit-audit {self.version}"}
        if tuple(command) in versions:
            return subprocess.CompletedProcess(command, 0, versions[tuple(command)].encode(), b"")
        self.assertEqual(command, ["cargo", "audit", "--file", str(self.projection / "Cargo.lock"),
                                   "--json", "--deny", "warnings"])
        self.assertEqual((Path(kwargs["cwd"]) / ".cargo/audit.toml").read_text(), audit_policy())
        self.effect()
        return subprocess.CompletedProcess(command, self.returncode,
                                           json.dumps(self.report).encode(), b"audit diagnostic")

    def run_audit(self):
        with patch("archive_dependencies.subprocess.run", side_effect=self.command):
            return audit(self.artifacts, self.release)

    def test_exact_retained_lock_is_audited_with_isolated_policy_and_tool_identity(self):
        destination, expected = self.run_audit()
        receipt = json.loads((destination / "receipt.json").read_text())
        self.assertEqual(receipt["package_count"], expected["package_count"])
        self.assertEqual(receipt["registry_count"], 1)
        self.assertEqual(receipt["report"], self.report)
        self.assertEqual(receipt["projection_receipt_sha256"],
                         hashlib.sha256((self.projection / "receipt.json").read_bytes()).hexdigest())
        self.assertEqual((destination / "audit-policy.toml").read_text(), audit_policy())
        self.assertEqual(receipt["versions"]["cargo-audit"], f"cargo-audit-audit {AUDIT_VERSION}")
        policy = tomllib.loads(audit_policy())
        self.assertTrue(policy["database"]["fetch"])
        self.assertFalse(policy["database"]["stale"])
        self.assertTrue(policy["yanked"]["enabled"])
        self.assertEqual(policy["output"]["deny"], ["warnings"])

    def test_substituted_projection_or_receipt_is_refused_before_running_tools(self):
        for name in ("Cargo.lock", "receipt.json"):
            path = self.projection / name
            original = path.read_text()
            path.write_text(original + "\n# changed\n" if name == "Cargo.lock" else "{}")
            with self.subTest(name=name), patch("archive_dependencies.subprocess.run") as run:
                with self.assertRaisesRegex(ValueError, "projection differs"):
                    audit(self.artifacts, self.release)
                run.assert_not_called()
                self.assertFalse(self.destination.exists())
            path.write_text(original)

    def test_projection_symlinks_are_refused(self):
        path = self.projection / "Cargo.lock"
        saved = self.fixture.root / "saved.lock"
        path.rename(saved)
        path.symlink_to(saved)
        with self.assertRaisesRegex(ValueError, "regular projection file"):
            self.run_audit()
        path.unlink()
        saved.rename(path)
        saved = self.fixture.root / "saved-projection"
        self.projection.rename(saved)
        self.projection.symlink_to(saved, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "projection is missing"):
            self.run_audit()

    def test_wrong_checker_version_is_refused_with_version_diagnostics(self):
        self.version = "0.22.1"
        with self.assertRaisesRegex(ValueError, "install cargo-audit"):
            self.run_audit()
        self.assertIn(self.version, (self.destination / "cargo-audit-version.txt").read_text())
        self.assertFalse((self.destination / "receipt.json").exists())

    def test_failed_audit_keeps_raw_report_without_a_success_receipt(self):
        self.returncode = 1
        self.report["warnings"] = {"unmaintained": [{"package": "tls"}]}
        with self.assertRaisesRegex(ValueError, "refused the dependency graph"):
            self.run_audit()
        self.assertEqual(json.loads((self.destination / "audit.json").read_text()), self.report)
        self.assertEqual((self.destination / "audit.stderr").read_text(), "audit diagnostic")
        self.assertFalse((self.destination / "receipt.json").exists())

    def test_rewritten_source_and_consistent_projection_during_audit_are_refused(self):
        def rewrite():
            path = self.artifacts / "crate-versions.txt"
            path.write_text(path.read_text() + "private 1.0.0\n")
            _, receipt = derive(self.artifacts, self.release)
            (self.projection / "receipt.json").write_text(json.dumps(receipt))
        self.effect = rewrite
        with self.assertRaisesRegex(ValueError, "evidence changed during"):
            self.run_audit()
        self.assertFalse((self.destination / "receipt.json").exists())

    def test_existing_audit_is_never_overwritten(self):
        self.run_audit()
        original = (self.destination / "receipt.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.run_audit()
        self.assertEqual((self.destination / "receipt.json").read_bytes(), original)


if __name__ == "__main__":
    unittest.main()

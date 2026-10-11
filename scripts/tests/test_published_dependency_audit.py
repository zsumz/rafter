"""Adversarial controls for public graph coverage and advisory evidence."""

import copy
import json
from pathlib import Path
import tempfile
import subprocess
import tomllib
import unittest
from unittest.mock import patch

from scripts.published_dependency_audit import (
    PROBE, REGISTRY, audit_lock, probe_manifest, public_packages, validate_closure, validate_report,
)


class PublicClosure(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.probe = self.root / "consumer"
        names = ("rafter", "rafter-service", "rafter-transport-tls")
        self.packages = {name: dict(id=name, name=name, version="1.0.0", source=None,
                                   manifest_path=str(self.root / "crates" / name / "Cargo.toml"),
                                   publish=None, features={}) for name in names}
        self.packages["rafter-service"]["features"] = {"default": [], "durable": []}
        (self.root / "RELEASE.md").write_text(
            "Publish these crates:\n```text\n" + "\n".join(names) + "\n```\n")
        probe = dict(id=PROBE, name=PROBE, version="0.0.0", source=None,
                     manifest_path=str(self.probe / "Cargo.toml"))
        registry = dict(id="tls", name="tls", version="1.2.3", source=REGISTRY)
        self.graph = {
            "workspace_members": [PROBE],
            "packages": [*copy.deepcopy(list(self.packages.values())), probe, registry],
            "resolve": {"nodes": [
                *[dict(id=name, features=list(p["features"]), deps=[{"pkg": "tls"}])
                  for name, p in self.packages.items()],
                dict(id=PROBE, features=[], deps=[{"pkg": name} for name in names]),
                dict(id="tls", features=[], deps=[]),
            ]},
        }
        self.lock = '\n'.join(
            f'[[package]]\nname = "{name}"\nversion = "1.0.0"\n' for name in names)
        self.lock += (f'\n[[package]]\nname = "{PROBE}"\nversion = "0.0.0"\n'
                      f'\n[[package]]\nname = "tls"\nversion = "1.2.3"\n'
                      f'source = "{REGISTRY}"\nchecksum = "' + "a" * 64 + '"\n')

    def validate(self):
        return validate_closure(self.graph, self.packages, self.probe, self.lock, self.lock)

    def test_complete_graph_preserves_exact_versions_and_enables_all_features(self):
        receipt = self.validate()
        self.assertEqual(receipt["package_count"], 5)
        self.assertEqual(len(receipt["registry_packages"]), 1)
        manifest = tomllib.loads(probe_manifest(self.packages, "1.88"))
        self.assertEqual(manifest["dependencies"]["rafter-service"]["features"],
                         ["default", "durable"])
        self.assertEqual(manifest["dependencies"]["rafter"]["version"], "=1.0.0")

    def test_cargo_inventory_excludes_private_packages_and_matches_release(self):
        private = dict(self.packages["rafter"], id="private", name="private", publish=[])
        metadata = {"workspace_members": [*self.packages, "private"],
                    "packages": [*self.packages.values(), private]}
        self.assertEqual(public_packages(metadata, self.root), self.packages)
        (self.root / "RELEASE.md").write_text("Publish these crates:\n```text\nrafter\n```\n")
        with self.assertRaisesRegex(ValueError, "differs from RELEASE"):
            public_packages(metadata, self.root)

    def test_missing_or_indirect_public_root_is_refused(self):
        self.graph["resolve"]["nodes"][-2]["deps"].pop()
        with self.assertRaisesRegex(ValueError, "directly enable"):
            self.validate()
        self.graph["packages"].pop(0)
        with self.assertRaisesRegex(ValueError, "every public root"):
            self.validate()

    def test_omitted_optional_feature_is_refused(self):
        self.graph["resolve"]["nodes"][1]["features"] = ["default"]
        with self.assertRaisesRegex(ValueError, "feature coverage"):
            self.validate()

    def test_wrong_public_version_or_path_is_refused(self):
        original = copy.deepcopy(self.graph)
        self.graph["packages"][0]["version"] = "0.9.0"
        with self.assertRaisesRegex(ValueError, "version or feature"):
            self.validate()
        self.graph = original
        self.graph["packages"][0]["manifest_path"] = str(self.root / "foreign/Cargo.toml")
        with self.assertRaisesRegex(ValueError, "path dependency"):
            self.validate()

    def test_private_path_git_dependency_or_internal_hooks_are_refused(self):
        original = copy.deepcopy(self.graph)
        self.graph["packages"][-1]["source"] = None
        with self.assertRaisesRegex(ValueError, "path dependency"):
            self.validate()
        self.graph = copy.deepcopy(original)
        self.graph["packages"][-1]["source"] = "git+https://example.invalid/repo"
        with self.assertRaisesRegex(ValueError, "Git dependency"):
            self.validate()
        self.graph = original
        self.graph["resolve"]["nodes"][0]["features"] = ["internal-test-hooks"]
        with self.assertRaisesRegex(ValueError, "internal test hooks"):
            self.validate()

    def test_lock_omission_changed_version_and_checksum_are_refused(self):
        for resolved, reason in [
            (self.lock.replace('name = "tls"', 'name = "omitted"'), "identities differ"),
            (self.lock.replace('version = "1.2.3"', 'version = "1.2.4"'), "identities differ"),
            (self.lock.replace("a" * 64, "b" * 64), "checksum differs"),
        ]:
            with self.subTest(reason=reason), self.assertRaisesRegex(ValueError, reason):
                validate_closure(self.graph, self.packages, self.probe, self.lock, resolved)


class AuditEvidence(unittest.TestCase):
    def setUp(self):
        self.report = json.loads('''{
          "database": {"advisory-count": 1000, "last-commit": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                       "last-updated": "2026-10-01T00:00:00Z"},
          "lockfile": {"dependency-count": 5},
          "settings": {"target_arch": [], "target_os": [], "severity": null, "ignore": [],
                       "informational_warnings": ["unmaintained", "unsound", "notice"]},
          "vulnerabilities": {"found": false, "count": 0, "list": []}, "warnings": {}
        }''')

    def test_complete_clean_report_is_accepted(self):
        validate_report(self.report, 0, 5)

    def test_nonzero_exit_different_inventory_and_unfetched_database_fail(self):
        with self.assertRaisesRegex(ValueError, "exit 1"):
            validate_report(self.report, 1, 5)
        with self.assertRaisesRegex(ValueError, "different package inventory"):
            validate_report(self.report, 0, 6)
        self.report["database"]["last-commit"] = None
        with self.assertRaisesRegex(ValueError, "database identity"):
            validate_report(self.report, 0, 5)

    def test_ignores_targets_severity_and_suppressed_warning_categories_fail(self):
        for key, value in [("ignore", ["RUSTSEC-2026-0285"]), ("target_os", ["linux"]),
                           ("target_arch", ["x86_64"]), ("severity", "high"),
                           ("informational_warnings", ["unsound"])]:
            with self.subTest(key=key):
                report = copy.deepcopy(self.report)
                report["settings"][key] = value
                with self.assertRaises(ValueError):
                    validate_report(report, 0, 5)

    def test_vulnerabilities_and_each_warning_fail_even_on_zero_exit(self):
        for key, value in [("found", True), ("count", 1), ("list", [{}])]:
            report = copy.deepcopy(self.report)
            report["vulnerabilities"][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "require review"):
                validate_report(report, 0, 5)
        for kind in ("yanked", "unmaintained", "unsound", "notice"):
            report = copy.deepcopy(self.report)
            report["warnings"] = {kind: [{}]}
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, "require review"):
                validate_report(report, 0, 5)

    def lock(self, root, name, count):
        path = root / name
        path.write_text("\n".join(f'[[package]]\nname = "p{n}"\nversion = "1.0.0"'
                                  for n in range(count)))
        return path

    def test_clean_public_graph_cannot_mask_a_workspace_advisory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            public = self.lock(root, "public.lock", 5)
            workspace = self.lock(root, "workspace.lock", 6)
            bad = copy.deepcopy(self.report)
            bad["lockfile"]["dependency-count"] = 6
            bad["vulnerabilities"] = {"found": True, "count": 1,
                                      "list": [{"advisory": {"id": "RUSTSEC-2026-0194"}}]}
            results = [subprocess.CompletedProcess([], code, json.dumps(report).encode(), b"")
                       for code, report in [(0, self.report), (1, bad)]]
            with patch("scripts.published_dependency_audit.subprocess.run", side_effect=results):
                self.assertEqual(audit_lock(public, root, root)["package_count"], 5)
                with self.assertRaisesRegex(ValueError, "exit 1"):
                    audit_lock(workspace, root, root, "workspace-audit")
            self.assertEqual(json.loads((root / "audit.json").read_text()), self.report)
            self.assertEqual(json.loads((root / "workspace-audit.json").read_text()), bad)

    def test_audit_tool_failure_preserves_diagnostics(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            lock = self.lock(root, "workspace.lock", 5)
            failure = subprocess.CompletedProcess([], 2, b"", b"database unavailable")
            with patch("scripts.published_dependency_audit.subprocess.run", return_value=failure):
                with self.assertRaisesRegex(ValueError, "workspace-audit failed.*exit 2"):
                    audit_lock(lock, root, root, "workspace-audit")
            self.assertEqual((root / "workspace-audit.stderr").read_bytes(), b"database unavailable")

    def test_report_must_match_the_selected_lock_and_its_unchanged_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            lock = self.lock(root, "workspace.lock", 6)
            result = subprocess.CompletedProcess([], 0, json.dumps(self.report).encode(), b"")
            with patch("scripts.published_dependency_audit.subprocess.run", return_value=result):
                with self.assertRaisesRegex(ValueError, "different package inventory"):
                    audit_lock(lock, root, root)
            self.lock(root, "workspace.lock", 5)

            def change_during_audit(*args, **kwargs):
                lock.write_text(lock.read_text() + "\n# replaced during audit\n")
                return result

            with patch("scripts.published_dependency_audit.subprocess.run", side_effect=change_during_audit):
                with self.assertRaisesRegex(ValueError, "lock changed during inspection"):
                    audit_lock(lock, root, root)


if __name__ == "__main__":
    unittest.main()

"""Adversarial controls for the exact-archive public dependency projection."""

import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import tomllib
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from archive_dependencies import prepare, public_closure  # noqa: E402
from published_dependency_audit import REGISTRY  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()

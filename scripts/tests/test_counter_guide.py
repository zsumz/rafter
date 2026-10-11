"""Counter qualification refuses mixed graphs and retains child cleanup ownership."""
import copy
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock

from scripts import counter_guide_deps as deps

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "counter_smoke", ROOT / "docs/examples/counter/smoke.py")
SMOKE = importlib.util.module_from_spec(spec)
spec.loader.exec_module(SMOKE)
REVISION = "a" * 40
NAMES = {"rafter", "rafter-service", "rafter-transport-tls"}


class CounterGraph(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.project = self.root / "copy"
        self.crates = {name: self.root / "crates" / name for name in NAMES}
        self.graph = {"packages": [
            {"name": name, "manifest_path": str(directory / "Cargo.toml"), "source": None}
            for name, directory in sorted(self.crates.items())
        ]}

    def validate(self, graph, mode="checkout"):
        return deps.validate_graph(graph, self.crates, REVISION, NAMES, mode, self.project)

    def pinned(self):
        graph = copy.deepcopy(self.graph)
        for package in graph["packages"]:
            package["source"] = f"git+{deps.SOURCE}?rev={REVISION}#{REVISION}"
            package["manifest_path"] = str(self.root / "git-cache" / package["name"] / "Cargo.toml")
        return graph

    def test_checkout_graph_uses_every_required_crate(self):
        self.assertEqual(self.validate(self.graph), NAMES)

    def test_pinned_graph_uses_one_exact_git_source(self):
        self.assertEqual(self.validate(self.pinned(), "pinned"), NAMES)

    def test_checkout_lane_cannot_silently_build_the_git_pin(self):
        with self.assertRaisesRegex(ValueError, "did not resolve to the checkout"):
            self.validate(self.pinned())

    def test_path_dependency_outside_checkout_is_refused(self):
        self.graph["packages"][0]["manifest_path"] = str(self.root / "another/Cargo.toml")
        with self.assertRaisesRegex(ValueError, "did not resolve to the checkout"):
            self.validate(self.graph)

    def test_missing_required_crate_is_refused(self):
        self.graph["packages"].pop()
        with self.assertRaisesRegex(ValueError, "omitted required"):
            self.validate(self.graph)

    def test_duplicate_crate_sources_are_refused(self):
        self.graph["packages"].append(dict(self.graph["packages"][0]))
        with self.assertRaisesRegex(ValueError, "multiple copies"):
            self.validate(self.graph)

    def test_unpublished_rafter_dependency_is_refused(self):
        self.graph["packages"].append({
            "name": "rafter-invariants", "source": None,
            "manifest_path": str(self.root / "crates/rafter-invariants/Cargo.toml"),
        })
        with self.assertRaisesRegex(ValueError, "unpublished"):
            self.validate(self.graph)

    def test_pinned_lane_cannot_depend_on_checkout_paths(self):
        with self.assertRaisesRegex(ValueError, "pinned Git commit"):
            self.validate(self.graph, "pinned")

    def test_pinned_lane_refuses_a_mixed_revision(self):
        graph = self.pinned()
        graph["packages"][0]["source"] = graph["packages"][0]["source"].replace(REVISION, "b" * 40)
        with self.assertRaisesRegex(ValueError, "pinned Git commit"):
            self.validate(graph, "pinned")

    def test_private_manifests_do_not_enter_the_publishable_inventory(self):
        for name in NAMES | {"rafter-invariants"}:
            directory = self.root / "crates" / name
            directory.mkdir(parents=True)
            private = "publish = false\n" if name == "rafter-invariants" else ""
            (directory / "Cargo.toml").write_text(f'[package]\nname = "{name}"\n{private}')
        self.assertEqual(deps.published_crates(self.root), self.crates)

    def test_incomplete_publishable_inventory_is_refused(self):
        with self.assertRaisesRegex(ValueError, "inventory is incomplete"):
            deps.published_crates(self.root)

    def test_git_pins_must_be_full_and_uniform(self):
        manifest = self.root / "Cargo.toml"
        for first, second in [("abc123", REVISION), (REVISION, "b" * 40)]:
            manifest.write_text(
                f'[dependencies]\nrafter = {{ git = "{deps.SOURCE}", rev = "{first}" }}\n'
                f'rafter-service = {{ git = "{deps.SOURCE}", rev = "{second}" }}\n')
            with self.assertRaises(ValueError):
                deps.pinned_dependencies(manifest)
        manifest.write_text(f'[dependencies]\nrafter = {{ git = "{deps.SOURCE}", rev = "{REVISION}" }}\n')
        self.assertEqual(deps.pinned_dependencies(manifest), (REVISION, {"rafter"}))


class SmokeOwnership(unittest.TestCase):
    def cluster(self):
        cluster = SMOKE.Cluster.__new__(SMOKE.Cluster)
        child = Mock()
        child.poll.return_value = None
        child.returncode = 0
        cluster.children, cluster.logs = {1: child}, []
        cluster.request = Mock(return_value={"stopping": 1})
        return cluster, child

    def test_failed_stop_request_keeps_the_child_for_cleanup(self):
        cluster, child = self.cluster()
        cluster.request.side_effect = OSError("HTTP stop failed")
        with self.assertRaises(OSError):
            cluster.stop(1, graceful=True)
        cluster.close()
        child.kill.assert_called_once_with()
        child.wait.assert_called_once_with(timeout=15)

    def test_stop_timeout_keeps_the_child_for_cleanup(self):
        cluster, child = self.cluster()
        child.wait.side_effect = [subprocess.TimeoutExpired("counter", 15), 0]
        with self.assertRaises(subprocess.TimeoutExpired):
            cluster.stop(1, graceful=True)
        cluster.close()
        child.kill.assert_called_once_with()
        self.assertEqual(child.wait.call_count, 2)


if __name__ == "__main__":
    unittest.main()

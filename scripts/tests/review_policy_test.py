#!/usr/bin/env python3
"""Adversarial checks for reconciliation and full-qualification preflight."""

import copy
import hashlib
import importlib.machinery
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
def load_script(name, path):
    loader = importlib.machinery.SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


reconcile = load_script("reconcile", ROOT / "scripts/zrail-reconcile")
preflight = load_script("preflight", ROOT / "scripts/invariants-pr-preflight")
CONTRACT = '\n'.join([
    '[source.rust.macros]', 'mode = "deny-unreviewed"',
    '[[source.rust.macros.allow]]', 'name = "assert"', 'reason = "std assertion"',
    reconcile.BEGIN.rstrip(), '[[source.rust.macros.allow]]',
    'name = "support::assert"', 'reason = "reviewed exact identity"',
    '[[source.rust.item_macros]]', 'path = "old.rs"', 'name = "known"',
    'reason = "reviewed exact site"', reconcile.END.rstrip(), '',
])
STALE = {"id": "RUST-MACRO-002", "message": 'allowed macro expansion "support::assert" is stale'}


class ReconcileTests(unittest.TestCase):
    def assert_denied_without_mutation(self, findings):
        with tempfile.TemporaryDirectory() as directory:
            contract = Path(directory) / "zrail.toml"
            contract.write_text(CONTRACT)
            report = subprocess.CompletedProcess([], 1, json.dumps({"findings": findings}), "")
            with patch.object(reconcile, "CONTRACT", contract), patch.object(sys, "argv", ["zrail-reconcile"]), patch.object(reconcile.subprocess, "run", return_value=report):
                with self.assertRaises(ValueError):
                    reconcile.main()
            self.assertEqual(contract.read_text(), CONTRACT)

    def test_unknown_item_is_not_reviewed_by_generation(self):
        self.assert_denied_without_mutation([
            {"id": "RUST-GRAPH-003", "path": "new.rs", "message": "item-position macro never_reviewed!"},
            {"id": "RUST-MACRO-001", "message": "macro expansion never_reviewed"}, STALE,
        ])

    def test_known_macro_at_new_item_site_needs_review(self):
        self.assert_denied_without_mutation([
            {"id": "RUST-GRAPH-003", "path": "new.rs", "message": "item-position macro assert!"},
        ])

    def test_new_literal_include_site_needs_review(self):
        self.assert_denied_without_mutation([
            {"id": "RUST-GRAPH-003", "path": "new.rs", "message": "literal include needs an exemption"},
        ])

    def test_stale_item_exemption_is_pruned(self):
        result = reconcile.reconcile(CONTRACT, [
            {"id": "RUST-GRAPH-005", "path": "old.rs", "message": "item macro exemption known! is stale"},
        ])
        self.assertNotIn('name = "known"', result)
        self.assertIn('name = "support::assert"', result)

    def test_other_origin_cannot_borrow_a_reviewed_suffix(self):
        self.assert_denied_without_mutation([
            {"id": "RUST-MACRO-001", "message": "macro expansion other_dependency::assert"},
        ])

    def test_builtin_spelling_does_not_prove_identity(self):
        self.assert_denied_without_mutation([
            {"id": "RUST-MACRO-001", "message": "macro expansion foreign::include"},
        ])

    def test_changed_opaque_input_needs_review(self):
        self.assert_denied_without_mutation([
            {"id": "RUST-MACRO-003", "message": "macro assert has unreviewed opaque input"},
        ])

    def test_unknown_macro_diagnostic_fails_closed(self):
        self.assert_denied_without_mutation([
            {"id": "RUST-MACRO-999", "message": "identity is unresolved"}, STALE,
        ])

    def test_stale_exact_entries_are_pruned_without_rewording_survivors(self):
        result = reconcile.reconcile(CONTRACT, [STALE])
        self.assertNotIn('name = "support::assert"', result)
        self.assertIn('reason = "reviewed exact site"', result)
        self.assertIn('name = "assert"', result)

    def test_unrelated_error_does_not_claim_architecture_success(self):
        self.assertEqual(reconcile.reconcile(CONTRACT, [
            {"id": "SIZE-001", "message": "file too large"},
        ]), CONTRACT)


class PreflightTests(unittest.TestCase):
    def test_repository_budget_covers_all_required_layers(self):
        preflight.check_budget(ROOT)

    def test_old_45_minute_budget_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "verification").mkdir()
            (root / "verification/raft-invariant-profiles.json").write_bytes((ROOT / "verification/raft-invariant-profiles.json").read_bytes())
            (root / "zcheck.toml").write_text('[tasks.invariants-pr]\ntimeout = "45m"\n')
            with self.assertRaisesRegex(ValueError, "at least 478m"):
                preflight.check_budget(root)

    def test_wrong_java_missing_jar_and_swapped_jar_fail_before_execution(self):
        profile = copy.deepcopy(preflight.check_budget(ROOT))
        data = b"test-only reviewed tool"
        digest = hashlib.sha256(data).hexdigest()
        profile["runners"]["tla"]["configuration"]["tool_sha256"] = digest
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "tools/tla").mkdir(parents=True)
            (root / "tools/cache").mkdir()
            (root / "tools/tla/SHA256SUMS").write_text(f"{digest}  tla2tools.jar\n")
            jar = root / "tools/cache/tla2tools.jar"
            probe = subprocess.CompletedProcess([], 0, "", 'openjdk version "17.0.1"')
            with patch.object(preflight.subprocess, "run", return_value=probe):
                with self.assertRaisesRegex(ValueError, "Java 21"):
                    preflight.check_tools(root, profile)
                probe.stderr = 'openjdk version "21.0.11"'
                with self.assertRaisesRegex(ValueError, "install the pinned"):
                    preflight.check_tools(root, profile)
                jar.write_bytes(b"wrong tool")
                with self.assertRaisesRegex(ValueError, "does not match"):
                    preflight.check_tools(root, profile)
                jar.write_bytes(data)
                preflight.check_tools(root, profile)


if __name__ == "__main__":
    unittest.main()

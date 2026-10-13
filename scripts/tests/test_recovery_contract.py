"""The focused recovery command must not turn incomplete checks into a pass."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
INVENTORY = "reference-process-test-inventory.fenced-lock-production.txt"


class RecoveryContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="recovery contract ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ("scripts", "reference", "verification", "bin"):
            (self.root / name).mkdir()
        for name in ("recovery-contract-check", "cargo-test-exact"):
            shutil.copy2(ROOT / "scripts" / name, self.root / "scripts" / name)
        shutil.copy2(ROOT / "verification" / INVENTORY, self.root / "verification" / INVENTORY)
        self.shell("scripts/reference-source-check", 'printf "%s\\n" --config "patch.crates-io.rafter.path=\'/test checkout\'"')
        self.shell("bin/git", "echo fixture-source")
        self.shell("bin/rustc", "echo fixture-compiler")
        cargo = self.root / "bin/cargo"
        cargo.write_text(f"#!{sys.executable}\n" + '''
import json, os, pathlib, sys
args = sys.argv[1:]
if args == ["--version"]:
    print("fixture-cargo")
    raise SystemExit(0)
root = pathlib.Path(os.environ["FIXTURE_ROOT"])
with (root / "calls").open("a") as out:
    out.write(json.dumps(args) + "\\n")
assert "patch.crates-io.rafter.path='/test checkout'" in args
assert "--test-threads=1" in args
target = args[args.index("--test") + 1]
if target == "process_production":
    assert "--ignored" in args
    names = (root / "verification" / os.environ["INVENTORY"]).read_text().splitlines()
else:
    assert "--exact" in args
    names = [args[args.index("--exact") + 1]]
if "--list" in args:
    if os.environ.get("BAD_LIST") == "empty":
        names = []
    elif os.environ.get("BAD_LIST") == "renamed":
        names[-1] = "wrong_test_with_the_same_count"
    for name in names:
        print(name + ": test")
else:
    with (root / "executions").open("a") as out:
        out.write(target + "\\n")
    raise SystemExit(int(os.environ.get("EXECUTION_EXIT", "0")))
''')
        cargo.chmod(0o755)
        self.env = dict(os.environ, PATH=str(self.root / "bin") + os.pathsep + os.environ["PATH"],
                        FIXTURE_ROOT=str(self.root), INVENTORY=INVENTORY)

    def shell(self, name, body):
        path = self.root / name
        path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + body + "\n")
        path.chmod(0o755)

    def run_contract(self, *args):
        return subprocess.run([str(self.root / "scripts/recovery-contract-check"), *args],
                              cwd=self.root.parent, env=self.env, text=True,
                              capture_output=True, check=False)

    def test_complete_run_executes_every_selection(self):
        result = self.run_contract()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("all 15 selected tests passed", result.stdout)
        executions = (self.root / "executions").read_text().splitlines()
        self.assertEqual(len(executions), 8)
        self.assertEqual(executions[-1], "process_production")

    def test_listing_cannot_claim_execution(self):
        result = self.run_contract("--list-only")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("execution not performed", result.stdout)
        self.assertNotIn("selected tests passed", result.stdout)
        self.assertFalse((self.root / "executions").exists())

    def test_failed_selection_stops_before_later_cases(self):
        self.env["EXECUTION_EXIT"] = "23"
        result = self.run_contract()
        self.assertEqual(result.returncode, 23, result.stdout + result.stderr)
        self.assertEqual(len((self.root / "executions").read_text().splitlines()), 1)
        self.assertNotIn("selected tests passed", result.stdout)

    def test_missing_or_renamed_case_never_executes(self):
        for failure in ("empty", "renamed"):
            with self.subTest(failure=failure):
                self.env["BAD_LIST"] = failure
                result = self.run_contract()
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((self.root / "executions").exists())
                self.assertNotIn("selected tests passed", result.stdout)

    def test_patch_failure_or_empty_output_never_reaches_cargo(self):
        for body in ("exit 17", "exit 0"):
            with self.subTest(body=body):
                self.shell("scripts/reference-source-check", body)
                result = self.run_contract()
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((self.root / "calls").exists())

    def test_unknown_option_fails_before_building(self):
        result = self.run_contract("--skip-reseed")
        self.assertEqual(result.returncode, 2)
        self.assertFalse((self.root / "calls").exists())


if __name__ == "__main__":
    unittest.main()

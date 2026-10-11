#!/usr/bin/env python3
"""The soak runner must reject missing source identity and changed selections."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
NAME = (ROOT / "verification/reference-owner-soak-inventory.txt").read_text().strip()


class OwnerSoakRunnerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="rafter-soak-runner-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        for directory in ("scripts", "reference", "verification", "bin"):
            (self.root / directory).mkdir()
        for script in ("reference-owner-soak", "cargo-test-exact"):
            shutil.copy2(ROOT / "scripts" / script, self.root / "scripts" / script)
        shutil.copy2(
            ROOT / "verification/reference-owner-soak-inventory.txt",
            self.root / "verification/reference-owner-soak-inventory.txt",
        )
        self.script(
            "scripts/reference-source-check",
            'printf "%s\\n" --config "patch.crates-io.rafter.path=\'/test checkout\'"',
        )
        self.script("bin/git", 'echo test-source')
        self.script("bin/rustc", 'echo test-toolchain')
        self.script("bin/cargo", '''
if [[ "$1" == --version ]]; then echo test-cargo; exit 0; fi
printf '%s\\n' "$@" >> "$CALLS"
if [[ " $* " == *" --list "* ]]; then
    printf '%s: test\\n' "$LISTED_TEST"
else
    touch "$EXECUTED"
    exit "${EXECUTION_EXIT:-0}"
fi
''')
        self.env = dict(
            os.environ,
            PATH=str(self.root / "bin") + os.pathsep + os.environ["PATH"],
            LISTED_TEST=NAME,
            CALLS=str(self.root / "calls"),
            EXECUTED=str(self.root / "executed"),
        )

    def script(self, name, body):
        path = self.root / name
        path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + body + "\n")
        path.chmod(0o755)

    def run_soak(self, *arguments):
        return subprocess.run(
            [str(self.root / "scripts/reference-owner-soak"), *arguments],
            cwd=self.root.parent,
            env=self.env,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_list_only_checks_inventory_without_executing(self):
        result = self.run_soak("--list-only")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.root / "executed").exists())
        arguments = (self.root / "calls").read_text().splitlines()
        for argument in (
            "--ignored", "--test-threads=1", "--nocapture", "service_owner_tls",
            "patch.crates-io.rafter.path='/test checkout'",
        ):
            self.assertIn(argument, arguments)

    def test_test_execution_failure_propagates(self):
        self.env["EXECUTION_EXIT"] = "23"
        result = self.run_soak()
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertTrue((self.root / "executed").exists())

    def test_wrong_inventory_never_executes(self):
        self.env["LISTED_TEST"] = "different_test_with_the_same_count"
        result = self.run_soak()
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / "executed").exists())

    def test_empty_selection_never_executes(self):
        self.script("bin/cargo", 'echo "0 tests, 0 benchmarks"')
        result = self.run_soak()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("matched 0 tests", result.stderr)
        self.assertFalse((self.root / "executed").exists())

    def test_failed_or_empty_patching_never_reaches_cargo(self):
        for body in ("exit 17", "exit 0"):
            with self.subTest(body=body):
                self.script("scripts/reference-source-check", body)
                result = self.run_soak()
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((self.root / "calls").exists())

    def test_missing_source_identity_never_reaches_cargo(self):
        self.script("bin/git", "exit 19")
        result = self.run_soak()
        self.assertEqual(result.returncode, 19, result.stderr)
        self.assertFalse((self.root / "calls").exists())


if __name__ == "__main__":
    unittest.main()

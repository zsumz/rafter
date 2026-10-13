"""Runtime retrieval retries keep installation behind complete verified downloads.

These are shell execution/argument tests with fake package tools. Ubuntu's real
signature/hash verification and runtime identity remain hosted qualification.
"""

import re
import unittest

from scripts.tests.invariant_runtime_fixture import Fixture, PACKAGES, ROOT


class RuntimeSetupTests(unittest.TestCase):
    def assert_pins_and_source(self, fixture):
        calls = fixture.calls("apt-get")
        for call in calls:
            args = call["args"]
            self.assertEqual(call["frontend"], "noninteractive")
            for value in [f"Dir::Etc::sourcelist={fixture.state}/ubuntu.sources",
                          "Dir::Etc::sourceparts=-",
                          f"Dir::State::lists={fixture.state}/lists",
                          f"Dir::Cache::archives={fixture.state}/archives",
                          "Acquire::Retries=3", "Acquire::Languages=none"]:
                self.assertIn(value, args)
                self.assertEqual(args[args.index(value) - 1], "-o")
            for forbidden in ["--ignore-missing", "--fix-missing", "--allow-unauthenticated",
                              "--allow-insecure-repositories", "--force-yes"]:
                self.assertNotIn(forbidden, args)
            if call["phase"] == "update":
                self.assertIn("--error-on=any", args)
            else:
                self.assertEqual(args[-len(fixture.pins):], fixture.pins)
                for option in ["-y", "--no-remove", "--no-install-recommends",
                               "--allow-downgrades", "--reinstall"]:
                    self.assertIn(option, args)
                required = "--download-only" if call["phase"] == "download" else "--no-download"
                self.assertIn(required, args)
        return [call["phase"] for call in calls]

    def test_success_installs_once_from_the_same_cache_then_verifies_every_pin(self):
        with Fixture() as fixture:
            result = fixture.run()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.assert_pins_and_source(fixture), ["update", "download", "install"])
            queries = fixture.calls("dpkg-query")
            self.assertEqual([row["args"] for row in queries],
                             [["-W", "-f=${Version}", package.split("=", 1)[0]] for package in PACKAGES])
            self.assertEqual(fixture.calls("sleep"), [])

    def test_transient_metadata_and_package_failures_resume_with_bounded_backoff(self):
        with Fixture(update=[100, 0], download=[100, 100, 0]) as fixture:
            result = fixture.run()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.assert_pins_and_source(fixture),
                             ["update", "update", "download", "download", "download", "install"])
            self.assertEqual([row["args"] for row in fixture.calls("sleep")], [["10"], ["10"], ["20"]])
            self.assertEqual((fixture.state / "archives/retained.deb").read_text(), "verified partial download")

    def test_persistent_retrieval_errors_never_install_or_check_versions(self):
        for phase in ["update", "download"]:
            for diagnostic in ["503 Service Unavailable", "authentication failed", "Hash Sum mismatch"]:
                with self.subTest(phase=phase, diagnostic=diagnostic), Fixture(**{phase: [100]}, diagnostic=diagnostic) as fixture:
                    result = fixture.run()
                    self.assertEqual(result.returncode, 100, result.stderr)
                    expected = [phase] * 4 if phase == "update" else ["update"] + [phase] * 4
                    self.assertEqual(self.assert_pins_and_source(fixture), expected)
                    self.assertEqual([row["args"] for row in fixture.calls("sleep")], [["10"], ["20"], ["40"]])
                    self.assertEqual(fixture.calls("dpkg-query"), [])
                    self.assertIn(diagnostic, result.stderr)
                    self.assertIn("attempt 4", result.stderr)

    def test_signal_and_execution_failures_are_not_retried(self):
        for status in [2, 124, 127, 130, 143]:
            with self.subTest(status=status), Fixture(download=[status]) as fixture:
                result = fixture.run()
                self.assertEqual(result.returncode, status, result.stderr)
                self.assertEqual(self.assert_pins_and_source(fixture), ["update", "download"])
                self.assertEqual(fixture.calls("sleep"), [])
                self.assertEqual(fixture.calls("dpkg-query"), [])

    def test_install_failure_is_not_retried_or_reported_as_ready(self):
        with Fixture(install=[100]) as fixture:
            result = fixture.run()
            self.assertEqual(result.returncode, 100, result.stderr)
            self.assertEqual(self.assert_pins_and_source(fixture), ["update", "download", "install"])
            self.assertEqual(fixture.calls("sleep"), [])
            self.assertEqual(fixture.calls("dpkg-query"), [])

    def test_installed_version_mismatch_and_query_failure_refuse_setup(self):
        for scenario in [{"installed": "wrong-version"}, {"query_exit": 9}]:
            with self.subTest(scenario=scenario), Fixture(**scenario) as fixture:
                result = fixture.run()
                self.assertNotEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.assert_pins_and_source(fixture), ["update", "download", "install"])
                self.assertEqual(len(fixture.calls("dpkg-query")), 1)

    def test_invalid_pins_are_rejected_before_package_tools_run(self):
        for packages in [[], ["bash"], ["--allow-unauthenticated"], ["bash=1 extra"], ["bash=1\nperl=2"]]:
            with self.subTest(packages=packages), Fixture() as fixture:
                result = fixture.run(packages)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(fixture.calls(), [])

    def test_cancellation_during_download_or_backoff_stops_before_installation(self):
        for scenario in [{"block": "download"}, {"download": [100], "block": "sleep"}]:
            with self.subTest(scenario=scenario), Fixture(**scenario) as fixture:
                result = fixture.cancel_when_blocked()
                self.assertEqual(result.returncode, 143, result.stderr)
                self.assertEqual(self.assert_pins_and_source(fixture), ["update", "download"])
                self.assertEqual(fixture.calls("dpkg-query"), [])

    def test_a_failed_backoff_does_not_fall_through_to_installation(self):
        with Fixture(download=[100], sleep_exit=3) as fixture:
            result = fixture.run()
            self.assertEqual(result.returncode, 3, result.stderr)
            self.assertEqual(self.assert_pins_and_source(fixture), ["update", "download"])

    def test_all_reviewed_runtime_and_analysis_pins_reach_the_helper_unchanged(self):
        action = (ROOT / ".github/actions/setup-invariant-runtime/action.yml").read_text()
        blocks = re.findall(r"packages\+?=\((.*?)\)", action, re.DOTALL)
        self.assertEqual(len(blocks), 2)
        pins = re.findall(r"[a-z0-9][a-z0-9+.-]*=[a-zA-Z0-9.+:~_-]+", "\n".join(blocks))
        self.assertEqual(len(pins), 11)
        with Fixture(pins=pins) as fixture:
            result = fixture.run()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assert_pins_and_source(fixture)
            self.assertEqual(len(fixture.calls("dpkg-query")), 11)

    def test_every_caller_reserves_time_for_setup_cleanup(self):
        callers = 0
        for name in ["ci", "nightly", "weekly"]:
            workflow = (ROOT / f".github/workflows/{name}.yml").read_text()
            blocks = re.findall(r"uses: \./\.github/actions/setup-invariant-runtime\n(.*?)(?=\n      - |\Z)",
                                workflow, re.DOTALL)
            callers += len(blocks)
            for block in blocks:
                timeout = re.search(r"timeout-minutes: (\d+)", block)
                self.assertIsNotNone(timeout, name)
                self.assertGreaterEqual(int(timeout.group(1)), 17, name)
        self.assertEqual(callers, 16)

    def test_action_and_local_and_ci_gates_enforce_the_helper(self):
        action = (ROOT / ".github/actions/setup-invariant-runtime/action.yml").read_text()
        invoke = 'bash "$GITHUB_ACTION_PATH/install-packages.sh" "$state" "${packages[@]}"'
        self.assertEqual(action.count(invoke), 1)
        self.assertIn("timeout --signal=TERM --kill-after=10s 15m", action)
        self.assertLess(action.index(invoke), action.index("sha256sum"))
        self.assertIn("Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg", action)
        self.assertIn("snapshot=20261009T000000Z", action)
        self.assertIn("test \"$VERSION_ID\" = 24.04", action)
        for path in [".github/workflows/ci.yml", "zcheck.toml", "zrail.toml"]:
            self.assertIn("scripts/tests/test_invariant_runtime_setup.py", (ROOT / path).read_text())
        self.assertIn('"runtime-setup-tests",', (ROOT / "zcheck.toml").read_text())
        self.assertIn(".github/actions/setup-invariant-runtime/install-packages.sh", (ROOT / "zrail.toml").read_text())


if __name__ == "__main__":
    unittest.main()

"""Partial reruns keep producer provenance and cannot resurrect stale success."""

import contextlib
import copy
import io
import json
import os
import re
import textwrap
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts import invariant_evidence as selector


HEAD = "a" * 40
REPO = "example/rafter"


def fixture(profile="pr", attempts=(2, 2, 1, 2)):
    run = {"id": 42, "run_attempt": 2, "head_sha": HEAD,
           "repository": {"id": 9, "full_name": REPO}}
    jobs, artifacts = [], []
    layers = ("tests", "simulator", "tla") + (() if profile == "pr" else ("maelstrom",))
    for number, (layer, attempt) in enumerate(zip(layers, attempts), 1):
        jobs.append({"id": number, "run_id": 42, "run_attempt": attempt, "head_sha": HEAD,
                     "name": f"invariants-{layer}" + ("" if profile == "pr" else f"-{profile}"),
                     "status": "completed", "conclusion": "success"})
        artifacts.append({"id": 100 + number,
                          "name": f"invariants-{profile}-evidence-{layer}-42-{attempt}",
                          "expired": False,
                          "workflow_run": {"id": 42, "head_sha": HEAD, "repository_id": 9}})
    return run, jobs, artifacts


class EvidenceSelectionTests(unittest.TestCase):
    def select(self, data, profile="pr"):
        return selector.select(*data, profile, 42, 2, HEAD, REPO)

    def test_partial_rerun_preserves_tla_attempt_for_every_profile(self):
        for profile in ("pr", "nightly", "weekly"):
            with self.subTest(profile=profile):
                result = self.select(fixture(profile), profile)
                self.assertEqual(result["tests"]["producer_attempt"], 2)
                self.assertEqual(result["tla"]["producer_attempt"], 1)
                self.assertEqual(result["tla"]["artifact_id"], 103)
                self.assertEqual(len(result), 3 if profile == "pr" else 4)

    def test_aggregate_only_retry_keeps_all_original_producers(self):
        result = self.select(fixture(attempts=(1, 1, 1)))
        self.assertEqual({row["producer_attempt"] for row in result.values()}, {1})

    def test_newer_job_wins_regardless_of_api_or_artifact_order(self):
        data = fixture()
        old_job = dict(data[1][0], id=77, run_attempt=1, conclusion="failure")
        data[1].append(old_job)
        old_artifact = dict(data[2][0], id=177, name="invariants-pr-evidence-tests-42-1")
        data[2].append(old_artifact)
        data[1].reverse()
        data[2].reverse()
        self.assertEqual(self.select(data)["tests"]["artifact_id"], 101)

    def test_failed_or_unfinished_latest_job_never_falls_back(self):
        for status, conclusion in [("completed", "failure"), ("completed", "cancelled"),
                                   ("completed", "timed_out"), ("completed", "skipped"),
                                   ("in_progress", None), ("queued", None)]:
            with self.subTest(status=status, conclusion=conclusion):
                data = fixture(attempts=(1, 1, 1))
                data[1].append(dict(data[1][2], id=78, run_attempt=2,
                                    status=status, conclusion=conclusion))
                with self.assertRaisesRegex(ValueError, "latest producer did not succeed"):
                    self.select(data)

    def test_missing_latest_artifact_does_not_use_earlier_success(self):
        data = fixture(attempts=(1, 1, 1))
        data[1].append(dict(data[1][2], id=78, run_attempt=2))
        with self.assertRaisesRegex(ValueError, "missing or ambiguous evidence"):
            self.select(data)

    def test_stale_or_expired_or_duplicate_metadata_is_rejected(self):
        mutations = [
            lambda r, j, a: r.update(id=41),
            lambda r, j, a: r.update(run_attempt=3),
            lambda r, j, a: r.update(head_sha="b" * 40),
            lambda r, j, a: r["repository"].update(full_name="other/rafter"),
            lambda r, j, a: j[0].update(run_id=41),
            lambda r, j, a: j[0].update(head_sha="b" * 40),
            lambda r, j, a: j[0].update(run_attempt=3),
            lambda r, j, a: j[0].update(run_attempt=True),
            lambda r, j, a: j.append(dict(j[0], id=81)),
            lambda r, j, a: j.pop(),
            lambda r, j, a: a[0].update(expired=True),
            lambda r, j, a: a[0].update(id="101\ntla=999"),
            lambda r, j, a: a[0]["workflow_run"].update(id=41),
            lambda r, j, a: a[0]["workflow_run"].update(head_sha="b" * 40),
            lambda r, j, a: a[0]["workflow_run"].update(repository_id=10),
            lambda r, j, a: a.append(copy.deepcopy(a[0])),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                data = fixture()
                mutate(*data)
                with self.assertRaises(ValueError):
                    self.select(data)

    def test_api_pages_are_complete_and_unique(self):
        pages = [{"total_count": 2, "jobs": [{"id": 1}]},
                 {"total_count": 2, "jobs": [{"id": 2}]}]
        with patch.object(selector.subprocess, "run") as run:
            run.return_value.stdout = json.dumps(pages)
            self.assertEqual(selector.api("fixture", "jobs"), [{"id": 1}, {"id": 2}])
            self.assertEqual(run.call_args.args[0], ["gh", "api", "fixture", "--paginate", "--slurp"])
            self.assertEqual(run.call_args.kwargs["timeout"], 30)
            self.assertTrue(run.call_args.kwargs["check"])
            for bad in [[], pages[:1], [pages[0], pages[0]]]:
                run.return_value.stdout = json.dumps(bad)
                with self.assertRaises(ValueError):
                    selector.api("fixture", "jobs")

    def test_cli_outputs_only_validated_ids_and_logs_original_attempt(self):
        data = fixture()
        with tempfile.TemporaryDirectory(prefix="invariant selection ") as directory:
            output = Path(directory) / "outputs"
            env = {"GITHUB_REPOSITORY": REPO, "GITHUB_RUN_ID": "42", "GITHUB_RUN_ATTEMPT": "2",
                   "INVARIANT_RUN_HEAD": HEAD, "GITHUB_OUTPUT": str(output)}
            responses = [data[0], [{"total_count": 3, "jobs": data[1]}],
                         [{"total_count": 3, "artifacts": data[2]}]]
            with patch.dict(os.environ, env), patch.object(selector.sys, "argv", ["selector", "--profile", "pr"]), \
                    patch.object(selector.subprocess, "run") as run, contextlib.redirect_stdout(io.StringIO()) as log:
                run.side_effect = [subprocess.CompletedProcess([], 0, json.dumps(row)) for row in responses]
                selector.main()
                self.assertEqual(output.read_text(), "tests=101\nsimulator=102\ntla=103\n")
                self.assertEqual(json.loads(log.getvalue())["producers"]["tla"]["producer_attempt"], 1)
                self.assertIn("/jobs?filter=all&per_page=100", run.call_args_list[1].args[0][2])

    def test_failed_selection_or_api_cannot_publish_partial_outputs(self):
        for failure in ("selection", "api", "json"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "outputs"
                env = {"GITHUB_REPOSITORY": REPO, "GITHUB_RUN_ID": "42", "GITHUB_RUN_ATTEMPT": "2",
                       "INVARIANT_RUN_HEAD": HEAD, "GITHUB_OUTPUT": str(output)}
                data = fixture()
                data[2][-1]["expired"] = True
                with patch.dict(os.environ, env), patch.object(selector.sys, "argv", ["selector", "--profile", "pr"]), \
                        patch.object(selector.subprocess, "run") as run:
                    if failure == "api":
                        run.side_effect = subprocess.CalledProcessError(1, ["gh", "api"])
                    elif failure == "json":
                        run.return_value.stdout = "invalid json"
                    else:
                        rows = [data[0], [{"total_count": 3, "jobs": data[1]}],
                                [{"total_count": 3, "artifacts": data[2]}]]
                        run.side_effect = [subprocess.CompletedProcess([], 0, json.dumps(row)) for row in rows]
                    with self.assertRaises((ValueError, subprocess.SubprocessError)):
                        selector.main()
                    self.assertFalse(output.exists())


class WorkflowSelectionTests(unittest.TestCase):
    def aggregates(self):
        root = Path(__file__).resolve().parents[2]
        for workflow, profile in [("ci", "pr"), ("nightly", "nightly"), ("weekly", "weekly")]:
            source = (root / f".github/workflows/{workflow}.yml").read_text()
            job = source.split(f"  invariants-{profile}:\n", 1)[1]
            yield profile, re.split(r"\n  [a-z0-9-]+:\n", job, maxsplit=1)[0]

    def test_every_download_requires_a_selected_id_and_scoped_read_access(self):
        for profile, job in self.aggregates():
            with self.subTest(profile=profile):
                self.assertIn("    permissions:\n      contents: read\n      actions: read\n", job)
                self.assertIn(f"run: python3 scripts/invariant_evidence.py --profile {profile}", job)
                self.assertIn("INVARIANT_RUN_HEAD: ${{ github.event.pull_request.head.sha || github.sha }}", job)
                selected = job.split("      - name: Select current-run producer evidence\n")[1].split("      - ")[0]
                for required in ["id: evidence", "if: always()", "timeout-minutes: 2", "GH_TOKEN: ${{ github.token }}"]:
                    self.assertIn(required, selected)
                self.assertNotIn("continue-on-error", selected)
                downloads = [block for block in job.split("      - ") if block.startswith("name: Download available")]
                self.assertEqual(len(downloads), 3 if profile == "pr" else 4)
                for block in downloads:
                    layer = re.search(r"artifact-ids: \$\{\{ steps.evidence.outputs.(\w+) \}\}", block).group(1)
                    self.assertIn(f"if: always() && steps.evidence.outputs.{layer} != ''", block)
                    self.assertIn("github-token: ${{ github.token }}", block)
                    self.assertIn("run-id: ${{ github.run_id }}", block)
                    self.assertNotIn("          name:", block)
                    self.assertNotIn("pattern:", block)
                    self.assertIn("merge-multiple: true", block)

    def test_final_gate_accepts_complete_success_but_rejects_selection_failure(self):
        for profile, job in self.aggregates():
            gate = next(block for block in job.split("      - ") if block.startswith("name: Require 44"))
            script = textwrap.dedent(gate.split("        run: |\n", 1)[1])
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as directory:
                fake_jq = Path(directory) / "jq"
                fake_jq.write_text("#!/bin/sh\nexit 0\n")
                fake_jq.chmod(0o755)
                env = dict(os.environ, PATH=directory + os.pathsep + os.environ["PATH"],
                           RUNNER_TEMP=directory, INVARIANT_REPORT_DIR="report",
                           AGGREGATE_STATUS="0", REPORT_READY="true", EVIDENCE_SELECTION="success")
                for name in re.findall(r'^          ([A-Z_]+):', gate, re.MULTILINE):
                    env.setdefault(name, "success")
                success = subprocess.run(["bash", "-euo", "pipefail", "-c", script], env=env, capture_output=True)
                self.assertEqual(success.returncode, 0, success.stderr)
                env["EVIDENCE_SELECTION"] = "failure"
                failure = subprocess.run(["bash", "-euo", "pipefail", "-c", script], env=env, capture_output=True)
                self.assertNotEqual(failure.returncode, 0)


if __name__ == "__main__":
    unittest.main()

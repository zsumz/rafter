#!/usr/bin/env python3
"""Resolve immutable evidence IDs from each latest producer in this workflow run.

A partial rerun keeps successful producers at their original run_attempt. Never
substitute the aggregate's attempt, pick by upload time, or fall back from a
failed newer producer. Downloaded receipts still undergo source/hash validation.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def positive(value):
    return type(value) is int and value > 0


def api(endpoint, collection=None):
    command = ["gh", "api", endpoint]
    if collection:
        command += ["--paginate", "--slurp"]
    result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=30)
    payload = json.loads(result.stdout)
    if not collection:
        return payload
    require(isinstance(payload, list) and payload, "missing API pages")
    rows = [row for page in payload for row in page[collection]]
    require(all(page["total_count"] == len(rows) for page in payload), "incomplete API pagination")
    require(len({row["id"] for row in rows}) == len(rows), "duplicate API rows")
    return rows


def select(run, jobs, artifacts, profile, run_id, attempt, head, repository):
    require(profile in ("pr", "nightly", "weekly"), "unknown invariant profile")
    require(positive(run_id) and positive(attempt), "invalid run identity")
    require(re.fullmatch(r"[0-9a-f]{40}", head), "invalid expected head")
    require(run["id"] == run_id and run["run_attempt"] == attempt, "workflow run/attempt changed")
    require(run["head_sha"] == head, "workflow head mismatch")
    require(run["repository"]["full_name"].lower() == repository.lower(), "repository mismatch")
    repository_id = run["repository"]["id"]
    require(positive(repository_id), "invalid repository identity")
    layers = ("tests", "simulator", "tla") + (() if profile == "pr" else ("maelstrom",))
    selected = {}
    for layer in layers:
        name = f"invariants-{layer}" + ("" if profile == "pr" else f"-{profile}")
        candidates = [job for job in jobs if job["name"] == name]
        require(candidates, f"missing producer: {name}")
        for job in candidates:
            require(job["run_id"] == run_id and job["head_sha"] == head, f"producer identity mismatch: {name}")
            require(positive(job["run_attempt"]) and job["run_attempt"] <= attempt,
                    f"invalid producer attempt: {name}")
        latest = max(job["run_attempt"] for job in candidates)
        candidates = [job for job in candidates if job["run_attempt"] == latest]
        require(len(candidates) == 1, f"ambiguous latest producer: {name}")
        job = candidates[0]
        require(positive(job["id"]), f"invalid producer ID: {name}")
        require(job["status"] == "completed" and job["conclusion"] == "success",
                f"latest producer did not succeed: {name} (attempt {latest})")
        artifact_name = f"invariants-{profile}-evidence-{layer}-{run_id}-{latest}"
        matches = [artifact for artifact in artifacts if artifact["name"] == artifact_name]
        require(len(matches) == 1, f"missing or ambiguous evidence: {artifact_name}")
        artifact = matches[0]
        require(positive(artifact["id"]) and artifact["expired"] is False, f"unavailable evidence: {artifact_name}")
        source = artifact["workflow_run"]
        require(source["id"] == run_id and source["head_sha"] == head
                and source["repository_id"] == repository_id, f"artifact identity mismatch: {artifact_name}")
        selected[layer] = {"job_id": job["id"], "producer_attempt": latest,
                           "artifact_id": artifact["id"], "artifact_name": artifact_name}
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("pr", "nightly", "weekly"), required=True)
    args = parser.parse_args()
    repository = os.environ["GITHUB_REPOSITORY"]
    require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository), "invalid repository")
    run_id = int(os.environ["GITHUB_RUN_ID"])
    attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
    head = os.environ["INVARIANT_RUN_HEAD"]
    require(positive(run_id) and positive(attempt), "invalid run identity")
    endpoint = f"repos/{repository}/actions/runs/{run_id}"
    run = api(endpoint)
    jobs = api(endpoint + "/jobs?filter=all&per_page=100", "jobs")
    artifacts = api(endpoint + "/artifacts?per_page=100", "artifacts")
    selected = select(run, jobs, artifacts, args.profile, run_id, attempt, head, repository)
    # Publish outputs only after every layer is validated. Empty IDs must never
    # reach download-artifact, where omitted selection means all artifacts.
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        for layer, evidence in selected.items():
            output.write(f"{layer}={evidence['artifact_id']}\n")
    print(json.dumps({"run_id": run_id, "aggregate_attempt": attempt,
                      "head": head, "producers": selected}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError) as error:
        print(f"invariant evidence selection refused: {error}", file=sys.stderr)
        raise SystemExit(1)

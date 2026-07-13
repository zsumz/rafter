#!/usr/bin/env python3
"""Plan and aggregate fail-closed model-check contract migrations."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

from model_check_profile_parser import (
    ReportError,
    compare_contract_migration,
    compare_revisions,
    evaluate_comparisons,
    summarize_samples,
)

SCHEMA_VERSION = 1
REPORT_SCHEMA_VERSION = 3
STRUCTURED_EVENT_BASELINE = "9770d1aff12999dcc949dffe63c5d75fdda9c573"
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


class MigrationError(ValueError):
    """A migration manifest, graph, or evidence bundle is invalid."""


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(content)
    temporary.replace(path)


def _require_keys(value: dict, expected: set[str], context: str) -> None:
    if not isinstance(value, dict) or set(value) != expected:
        actual = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise MigrationError(f"{context}: keys {actual} != {sorted(expected)}")


def validate_manifest(document: dict) -> dict:
    _require_keys(document, {"schema_version", "migrations"}, "manifest")
    if document["schema_version"] != SCHEMA_VERSION:
        raise MigrationError(
            f"manifest schema_version must be {SCHEMA_VERSION}, got "
            f"{document['schema_version']!r}"
        )
    migrations = document["migrations"]
    if not isinstance(migrations, list) or not migrations:
        raise MigrationError("manifest migrations must be a non-empty array")
    seen_ids = set()
    for migration in migrations:
        _require_keys(
            migration,
            {"id", "pivot_commit", "pivot_parent", "changed_paths", "profiles"},
            "migration",
        )
        migration_id = migration["id"]
        if not isinstance(migration_id, str) or not migration_id or migration_id in seen_ids:
            raise MigrationError(f"invalid or duplicate migration id {migration_id!r}")
        seen_ids.add(migration_id)
        for field in ("pivot_commit", "pivot_parent"):
            if not isinstance(migration[field], str) or not COMMIT_RE.fullmatch(
                migration[field]
            ):
                raise MigrationError(f"{migration_id}: invalid {field}")
        paths = migration["changed_paths"]
        if (
            not isinstance(paths, list)
            or not paths
            or paths != sorted(set(paths))
            or any(not isinstance(path, str) or not path for path in paths)
        ):
            raise MigrationError(f"{migration_id}: changed_paths must be sorted and unique")
        profiles = migration["profiles"]
        if not isinstance(profiles, dict) or not profiles:
            raise MigrationError(f"{migration_id}: profiles must be a non-empty object")
        changed = 0
        for profile, contract in profiles.items():
            if not isinstance(profile, str) or not profile:
                raise MigrationError(f"{migration_id}: invalid profile name")
            _require_keys(
                contract,
                {
                    "from_contract_sha256",
                    "to_contract_sha256",
                    "configured_depth_changes",
                },
                f"{migration_id}/{profile}",
            )
            for field in ("from_contract_sha256", "to_contract_sha256"):
                if not isinstance(contract[field], str) or not DIGEST_RE.fullmatch(
                    contract[field]
                ):
                    raise MigrationError(f"{migration_id}/{profile}: invalid {field}")
            if contract["from_contract_sha256"] == contract["to_contract_sha256"]:
                raise MigrationError(
                    f"{migration_id}/{profile}: contract digests must differ"
                )
            changes = contract["configured_depth_changes"]
            if not isinstance(changes, list) or changes != sorted(
                changes, key=lambda item: item.get("check_id", "")
            ):
                raise MigrationError(
                    f"{migration_id}/{profile}: configured_depth_changes must be sorted"
                )
            seen_checks = set()
            for change in changes:
                _require_keys(
                    change,
                    {"check_id", "from_depth", "to_depth"},
                    f"{migration_id}/{profile} change",
                )
                check_id = change["check_id"]
                if (
                    not isinstance(check_id, str)
                    or not check_id
                    or check_id in seen_checks
                ):
                    raise MigrationError(
                        f"{migration_id}/{profile}: invalid or duplicate check_id"
                    )
                seen_checks.add(check_id)
                before, after = change["from_depth"], change["to_depth"]
                if (
                    not isinstance(before, int)
                    or isinstance(before, bool)
                    or not isinstance(after, int)
                    or isinstance(after, bool)
                    or before < 0
                    or after <= before
                ):
                    raise MigrationError(
                        f"{migration_id}/{profile}/{check_id}: depths are not monotone"
                    )
                changed += 1
        if changed == 0:
            raise MigrationError(f"{migration_id}: no configured-depth changes")
    return document


def load_manifest(path: Path) -> dict:
    try:
        document = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise MigrationError(f"cannot load migration manifest: {error}") from error
    return validate_manifest(document)


def _git(repo: Path, *args: str, check: bool = True) -> str:
    result = subprocess.run(
        ("git", *args),
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and result.returncode != 0:
        raise MigrationError(
            f"git {' '.join(args)} failed: {result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout.strip()


def _is_ancestor(repo: Path, ancestor: str, descendant: str) -> bool:
    result = subprocess.run(
        ("git", "merge-base", "--is-ancestor", ancestor, descendant),
        cwd=repo,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode not in (0, 1):
        raise MigrationError(
            f"cannot test ancestry {ancestor} -> {descendant}: {result.stderr.strip()}"
        )
    return result.returncode == 0


def validate_repository_migration(repo: Path, migration: dict) -> None:
    migration_id = migration["id"]
    pivot = _git(repo, "rev-parse", f"{migration['pivot_commit']}^{{commit}}")
    if pivot != migration["pivot_commit"]:
        raise MigrationError(f"{migration_id}: pivot commit does not resolve exactly")
    parents = _git(repo, "show", "-s", "--format=%P", pivot).split()
    if parents != [migration["pivot_parent"]]:
        raise MigrationError(
            f"{migration_id}: pivot parents {parents} != {[migration['pivot_parent']]}"
        )
    paths = sorted(
        filter(
            None,
            _git(repo, "diff-tree", "--no-commit-id", "--name-only", "-r", pivot).splitlines(),
        )
    )
    if paths != migration["changed_paths"]:
        raise MigrationError(
            f"{migration_id}: pivot changed paths {paths} != {migration['changed_paths']}"
        )


def build_plan(
    repo: Path,
    manifest_path: Path,
    base_ref: str,
    current_ref: str,
    selected_profiles: list[str],
) -> dict:
    if not selected_profiles or len(selected_profiles) != len(set(selected_profiles)):
        raise MigrationError("selected profiles must be non-empty and unique")
    manifest = load_manifest(manifest_path)
    base = _git(repo, "rev-parse", f"{base_ref}^{{commit}}")
    current = _git(repo, "rev-parse", f"{current_ref}^{{commit}}")
    if base == current:
        raise MigrationError(f"baseline and current resolve to the same commit: {current}")
    if not _is_ancestor(repo, base, current):
        raise MigrationError(f"requested baseline {base} is not an ancestor of {current}")
    crossings = []
    for migration in manifest["migrations"]:
        validate_repository_migration(repo, migration)
        if _is_ancestor(repo, base, migration["pivot_parent"]) and _is_ancestor(
            repo, migration["pivot_commit"], current
        ):
            crossings.append(migration)
    if len(crossings) > 1:
        raise MigrationError("comparison crosses multiple contract migrations")
    common = {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "requested_base_ref": base_ref,
        "base_commit": base,
        "current_ref": current_ref,
        "current_commit": current,
        "selected_profiles": selected_profiles,
        "manifest": {
            "path": str(manifest_path),
            "sha256": sha256(manifest_path),
        },
    }
    if not crossings:
        return {
            **common,
            "comparison_mode": "like-for-like",
            "segments": [
                {
                    "name": "like-for-like",
                    "required": True,
                    "base_commit": base,
                    "current_commit": current,
                    "profiles": selected_profiles,
                }
            ],
        }
    migration = crossings[0]
    unknown_profiles = sorted(set(selected_profiles) - set(migration["profiles"]))
    if unknown_profiles:
        raise MigrationError(
            f"{migration['id']}: selected profiles are not pinned: {unknown_profiles}"
        )
    parent = migration["pivot_parent"]
    pivot = migration["pivot_commit"]
    return {
        **common,
        "comparison_mode": "monotone-bound-migration",
        "migration": migration,
        "segments": [
            {
                "name": "pre-migration",
                "required": base != parent,
                "base_commit": base,
                "current_commit": parent,
                "profiles": selected_profiles,
            },
            {
                "name": "post-migration",
                "required": pivot != current,
                "base_commit": pivot,
                "current_commit": current,
                "profiles": selected_profiles,
            },
            {
                "name": "migration-delta",
                "required": True,
                "base_commit": parent,
                "current_commit": pivot,
                "profiles": sorted(migration["profiles"]),
            },
        ],
    }


def failed_plan(error: Exception, base_ref: str, current_ref: str) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "fail",
        "comparison_mode": "validation-failure",
        "requested_base_ref": base_ref,
        "current_ref": current_ref,
        "errors": [f"{type(error).__name__}: {error}"],
    }


def _read_report(path: Path) -> dict:
    try:
        report = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise MigrationError(f"cannot load {path}: {error}") from error
    if not isinstance(report, dict):
        raise MigrationError(f"{path}: report must be an object")
    return report


def _validate_segment_report(
    report: dict,
    *,
    name: str,
    mode: str,
    requested_base: str,
    current: str,
    profiles: list[str],
    max_wall_ratio: float,
    max_rss_ratio: float,
) -> list[str]:
    failures = []
    if report.get("schema_version") != REPORT_SCHEMA_VERSION:
        failures.append(f"{name}: unsupported or missing report schema")
    if report.get("comparison_mode") != mode:
        failures.append(f"{name}: comparison mode mismatch")
    if report.get("requested_base_commit") != requested_base:
        failures.append(f"{name}: requested base source mismatch")
    if report.get("sources", {}).get("current", {}).get("commit") != current:
        failures.append(f"{name}: current source mismatch")
    sources = report.get("sources", {})
    baseline_policy = report.get("baseline_policy")
    expected_effective_base = (
        STRUCTURED_EVENT_BASELINE
        if baseline_policy == "structured-migration-baseline"
        else requested_base
    )
    if baseline_policy not in {
        "requested-structured-baseline",
        "structured-migration-baseline",
    }:
        failures.append(f"{name}: unsupported baseline policy")
    if sources.get("base", {}).get("commit") != expected_effective_base:
        failures.append(f"{name}: effective base source mismatch")
    if not sources.get("base", {}).get("clean", False) or not sources.get(
        "current", {}
    ).get("clean", False):
        failures.append(f"{name}: measured source checkout is dirty")
    if report.get("methodology", {}).get("profiles") != sorted(profiles):
        failures.append(f"{name}: measured profile set mismatch")
    acceptance = report.get("acceptance", {})
    if acceptance.get("max_median_wall_ratio") != max_wall_ratio:
        failures.append(f"{name}: wall ceiling mismatch")
    if acceptance.get("max_median_peak_rss_ratio") != max_rss_ratio:
        failures.append(f"{name}: RSS ceiling mismatch")
    if acceptance.get("status") != "pass":
        details = acceptance.get("failures") or ["missing acceptance verdict"]
        failures.extend(f"{name}: {detail}" for detail in details)
    if mode == "like-for-like":
        run_count = report.get("methodology", {}).get(
            "run_count_per_revision_and_profile"
        )
        try:
            summaries = summarize_samples(report.get("samples", []), run_count)
            comparisons = compare_revisions(report.get("samples", []), run_count)
            evaluated = evaluate_comparisons(
                comparisons, max_wall_ratio, max_rss_ratio
            )
            if report.get("summaries") != summaries:
                failures.append(f"{name}: summary does not match raw evidence")
            if report.get("comparisons") != comparisons:
                failures.append(f"{name}: comparisons do not match raw evidence")
            if acceptance != evaluated:
                failures.append(f"{name}: acceptance does not match raw evidence")
        except (ReportError, KeyError, TypeError) as error:
            failures.append(f"{name}: invalid raw evidence: {error}")
    return failures


def _load_segment(
    artifact_dir: Path,
    segment: dict,
    mode: str,
    max_wall_ratio: float,
    max_rss_ratio: float,
) -> tuple[dict, list[str]]:
    name = segment["name"]
    path = artifact_dir / "segments" / name / "compare.json"
    if not segment["required"]:
        if path.exists():
            return {}, [f"{name}: evidence exists for a non-required empty segment"]
        return {"name": name, "required": False, "status": "not-required"}, []
    if not path.is_file():
        return {}, [f"{name}: required report is missing"]
    try:
        report = _read_report(path)
    except MigrationError as error:
        return {}, [str(error)]
    failures = _validate_segment_report(
        report,
        name=name,
        mode=mode,
        requested_base=segment["base_commit"],
        current=segment["current_commit"],
        profiles=segment["profiles"],
        max_wall_ratio=max_wall_ratio,
        max_rss_ratio=max_rss_ratio,
    )
    return {
        "name": name,
        "required": True,
        "status": "pass" if not failures else "fail",
        "report": report,
    }, failures


def _validate_delta(report: dict, migration: dict) -> dict:
    run_count = report.get("methodology", {}).get("run_count_per_revision_and_profile")
    if not isinstance(run_count, int):
        raise MigrationError("migration-delta: missing run count")
    try:
        actual = compare_contract_migration(report.get("samples", []), run_count)
    except (ReportError, KeyError, TypeError) as error:
        raise MigrationError(f"migration-delta: invalid raw evidence: {error}") from error
    snapshots = {
        (snapshot["label"], snapshot["profile"]): snapshot
        for snapshot in actual["contract_snapshots"]
    }
    expected_profiles = set(migration["profiles"])
    actual_profiles = {profile for _, profile in snapshots}
    if actual_profiles != expected_profiles or len(snapshots) != 2 * len(expected_profiles):
        raise MigrationError(
            f"migration-delta: profile set {sorted(actual_profiles)} != "
            f"{sorted(expected_profiles)}"
        )
    expected_changes = []
    for profile, contract in sorted(migration["profiles"].items()):
        base_digest = snapshots[("base", profile)]["sha256"]
        current_digest = snapshots[("current", profile)]["sha256"]
        if base_digest != contract["from_contract_sha256"]:
            raise MigrationError(f"migration-delta: {profile} old contract digest mismatch")
        if current_digest != contract["to_contract_sha256"]:
            raise MigrationError(f"migration-delta: {profile} new contract digest mismatch")
        expected_changes.extend(
            {"profile": profile, **change}
            for change in contract["configured_depth_changes"]
        )
    if actual["configured_depth_changes"] != expected_changes:
        raise MigrationError(
            "migration-delta: configured-depth changes do not match the pinned manifest"
        )
    return actual


def build_aggregate(
    plan: dict,
    artifact_dir: Path,
    max_wall_ratio: float,
    max_rss_ratio: float,
    repo: Path | None = None,
) -> dict:
    failures = []
    orchestration_errors = artifact_dir / "orchestration-errors.log"
    if orchestration_errors.is_file():
        failures.extend(
            line
            for line in orchestration_errors.read_text().splitlines()
            if line.strip()
        )
    if plan.get("status") != "pass":
        failures.extend(plan.get("errors") or ["migration plan is missing or invalid"])
        return _aggregate_failure(plan, max_wall_ratio, max_rss_ratio, failures)
    if repo is not None:
        try:
            manifest_path = Path(plan["manifest"]["path"])
            if sha256(manifest_path) != plan["manifest"]["sha256"]:
                raise MigrationError("migration manifest digest changed after planning")
            manifest = load_manifest(manifest_path)
            if plan["comparison_mode"] == "monotone-bound-migration":
                matches = [
                    migration
                    for migration in manifest["migrations"]
                    if migration["id"] == plan["migration"]["id"]
                ]
                if matches != [plan["migration"]]:
                    raise MigrationError("planned migration does not match the manifest")
                validate_repository_migration(repo, plan["migration"])
                if not _is_ancestor(repo, plan["base_commit"], plan["current_commit"]):
                    raise MigrationError("planned comparison ancestry is no longer valid")
        except (KeyError, MigrationError, OSError) as error:
            failures.append(f"plan revalidation failed: {error}")
    mode = plan["comparison_mode"]
    if mode == "like-for-like":
        segment, segment_failures = _load_segment(
            artifact_dir,
            plan["segments"][0],
            "like-for-like",
            max_wall_ratio,
            max_rss_ratio,
        )
        failures.extend(segment_failures)
        if segment:
            report = dict(segment.get("report", {}))
        else:
            report = {}
        report.update(
            {
                "schema_version": REPORT_SCHEMA_VERSION,
                "comparison_mode": "like-for-like",
                "orchestration": {"plan": plan},
                "segments": [segment] if segment else [],
                "acceptance": {
                    "status": "pass" if not failures else "fail",
                    "max_median_wall_ratio": max_wall_ratio,
                    "max_median_peak_rss_ratio": max_rss_ratio,
                    "failures": failures,
                },
            }
        )
        return report
    if mode != "monotone-bound-migration":
        raise MigrationError(f"unsupported plan comparison mode {mode!r}")
    segments = []
    delta_report = None
    for segment_plan in plan["segments"]:
        segment_mode = (
            "migration-delta"
            if segment_plan["name"] == "migration-delta"
            else "like-for-like"
        )
        segment, segment_failures = _load_segment(
            artifact_dir,
            segment_plan,
            segment_mode,
            max_wall_ratio,
            max_rss_ratio,
        )
        segments.append(segment)
        failures.extend(segment_failures)
        if segment_plan["name"] == "migration-delta" and segment.get("report"):
            delta_report = segment["report"]
    migration_delta = None
    if delta_report is not None:
        try:
            migration_delta = _validate_delta(delta_report, plan["migration"])
        except MigrationError as error:
            failures.append(str(error))
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "comparison_mode": "monotone-bound-migration",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "requested_base_ref": plan["requested_base_ref"],
        "requested_base_commit": plan["base_commit"],
        "sources": {
            "base": {"commit": plan["base_commit"]},
            "current": {"commit": plan["current_commit"]},
        },
        "migration": {
            **plan["migration"],
            "manifest": plan["manifest"],
        },
        "segments": segments,
        "contract_snapshots": (
            migration_delta["contract_snapshots"] if migration_delta else []
        ),
        "coverage_delta": migration_delta["coverage_delta"] if migration_delta else [],
        "acceptance": {
            "status": "pass" if not failures else "fail",
            "max_median_wall_ratio": max_wall_ratio,
            "max_median_peak_rss_ratio": max_rss_ratio,
            "failures": failures,
        },
    }
    return report


def _aggregate_failure(
    plan: dict, max_wall_ratio: float, max_rss_ratio: float, failures: list[str]
) -> dict:
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "comparison_mode": plan.get("comparison_mode", "validation-failure"),
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "orchestration": {"plan": plan},
        "segments": [],
        "acceptance": {
            "status": "fail",
            "max_median_wall_ratio": max_wall_ratio,
            "max_median_peak_rss_ratio": max_rss_ratio,
            "failures": failures,
        },
    }


def render_aggregate_markdown(report: dict) -> str:
    acceptance = report["acceptance"]
    lines = [
        "# Model-Check Profile Comparison",
        "",
        f"- Comparison mode: `{report['comparison_mode']}`",
        f"- Acceptance: **{acceptance['status']}**",
        f"- Ceilings: median wall <= {acceptance['max_median_wall_ratio']:.3f}x; "
        f"median peak RSS <= {acceptance['max_median_peak_rss_ratio']:.3f}x",
    ]
    if report.get("sources"):
        lines.extend(
            [
                f"- Base: `{report['sources']['base']['commit']}`",
                f"- Current: `{report['sources']['current']['commit']}`",
            ]
        )
    if report.get("migration"):
        lines.extend(
            [
                f"- Migration: `{report['migration']['id']}`",
                f"- Pivot: `{report['migration']['pivot_commit']}`",
                "",
                "| Segment | Required | Status | Base | Current |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for segment in report["segments"]:
            nested = segment.get("report", {})
            base = nested.get("requested_base_commit", "n/a")
            current = nested.get("sources", {}).get("current", {}).get("commit", "n/a")
            lines.append(
                f"| {segment.get('name', 'missing')} | "
                f"{str(segment.get('required', True)).lower()} | "
                f"{segment.get('status', 'missing')} | `{base}` | `{current}` |"
            )
        if report.get("coverage_delta"):
            lines.extend(
                [
                    "",
                    "| Profile/check | Depth | Protocol-state delta | Verifier-state delta | Action delta |",
                    "| --- | ---: | ---: | ---: | ---: |",
                ]
            )
            for delta in report["coverage_delta"]:
                if delta["from_depth"] == delta["to_depth"]:
                    continue
                lines.append(
                    f"| {delta['profile']}/{delta['check_id']} | "
                    f"{delta['from_depth']} -> {delta['to_depth']} | "
                    f"{delta['protocol_states']} | {delta['verifier_states']} | "
                    f"{delta['explored_actions']} |"
                )
    performance_rows = []
    if report["comparison_mode"] == "like-for-like":
        performance_reports = [("like-for-like", report)]
    else:
        performance_reports = [
            (segment["name"], segment.get("report", {}))
            for segment in report.get("segments", [])
            if segment.get("name") != "migration-delta"
        ]
    for segment_name, nested in performance_reports:
        for comparison in nested.get("comparisons", []):
            performance_rows.append(
                (
                    segment_name,
                    comparison["profile"],
                    comparison["paired_current_over_base_wall_time"]["median"],
                    comparison["paired_current_over_base_peak_rss"]["median"],
                    comparison["like_for_like_protocol_state_shape"],
                )
            )
    if performance_rows:
        lines.extend(
            [
                "",
                "| Segment/profile | Median wall ratio | Median RSS ratio | Protocol shape equal |",
                "| --- | ---: | ---: | --- |",
            ]
        )
        for segment, profile, wall, rss, shape_equal in performance_rows:
            lines.append(
                f"| {segment}/{profile} | {wall:.3f} | {rss:.3f} | "
                f"{str(shape_equal).lower()} |"
            )
    if acceptance["failures"]:
        lines.extend(["", "## Acceptance Failures", ""])
        lines.extend(f"- {failure}" for failure in acceptance["failures"])
    return "\n".join(lines) + "\n"


def plan_command(args: argparse.Namespace) -> int:
    try:
        plan = build_plan(
            args.repo.resolve(),
            args.manifest.resolve(),
            args.base_ref,
            args.current_ref,
            args.profiles.split(),
        )
    except Exception as error:
        plan = failed_plan(error, args.base_ref, args.current_ref)
    atomic_write(args.output, json.dumps(plan, indent=2) + "\n")
    return 0 if plan["status"] == "pass" else 1


def aggregate_command(args: argparse.Namespace) -> int:
    try:
        plan = _read_report(args.plan)
        report = build_aggregate(
            plan,
            args.artifact_dir.resolve(),
            args.max_wall_ratio,
            args.max_rss_ratio,
            args.repo.resolve(),
        )
    except Exception as error:
        report = _aggregate_failure(
            {},
            args.max_wall_ratio,
            args.max_rss_ratio,
            [f"{type(error).__name__}: {error}"],
        )
    atomic_write(args.output_json, json.dumps(report, indent=2) + "\n")
    atomic_write(args.output_markdown, render_aggregate_markdown(report))
    for failure in report["acceptance"]["failures"]:
        print(f"model-check overhead gate: {failure}", file=sys.stderr)
    return 0 if report["acceptance"]["status"] == "pass" else 1


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--repo", type=Path, required=True)
    plan.add_argument("--manifest", type=Path, required=True)
    plan.add_argument("--base-ref", required=True)
    plan.add_argument("--current-ref", required=True)
    plan.add_argument("--profiles", required=True)
    plan.add_argument("--output", type=Path, required=True)
    aggregate = subparsers.add_parser("aggregate")
    aggregate.add_argument("--plan", type=Path, required=True)
    aggregate.add_argument("--repo", type=Path, required=True)
    aggregate.add_argument("--artifact-dir", type=Path, required=True)
    aggregate.add_argument("--max-wall-ratio", type=float, required=True)
    aggregate.add_argument("--max-rss-ratio", type=float, required=True)
    aggregate.add_argument("--output-json", type=Path, required=True)
    aggregate.add_argument("--output-markdown", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = arguments()
    status = plan_command(args) if args.command == "plan" else aggregate_command(args)
    raise SystemExit(status)


if __name__ == "__main__":
    main()

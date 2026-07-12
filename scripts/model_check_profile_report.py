#!/usr/bin/env python3
"""Build a source-bound model-check cost report from strict parsed evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import subprocess
from pathlib import Path

from model_check_profile_parser import (
    ReportError,
    compare_revisions,
    read_samples,
    summarize_samples,
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command(*args: str, cwd: Path | None = None) -> str:
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def source_metadata(root: Path, binary: Path) -> dict:
    status = command("git", "status", "--porcelain", cwd=root)
    return {
        "commit": command("git", "rev-parse", "HEAD^{commit}", cwd=root),
        "tree": command("git", "rev-parse", "HEAD^{tree}", cwd=root),
        "clean": not status,
        "worktree_status_sha256": hashlib.sha256(status.encode()).hexdigest(),
        "cargo_lock_sha256": sha256(root / "Cargo.lock"),
        "binary_sha256": sha256(binary),
    }


def build_report(args: argparse.Namespace) -> dict:
    if args.expected_runs <= 0 or args.expected_runs % 2 != 0:
        raise ReportError("expected runs must be a positive even number")
    samples = read_samples(args.artifact_dir.resolve(), args.time_style)
    profiles = sorted({sample["profile"] for sample in samples})
    return {
        "schema_version": 1,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "base_ref": args.base_ref,
        "sources": {
            "base": source_metadata(args.base_root, args.base_binary),
            "current": source_metadata(args.current_root, args.current_binary),
        },
        "toolchain": {
            "cargo": command(args.cargo, "--version"),
            "rustc": command("rustc", "--version", "--verbose"),
            "python": platform.python_version(),
            "timing_binary_sha256": sha256(args.timing_binary),
        },
        "host": {
            "architecture": platform.machine(),
            "operating_system": platform.platform(),
            "processor": platform.processor(),
            "logical_cpus": os.cpu_count(),
            "memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
        },
        "ci": {
            key.lower(): os.environ.get(key)
            for key in (
                "GITHUB_ACTIONS",
                "GITHUB_EVENT_NAME",
                "GITHUB_REF",
                "GITHUB_RUN_ATTEMPT",
                "GITHUB_RUN_ID",
                "GITHUB_SHA",
            )
        },
        "methodology": {
            "profiles": profiles,
            "time_style": args.time_style,
            "build_profile": "release",
            "build_locked": True,
            "alternating_execution_order": True,
            "alternating_profile_order": True,
            "run_count_per_revision_and_profile": args.expected_runs,
            "totals_are_additive_across_checks": True,
        },
        "summaries": summarize_samples(samples, args.expected_runs),
        "comparisons": compare_revisions(samples, args.expected_runs),
        "samples": samples,
    }


def render_markdown(report: dict) -> str:
    lines = [
        "# Model-Check Profile Comparison",
        "",
        f"- Base: `{report['sources']['base']['commit']}`",
        f"- Current: `{report['sources']['current']['commit']}`",
        "- Method: median of repeated alternating-order release runs; state totals are additive across checks.",
        "",
        "| Label | Profile | Runs | Wall ms (min/median/max) | Peak RSS MiB (min/median/max) | Protocol states | Verifier states | Verifier overhead |",
        "| --- | --- | ---: | --- | --- | ---: | ---: | ---: |",
    ]
    for summary in report["summaries"]:
        wall = summary["wall_time_ms"]
        rss = {key: value / (1024 * 1024) for key, value in summary["peak_rss_bytes"].items()}
        totals = summary["totals"]
        lines.append(
            f"| {summary['label']} | {summary['profile']} | {summary['runs']} | "
            f"{wall['min']}/{wall['median']}/{wall['max']} | "
            f"{rss['min']:.1f}/{rss['median']:.1f}/{rss['max']:.1f} | "
            f"{totals['sum_unique_protocol_states_across_checks']} | "
            f"{totals['sum_unique_verifier_states_across_checks']} | "
            f"{totals['verifier_state_overhead']} |"
        )
    lines.extend(
        [
            "",
            "| Profile | Like-for-like state shape | Current/base wall ratio (min/median/max) | Current/base RSS ratio (min/median/max) |",
            "| --- | --- | --- | --- |",
        ]
    )
    for comparison in report["comparisons"]:
        wall = comparison["paired_current_over_base_wall_time"]
        rss = comparison["paired_current_over_base_peak_rss"]
        lines.append(
            f"| {comparison['profile']} | {str(comparison['like_for_like_state_shape']).lower()} | "
            f"{wall['min']:.3f}/{wall['median']:.3f}/{wall['max']:.3f} | "
            f"{rss['min']:.3f}/{rss['median']:.3f}/{rss['max']:.3f} |"
        )
    return "\n".join(lines) + "\n"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--base-root", type=Path, required=True)
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--base-binary", type=Path, required=True)
    parser.add_argument("--current-root", type=Path, required=True)
    parser.add_argument("--current-binary", type=Path, required=True)
    parser.add_argument("--cargo", required=True)
    parser.add_argument("--timing-binary", type=Path, required=True)
    parser.add_argument("--time-style", choices=("bsd", "gnu"), required=True)
    parser.add_argument("--expected-runs", type=int, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-markdown", type=Path, required=True)
    return parser.parse_args()


def atomic_write(path: Path, content: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(content)
    temporary.replace(path)


def main() -> None:
    args = arguments()
    report = build_report(args)
    atomic_write(args.output_json, json.dumps(report, indent=2) + "\n")
    atomic_write(args.output_markdown, render_markdown(report))
    print(f"wrote {args.output_json}")
    print(f"wrote {args.output_markdown}")


if __name__ == "__main__":
    main()

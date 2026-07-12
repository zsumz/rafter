"""Strict parsers and aggregators for model-check cost evidence."""

from __future__ import annotations

import csv
import json
import re
import statistics
from pathlib import Path

EVENT_PREFIX = "RAFTER_EVENT "
REQUIRED_METRICS = (
    "configured_depth",
    "reached_depth",
    "unique_states",
    "unique_protocol_states",
    "unique_verifier_states",
    "explored_states",
    "explored_actions",
    "duration_ms",
)
PROFILE_HEADER = re.compile(
    r'^model-check profile=(?P<profile>\S+) expected_runtime=(?P<runtime>\S+) '
    r'.* bounds="(?P<bounds>[^"]+)" schedule_classes=(?P<schedules>.+)$'
)


class ReportError(ValueError):
    """Malformed or semantically incomplete benchmark evidence."""


def parse_events(path: Path) -> list[dict]:
    events = []
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.startswith(EVENT_PREFIX):
            continue
        try:
            event = json.loads(line[len(EVENT_PREFIX) :])
        except json.JSONDecodeError as error:
            raise ReportError(f"{path.name}:{line_number}: invalid RAFTER_EVENT: {error}") from error
        if not isinstance(event, dict):
            raise ReportError(f"{path.name}:{line_number}: event must be an object")
        events.append(event)
    if not events:
        raise ReportError(f"{path.name}: no RAFTER_EVENT records")
    return events


def parse_profile_header(path: Path, expected_profile: str | None) -> dict:
    matches = [
        PROFILE_HEADER.fullmatch(line)
        for line in path.read_text().splitlines()
        if line.startswith("model-check profile=")
    ]
    if len(matches) != 1 or matches[0] is None:
        raise ReportError(f"{path.name}: expected exactly one valid profile header")
    fields = matches[0].groupdict()
    if expected_profile is not None and fields["profile"] != expected_profile:
        raise ReportError(
            f"{path.name}: profile header {fields['profile']} != requested {expected_profile}"
        )
    return {
        "profile": fields["profile"],
        "expected_runtime": fields["runtime"],
        "bounds": fields["bounds"],
        "schedule_classes": [item.strip() for item in fields["schedules"].split(",")],
    }


def parse_output(path: Path, expected_profile: str | None = None) -> dict:
    profile_contract = parse_profile_header(path, expected_profile)
    checks = []
    soak_checks = []
    seen_exhaustive = set()
    seen_soak = set()
    for event in parse_events(path):
        event_type = event.get("event")
        if event_type not in {"exhaustive-check", "soak-check"}:
            raise ReportError(f"{path.name}: unsupported event type {event_type!r}")
        check_id = event.get("check_id")
        if not isinstance(check_id, str) or not check_id:
            raise ReportError(f"{path.name}: event has no check_id")
        if event.get("status") != "pass" or event.get("classification") is not None:
            raise ReportError(f"{path.name}: check {check_id} did not pass cleanly")
        if event_type == "soak-check":
            seed = event.get("seed")
            if not isinstance(seed, int) or isinstance(seed, bool) or seed < 0:
                raise ReportError(f"{path.name}: soak check {check_id} has invalid seed")
            key = (check_id, seed)
            if key in seen_soak:
                raise ReportError(f"{path.name}: duplicate soak check {check_id} seed {seed}")
            seen_soak.add(key)
            soak_checks.append(event)
            continue
        if check_id in seen_exhaustive:
            raise ReportError(f"{path.name}: duplicate exhaustive check_id {check_id}")
        seen_exhaustive.add(check_id)
        _validate_exhaustive(path, check_id, event)
        checks.append({key: event[key] for key in ("check_id", *REQUIRED_METRICS)})
    if not checks:
        raise ReportError(f"{path.name}: no exhaustive-check records")
    return {
        "profile_contract": profile_contract,
        "checks": checks,
        "soak_checks": soak_checks,
    }


def _validate_exhaustive(path: Path, check_id: str, event: dict) -> None:
    if event.get("completion") != "frontier_exhausted":
        raise ReportError(f"{path.name}: check {check_id} did not exhaust its frontier")
    for metric in REQUIRED_METRICS:
        value = event.get(metric)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ReportError(f"{path.name}: check {check_id} has invalid {metric}")
    if event["unique_states"] != event["unique_verifier_states"]:
        raise ReportError(f"{path.name}: check {check_id} has inconsistent verifier counts")
    if not (
        0 < event["unique_protocol_states"]
        <= event["unique_verifier_states"]
        <= event["explored_states"]
    ):
        raise ReportError(f"{path.name}: check {check_id} has impossible state counts")
    if event["reached_depth"] > event["configured_depth"]:
        raise ReportError(f"{path.name}: check {check_id} reached beyond configured depth")


def parse_checks(path: Path, expected_profile: str | None = None) -> list[dict]:
    return parse_output(path, expected_profile)["checks"]


def parse_time(path: Path, style: str) -> dict:
    text = path.read_text()
    if style == "bsd":
        elapsed = re.search(r"(?m)^\s*([0-9.]+)\s+real\b", text)
        rss = re.search(r"(?m)^\s*(\d+)\s+maximum resident set size$", text)
        wall_ms = round(float(elapsed.group(1)) * 1000) if elapsed else None
        peak_rss = int(rss.group(1)) if rss else None
    elif style == "gnu":
        elapsed = re.search(r"Elapsed \(wall clock\) time .*: ([0-9:.]+)", text)
        rss = re.search(r"Maximum resident set size \(kbytes\): (\d+)", text)
        wall_ms = _parse_elapsed(elapsed.group(1)) if elapsed else None
        peak_rss = int(rss.group(1)) * 1024 if rss else None
    else:
        raise ReportError(f"unsupported timing style {style!r}")
    if wall_ms is None or peak_rss is None or wall_ms <= 0 or peak_rss <= 0:
        raise ReportError(f"{path.name}: incomplete wall-time or peak-RSS telemetry")
    return {"wall_time_ms": wall_ms, "peak_rss_bytes": peak_rss}


def _parse_elapsed(value: str) -> int:
    parts = value.split(":")
    if len(parts) == 1:
        seconds = float(parts[0])
    elif len(parts) == 2:
        seconds = int(parts[0]) * 60 + float(parts[1])
    elif len(parts) == 3:
        seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
    else:
        raise ReportError(f"invalid elapsed time {value!r}")
    return round(seconds * 1000)


def read_samples(artifact_dir: Path, time_style: str) -> list[dict]:
    with (artifact_dir / "runs.tsv").open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        required = {"run", "label", "profile", "stdout", "time_log", "execution_order"}
        if set(reader.fieldnames or ()) != required:
            raise ReportError("runs.tsv has an unsupported header")
        rows = list(reader)
    if not rows:
        raise ReportError("runs.tsv contains no samples")
    _validate_execution_order(rows)
    samples = []
    for row in rows:
        stdout = artifact_dir / row["stdout"]
        time_log = artifact_dir / row["time_log"]
        if not stdout.is_file() or not time_log.is_file():
            raise ReportError(f"sample artifacts are missing for {row}")
        output = parse_output(stdout, row["profile"])
        checks = output["checks"]
        samples.append(
            {
                "run": int(row["run"]),
                "label": row["label"],
                "profile": row["profile"],
                "execution_order": row["execution_order"],
                "stdout": row["stdout"],
                "time_log": row["time_log"],
                **parse_time(time_log, time_style),
                "profile_contract": output["profile_contract"],
                "totals": _total_checks(checks),
                "checks": checks,
                "soak_checks": output["soak_checks"],
            }
        )
    return samples


def _validate_execution_order(rows: list[dict]) -> None:
    by_run = {}
    for row in rows:
        if row["label"] not in {"base", "current"} or not row["profile"]:
            raise ReportError(f"invalid sample identity: {row}")
        by_run.setdefault(int(row["run"]), []).append((row["profile"], row["label"]))
    first_profiles = [profile for profile, label in by_run[min(by_run)] if label == "base"]
    if len(first_profiles) != len(set(first_profiles)):
        raise ReportError("first run does not contain one complete profile set")
    for run, actual in sorted(by_run.items()):
        profiles = first_profiles if run % 2 == 1 else list(reversed(first_profiles))
        labels = ["base", "current"] if run % 2 == 1 else ["current", "base"]
        expected = [(profile, label) for profile in profiles for label in labels]
        if actual != expected:
            raise ReportError(f"run {run}: execution sequence {actual} != {expected}")


def _total_checks(checks: list[dict]) -> dict:
    totals = {"check_count": len(checks)}
    for metric in (
        "explored_actions",
        "explored_states",
        "unique_protocol_states",
        "unique_verifier_states",
    ):
        totals[f"sum_{metric}_across_checks"] = sum(check[metric] for check in checks)
    totals["verifier_state_overhead"] = (
        totals["sum_unique_verifier_states_across_checks"]
        - totals["sum_unique_protocol_states_across_checks"]
    )
    return totals


def _exact_shape(sample: dict) -> list[dict]:
    return [
        {key: value for key, value in check.items() if key != "duration_ms"}
        for check in sample["checks"]
    ]


def summarize_samples(samples: list[dict], expected_runs: int) -> list[dict]:
    groups = {}
    for sample in samples:
        groups.setdefault((sample["label"], sample["profile"]), []).append(sample)
    summaries = []
    for (label, profile), group in sorted(groups.items()):
        actual_runs = sorted(sample["run"] for sample in group)
        if actual_runs != list(range(1, expected_runs + 1)):
            raise ReportError(f"{label}/{profile}: incomplete run set {actual_runs}")
        if any(sample["profile_contract"] != group[0]["profile_contract"] for sample in group[1:]):
            raise ReportError(f"{label}/{profile}: profile contract changed between samples")
        if any(_exact_shape(sample) != _exact_shape(group[0]) for sample in group[1:]):
            raise ReportError(f"{label}/{profile}: deterministic state shape changed between samples")
        summaries.append(
            {
                "label": label,
                "profile": profile,
                "runs": expected_runs,
                "wall_time_ms": _number_summary([sample["wall_time_ms"] for sample in group]),
                "peak_rss_bytes": _number_summary([sample["peak_rss_bytes"] for sample in group]),
                "totals": group[0]["totals"],
            }
        )
    return summaries


def compare_revisions(samples: list[dict], expected_runs: int) -> list[dict]:
    comparisons = []
    for profile in sorted({sample["profile"] for sample in samples}):
        indexed = {(s["label"], s["run"]): s for s in samples if s["profile"] == profile}
        base, current = indexed[("base", 1)], indexed[("current", 1)]
        if base["profile_contract"] != current["profile_contract"]:
            raise ReportError(f"{profile}: base/current profile contracts differ")
        contract = lambda sample: [
            (check["check_id"], check["configured_depth"]) for check in sample["checks"]
        ]
        if contract(base) != contract(current):
            raise ReportError(f"{profile}: base/current check sets or bounds differ")
        pairs = [
            (indexed[("base", run)], indexed[("current", run)])
            for run in range(1, expected_runs + 1)
        ]
        comparisons.append(
            {
                "profile": profile,
                "like_for_like_state_shape": _exact_shape(base) == _exact_shape(current),
                "paired_current_over_base_wall_time": _number_summary(
                    [current["wall_time_ms"] / base["wall_time_ms"] for base, current in pairs]
                ),
                "paired_current_over_base_peak_rss": _number_summary(
                    [current["peak_rss_bytes"] / base["peak_rss_bytes"] for base, current in pairs]
                ),
            }
        )
    return comparisons


def _number_summary(values: list[int | float]) -> dict:
    return {
        "min": round(min(values), 6),
        "median": round(statistics.median(values), 6),
        "max": round(max(values), 6),
    }

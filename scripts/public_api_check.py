"""Compare the public workspace against its explicit published alpha baseline."""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile

from published_dependency_audit import public_packages

BASELINE = "0.0.2-alpha.1"
TOOL_VERSION = "0.51.0"
TOOLCHAIN = "1.97.1"
MODES = ("default-features", "all-features")
# The pinned tool's patch-level policy enables 229 lints and leaves 31 at its
# built-in allow level. Changing this inventory requires a reviewed tool/policy
# update, rather than silently accepting an incomplete report.
CHECKS, SKIPPED = 229, 31


def digest(data):
    return hashlib.sha256(data).hexdigest()


def inputs(root, destination=None):
    """Read working bytes, including additions/deletions, without checking out HEAD."""
    names = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root)
    records = {}
    for name in sorted(set(os.fsdecode(name) for name in names.split(b"\0") if name)):
        path = root / name
        if path.resolve() != path or path.is_symlink():
            raise ValueError(f"source symlinks are unsupported: {name}")
        if not path.exists():
            records[name] = None  # A local deletion is part of the checked source.
            continue
        if not path.is_file():
            raise ValueError(f"expected a regular source file: {name}")
        data = path.read_bytes()
        executable = bool(path.stat().st_mode & 0o111)
        records[name] = {"sha256": digest(data), "executable": executable}
        if destination is not None:
            target = destination / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            target.chmod(0o755 if executable else 0o644)
    if not all(records.get(name) for name in ("Cargo.toml", "Cargo.lock", "RELEASE.md")):
        raise ValueError("the source snapshot lacks required workspace inputs")
    return records


def verify_snapshot(source, records):
    present = {str(path.relative_to(source)) for path in source.rglob("*") if path.is_file()}
    if present != {name for name, record in records.items() if record is not None}:
        raise ValueError("API checker changed the source file inventory")
    for name, expected in records.items():
        path = source / name
        if expected is None:
            if path.exists() or path.is_symlink():
                raise ValueError(f"deleted source was recreated: {name}")
        elif (path.resolve() != path or not path.is_file()
              or digest(path.read_bytes()) != expected["sha256"]
              or bool(path.stat().st_mode & 0o111) != expected["executable"]):
            raise ValueError(f"API checker changed a source input: {name}")


def validate_metadata(metadata, root):
    packages = public_packages(metadata, root)
    if "cargo-semver-checks" in (metadata.get("metadata") or {}):
        raise ValueError("workspace API lint overrides require explicit policy review")
    for package in metadata["packages"]:
        if "cargo-semver-checks" in (package.get("metadata") or {}):
            raise ValueError(f"API lint overrides are forbidden: {package['name']}")
        if package["source"] is None and not Path(package["manifest_path"]).resolve().is_relative_to(root):
            raise ValueError("workspace manifest escapes the source snapshot")
        for dependency in package["dependencies"]:
            if dependency.get("path") and not Path(dependency["path"]).resolve().is_relative_to(root):
                raise ValueError("path dependency escapes the source snapshot")
    return packages


def comparison_command(source, packages, mode, host):
    if mode not in MODES:
        raise ValueError("unreviewed API feature mode")
    command = ["rustup", "run", TOOLCHAIN, "cargo", "semver-checks",
               "--manifest-path", str(source / "Cargo.toml"),
               "--baseline-version", BASELINE, "--release-type", "patch",
               f"--{mode}", "--target", host, "--color", "never"]
    for name in sorted(packages):
        command.extend(["-p", name])
    return command


def validate_report(output, returncode, packages):
    """Require one complete, warning-free comparison per public crate."""
    if returncode:
        raise ValueError(f"API comparison failed (exit {returncode})")
    # A warning-level lint does not necessarily cause the tool to exit nonzero.
    if re.search(r"(?im)(?:^\s*(?:warning|error)\b|^--- (?:warning|failure)\b)", output):
        raise ValueError("API warnings or failures require review")
    selected = re.findall(r"(?m)^\s*Checking (\S+) v(\S+) -> v(\S+) \(([^\n]+)\)$", output)
    expected = [(name, BASELINE, package["version"], "assume patch change")
                for name, package in sorted(packages.items())]
    if sorted(selected) != expected or not expected:
        raise ValueError("API report has missing, duplicate or unexpected package comparisons")
    checked = re.findall(r"(?m)^\s*Checked \[[^\n]+?s\] ([^\n]+)$", output)
    complete = f"{CHECKS} checks: {CHECKS} pass, {SKIPPED} skip"
    if checked != [complete] * len(expected):
        raise ValueError("API report has incomplete or narrowed lint coverage")
    finished = re.findall(r"(?m)^\s*Finished \[[^\n]+?s\] (\S+)$", output)
    if sorted(finished) != sorted(packages):
        raise ValueError("API report has missing or duplicate completions")
    summaries = re.findall(r"(?m)^\s*Summary ([^\n]+)$", output)
    if summaries != ["no semver update required"] * len(expected):
        raise ValueError("API report did not finish cleanly")


def run(command, cwd, env, artifacts, name):
    path = artifacts / f"{name}.log"
    with path.open("xb") as stream:
        result = subprocess.run(command, cwd=cwd, env=env, stdout=stream,
                                stderr=subprocess.STDOUT, check=False)
    record = {"argv": command, "cwd": str(cwd), "exit_code": result.returncode,
              "log_sha256": digest(path.read_bytes())}
    (artifacts / f"{name}.json").write_text(json.dumps(record, indent=2) + "\n")
    return path.read_text(), result.returncode


def check(root, artifacts):
    env = os.environ.copy()
    # Do not let ambient cfg flags hide an API. The target and compiler are
    # explicit; the checkout's Rust 1.88 selection remains untouched.
    overrides = [name for name, value in env.items() if value and
                 (name in {"RUSTFLAGS", "RUSTDOCFLAGS", "RUSTC", "RUSTDOC",
                           "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"}
                  or name.startswith("CARGO_") and name.endswith(("RUSTFLAGS", "RUSTDOCFLAGS")))]
    if overrides:
        raise ValueError(f"custom compiler overrides are unsupported: {', '.join(sorted(overrides))}")
    env.update(CARGO_TERM_COLOR="never", RUSTUP_TOOLCHAIN=TOOLCHAIN)
    with tempfile.TemporaryDirectory(prefix="rafter-public-api-") as temporary:
        work = Path(temporary).resolve()
        source = work / "source"
        source.mkdir()
        env["CARGO_TARGET_DIR"] = str(work / "target")
        records = inputs(root, source)
        source_record = {"commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(), "files": records}
        (artifacts / "source.json").write_text(json.dumps(source_record, indent=2) + "\n")
        with tarfile.open(artifacts / "source.tar.gz", "w:gz") as archive:
            archive.add(source, arcname="source")

        def required(arguments, name):
            output, code = run(["rustup", "run", TOOLCHAIN, *arguments], source, env, artifacts, name)
            if code:
                raise ValueError(f"{name} failed (exit {code}); see {name}.log")
            return output

        tool = required(["cargo", "semver-checks", "--version"], "tool-version").strip()
        if tool != f"cargo-semver-checks {TOOL_VERSION}":
            raise ValueError(f"install cargo-semver-checks {TOOL_VERSION} with --locked")
        rustc = required(["rustc", "--version", "--verbose"], "rustc-version")
        cargo = required(["cargo", "--version", "--verbose"], "cargo-version")
        if f"release: {TOOLCHAIN}\n" not in rustc:
            raise ValueError("unexpected API-check compiler release")
        host = re.search(r"(?m)^host: (\S+)$", rustc).group(1)
        metadata = json.loads(required(
            ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], "metadata"))
        packages = validate_metadata(metadata, source)
        coverage = {name: {"version": p["version"], "features": sorted(p["features"])}
                    for name, p in sorted(packages.items())}
        (artifacts / "coverage.json").write_text(json.dumps(coverage, indent=2) + "\n")
        for mode in MODES:
            print(f"public API: {len(packages)} crates, {mode}, baseline {BASELINE}", flush=True)
            output, code = run(comparison_command(source, packages, mode, host),
                               source, env, artifacts, mode)
            validate_report(output, code, packages)
            verify_snapshot(source, records)
        if inputs(root) != records:
            raise ValueError("working source changed during API qualification")
    receipt = {"schema": 1, "status": "passed", "baseline": BASELINE,
               "release_type": "patch", "modes": list(MODES), "target": host,
               "tool": tool, "rustc": rustc, "cargo": cargo, "coverage": coverage,
               "artifact_sha256": {p.name: digest(p.read_bytes()) for p in sorted(artifacts.iterdir())}}
    (artifacts / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(f"PASS: public API comparisons; evidence: {artifacts}")

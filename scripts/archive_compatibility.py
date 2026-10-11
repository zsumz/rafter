"""Compile isolated public-archive probes and retain their qualification evidence."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib

from archive_inputs import audit_graph, baseline_archive, baseline_lock, candidate_archive, sha256, unpack

ROOT = Path(__file__).resolve().parents[1]


class ArchiveRun:
    # Each concrete matrix declares its public archive closure and fixture.
    label = ""
    scope = ""
    packages = frozenset()
    dependencies = ()
    probe_name = ""
    fixture = None
    baseline = None
    script = ""

    def __init__(self, artifacts, toolchain, offline):
        self.artifacts = artifacts
        self.toolchain = toolchain
        self.offline = offline
        self.receipt = {"schema": 1, "status": "running", "commands": [], "cases": []}

    def command(self, name, arguments, cwd, env=None):
        stdout = self.artifacts / f"{name}.stdout"
        stderr = self.artifacts / f"{name}.stderr"
        record = {"name": name, "argv": list(map(str, arguments)), "cwd": str(cwd),
                  "stdout": stdout.name, "stderr": stderr.name}
        self.receipt["commands"].append(record)
        print(f"{self.label}: {name}", flush=True)
        with stdout.open("xb") as out, stderr.open("xb") as err:
            try:
                result = subprocess.run(arguments, cwd=cwd, env=env, stdout=out, stderr=err,
                                        timeout=900, check=False)
            except subprocess.TimeoutExpired:
                record["status"] = "timeout"
                raise
        record["exit_code"] = result.returncode
        if result.returncode:
            raise RuntimeError(f"{name} failed ({result.returncode}); see {stdout} and {stderr}")
        return stdout

    def cargo(self, name, arguments, cwd, target):
        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(target)
        argv = ["rustup", "run", self.toolchain, "cargo", *arguments]
        if self.offline:
            argv.append("--offline")
        return self.command(name, argv, cwd, env)

    def build(self, role, archives, work):
        directory = work / role
        directory.mkdir()
        roots = {}
        versions = set()
        archive_records = {}
        for name in sorted(self.packages):
            original = candidate_archive(archives, name)
            archive = directory / original.name
            shutil.copyfile(original, archive)
            roots[name], version = unpack(archive, directory / name, name)
            versions.add(version)
            archive_records[name] = {"version": version, "sha256": sha256(archive)}
        if len(versions) != 1:
            raise ValueError(f"{role} archives are not a coordinated version set")
        version = versions.pop()
        consumer = directory / "probe"
        shutil.copytree(self.fixture, consumer / "src")
        manifest = (
            f'[package]\nname = "{self.probe_name}"\nversion = "0.0.0"\n'
            'edition = "2021"\npublish = false\n\n[workspace]\n\n[dependencies]\n'
            + "".join(f'{name} = "={version}"\n' for name in self.dependencies))
        (consumer / "Cargo.toml").write_text(manifest)
        (consumer / ".cargo").mkdir()
        (consumer / ".cargo/config.toml").write_text(
            "[patch.crates-io]\n" + "".join(
                f"{name} = {{ path = {json.dumps(str(path))} }}\n"
                for name, path in sorted(roots.items())))
        target = directory / "target"
        self.cargo(f"{role}-lock", ["generate-lockfile"], consumer, target)
        metadata_path = self.cargo(f"{role}-metadata", ["metadata", "--locked", "--format-version=1"],
                                   consumer, target)
        metadata = json.loads(metadata_path.read_text())
        audit_graph(metadata, ROOT, roots, consumer, self.probe_name)
        self.cargo(f"{role}-build", ["build", "--locked"], consumer, target)
        binary = target / "debug" / self.probe_name
        lock = self.artifacts / f"{role}-Cargo.lock"
        shutil.copyfile(consumer / "Cargo.lock", lock)
        self.receipt[role] = {"archives": archive_records, "binary_sha256": sha256(binary),
                              "lock_sha256": sha256(lock), "metadata_sha256": sha256(metadata_path)}
        return binary


def arguments(run_type, description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--candidate-archives", type=Path)
    parser.add_argument("--baseline-cache", type=Path, default=ROOT / "target" / run_type.label / "archives")
    parser.add_argument("--artifacts", type=Path, help="new directory; an existing path is refused")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--keep", action="store_true", help="keep temporary unpacked sources and binaries")
    parser.add_argument("--toolchain", help="rustup toolchain; defaults to the selected checkout toolchain")
    return parser.parse_args()


def run_matrix(run_type, description):
    args = arguments(run_type, description)
    toolchain = args.toolchain or os.environ.get("RUSTUP_TOOLCHAIN") or tomllib.loads(
        (ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
    parent = ROOT / "target" / run_type.label
    parent.mkdir(parents=True, exist_ok=True)
    if args.artifacts:
        artifacts = args.artifacts.resolve()
        artifacts.mkdir(parents=True)
    else:
        artifacts = Path(tempfile.mkdtemp(prefix="run-", dir=parent))
    work = Path(tempfile.mkdtemp(prefix=f"rafter-{run_type.label}-")).resolve()
    run = run_type(artifacts, toolchain, args.offline)
    try:
        if work.is_relative_to(ROOT):
            raise ValueError("temporary consumer workspace must be outside the checkout; change TMPDIR")
        run.receipt.update({"toolchain": toolchain, "scope": run.scope,
                            "fixture_sha256": {str(path.relative_to(run.fixture)): sha256(path)
                                               for path in run.fixture.rglob("*.rs")},
                            "runner_sha256": {name: sha256(ROOT / "scripts" / name) for name in
                                              [run.script, "archive_compatibility.py", "archive_inputs.py"]}})
        version = run.command("rustc-version", ["rustup", "run", toolchain, "rustc", "-Vv"], ROOT)
        run.receipt["rustc"] = version.read_text()
        lock = baseline_lock(run.baseline, run.packages)
        run.receipt["baseline_lock"] = lock
        cache = args.baseline_cache.resolve()
        cache.mkdir(parents=True, exist_ok=True)
        baseline_archives = work / "baseline-archives"
        baseline_archives.mkdir()
        for name, checksum in sorted(lock["packages"].items()):
            archive = baseline_archive(cache, name, lock["version"], checksum, args.offline)
            copied = baseline_archives / archive.name
            shutil.copyfile(archive, copied)
            if sha256(copied) != checksum:
                raise ValueError(f"copied baseline checksum mismatch: {name}")
        candidate_archives = args.candidate_archives
        if candidate_archives is None:
            package_target = work / "package-target"
            run.cargo("candidate-package", ["package", "--locked", "--allow-dirty", "--no-verify",
                                            *[arg for name in sorted(run.packages) for arg in ("-p", name)]],
                      ROOT, package_target)
            candidate_archives = package_target / "package"
        baseline = run.build("baseline", baseline_archives, work)
        candidate = run.build("candidate", candidate_archives.resolve(), work)
        run.matrix(baseline, candidate)
        run.receipt["status"] = "pass"
    except Exception as error:
        run.receipt.update(status="fail", error=str(error))
        print(f"{run.label}: {error}", file=sys.stderr)
    finally:
        (artifacts / "receipt.json").write_text(json.dumps(run.receipt, indent=2) + "\n")
        if args.keep:
            print(f"{run.label}: kept {work}")
        else:
            shutil.rmtree(work)
        print(f"{run.label}: {run.receipt['status']}; receipt: {artifacts / 'receipt.json'}")
    return 0 if run.receipt["status"] == "pass" else 1

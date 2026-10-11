"""Prove the dependency closure audited for the publishable Rafter libraries."""

import hashlib
import json
from pathlib import Path
import re
import subprocess
import tomllib

AUDIT_VERSION = "0.22.2"
PROBE = "rafter-published-advisory-audit"
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
ADVISORY_URL = "https://github.com/RustSec/advisory-db"


def audit_policy():
    """Override inherited exclusions and require fresh advisories and yank checks."""
    return (f'[database]\nurl = "{ADVISORY_URL}"\nfetch = true\nstale = false\n'
            '[output]\ndeny = ["warnings"]\nquiet = false\n[yanked]\nenabled = true\n')


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def public_packages(metadata, root):
    """Use Cargo's resolved manifest fields, including implicit optional features."""
    members = set(metadata["workspace_members"])
    packages = {}
    for package in metadata["packages"]:
        if package["id"] not in members or package["publish"] == []:
            continue
        name = package["name"]
        manifest = Path(package["manifest_path"]).resolve()
        if (name in packages or package["source"] is not None
                or manifest != root / "crates" / name / "Cargo.toml"):
            raise ValueError(f"unexpected public package identity: {name}")
        packages[name] = package
    if not {"rafter", "rafter-service", "rafter-transport-tls"} <= packages.keys():
        raise ValueError("publishable package inventory is incomplete")
    # Bind discovery to the existing release inventory instead of creating a
    # second manually maintained list that can silently omit a new library.
    release = (root / "RELEASE.md").read_text().split("Publish these crates", 1)[1]
    declared = release.split("```text\n", 1)[1].split("```", 1)[0].splitlines()
    if len(declared) != len(set(declared)) or set(declared) != set(packages):
        raise ValueError("Cargo's public package set differs from RELEASE.md")
    return packages


def probe_manifest(packages, msrv):
    lines = ["[package]", f'name = "{PROBE}"', 'version = "0.0.0"',
             'edition = "2021"', f"rust-version = {json.dumps(msrv)}",
             "publish = false", "[workspace]", 'resolver = "2"', "[dependencies]"]
    for name, package in sorted(packages.items()):
        directory = str(Path(package["manifest_path"]).parent)
        features = sorted(package["features"])
        lines.append(f'{json.dumps(name)} = {{ path = {json.dumps(directory)}, '
                     f'version = "={package["version"]}", '
                     f'features = {json.dumps(features)} }}')
    return "\n".join(lines) + "\n"


def lock_identity(package):
    return (package["name"], package["version"], package.get("source"),
            package.get("checksum"))


def validate_closure(metadata, packages, probe, seed_lock, resolved_lock):
    """Refuse omissions, feature narrowing, private paths and re-resolution."""
    expected = {name: Path(package["manifest_path"]).resolve()
                for name, package in packages.items()}
    expected[PROBE] = (probe / "Cargo.toml").resolve()
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    seen = set()
    identities = set()
    member = None
    for package in metadata["packages"]:
        name, source = package["name"], package["source"]
        if package["id"] not in nodes:
            raise ValueError(f"package omitted from resolved graph: {name}")
        identities.add((name, package["version"], source))
        if source is not None:
            if source != REGISTRY or name in expected or name.startswith("rafter-"):
                raise ValueError(f"unexpected registry or Git dependency: {name}")
            continue
        if (name not in expected or name in seen
                or Path(package["manifest_path"]).resolve() != expected[name]):
            raise ValueError(f"unexpected path dependency: {name}")
        seen.add(name)
        if name == PROBE:
            member = package["id"]
        elif (package["version"] != packages[name]["version"]
              or not set(packages[name]["features"]) <= set(nodes[package["id"]]["features"])):
            raise ValueError(f"public version or feature coverage changed: {name}")
    if seen != set(expected) or metadata["workspace_members"] != [member]:
        raise ValueError("audit must resolve every public root in one isolated consumer")
    root_deps = {dependency["pkg"] for dependency in nodes[member]["deps"]}
    public_ids = {package["id"] for package in metadata["packages"]
                  if package["name"] in packages}
    if root_deps != public_ids:
        raise ValueError("consumer must directly enable every public root")
    if any("internal-test-hooks" in node["features"] for node in nodes.values()):
        raise ValueError("internal test hooks are forbidden")
    resolved = tomllib.loads(resolved_lock)["package"]
    locked = {(p["name"], p["version"], p.get("source")) for p in resolved}
    if len(locked) != len(resolved) or locked != identities:
        raise ValueError("audit lock and metadata package identities differ")
    seed = {lock_identity(p) for p in tomllib.loads(seed_lock)["package"]}
    registry = [p for p in resolved if p.get("source") is not None]
    if not registry or any(lock_identity(p) not in seed or not p.get("checksum")
                           for p in registry):
        raise ValueError("registry version or checksum differs from the workspace lock")
    return {"public_packages": sorted(packages),
            "public_features": {name: sorted(p["features"]) for name, p in packages.items()},
            "package_count": len(resolved),
            "registry_packages": [dict(name=p["name"], version=p["version"],
                                       source=p["source"], checksum=p["checksum"])
                                  for p in registry]}


def validate_report(report, returncode, package_count):
    """A successful tool exit alone is insufficient evidence of complete coverage."""
    if returncode:
        raise ValueError(f"cargo-audit refused the dependency graph (exit {returncode})")
    settings = report["settings"]
    expected = {"ignore": [], "target_arch": [], "target_os": [], "severity": None}
    if any(settings[key] != value for key, value in expected.items()):
        raise ValueError("advisory exclusions, severity or platform filters are forbidden")
    if set(settings["informational_warnings"]) != {"unmaintained", "unsound", "notice"}:
        raise ValueError("all informational advisory categories must be checked")
    if report["lockfile"]["dependency-count"] != package_count:
        raise ValueError("cargo-audit inspected a different package inventory")
    database = report["database"]
    if (not re.fullmatch(r"[0-9a-f]{40}", database["last-commit"] or "")
            or not database["last-updated"] or database["advisory-count"] <= 0):
        raise ValueError("a fetched advisory database identity is required")
    vulnerabilities = report["vulnerabilities"]
    if (vulnerabilities["found"] is not False or vulnerabilities["count"] != 0
            or vulnerabilities["list"] != [] or report["warnings"] != {}):
        raise ValueError("dependency advisories or warnings require review")


def audit_lock(lockfile, project, artifacts, role="audit"):
    """Keep each graph's raw evidence and refuse a successful but narrowed report."""
    lock_hash = digest(lockfile)
    packages = tomllib.loads(lockfile.read_text())["package"]
    result = subprocess.run(
        ["cargo", "audit", "--file", str(lockfile), "--json", "--deny", "warnings"],
        cwd=project, capture_output=True)
    (artifacts / f"{role}.json").write_bytes(result.stdout)
    (artifacts / f"{role}.stderr").write_bytes(result.stderr)
    if not result.stdout and result.returncode:
        raise ValueError(f"{role} failed (exit {result.returncode}); see {role}.stderr")
    report = json.loads(result.stdout)
    validate_report(report, result.returncode, len(packages))
    if digest(lockfile) != lock_hash:
        raise ValueError(f"{role} lock changed during inspection")
    return {"lock_sha256": lock_hash, "package_count": len(packages),
            "registry_count": sum(p.get("source", "").startswith("registry+") for p in packages),
            "report": report}

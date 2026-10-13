"""Bind counter qualification to either this checkout or its standalone Git pin."""
import json
from pathlib import Path
import re

try:
    import tomllib
except ModuleNotFoundError as error:
    raise SystemExit("counter guide qualification requires Python 3.11 or newer") from error

SOURCE = "https://github.com/zsumz/rafter"


def is_rafter(name):
    return name == "rafter" or name.startswith("rafter-")


def published_crates(root):
    crates = {}
    for manifest in sorted((root / "crates").glob("*/Cargo.toml")):
        package = tomllib.loads(manifest.read_text())["package"]
        if package.get("publish") is not False:
            name = package["name"]
            if name in crates:
                raise ValueError(f"duplicate publishable crate: {name}")
            crates[name] = manifest.parent.resolve()
    if not {"rafter", "rafter-service", "rafter-transport-tls"} <= crates.keys():
        raise ValueError("publishable crate inventory is incomplete")
    return crates


def pinned_dependencies(manifest):
    dependencies = tomllib.loads(manifest.read_text())["dependencies"]
    pins = {}
    for name, dependency in dependencies.items():
        if not is_rafter(name):
            continue
        if not isinstance(dependency, dict) or dependency.get("git") != SOURCE:
            raise ValueError(f"{name} must use the standalone Rafter Git source")
        revision = dependency.get("rev", "")
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ValueError(f"{name} must pin a full Git commit")
        pins[name] = revision
    if not pins or len(set(pins.values())) != 1:
        raise ValueError("standalone Rafter dependencies must share one exact commit")
    return next(iter(pins.values())), set(pins)


def checkout_args(crates):
    args = []
    for name, directory in sorted(crates.items()):
        # This is a TOML value passed as one argv element, never shell text.
        args.extend(["--config", f'patch."{SOURCE}".{name}.path={json.dumps(str(directory))}'])
    return args


def validate_graph(metadata, crates, revision, required, mode, project):
    if mode not in {"checkout", "pinned"}:
        raise ValueError(f"unknown dependency mode: {mode}")
    expected_source = f"git+{SOURCE}?rev={revision}#{revision}"
    seen = set()
    for package in metadata["packages"]:
        name = package["name"]
        manifest = Path(package["manifest_path"]).resolve()
        if name == "rafter-counter" and manifest == (project / "Cargo.toml").resolve():
            continue
        if not is_rafter(name):
            continue
        if name not in crates:
            raise ValueError(f"counter reaches an unpublished Rafter package: {name}")
        if name in seen:
            raise ValueError(f"counter resolves multiple copies of {name}")
        seen.add(name)
        if mode == "checkout":
            expected = (crates[name] / "Cargo.toml").resolve()
            if package.get("source") is not None or manifest != expected:
                raise ValueError(f"{name} did not resolve to the checkout: {manifest}")
        elif package.get("source") != expected_source:
            raise ValueError(f"{name} did not resolve to the pinned Git commit")
    if missing := required - seen:
        raise ValueError(f"counter graph omitted required Rafter crates: {sorted(missing)}")
    return seen

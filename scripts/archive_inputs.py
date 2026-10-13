"""Archive and dependency checks for public compatibility fixtures."""

import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import tarfile
import tomllib
import urllib.request

MAX_ARCHIVE_BYTES = 32 * 1024 * 1024
MAX_UNPACKED_BYTES = 128 * 1024 * 1024
MAX_ENTRIES = 10_000


def sha256(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def baseline_lock(path, packages):
    lock = json.loads(Path(path).read_text())
    if lock.get("schema") != 1 or set(lock.get("packages", {})) != packages:
        raise ValueError("baseline must pin exactly the public fixture packages")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?", lock["version"]):
        raise ValueError("invalid baseline version")
    if any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in lock["packages"].values()):
        raise ValueError("baseline checksums must be SHA-256 hex digests")
    return lock


def baseline_archive(cache, name, version, checksum, offline):
    path = cache / f"{name}-{version}.crate"
    if not path.exists():
        if offline:
            raise ValueError(f"offline baseline archive missing: {path}")
        url = f"https://static.crates.io/crates/{name}/{name}-{version}.crate"
        # Verify bytes before publishing them to the reusable download cache.
        with urllib.request.urlopen(url, timeout=60) as response:
            data = response.read(MAX_ARCHIVE_BYTES + 1)
        if len(data) > MAX_ARCHIVE_BYTES or hashlib.sha256(data).hexdigest() != checksum:
            raise ValueError(f"downloaded baseline checksum mismatch: {name}")
        with path.open("xb") as output:
            output.write(data)
    if sha256(path) != checksum:
        raise ValueError(f"cached baseline checksum mismatch: {name}")
    return path


def candidate_archive(directory, name):
    matches = [path for path in directory.glob(f"{name}-*.crate")
               if re.fullmatch(re.escape(name) + r"-[0-9][0-9A-Za-z.+-]*\.crate", path.name)]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one candidate archive for {name}, found {len(matches)}")
    return matches[0]


def unpack(archive, destination, name):
    """Extract only regular files/directories into a fresh, bounded archive root."""
    if archive.stat().st_size > MAX_ARCHIVE_BYTES:
        raise ValueError("archive exceeds compressed size budget")
    destination.mkdir()
    root_name = archive.name.removesuffix(".crate")
    seen = set()
    size = 0
    with tarfile.open(archive, "r|gz") as source:
        for entry in source:
            path = PurePosixPath(entry.name)
            if (not path.parts or path.is_absolute() or ".." in path.parts
                    or "\\" in entry.name or path.parts[0] != root_name):
                raise ValueError(f"unsafe archive path: {entry.name}")
            if not (entry.isfile() or entry.isdir()) or path in seen:
                raise ValueError(f"non-regular or duplicate archive entry: {entry.name}")
            seen.add(path)
            size += entry.size
            if len(seen) > MAX_ENTRIES or size > MAX_UNPACKED_BYTES:
                raise ValueError("archive exceeds extraction budget")
            output = destination.joinpath(*path.parts)
            if entry.isdir():
                output.mkdir(parents=True, exist_ok=True)
            else:
                output.parent.mkdir(parents=True, exist_ok=True)
                with source.extractfile(entry) as content, output.open("xb") as target:
                    while block := content.read(1024 * 1024):
                        target.write(block)
    root = destination / root_name
    package = tomllib.loads((root / "Cargo.toml").read_text())["package"]
    if package["name"] != name or root_name != f"{name}-{package['version']}":
        raise ValueError("archive filename and package identity disagree")
    return root, package["version"]


def audit_graph(metadata, checkout, roots, consumer, probe_name, *, workspace=None):
    """Prove both builds resolve Rafter only from their respective archive set."""
    packages = {package["id"]: package for package in metadata["packages"]}
    expected = workspace or {probe_name: consumer / "Cargo.toml"}
    members = {
            package_id for package_id, package in packages.items()
            if Path(package["manifest_path"]).resolve() == expected.get(package["name"])}
    if len(members) != len(expected) or set(metadata["workspace_members"]) != members:
        raise ValueError("consumer must have exactly the expected workspace members")
    found = set()
    allowed_paths = {root / "Cargo.toml" for root in roots.values()} | set(expected.values())
    for package in packages.values():
        manifest = Path(package["manifest_path"]).resolve()
        if manifest.is_relative_to(checkout):
            raise ValueError(f"dependency reaches checkout: {manifest}")
        if package["source"] is None and manifest not in allowed_paths:
            raise ValueError(f"unpublished path dependency: {manifest}")
        name = package["name"]
        if name == "rafter" or name.startswith("rafter-"):
            if manifest == expected.get(name):
                continue
            if name not in roots or name in found or manifest != roots[name] / "Cargo.toml":
                raise ValueError(f"unexpected Rafter dependency: {name} at {manifest}")
            found.add(name)
    if found != set(roots):
        raise ValueError("fixture did not resolve the complete public archive dependency set")
    for node in metadata["resolve"]["nodes"]:
        if "internal-test-hooks" in node["features"]:
            raise ValueError("internal test hooks are forbidden in archive upgrade checks")


def tree_inventory(directory):
    """Include names, directory shape and bytes, including the ownership lock."""
    inventory = {}
    for path in sorted(directory.rglob("*")):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise ValueError(f"unexpected durable fixture object: {path}")
        inventory[str(path.relative_to(directory))] = (
            {"kind": "directory"} if path.is_dir() else
            {"kind": "file", "bytes": path.stat().st_size, "sha256": sha256(path)})
    return inventory

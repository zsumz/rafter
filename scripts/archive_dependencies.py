"""Project the exact archive consumer lock onto public normal/build dependencies."""

import argparse
import hashlib
import json
from pathlib import Path, PurePath
import re
import tomllib

from published_dependency_audit import REGISTRY


def identity(package):
    return package["name"], package["version"], package.get("source")


def dependency_identity(specification, identities):
    match = re.fullmatch(r"([A-Za-z0-9_-]+)(?: ([^ ()]+))?(?: \(([^)]+)\))?", specification)
    if match is None:
        raise ValueError(f"invalid locked dependency: {specification}")
    name, version, source = match.groups()
    matches = [key for key in identities if key[0] == name
               and (version is None or key[1] == version)
               and (source is None or key[2] == source)]
    if len(matches) != 1:
        raise ValueError(f"ambiguous or missing locked dependency: {specification}")
    return matches[0]


def public_closure(metadata, lock_text, public_versions, unpack_root):
    """Retain exact package blocks after proving their edges and archive roots."""
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    if (len(packages) != len(metadata["packages"])
            or len(nodes) != len(metadata["resolve"]["nodes"])
            or packages.keys() != nodes.keys()):
        raise ValueError("metadata package and node identities must be complete and unique")
    keys = {identity(package): package_id for package_id, package in packages.items()}
    lock = tomllib.loads(lock_text)
    locked = {identity(package): package for package in lock["package"]}
    if (len(keys) != len(packages) or len(locked) != len(lock["package"])
            or locked.keys() != keys.keys()):
        raise ValueError("final lock and metadata package identities differ")
    if lock.get("version") not in {3, 4}:
        raise ValueError("unsupported Cargo lock format")
    if not public_versions:
        raise ValueError("public archive inventory is empty")

    roots = {}
    feature_coverage = {}
    for name, version in public_versions.items():
        candidates = [package for package in packages.values() if package["name"] == name]
        if len(candidates) != 1:
            raise ValueError(f"expected exactly one resolved public archive: {name}")
        package = candidates[0]
        expected = PurePath(unpack_root) / f"{name}-{version}" / "Cargo.toml"
        if (package["source"] is not None or package["version"] != version
                or PurePath(package["manifest_path"]) != expected):
            raise ValueError(f"public dependency is not the expected archive: {name}")
        package_id = package["id"]
        if package_id in metadata["workspace_members"]:
            raise ValueError("public archives must be dependencies of the isolated consumers")
        features = set(package["features"])
        if not features <= set(nodes[package_id]["features"]):
            raise ValueError(f"consumer omitted public features: {name}")
        roots[name] = package_id
        feature_coverage[name] = sorted(features)
    archived = {package_id for package_id, package in packages.items()
                if package["source"] is None
                and PurePath(package["manifest_path"]).is_relative_to(PurePath(unpack_root))}
    if archived != set(roots.values()):
        raise ValueError("resolved archive set differs from the public inventory")

    selected = set()
    edges = {}
    pending = list(roots.values())
    while pending:
        package_id = pending.pop()
        if package_id in selected:
            continue
        if package_id not in packages:
            raise ValueError("resolved edge names an unknown package")
        package = packages[package_id]
        if package["source"] is None:
            if roots.get(package["name"]) != package_id:
                raise ValueError(f"public closure reaches a private path: {package['name']}")
        elif package["source"] != REGISTRY:
            raise ValueError(f"unreviewed dependency source: {package['name']}")
        if "internal-test-hooks" in nodes[package_id]["features"]:
            raise ValueError("public closure enables internal test hooks")
        selected.add(package_id)
        edges[package_id] = set()
        for dependency in nodes[package_id]["deps"]:
            kinds = {kind["kind"] for kind in dependency["dep_kinds"]}
            if not kinds or not kinds <= {None, "build", "dev"}:
                raise ValueError("missing or unknown dependency kind")
            if kinds & {None, "build"}:
                edges[package_id].add(dependency["pkg"])
                pending.append(dependency["pkg"])

    # Keeping original blocks preserves versions, checksums and dependency
    # disambiguation. Refuse a block whose edge set disagrees with metadata;
    # never quietly drop an unresolved edge to make the projected lock parse.
    selected_keys = {identity(packages[package_id]) for package_id in selected}
    for package_id in selected:
        package = locked[identity(packages[package_id])]
        dependencies = {dependency_identity(specification, locked)
                        for specification in package.get("dependencies", [])}
        expected = {identity(packages[dependency]) for dependency in edges[package_id]}
        if dependencies != expected or not dependencies <= selected_keys:
            raise ValueError(f"locked dependency edges disagree: {package['name']}")
        if package.get("source") is not None and not re.fullmatch(
                r"[0-9a-f]{64}", package.get("checksum", "")):
            raise ValueError(f"registry checksum missing: {package['name']}")

    blocks = re.split(r"(?m)(?=^\[\[package\]\]$)", lock_text)
    if set(tomllib.loads(blocks[0])) != {"version"}:
        raise ValueError("unexpected lock header")
    output = [blocks[0].rstrip()]
    emitted = set()
    for block in blocks[1:]:
        parsed = tomllib.loads(block)
        if set(parsed) != {"package"} or len(parsed["package"]) != 1:
            raise ValueError("unexpected trailing lock data")
        package = parsed["package"][0]
        key = identity(package)
        if key in selected_keys:
            if key in emitted or package != locked[key]:
                raise ValueError("duplicate or changed package block")
            output.append(block.rstrip())
            emitted.add(key)
    if emitted != selected_keys:
        raise ValueError("projected lock omitted a public dependency")
    receipt = {"public_versions": public_versions, "public_features": feature_coverage,
               "package_count": len(selected),
               "registry_count": sum(key[2] is not None for key in selected_keys)}
    return "\n\n".join(output) + "\n", receipt


def digest(data):
    return hashlib.sha256(data).hexdigest()


def records(text, label):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) != 2 or fields[0] in result:
            raise ValueError(f"invalid or duplicate {label} record")
        result[fields[0]] = fields[1]
    return result


def hash_records(text):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if (len(fields) != 2 or not re.fullmatch(r"[0-9a-f]{64}", fields[0])
                or fields[1] in result):
            raise ValueError("invalid or duplicate SHA-256 record")
        result[fields[1]] = fields[0]
    return result


def prepare(artifacts, release):
    """Consume retained inputs only: do not resolve Cargo or follow archive paths."""
    names = ("Cargo.resolved.lock", "consumer-metadata.json", "resolved-inputs.sha256",
             "package-sha256.txt", "crate-versions.txt", "metadata-boundary.txt")
    inputs = {}
    for name in names:
        path = artifacts / name
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"required regular artifact missing: {name}")
        inputs[name] = path.read_bytes()
    checked = hash_records(inputs["resolved-inputs.sha256"].decode())
    if set(checked) != {"Cargo.resolved.lock", "consumer-metadata.json"}:
        raise ValueError("resolution digest manifest must bind exactly the final lock and metadata")
    if any(digest(inputs[name]) != checksum for name, checksum in checked.items()):
        raise ValueError("retained resolution digest mismatch")
    boundary = {}
    for line in inputs["metadata-boundary.txt"].decode().splitlines():
        key, value = line.split("=", 1)
        if key in boundary:
            raise ValueError("duplicate boundary verdict field")
        boundary[key] = value
    fields = {"checkout-paths", "forbidden-path-dependencies", "unpublished-crates",
              "internal-test-hooks"}
    if (set(boundary) != fields | {"rafter-source-root"}
            or any(boundary[field] != "none" for field in fields)):
        raise ValueError("a passing exact-package boundary verdict is required")
    unpack_root = PurePath(boundary["rafter-source-root"])
    if not unpack_root.is_absolute() or ".." in unpack_root.parts:
        raise ValueError("invalid recorded archive root")

    release_bytes = release.read_bytes()
    public_names = (release_bytes.decode().split("Publish these crates", 1)[1]
                    .split("```text\n", 1)[1].split("```", 1)[0].splitlines())
    if (not public_names or len(set(public_names)) != len(public_names)
            or any(not re.fullmatch(r"[A-Za-z0-9_-]+", name) for name in public_names)):
        raise ValueError("invalid public release inventory")
    versions = records(inputs["crate-versions.txt"].decode(), "crate version")
    if not set(public_names) <= versions.keys():
        raise ValueError("archive version inventory omits public crates")
    public_versions = {name: versions[name] for name in public_names}
    if any(not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?",
                            version) for version in public_versions.values()):
        raise ValueError("invalid public archive version")
    archive_hashes = hash_records(inputs["package-sha256.txt"].decode())
    if set(archive_hashes) != {f"{name}-{version}.crate" for name, version in public_versions.items()}:
        raise ValueError("archive digest inventory differs from the public release set")

    projected, receipt = public_closure(
        json.loads(inputs["consumer-metadata.json"]), inputs["Cargo.resolved.lock"].decode(),
        public_versions, unpack_root)
    receipt.update(schema=1, source_sha256={name: digest(data) for name, data in inputs.items()},
                   release_sha256=digest(release_bytes), archive_sha256=archive_hashes,
                   projected_lock_sha256=digest(projected.encode()))
    destination = artifacts / "public-dependencies"
    destination.mkdir(exist_ok=False)
    (destination / "Cargo.lock").write_text(projected)
    (destination / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return destination, receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--release", type=Path,
                        default=Path(__file__).resolve().parents[1] / "RELEASE.md")
    args = parser.parse_args()
    try:
        destination, receipt = prepare(args.artifacts.resolve(), args.release)
    except (ValueError, KeyError, IndexError, TypeError, OSError) as error:
        raise SystemExit(f"FAIL: archive dependency projection: {error}") from error
    print(f'PASS: {len(receipt["public_versions"])} public archives, '
          f'{receipt["package_count"]} packages, {receipt["registry_count"]} registry dependencies; '
          f'evidence: {destination}')


if __name__ == "__main__":
    main()

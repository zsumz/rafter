"""Retain and verify the exact crate bytes selected by package qualification."""

import argparse
import hashlib
from pathlib import Path
import re
import shutil

from archive_dependencies import hash_records


def regular(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"required regular archive input missing: {path.name}")
    return path


def inventory(artifacts):
    manifest = regular(artifacts / "package-sha256.txt")
    expected = hash_records(manifest.read_text())
    if not expected or any(not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.+\-]*\.crate", name)
                           for name in expected):
        raise ValueError("archive inventory must contain only crate basenames")
    return expected


def checksum(path):
    with regular(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verify(artifacts):
    """Read bytes only; never unpack archives or execute their contents."""
    expected = inventory(artifacts)
    destination = artifacts / "archives"
    if destination.is_symlink() or not destination.is_dir():
        raise ValueError("retained archive directory missing")
    if {path.name for path in destination.iterdir()} != set(expected):
        raise ValueError("retained archive inventory differs from package hashes")
    for name, digest in expected.items():
        if checksum(destination / name) != digest:
            raise ValueError(f"retained archive digest mismatch: {name}")
    return len(expected)


def retain(artifacts, source):
    """Copy the first packaging output; refuse replacement or changed bytes."""
    expected = inventory(artifacts)
    destination = artifacts / "archives"
    destination.mkdir(exist_ok=False)
    for name in expected:
        shutil.copyfile(regular(source / name), destination / name)
    return verify(artifacts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--source", type=Path,
                        help="retain this packaging output; omit to verify existing retained bytes")
    args = parser.parse_args()
    try:
        count = (retain(args.artifacts, args.source) if args.source is not None
                 else verify(args.artifacts))
    except (ValueError, OSError) as error:
        raise SystemExit(f"FAIL: archive retention: {error}") from error
    print(f"PASS: {count} retained crate archives match package-sha256.txt")


if __name__ == "__main__":
    main()

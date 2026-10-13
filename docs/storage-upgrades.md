# Storage upgrade qualification

`scripts/storage-upgrade-check` tests forward recovery from the published
`0.0.2-alpha.1` archives into a candidate archive set. The current candidate
still has that version in its manifests; archive checksums distinguish its
bytes from the published release. This check does not change versions or
establish a general compatibility promise for the alpha product family.

## Executable matrix

| Writer | Reader | Durable state exercised | Required result |
| --- | --- | --- | --- |
| Published `0.0.2-alpha.1` | Candidate | RFHS hard state, vote, commit floor, retained entries including an uncommitted tail | Recover exact values, append and commit more work, reopen successfully. |
| Published `0.0.2-alpha.1` | Candidate | Installed snapshot with membership metadata and compacted log prefix | Read exact descriptor and payload in chunks, replace the snapshot, compact, reopen. |
| Published `0.0.2-alpha.1` | Candidate | Installed snapshot plus an interrupted incoming snapshot transfer | Recover the transfer identity and received offset, resume, promote, compact, reopen. |
| Synthetic RFHS version 99 with a valid checksum | Candidate | Future hard-state envelope in an existing store directory | Return the typed unsupported-version error without changing any file bytes or directory shape. |

Each write, inspection, extension and subsequent reopen executes in a separate
process. The fixture asserts complete expected values, including snapshot
membership, term, vote, retained entry payloads, transfer checksum and offset.
The unsupported-version check requires the exact error variant; a generic
corruption failure cannot satisfy it.

The baseline lock in
[`verification/storage-upgrade-baseline.json`](../verification/storage-upgrade-baseline.json)
pins the crates.io SHA-256 checksums of `rafter`, `rafter-storage`, and
`rafter-crc32`. Archives are verified before extraction. Extraction refuses
links, traversal, duplicate paths and special files, and bounds file count and
payload bytes. Each role builds the same
[`storage-format-probe`](../reference/fenced-lock/src/bin/storage-format-probe/main.rs)
against only its own unpacked public archives in a workspace outside the
checkout. Dependency inspection rejects checkout paths, unexpected Rafter
crates, unpublished path dependencies and enabled internal test hooks.

Third-party dependencies resolve afresh and their lockfiles are retained. This
checks the published Rafter source archives compiled with the selected Rust
toolchain; it does not reproduce a historical executable or its original
third-party dependency closure. Compiling the fixture against both archive
sets establishes only the API surface it actually uses.

## Running and retaining evidence

Use Python 3.11+ and an installed rustup toolchain:

```sh
scripts/storage-upgrade-check
scripts/storage-upgrade-check --offline
scripts/storage-upgrade-check --candidate-archives /absolute/path/to/package
```

Without `--candidate-archives`, the runner packages the three candidate crates
from the checkout. The archive argument must contain exactly one version of
each required package; unrelated sibling archives are allowed. Baselines are
cached under `target/storage-upgrade/archives`. Offline mode requires both that
cache and Cargo's dependency/index cache to be warm and never fetches them.

Every run uses a fresh artifact directory under `target/storage-upgrade/`.
`--artifacts PATH` selects a new directory and refuses an existing one.
`receipt.json` records success only after every case passes, together with
archive and binary checksums, fixture hashes, compiler identity, command
results and durable-state inventories. Logs, both Cargo lockfiles, dependency
metadata and the final durable directories accompany it. Failed runs retain
their diagnostics. `--keep` additionally preserves temporary builds.

`scripts/reference-package-check` includes this matrix as its final phase in
deterministic and MSRV modes, reusing exactly the archives already qualified
by the reference consumers. The MSRV modes compile both roles with their
requested MSRV toolchain. The PR package job uploads its receipts, and
`zcheck run reference-package` first checks the runner's refusal boundaries.

## Remaining release work

This initial matrix covers `FileRaftNodeStores` and small application fixtures.
RFHJ journal stores, shared WAL stores, migration interruption, arbitrary
configuration histories, large snapshots, older `0.0.1` upgrades and downgrade
behavior remain unqualified. There is no implicit migration between storage
layouts. Treat these combinations as unsupported until their policy and
executable cases are added.

The [peer-codec matrix](wire-compatibility.md) separately checks both wire
directions against released archives. Rolling three-node upgrades remain work:
the next matrix should exercise acknowledged writes, linearizable reads,
membership changes and snapshot catch-up while old and candidate processes
coexist. Public API compatibility also needs a broader baseline than this
fixture. Keep the format vectors and same-version process suites alongside
these upgrade checks.

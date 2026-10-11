# Published-library rolling-upgrade qualification

`scripts/rolling-upgrade-check` builds the current fenced-lock public consumer
twice outside the checkout: once with the eleven published `0.0.2-alpha.1`
archives and once with the exact candidate archives. It runs real processes
using the existing mutually authenticated TLS transport and durable file stores.
The same consumer source, application format, and test credentials are used for
both builds. This qualifies library interoperability; it does not represent a
historical application executable or an application-schema migration.

The scenario starts three published replicas and replaces them with candidate
replicas in their existing directories, using ordinary `open` recovery. Writes,
linearizable reads, cached command replay, and local application-state agreement
are checked across the replacements. With both versions still present, a fourth
candidate joins as a learner, catches up, is promoted through joint consensus,
and is removed. After every final candidate restart, the retired identity
high-water must remain 4. The independent history checker checks all 24 ordinary
client operations plus every uncertain attempt, using an explicit 32-operation
limit and the unchanged 200,000-configuration limit. The default history limit
for other callers remains 24. Exhausting either bound fails qualification.
Every live replica, including the published binary, must observe the joint and
stable phases before the next membership step can proceed.

Leader turnover can discard local proposal tracking after append. An unknown
write outcome is therefore legitimate during a restart. This application retries
the identical command, session, sequence, and fingerprint, under its existing
deduplication contract. Every attempt receives its own invocation and terminal
event; uncertain attempts are never removed from the checked history. A shared
eight-unknown-outcome allowance and a deadline around each retry sequence bound
the work. Each underlying connection and leader wait also has its own deadline.
The client does not advance its sequence until the command settles.

Every run also sends one real request and closes that client connection without
reading its reply. This deliberately creates uncertainty without assuming whether
the first attempt applied. The receipt requires the lost-reply marker, ordered
retry markers, and exactly 24 plus the number of unknown attempts in the checked
history. All existing reads, snapshot, membership, and restart assertions remain.
These are explicit application retries; the library still reports uncertain
writes as uncertain and does not deduplicate arbitrary Raft commands.

Snapshot installation is mandatory in both directions: a candidate must install
a published sender's snapshot, then a published replica must install a candidate
sender's snapshot. The readiness observation must expose an installed snapshot
at or beyond the required compacted boundary. Before each transfer, all eligible
senders checkpoint and cold-restart while the receiver is absent. This explicit
recovery phase clears queued pre-compaction append messages that would otherwise
allow ordinary log catch-up. It temporarily removes quorum availability; the
scenario does not claim uninterrupted service during this phase.

Every requested stop requires the `BYE` reply, successful process exit, and the
durable `STOPPED` lifecycle event. No repair, reseeding, or durable-directory
replacement is allowed during the scenario. Final state is copied to the
evidence directory only after all processes have stopped.

## Running and reviewing evidence

```sh
scripts/rolling-upgrade-check
scripts/rolling-upgrade-check --offline
scripts/rolling-upgrade-check --candidate-archives /absolute/path/to/package
```

Python 3.11+, Rust/rustup, and the existing package boundary check's `jq` are
required. The defaults retain a receipt under `target/rolling-upgrade/run-*`
and checksum-verified baseline archives under `target/rolling-upgrade/archives`.
`--artifacts` names a fresh output directory; `--keep` retains temporary builds.
`--baseline-cache` and `--toolchain` select the download cache and build compiler.
Offline mode requires all baseline and third-party dependency archives to be
cached first.

The [baseline lock](../verification/rolling-upgrade-baseline.json) pins the
published archive SHA-256 digests. Each dependency graph must resolve every
Rafter library from its own exact archive set and only the four expected public
consumer workspace members. Checkout paths, other unpublished local crates,
registry substitutions for Rafter, and internal test hooks are refused. The
existing package manifest scanner also checks dormant forbidden declarations.

Receipts retain source/TLS-fixture hashes, runner hashes, archive hashes,
compiler identity, both dependency metadata files and lockfiles, binary hashes,
commands, complete stdout/stderr, six stages of three replica observations, and
the final durable state with a file inventory. The test inventory must contain
exactly the expected test. Missing stages, duplicate or unready observations,
an empty test selection, and a failed process cannot produce a passing receipt.
Changing fixture or runner inputs during qualification also invalidates the
receipt, so the two builds cannot silently use different revisions.

The deterministic and MSRV package lanes run this check with the same candidate
archives already qualified by the other consumers. Their compatibility receipts
are uploaded by CI. The source process inventory remains separate because it
does not supply published baseline binaries.

## Scope remaining before stability

This bounded localhost scenario complements the [codec matrix](wire-compatibility.md)
and [file-store recovery matrix](storage-upgrades.md). It does not establish
general downgrade support, mixed application schemas, journal/shared-WAL
migration, multi-host network behavior, crash-during-upgrade safety, or long-run
availability/performance bounds. Those require explicit support policies and
additional qualification before a stable-release promise.

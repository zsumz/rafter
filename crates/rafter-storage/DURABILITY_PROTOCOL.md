# Rafter storage durability protocol

This document describes how the file-backed stores publish state, what each
successful return guarantees, what residue an interrupted operation may leave,
and how reopen interprets that residue.

It complements `STORAGE_FORMAT_V1.md`: the format document defines bytes; this
document defines ordering and recovery.

## Scope and ownership

`rafter-storage` owns durable protocol state only:

- hard state;
- the retained Raft log suffix and compacted-prefix boundary;
- current snapshot selection;
- resumable inbound snapshot staging.

It does not persist application state or the application's applied index. It
does not decide when a recovered process may serve traffic. The runtime and
application layers must validate cross-store recovery and provide an applied
floor before committed commands are replayed.

## Filesystem assumptions

The file-backed implementation assumes:

1. a file sync makes previously written file data durable according to the host
   filesystem's contract;
2. a same-directory rename atomically replaces the destination name;
3. syncing the parent directory makes the rename, creation, or removal durable;
4. only one live writer owns a store path or replica directory at a time.

The standard `FileRaftNodeStores` bundle acquires an operating-system lock for
the replica directory before it opens or repairs any store. Direct single-store
constructors do not acquire that bundle lock, so callers using custom layouts
must enforce equivalent exclusive ownership. Opening multiple mutable handles
over the same paths is unsupported because handles cache logical state and
writers use predictable sibling temporary paths.

These guarantees are filesystem and platform dependent. Deployments should
validate them for their selected storage stack.

## General mutation rule

A mutation follows three conceptual phases:

1. **prepare** — encode and write a temporary or append-only representation;
2. **publish** — make the new logical state durable at its authoritative path;
3. **cleanup** — remove superseded or optional artifacts and reclaim space.

`Ok` means the method's documented durable state is visible to a fresh opener.
Encoding and validation errors occur before publication and leave logical state
unchanged.

An I/O error may occur after the filesystem has accepted some or all writes.
The safe caller rule is therefore:

> After any file-backed mutator returns an I/O error, discard that store handle
> and reopen the store before issuing another mutation.

`FileRaftHardStateStore`, `FileRaftLogSegment`, and
`FileRaftSnapshotStore` enforce this rule in the handle itself. Their first
mutating I/O error marks the handle as requiring reopen; every later mutator
returns `StoreRequiresReopen` without touching storage. Snapshot publication
additionally reports when its current-manifest commit point was crossed before
a later cleanup failure.

Reopen is the recovery oracle: it verifies checksums, selects manifests,
filters compacted entries, and either reconstructs one valid state or returns a
typed error. Code must not infer that `Err` means "no bytes changed."

## Operational error sources

Format and validation failures remain deterministic typed errors. Operational
filesystem failures retain the original [`std::io::Error`] instead of reducing
it to a string. Each storage I/O variant exposes that error as its immediate
[`std::error::Error::source`], so callers can inspect [`std::io::ErrorKind`],
raw operating-system codes, and any nested cause.

The public `StorageIoError` wrapper keeps those errors cloneable and equatable
for runtime poison state and deterministic tests. Clones share the same original
I/O error allocation; equality compares its portable kind, raw OS code, and
rendered diagnostic. Display text for the enclosing storage errors is unchanged.

## Exclusive replica-directory ownership

`FileRaftNodeStores` opens `<replica>/.rafter-storage.lock` and acquires a
non-blocking exclusive operating-system file lock before opening hard state,
replaying or repairing the log, or inspecting snapshots. Contention returns
`OpenFileRaftNodeStoresError::AlreadyOpen`; a contended repair attempt therefore
cannot mutate the retained log.

The lock file is persistent coordination metadata, not Raft state. Its mere
presence does not mean the directory is in use, and it must not be deleted while
a store is open. The live lock is held through a shared guard attached to all
three concrete stores, so `FileRaftNodeStores::into_parts` preserves ownership
until every returned store is dropped.

On Unix-family platforms the lock is advisory: every cooperating writer must
open the directory through `FileRaftNodeStores` or enforce an equivalent lock.
The backing filesystem must honor process file locks; deployments where it does
not must supply equivalent exclusive ownership externally. On Windows the
underlying lock is mandatory. Direct
`FileRaftHardStateStore::open`, `FileRaftLogSegment::open`, and
`FileRaftSnapshotStore::open` remain available for custom layouts, but they do
not acquire the standard bundle lock and therefore require caller-enforced
single-writer ownership.

## Hard-state publication

Stable path: `hard-state`

Publication sequence:

1. encode one complete `RFHS` envelope;
2. create or truncate `hard-state.tmp`;
3. write the envelope and sync the temp file;
4. rename the temp file over `hard-state`;
5. sync the parent directory;
6. update the handle's cached current value.

A crash before the rename leaves the previous `hard-state` authoritative and
may leave an ignored temp file. A crash after the rename exposes either the old
or new complete file according to the filesystem's rename and directory-sync
semantics; reopen never reads the temp path.

Any I/O failure during this sequence marks that concrete store handle as
requiring reopen. `FileRaftHardStateStore::requires_reopen` exposes the state
for diagnostics, and later writes fail with `StoreRequiresReopen` before they
perform filesystem work.

## Opt-in hard-state journal

`JournalRaftNodeStores` acquires the same directory lock before opening the
journal at `hard-state`. Each split store retains that ownership. The default
`FileRaftNodeStores` and its replacement protocol remain unchanged.

Open creates or validates the RFHJ header, replays every complete RFHS record,
and truncates an incomplete final record. It syncs the recovered file and its
parent directory before exposing state, including on retries after creation or
repair failed. A partial header or complete corrupt record fails closed.

Normal publication appends one RFHS envelope to the already-open file, calls
`sync_data` once, then updates the cached state. It performs no rename or
parent-directory sync. Any append or sync failure poisons the handle; later
writes fail before I/O. A complete unacknowledged record may survive such an
error and become current after reopen validates and resyncs it.

After 4,096 complete records, the next write checkpoints the incoming state.
It writes the same RFHJ header and one RFHS record to the reserved sibling
`hard-state.checkpoint.tmp`, syncs it, renames it over `hard-state`, and syncs
the directory before changing the live file handle or acknowledged cache.
Before rename, the old journal remains authoritative; after rename, reopen
validates and resyncs the replacement. Unpublished temporary bytes are ignored.
Any checkpoint failure poisons the handle, including failure after rename.

The journal remains at most 208,905 bytes during successful steady-state use.
An older oversized journal is fully validated on its first open, then rotated
on its next write. This does not reclaim the separate Raft log or the atomic
WAL. The format is unchanged; see `STORAGE_FORMAT_V1.md`.

## Log append

Stable path: `log`

Each append batch is fully encoded into length-framed `RFLE` records before the
file is mutated. The batch is appended in order and the log file is synced
before the in-memory contiguous suffix is extended.

A failed or interrupted append may leave a partial final frame. Strict open
fails loudly at that frame. Explicit uncommitted-tail repair may truncate at
that offset only when the durable hard-state commit index proves the valid
contiguous prefix already covers all committed state.

Repair never skips a bad frame and continues scanning: the first bad frame or
index gap ends the trusted prefix. Any append I/O failure marks that concrete
log handle as requiring reopen, and later append, truncate, and compact calls
fail before performing filesystem work.

## Log suffix truncation

Suffix truncation is a replacement operation:

1. encode retained entries before the requested index;
2. write and sync a sibling rewrite temp file;
3. rename it over `log`;
4. sync the parent directory;
5. reopen the append handle;
6. update the in-memory suffix.

An abandoned rewrite temp is ignored. The stable `log` path is authoritative.
Truncation may not erase through the compacted-prefix boundary. Any rewrite I/O
failure marks the handle as requiring reopen because the stable replacement or
its directory entry may already have changed.

## Log prefix compaction

Compaction prepares the replacement log before publishing the logical boundary:

1. encode the retained suffix without mutating storage;
2. write and sync a temporary `RFLC` marker;
3. rename it over `log.compact` and sync the parent directory;
4. update the in-memory compacted boundary and suffix;
5. rewrite `log` with the prepared bytes to reclaim obsolete frames.

This order makes the marker the logical commit point while keeping encoding
errors before publication. If a crash or rewrite failure occurs after marker
publication, reopen filters old frames at or below the marker and reconstructs
the correct suffix. The leftover frames waste space but cannot re-enter the
logical log. A post-marker rewrite failure returns `CompactedButReclamationFailed`,
including the committed boundary, and marks the handle as requiring reopen. An
I/O failure before the marker is confirmed durable returns the ordinary
compaction I/O error; reopen decides which state won.

Compaction may advance beyond the local tail when a follower installs a leader
snapshot that replaces missing local history.

## Snapshot publication

Stable directory: `snapshots/`

A complete snapshot is immutable once published. Publication sequence:

1. encode the snapshot metadata header;
2. stream header and payload to a snapshot temp file while computing payload
   and envelope CRCs;
3. append both checksums and sync the temp file;
4. rename the temp file to its unique `snapshot-*.rfsn` name;
5. sync the snapshot directory;
6. encode, write, and sync a temporary `RFSM` current manifest;
7. rename it over `current.snapshot` and sync the directory;
8. update the handle's cached current descriptor;
9. clear any pending inbound transfer.

The current manifest is the logical commit point. Before that rename, the old
manifest continues to select the old snapshot and an unmanifested complete file
is ignored. After that rename, the new immutable snapshot is current even if
later staging cleanup fails. Such a failure returns
`SnapshotCommittedButReopenRequired`, names the selected snapshot file, and
marks the handle as requiring reopen. I/O failures before the manifest is
confirmed durable return `Io`; reopen decides which manifest state won.

Previous complete snapshots are retained. Their presence does not affect
recovery because only `current.snapshot` selects current state.

## Snapshot opening

Open performs a streaming verification pass over the manifest-selected
snapshot:

1. decode and verify the current manifest;
2. reject a missing selected file;
3. parse a bounded metadata prefix;
4. verify the exact file length;
5. stream the payload through payload and envelope CRCs;
6. retain only the descriptor, selected file name, and payload offset.

Payload bytes are served later through positioned reads and are not kept
resident by the file-backed store. A chunk request must match the selected
snapshot's complete descriptor — metadata, transfer id, payload length, and
payload checksum — before either implementation serves bytes.

## Pending snapshot-transfer staging

Stable paths:

```text
pending.snapshot-transfer.body
pending.snapshot-transfer
```

Before storage is mutated, staging validates the complete public chunk shape:
its checked end offset must not exceed the advertised payload, empty non-final
chunks are rejected, `done` must mean exact completion, and the transfer id must
be derived from the supplied metadata, payload length, and payload checksum. A
continuation must additionally match the staged descriptor and begin exactly at
the staged length.

The next `RFPT` manifest is then encoded before the body is mutated, so metadata
encoding failures leave staging untouched. For an offset-zero chunk, the body
is replaced through temp-file publication. For a continuation, bytes are
appended to the current body and the body file is synced. The prepared manifest
is finally written through temp-file replacement and parent-directory sync.

The manifest is the authoritative staged length. A crash after body update but
before manifest publication may leave a longer body; continuation truncates the
body back to the manifest length before appending new bytes. Recovery verifies
the manifest-described prefix and ignores any longer suffix.

A body shorter than the manifest length or with a mismatched body checksum is
inconsistent optional progress, typically from an interrupted two-file update.
Recovery durably discards both staging files and resumes with no pending
transfer. Corruption of the manifest itself remains a hard open error.

## Pending transfer promotion

Promotion requires the staged transfer to be complete and to match the requested
snapshot's full descriptor: transfer id, metadata, payload length, and payload
checksum. The full comparison remains mandatory even though transfer ids are
deterministic, because they are routing identities rather than cryptographic
digests. Promotion then opens the staged body and streams exactly the staged
prefix through the normal immutable snapshot publication sequence. The
assembled payload checksum must match the snapshot descriptor before the
snapshot file can become current.

Once the new current manifest is durable, staging is cleanup state. Failure to
remove staging does not make the selected snapshot uncommitted; reopen and the
runtime recovery path distinguish a current snapshot from stale or resumable
staging.

## Clearing pending staging

Clearing removes the pending manifest and body, then syncs the directory when
at least one file was removed. Missing files are accepted, making cleanup
idempotent.

Because a multi-file removal can fail between files, any clear I/O failure
marks the file-backed snapshot handle as requiring reopen. Later snapshot
mutations fail with `StoreRequiresReopen` before touching storage.

## Standard bundle open and repair

`FileRaftNodeStores::open` is strict. It opens hard state first, then log and
snapshot stores, and rejects corrupt persisted state.

`FileRaftNodeStores::open_repairing_uncommitted_log_tail` uses the hard-state
commit index as the repair floor. A corrupt, partial, or noncontiguous log tail
may be truncated only when the valid contiguous prefix already covers that
floor. Corruption that may contain committed state remains a hard error.

Creation syncs for a fresh log file and snapshot directory are batched and
flushed before the bundle is returned.

## Cross-store recovery order

Incoming snapshot installation uses the same runtime operation during a live
step and when reopening a complete staged transfer:

1. stage the complete validated transfer durably;
2. compare the retained entry at the snapshot boundary with the snapshot term;
   if it differs, truncate that boundary entry and the entire suffix above it;
3. promote the complete staged transfer to the current snapshot;
4. compact the log prefix through the snapshot boundary;
5. finish the step's log writes before publishing final hard-state commit metadata.

Truncation precedes promotion, so a crash during truncation leaves complete
staging that recovery can install again. A matching boundary preserves its
suffix. A conflicting boundary and suffix are both gone before promotion can
clear staging or compaction can erase the boundary evidence. A committed
boundary conflict is rejected instead of truncated.

A crash after promotion leaves either a matching retained full log, validated
at bootstrap, or a log ending below the snapshot boundary, whose compaction
recovery completes. Retained boundary-term validation remains mandatory.

The inverse state—a log compacted beyond the current snapshot boundary—means
the durable node has discarded history not covered by a durable snapshot and
must fail loudly.

These cross-store protocol decisions remain owned by `rafter-runtime`; the
storage crate provides the verified artifacts and durable primitives needed to
make them safely.

## Configuration recovery before commit publication

The log and snapshot may advance before the hard state's commit index and
configuration identity. Recovery keeps commitment at the greater of the durable
commit index and snapshot boundary. Configuration entries in a retained suffix
do not establish additional commitment merely by existing.

A valid catch-up append can leave several configuration entries above that
floor. Bootstrap retains this accepted history and uses its latest configuration
for effective membership, elections, and replication quorums. Committed membership
and application replay still stop at the recovered commit floor. New local
configuration proposals wait until the configuration suffix is committed.
Followers can accept prior-term configuration history and ordinary entries,
including a new leader's no-op, without treating those entries as committed.
This lets a recovered leader catch up peers missing part of its history before
relearning commitment. A configuration introduced in the current leader term
cannot overlap another above the frame's confirmed commit floor. These rules
allow progress without publishing commitment ahead of durable data.

Bootstrap reconciles configuration identity from hard state, the installed
snapshot, and retained committed entries. It validates overlapping identities
and selects the newest justified index within the recovered committed prefix.
Equal-index ID disagreements, missing retained identities, and snapshot identities
beyond their boundary are rejected. The normalized identity supplies exported
configuration state, later configuration IDs, and the next hard-state write.

## Executable crash matrix

Unit scenarios arm one thread-local, one-shot failpoint at named filesystem
boundaries. The hooks compile only for crate tests; production builds contain
no failpoint state or synchronization.

Each scenario follows the same proof shape:

1. establish acknowledged durable state;
2. stop immediately after one filesystem step;
3. assert that the live handle requires reopen;
4. discard the handle;
5. reopen from stable artifacts;
6. assert the documented logical state and any permitted cleanup residue.

The matrix covers:

- hard-state temp sync, final-path rename, and parent-directory sync;
- log append sync, replacement preparation and publication, and every
  compaction-marker publication phase;
- snapshot envelope preparation, immutable-file publication, and current
  manifest publication;
- pending-transfer manifest removal, body removal, and directory sync;
- the post-marker log-reclamation and post-manifest snapshot-cleanup outcomes
  where the logical operation is already committed.

A new durable publication step should add a named failpoint scenario before it
is considered covered by the recovery contract.

## Review checklist for a new mutation

A storage change is incomplete until its review answers all of these:

- What artifact is authoritative?
- What is the logical commit point?
- Which file and directory syncs precede `Ok`?
- What can a crash leave before and after the commit point?
- Does reopen ignore, resume, repair, or reject each residue shape?
- Can cleanup failure be distinguished from failure to commit?
- Is retry idempotent?
- Must the live handle be discarded after an I/O error?
- Is committed state ever inferred from an optional artifact?
- Is there a directed crash-window test for every publication step?

## Atomic log and hard-state batches

`WalRaftNodeStores` owns one active WAL file and one coordinator shared by its
non-cloneable log and hard-state handles. Its domain identity changes on each open. A batch
validates and encodes all mutations, writes one contiguous RFWB record, calls
`sync_data`, then advances both acknowledged views and returns a domain-scoped
operation receipt. Successful legacy trait methods retain synchronous durability.
A backend returning `None` from `persist_batch` has attempted no mutation.

Any write or sync error poisons the coordinator and leaves acknowledged views at
the previous receipt. The uncertain complete record may nevertheless survive;
reopen verifies and synchronizes it before exposing recovery state. A partial
final record is truncated and synchronized. Open also syncs the parent directory,
including retries after a previous creation failed. Every split handle keeps the
exclusive directory lock alive. Snapshot data without its WAL is rejected.

After a compaction record is synchronized, the shared WAL checkpoints the exact
acknowledged hard state, compacted boundary, covering snapshot identity, retained
suffix (including uncommitted entries), and operation number. It synchronizes
the immutable checkpoint and a fresh generation segment, synchronizes their
directory entries, then replaces and directory-synchronizes the manifest that
selects them. That manifest is the reclamation authority. Only after it is
durable may the prior segment/checkpoint or legacy `hard-state` file be deleted.
Deletion is cleanup and is directory-synchronized.

A snapshot-binding or preparation failure occurs after logical compaction has
committed but before physical history is reclaimed, so callers receive
`CompactedButReclamationFailed` and must reopen. Failure after manifest rename is
also ambiguous to the live writer and requires reopen; either the old WAL (whose
compaction record is durable) or the new generation is recoverable after a real
crash. Failure after the manifest directory fence leaves the new generation
authoritative. Recovery validates the manifest-selected checkpoint and segment
before removing bounded obsolete residue and never falls back from selected
corruption. Publication numbers continue across generations, while the live
persistence domain remains new on every open.

The runtime currently combines only append-only application entries and ordinary
commit-index changes in an unchanged term, vote, and configuration. It checks the
receipt against the domain and both resulting store views before releasing any
output. Any failure or inconsistent receipt poisons the runtime. Term/vote
changes, conflicting suffixes, configuration transitions, and snapshot effects
retain the established explicit fences. Snapshot publication still precedes log
compaction. A client success additionally requires the application's durable
completion; a WAL receipt alone never acknowledges application durability.

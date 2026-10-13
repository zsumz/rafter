# Recovery and maintenance contract

This runbook describes the fenced-lock reference application's existing
`lock-production-node` process. Its storage, snapshots, session cache, fencing
marks, and recovery choices are application policy. A Rafter embedding must
define equivalent procedures for its own state machine and publication format.
These are experimental, tested recovery paths, not a stable support promise.

## Decide from the retained state

Start with the default `--recover open`. Recovery modes never escalate
automatically. Repeating the same opening of unchanged ambiguous bytes does not
resolve the ambiguity.

| Observation | Decision and prerequisite | Required outcome / evidence |
| --- | --- | --- |
| Healthy application state and intact Raft/identity/transport records | Ordinary restart with `--recover open`. | Wait for `READY` and a ready `OBSERVE` result after recovery, not merely an open socket. `readiness_and_checkpoint_recovery_fail_closed` exercises restart and ownership contention. |
| `UnreadableSlot` with a complete unsealed image | Keep the replica stopped and preserve its directory. An interrupted publication and a damaged formerly sealed image can look identical. Explicit `--recover repair` is a decision to discard one slot, not a retry. | `gen5_the_ordinary_crash_on_a_release_is_repairable` and the TLS unsealed-renewal test cover permitted repair. Strict refusals preserve application bytes and do not publish readiness. |
| Repair would discard a higher acknowledged fencing mark or a resource absent from the survivor | Refuse repair. There is no force-repair override. Assess a separate reseed only after establishing a recoverable authoritative history. | `DiscardWouldRegressMark`; the TLS refusal case preserves both slots and Raft/control-plane/transport records while the surviving quorum advances and rejects stale tokens. |
| Repair cannot decode the discarded image | Treat `marks_cross_checked=false` as an absence of local proof, not as a successful comparison. Verify the retained history before choosing destructive recovery. | `a_repair_that_cannot_read_the_discarded_image_says_so` checks the disclosed limitation. An allowed store opening alone does not establish complete application recovery. |
| An uncompacted log retains the complete committed application history | Explicit reseed can rebuild the projection from index zero. Preserve the Raft stores and other identity records. | `gen6_a_reseed_over_an_uncompacted_log_still_recovers_its_marks` verifies the positive control and recovered fencing floor. |
| A valid local application snapshot covers the compacted prefix, with a committed application suffix that triggers restoration | The qualified process reseed path installs that snapshot before applying the suffix, then catches up over TLS. | `explicit_reseed_restores_a_compacted_snapshot_before_replaying_its_suffix` verifies snapshot and suffix marks, session-cache replay, ordinary cold restart, and rejection of stale fencing tokens. |
| Application floor is below the compacted boundary, with no usable restoration path | Refuse service. A fully compacted replica with no application suffix and no recovery snapshot directive does not install its snapshot implicitly. Repeating reseed does not reconstruct missing history. | `AppliedIndexBelowSnapshotBoundary` on use, or `SnapshotRestoreRequired` before applying an unsupported suffix. Both composition regressions are included below. |
| Identity, peer-control-plane, or transport replay record is missing/corrupt | Stop and resolve that specific record through the embedding's identity/retirement recovery procedure. Application repair/reseed does not rebuild it. | The TLS readiness test rejects these losses. A removed replica identity stays retired; allocating a replacement is a separate membership operation. |

The two compacted-floor regressions exercise the application/runtime composition
with in-memory Raft stores and a real application store. They do not claim a TLS
startup test for every missing/invalid snapshot shape. The positive snapshot and
suffix path uses real processes, file stores, and the existing TLS transport.

## Before a destructive choice

1. Stop the affected replica, establish exclusive directory ownership, and keep
   the healthy quorum available. Record the refusal, binary/source version,
   node/group identity, configuration, and last observed applied/committed and
   snapshot boundaries.
2. Preserve the complete stopped `node-<id>` directory as evidence before trying
   repair or reseed. Keep `app/`, `raft/` and its snapshots, `control-plane`,
   `transport-replay`, and identity records together. Do not infer a consistent
   recovery image from copies taken while the replica is publishing.
3. Establish the source that can reconstruct committed application state,
   session results, and fencing marks. A log after compaction is only a suffix.
   A snapshot must belong to this group and application format, cover the missing
   prefix, and be accepted by the application recovery path before later entries
   apply. File existence or a checksum alone does not prove those conditions.
4. Choose `--recover repair` or `--recover reseed` explicitly for this one
   recovery attempt, using the replica's existing startup configuration. Resume
   ordinary `open` mode afterward. Do not configure a supervisor to restart
   indefinitely in `reseed` mode: even a healthy application store is deleted
   every time that mode is selected.

`repair` may roll back application/session progress and reports the discarded
and adopted generations. When discarded marks are decodable it requires the
surviving image to dominate them; undecodable marks cannot be cross-checked.
`reseed` deletes **both application slots**, including all locally retained
fencing marks and cached results. It preserves the separate Raft records, but
does not prove that they can rebuild the deleted state. A later recovery failure
does not undo the deletion. Preserve evidence and assess prerequisites first.

This runbook provides no automatic way to decide that a damaged replica is the
cluster's only remaining authoritative copy. Stop instead of discarding that
uncertainty. A validated restore or explicit replica replacement must follow the
embedding's own identity, authorization, and membership rules.

## Readiness and completion

The production fixture emits JSON Lines lifecycle records. `LISTENING` means a
client socket exists; requests remain `NOTREADY` during recovery. `CREATED`,
`RECOVERED`, `REPAIRED`, and `RESEEDED` describe a store action that occurs before
the complete Raft/application recovery path finishes. **None is a readiness
signal.** `CREATED` on an established replica also warrants checking why its
application slots are absent.

Require `READY` and `OBSERVE` with `ready=true` after identity, peer policy,
transport replay state, application recovery, and service readiness succeed.
Compare recovered application state and fencing/session facts with the retained
authoritative history. Application indexes can skip Raft no-ops and membership
entries; do not require numerical equality with every committed Raft index.
The TLS repair/reseed tests inspect the stopped replica's durable image and
session cache, then verify subsequent quorum progress and stale-token rejection.

`NEEDS_DECISION` means startup refused and needs an explicit choice. `FATAL`,
`CONTROL_PLANE_UNPERSISTED`, or `CONTROL_PLANE_CONTRADICTED` does not become safe
because an earlier store event succeeded. Ordinary shutdown requires `BYE`,
`STOPPED`, and successful process exit; a killed process has not completed that
shutdown contract.

## Snapshot maintenance

The reference client's `CHECKPOINT` command requires readiness and no outstanding
client waiters. `OK CHECKPOINT <index>` records local snapshot/compaction success;
`ERR CHECKPOINT <detail>` is a refusal or failure to investigate. Publication and
compaction are synchronous and can affect latency. The command is local: it does
not prove that every follower possesses a usable snapshot or has caught up.

Keep application snapshot schema/identity validation and applied-floor checks
enabled. Preserve a reconstruction path before pruning logs or snapshots. Reopen
and follower catch-up checks should accompany retention changes. Never advance an
application floor to silence a compacted-boundary error: that would claim effects
that the application has not restored.

## Execute the contract

From a checkout with Rust 1.88 and its dependencies available:

```sh
scripts/recovery-contract-check
```

This runs seven named existing publication/fencing/compaction regressions and
the eight-test authenticated process suite, serially. Every selection is checked
by `cargo-test-exact` against its expected names before execution. Public Rafter
dependencies are patched to the checkout through the existing reference-source
resolver; missing or failed patch discovery stops the command. Test failures and
empty or changed selections cannot produce the final passing message.

```sh
scripts/recovery-contract-check --list-only
```

List-only builds/lists and verifies the selections but executes no test bodies.
The script records the Git head, working-tree status, and compiler; retain its
complete output when reporting results. It is a focused source-mode check, not
an exact-package archive, full-CI, or TLA qualification. For the broader lanes,
use `scripts/reference-source-check` and `scripts/reference-process-check`.

The tests own temporary replica directories and local processes. Deliberate
seal-byte faults model ambiguous application images; they are not new evidence
of where a physical crash lands. Process-loss tests retain the operating system
and page cache and establish no physical-power-loss or arbitrary-database
publication guarantee. See [rolling qualification](rolling-upgrades.md) and
[platform qualification](platform-qualification.md).

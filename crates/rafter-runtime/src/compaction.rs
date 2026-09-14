//! Local snapshot installation and the log compaction that follows it.
//!
//! The kernel owns every boundary rule, so each entry point exclusively
//! prepares its transition and writes nothing until validation agrees. The
//! borrow prevents intervening kernel mutation while the durable snapshot is
//! published before the log prefix it covers is dropped; only then is the
//! prepared kernel transition committed. Inbound transfer snapshots are not
//! this module's business — those persist on the step path.

use rafter::{
    RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkSource, SnapshotCommittedConfiguration,
};
use rafter_storage::{
    telemetry::{Stage, Timer},
    FileRaftSnapshotStore, PersistedRaftSnapshot, RaftHardStateStore, RaftLogSegment,
    RaftSnapshotStore, SnapshotPruneError, SnapshotPruneReport, SnapshotRetention,
};

use crate::{DurableRaftNode, RaftRuntimeError};

mod chunk_source;
mod install_error;

use chunk_source::OriginalSnapshotChunkSource;
use install_error::local_snapshot_install_error;

impl<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore + SnapshotChunkSource>
    DurableRaftNode<H, L, S>
{
    /// Compacts the local durable Raft log through a durable snapshot boundary.
    ///
    /// This metadata-only convenience API records an empty application
    /// snapshot payload. Call [`Self::compact_log_with_snapshot`] when the
    /// application has a real snapshot payload to transfer to lagging
    /// followers.
    ///
    /// Because it records an empty payload, calling it at the *already
    /// installed* boundary is the supported repair for a durable log still
    /// behind that boundary: the kernel re-records the descriptor and moves
    /// nothing, and the compaction runs.
    ///
    /// # Errors
    ///
    /// As [`Self::compact_log_with_snapshot`] — every boundary rule is the
    /// kernel's — plus [`RaftRuntimeError::LogCompact`] when storage rejects or
    /// cannot persist the local compaction.
    pub fn compact_log_through_snapshot(
        &mut self,
        snapshot: &RaftSnapshotMetadata,
    ) -> Result<(), RaftRuntimeError> {
        self.compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata: snapshot.clone(),
            application_payload: Vec::new(),
        })
    }

    /// Installs a local application snapshot into the Raft node, persists the
    /// snapshot bytes, and compacts the durable Raft log through the snapshot
    /// boundary.
    ///
    /// This is the leader-side companion to the snapshot transfer protocol:
    /// after this succeeds, the kernel holds only the snapshot descriptor and
    /// replication streams the persisted payload from the snapshot store to
    /// followers whose `next_index` falls behind the compacted prefix.
    ///
    /// A snapshot's **author** and its **sender** are judged separately, and
    /// only the author rule constrains what this call may write. The author —
    /// `writer_id` — must be a replica, voter or learner, of the membership
    /// committed at the boundary; this runtime stamps that membership into a
    /// descriptor that carries none, so the ordinary call, signing with this
    /// node's own id, satisfies it whenever this node is still in the
    /// configuration. A node already removed from the configuration cannot
    /// compact: the kernel refuses, which is the outcome to want, because
    /// installing such a descriptor would leave the process unable to hydrate
    /// it at the next restart.
    ///
    /// Senders are no longer required to appear in the boundary they relay, so
    /// a leader added after an older snapshot boundary may serve that older
    /// descriptor from a reusable snapshot store. Receivers still refuse a
    /// sender they cannot place in any membership they can see — their own
    /// current one, or the descriptor's — so a replica joining the cluster
    /// should be configured with peers that the descriptor's boundary
    /// membership or its own bootstrap set will recognize.
    ///
    /// # Errors
    ///
    /// Every boundary refusal is the kernel's, rendered through
    /// [`RaftRuntimeError`]: [`RaftRuntimeError::SnapshotBelowInstalledBoundary`]
    /// when the boundary lies below the installed snapshot boundary — equal is
    /// not refused, it is the idempotent re-record —
    /// [`RaftRuntimeError::SnapshotAheadOfCommit`] when it is ahead of the
    /// committed prefix, [`RaftRuntimeError::SnapshotAheadOfApplied`] when it is
    /// committed but this node has not applied through it,
    /// [`RaftRuntimeError::SnapshotBoundaryTermMismatch`] when the local log
    /// cannot prove the supplied boundary term, and
    /// [`RaftRuntimeError::SnapshotMembershipMismatch`] or
    /// [`RaftRuntimeError::SnapshotCommittedConfigurationMismatch`] when
    /// caller-provided committed configuration metadata disagrees with the
    /// local boundary, and [`RaftRuntimeError::SnapshotRefusedByKernel`] when
    /// the descriptor's `writer_id` is not a replica of the boundary
    /// membership. Nothing is written on any of those. A fatal persistence
    /// error is returned, and poisons, when the snapshot or the compaction
    /// cannot be written.
    pub fn compact_log_with_snapshot(
        &mut self,
        mut snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftRuntimeError> {
        if let Some(cause) = &self.fatal_error {
            return Err(RaftRuntimeError::Poisoned {
                cause: cause.clone(),
            });
        }

        self.fill_local_snapshot_membership(&mut snapshot.metadata);
        let descriptor = RaftSnapshot::new(
            snapshot.metadata.clone(),
            snapshot.application_payload.len() as u64,
            rafter_storage::crc32(&snapshot.application_payload),
        );
        // The prepared transition owns an exclusive borrow of the kernel, so
        // validation happens before storage and no kernel input can invalidate
        // it while persistence runs. Commit is infallible and happens only
        // after both the snapshot and log compaction are durable.
        let prepared = {
            let _prepare = Timer::start(Stage::SnapshotKernelPrepare);
            self.node
                .prepare_local_snapshot_install(descriptor)
                .map_err(local_snapshot_install_error)?
        };
        let written = write_snapshot_and_compact_log(
            &mut self.snapshot_store,
            &mut self.log_segment,
            snapshot,
        );
        if let Err(error) = written {
            drop(prepared);
            return Err(self.poison(error));
        }
        {
            let _commit = Timer::start(Stage::SnapshotKernelCommit);
            let _ = prepared.commit();
        }
        Ok(())
    }

    /// As [`Self::compact_log_with_snapshot`], but the payload is pulled
    /// from `source` in bounded chunks and never materialized whole — the
    /// compaction path for state machines whose snapshots exceed memory.
    ///
    /// # Errors
    ///
    /// As [`Self::compact_log_with_snapshot`], plus a snapshot write error
    /// when the source cannot serve the snapshot it describes.
    pub fn compact_log_with_streamed_snapshot(
        &mut self,
        mut snapshot: RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftRuntimeError> {
        if let Some(cause) = &self.fatal_error {
            return Err(RaftRuntimeError::Poisoned {
                cause: cause.clone(),
            });
        }
        let source_snapshot = snapshot.clone();
        self.fill_local_snapshot_membership(&mut snapshot.metadata);
        let source = OriginalSnapshotChunkSource {
            source,
            snapshot: &source_snapshot,
        };

        let prepared = {
            let _prepare = Timer::start(Stage::SnapshotKernelPrepare);
            self.node
                .prepare_local_snapshot_install(snapshot.clone())
                .map_err(local_snapshot_install_error)?
        };

        let boundary_index = snapshot.metadata.last_included_index;
        let written = self
            .snapshot_store
            .write_snapshot_from_source(&snapshot, &source)
            .map_err(RaftRuntimeError::SnapshotWrite)
            .and_then(|()| {
                self.log_segment
                    .compact_prefix_through(boundary_index)
                    .map_err(RaftRuntimeError::LogCompact)
            });
        if let Err(error) = written {
            drop(prepared);
            return Err(self.poison(error));
        }
        {
            let _commit = Timer::start(Stage::SnapshotKernelCommit);
            let _ = prepared.commit();
        }
        Ok(())
    }

    /// Records the boundary's committed configuration in a descriptor that
    /// carries none, so the compacted-away membership survives in the bytes
    /// this runtime is about to persist.
    ///
    /// This only fills a gap. A descriptor that *does* carry the field is left
    /// exactly as the caller wrote it, and the kernel's install refuses it if it
    /// disagrees with the local boundary — this crate deliberately keeps no
    /// second copy of that comparison. Filling is the runtime's to do because
    /// it owns the snapshot store; the kernel will not rewrite a descriptor a
    /// caller may already have persisted elsewhere.
    fn fill_local_snapshot_membership(&self, metadata: &mut RaftSnapshotMetadata) {
        if metadata.committed_configuration.is_some() {
            return;
        }
        let snapshot_index = metadata.last_included_index;
        metadata.committed_configuration = Some(SnapshotCommittedConfiguration::new(
            self.node.committed_configuration_state_at(snapshot_index),
            self.node.membership_at_index(snapshot_index),
        ));
    }
}

fn write_snapshot_and_compact_log<L: RaftLogSegment, S: RaftSnapshotStore>(
    snapshot_store: &mut S,
    log_segment: &mut L,
    snapshot: PersistedRaftSnapshot,
) -> Result<(), RaftRuntimeError> {
    let boundary_index = snapshot.metadata.last_included_index;
    snapshot_store
        .write_snapshot(snapshot)
        .map_err(RaftRuntimeError::SnapshotWrite)?;
    log_segment
        .compact_prefix_through(boundary_index)
        .map_err(RaftRuntimeError::LogCompact)
}

impl<H, L> DurableRaftNode<H, L, FileRaftSnapshotStore> {
    /// Prunes noncurrent file-backed snapshot envelopes according to `retention`.
    ///
    /// This is storage maintenance, not a Raft state transition. The selected
    /// snapshot and its manifest are never removed, the installed kernel
    /// descriptor is unchanged, and a maintenance failure does not poison the
    /// runtime. The operation is safe to retry idempotently, including after a
    /// restart.
    ///
    /// Call this after a successful [`Self::compact_log_with_snapshot`] or
    /// [`Self::compact_log_with_streamed_snapshot`] to bound retained snapshot
    /// history. `CurrentOnly` is the smallest supported retention envelope.
    /// Unknown files and stable inbound-transfer staging are never removed.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotPruneError`] when the snapshot store requires reopen,
    /// inventory cannot establish the selected snapshot, or durable deletion
    /// cannot complete. Any reported deletion prefix may already be absent;
    /// callers may safely retry the same policy.
    pub fn prune_snapshot_files(
        &mut self,
        retention: SnapshotRetention,
    ) -> Result<SnapshotPruneReport, SnapshotPruneError> {
        self.snapshot_store.prune_snapshots(retention)
    }

    /// Removes recognized temporary snapshot files left by an interrupted writer.
    ///
    /// This is storage maintenance, not a Raft state transition. Selected and
    /// noncurrent snapshots, stable inbound-transfer staging, and unknown files
    /// are never removed. The operation is intended for startup after recovery,
    /// or another point where the embedding exclusively owns the node and no
    /// snapshot-store write is in progress.
    ///
    /// A maintenance failure does not poison the runtime. The operation is safe
    /// to retry idempotently, including after another restart.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotPruneError`] when the snapshot store requires reopen,
    /// inventory cannot establish the selected snapshot, or durable deletion
    /// cannot complete. Any reported deletion prefix may already be absent.
    pub fn cleanup_abandoned_snapshot_temporary_files(
        &mut self,
    ) -> Result<SnapshotPruneReport, SnapshotPruneError> {
        self.snapshot_store
            .cleanup_abandoned_snapshot_temporary_files()
    }
}

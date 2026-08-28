//! Local snapshot installation and the log compaction that follows it.
//!
//! The kernel owns every boundary rule, so each entry point asks a staged
//! clone first and writes nothing until it agrees; the durable snapshot is
//! published before the log prefix it covers is dropped. Inbound transfer
//! snapshots are not this module's business — those persist on the step path.

use rafter::{
    RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkSource, SnapshotCommittedConfiguration,
};
use rafter_storage::{
    PersistedRaftSnapshot, RaftHardStateStore, RaftLogSegment, RaftSnapshotStore,
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
        let descriptor =
            RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload);
        // Ask the kernel first, on a clone: it owns every boundary rule, and a
        // refusal must leave the durable medium untouched. Persisting only
        // after it agrees, and publishing the clone only after the persist,
        // keeps both boundaries — no write without the kernel's consent, and no
        // published kernel state ahead of the durable medium.
        let mut staged_node = self.node.clone();
        staged_node
            .install_local_snapshot(descriptor)
            .map_err(local_snapshot_install_error)?;
        if let Err(error) = self.write_snapshot_and_compact_log(snapshot) {
            return Err(self.poison(error));
        }
        self.node = staged_node;
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

        let mut staged_node = self.node.clone();
        staged_node
            .install_local_snapshot(snapshot.clone())
            .map_err(local_snapshot_install_error)?;

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
            return Err(self.poison(error));
        }
        self.node = staged_node;
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

    fn write_snapshot_and_compact_log(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftRuntimeError> {
        let boundary_index = snapshot.metadata.last_included_index;
        self.snapshot_store
            .write_snapshot(snapshot)
            .map_err(RaftRuntimeError::SnapshotWrite)?;
        self.log_segment
            .compact_prefix_through(boundary_index)
            .map_err(RaftRuntimeError::LogCompact)
    }
}

//! The single path from durable storage to a live kernel.
//!
//! Opening repairs the two crash windows a reopen can still see — a complete
//! staging that was never promoted, and a promotion whose compaction never
//! ran — then bootstraps the kernel from the snapshot descriptor and the
//! retained suffix above it. Every other constructor arrives here.

use rafter::{
    BootstrapLogEntry, BootstrapState, LogEntryKind, LogIndex, Node as RaftNode,
    NodeConfig as RaftNodeConfig, PendingSnapshotTransferResumeError, RaftSnapshot,
};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};

use crate::{DurableRaftNode, RaftRuntimeError};

impl<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore> DurableRaftNode<H, L, S> {
    /// Hydrates a deterministic Raft node from supplied hard-state, retained
    /// log, and a durable snapshot store.
    ///
    /// # Errors
    ///
    /// Returns [`RaftRuntimeError::Bootstrap`] when persisted hard state,
    /// snapshot metadata, or the retained log suffix is invalid for the
    /// supplied configuration.
    pub fn with_storage_and_snapshot_store(
        config: RaftNodeConfig,
        hard_state_store: H,
        log_segment: L,
        snapshot_store: S,
    ) -> Result<Self, RaftRuntimeError> {
        Self::with_storage_and_snapshot_store_applied_through(
            config,
            hard_state_store,
            log_segment,
            snapshot_store,
            LogIndex::ZERO,
        )
    }

    /// Like [`DurableRaftNode::with_storage_and_snapshot_store`], but the
    /// application declares it has already durably applied entries through
    /// `applied_through`; committed entries at or below the floor are not
    /// re-emitted after this restart.
    ///
    /// Prefer
    /// [`DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through`]
    /// for restart paths that need committed replay outputs immediately.
    ///
    /// # Errors
    ///
    /// As the plain constructor, plus a bootstrap error when the floor lies
    /// beyond the persisted log.
    pub fn with_storage_and_snapshot_store_applied_through(
        config: RaftNodeConfig,
        hard_state_store: H,
        mut log_segment: L,
        mut snapshot_store: S,
        applied_through: LogIndex,
    ) -> Result<Self, RaftRuntimeError> {
        let hard_state = hard_state_store.current();

        // Finish an installation interrupted between its final staged chunk
        // and its promotion: a COMPLETE durable staging cannot be resumed
        // (the kernel rejects complete transfers as not-installed), so left
        // in place it would fail every reopen. Promoting here is exactly the
        // write the crashed process would have performed next — the staged
        // content was validated chunk by chunk, and no acknowledgement of
        // the installed snapshot escaped before the crash. A complete
        // staging at or below the current snapshot boundary is instead the
        // leftover of a crash between promotion and the staging clear; it is
        // stale, and the kernel's resume path below reports it as such so
        // the existing branch clears it.
        if let Some(transfer) = snapshot_store.current_pending_snapshot_transfer() {
            let staged_boundary = transfer.metadata.last_included_index;
            let current_boundary = snapshot_store
                .current_snapshot()
                .map_or(LogIndex::ZERO, |snapshot| {
                    snapshot.metadata.last_included_index
                });
            if transfer.is_complete() && staged_boundary > current_boundary {
                let descriptor = RaftSnapshot::new(
                    transfer.metadata.clone(),
                    transfer.total_payload_len,
                    transfer.application_payload_crc32,
                );
                snapshot_store
                    .promote_staged_snapshot(&descriptor)
                    .map_err(RaftRuntimeError::SnapshotWrite)?;
                log_segment
                    .compact_prefix_through(staged_boundary)
                    .map_err(RaftRuntimeError::LogCompact)?;
            }
        }

        // The kernel bootstraps from the snapshot descriptor alone; the
        // payload bytes stay in the snapshot store and are streamed on
        // demand. Read after the promotion above so a just-finished
        // installation bootstraps as the current snapshot.
        let snapshot = snapshot_store.current_snapshot();
        let snapshot_index = snapshot.as_ref().map_or(LogIndex::ZERO, |snapshot| {
            snapshot.metadata.last_included_index
        });

        // Guard the snapshot-persist / log-compaction crash window. The write
        // path persists the snapshot first, then compacts the log, so a crash
        // between the two leaves a durable snapshot ahead of the log's
        // compacted prefix — which is indistinguishable from the supported
        // retained-full-log mode and boots correctly through bootstrap
        // filtering below. The inverse, a log compacted *past* what the
        // snapshot covers, is unrepairable acknowledged-data loss: fail loudly
        // with a precise error rather than a generic contiguity gap.
        let compacted_through = log_segment.compacted_through();
        if compacted_through > snapshot_index {
            return Err(RaftRuntimeError::CompactionAheadOfSnapshot {
                compacted_through,
                snapshot_index,
            });
        }

        // Within the supported direction, one shape is NOT equivalent to the
        // retained-full-log mode: the segment's tail sits strictly below the
        // snapshot boundary, so its next appendable index disagrees with the
        // kernel's first appendable index (boundary + 1) and a later append
        // would mislabel entries. That shape is a crash after the snapshot
        // promote but before `compact_prefix_through(boundary)`; complete the
        // interrupted compaction, which restores next_index() == boundary + 1.
        if compacted_through < snapshot_index && log_segment.next_index() <= snapshot_index {
            log_segment
                .compact_prefix_through(snapshot_index)
                .map_err(RaftRuntimeError::LogCompact)?;
        }

        let log = log_segment
            .replay_entries()
            .into_iter()
            .filter(|entry| entry.index >= snapshot_index)
            .map(persisted_entry_to_bootstrap)
            .collect();
        let mut node = RaftNode::from_bootstrap_applied_through(
            config,
            BootstrapState {
                current_term: hard_state.current_term,
                voted_for: hard_state.voted_for,
                commit_index: hard_state.commit_index,
                committed_configuration: hard_state.committed_configuration,
                snapshot,
                log,
            },
            applied_through,
        )
        .map_err(RaftRuntimeError::Bootstrap)?;

        if let Some(transfer) = snapshot_store.current_pending_snapshot_transfer() {
            match node.resume_pending_snapshot_transfer(transfer) {
                Ok(()) => {}
                Err(PendingSnapshotTransferResumeError::StaleSnapshot { .. }) => {
                    snapshot_store
                        .clear_pending_snapshot_transfer()
                        .map_err(RaftRuntimeError::SnapshotWrite)?;
                }
                Err(error) => return Err(RaftRuntimeError::PendingSnapshotTransferResume(error)),
            }
        }

        Ok(Self {
            persisted_tail: Some(crate::log_repair::PersistedTail::of_node(&node)),
            node,
            hard_state_store,
            log_segment,
            snapshot_store,
            fatal_error: None,
        })
    }
}

fn persisted_entry_to_bootstrap(entry: rafter_storage::PersistedRaftLogEntry) -> BootstrapLogEntry {
    match entry.kind {
        LogEntryKind::Application(payload) => {
            BootstrapLogEntry::application(entry.index, entry.term, payload.to_vec())
        }
        LogEntryKind::Configuration(configuration) => {
            BootstrapLogEntry::configuration(entry.index, entry.term, configuration)
        }
        LogEntryKind::Noop => BootstrapLogEntry::noop(entry.index, entry.term),
    }
}

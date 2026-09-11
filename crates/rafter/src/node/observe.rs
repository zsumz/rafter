//! Read-only protocol observability for [`Node`].
//!
//! These methods expose stable protocol facts without changing state. Query
//! methods that interpret membership live in `membership`; retained-log and
//! snapshot-boundary queries live in `log`.

use crate::{
    FollowerSnapshotTransferStatus, LeaderSnapshotTransferStatus, LogIndex, NodeId,
    ReplicationProgress, ReplicationState, ReplicationWindowProgress, SnapshotTransferStatus, Term,
};

use super::state::ProgressMode;
use super::{Node, Role};

impl Node {
    /// Returns this node's id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.config.id()
    }

    /// Returns this node's current role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.volatile.role
    }

    /// Returns this node's current term.
    #[must_use]
    pub fn current_term(&self) -> Term {
        self.persistent.current_term
    }

    /// Returns the candidate this node voted for in the current term.
    #[must_use]
    pub fn voted_for(&self) -> Option<NodeId> {
        self.persistent.voted_for
    }

    /// Returns the node this replica believes is the current leader, based on
    /// the most recently accepted leader traffic. Purely volatile.
    #[must_use]
    pub fn leader_hint(&self) -> Option<NodeId> {
        self.volatile.leader_hint
    }

    /// Returns this node's committed index.
    #[must_use]
    pub fn commit_index(&self) -> LogIndex {
        self.volatile.commit_index
    }

    /// Returns the committed prefix processed by the core for output dispatch.
    ///
    /// Includes no-ops and configuration entries, even when no application
    /// command is emitted, and starts at the effective recovery floor. This
    /// cursor does not establish that an embedding has executed or durably
    /// applied the outputs. The application must track that progress itself.
    #[must_use]
    pub fn dispatched_index(&self) -> LogIndex {
        self.volatile.dispatched_index
    }

    /// Compatibility alias for [`Node::dispatched_index`].
    ///
    /// This is the core's dispatch cursor, not external application execution.
    /// Kept without deprecation warnings so existing consumers remain source
    /// compatible; new code should use [`Node::dispatched_index`].
    #[must_use]
    pub fn applied_index(&self) -> LogIndex {
        self.dispatched_index()
    }

    /// Returns this node's last local log index.
    #[must_use]
    pub fn last_log_index(&self) -> LogIndex {
        LogIndex(self.snapshot_index().0 + self.persistent.log.len() as u64)
    }

    /// Returns leader-side replication progress for every effective
    /// *follower*, learners included.
    ///
    /// The leader's own slot is not a row here, and cannot be: the returned
    /// `next_index` and [`ReplicationState`] describe a stream toward a
    /// follower, and a leader has none toward itself. Its own match index is
    /// [`Node::last_log_index`] by construction, which is the value a caller
    /// doing quorum arithmetic over these rows has to add back.
    ///
    /// Empty on any node that is not the leader, for two independent reasons:
    /// the role guard below, and `LeaderState` being rebuilt on every entry to
    /// and exit from leadership, which leaves a non-leader with no progress
    /// set to report from. The guard is therefore defensive rather than
    /// behaviourally distinguishable, and is kept so the claim survives a
    /// future change to either.
    #[must_use]
    pub fn leader_replication_progress(&self) -> Vec<ReplicationProgress> {
        if self.role() != Role::Leader {
            return Vec::new();
        }
        self.leader
            .progress
            .iter_followers()
            .map(|(follower_id, progress)| {
                let state = match progress.mode {
                    ProgressMode::Probe { .. } => ReplicationState::Probing,
                    ProgressMode::Replicate => ReplicationState::Replicating,
                    ProgressMode::Snapshot { next_offset } => {
                        ReplicationState::Snapshotting { next_offset }
                    }
                };
                ReplicationProgress {
                    follower_id,
                    match_index: progress.match_index,
                    next_index: progress.next_index,
                    state,
                }
            })
            .collect()
    }

    /// Returns bounded in-flight replication-window usage for every effective
    /// follower while this node is leader.
    ///
    /// The result is empty on non-leaders. Probe and snapshot streams report an
    /// empty, non-full window because they do not use optimistic append batches.
    #[must_use]
    pub fn leader_replication_windows(&self) -> Vec<ReplicationWindowProgress> {
        if self.role() != Role::Leader {
            return Vec::new();
        }
        let max_batches = self.config.max_inflight_appends();
        let max_bytes = self.config.max_inflight_bytes();
        self.leader
            .progress
            .iter_followers()
            .map(|(follower_id, progress)| ReplicationWindowProgress {
                follower_id,
                in_flight_batches: progress.inflights.batch_count(),
                in_flight_bytes: progress.inflights.byte_count(),
                max_in_flight_batches: max_batches,
                max_in_flight_bytes: max_bytes,
                full: matches!(progress.mode, ProgressMode::Replicate)
                    && progress.inflights.is_full(max_batches, max_bytes),
            })
            .collect()
    }

    /// Whether a read barrier requested right now would grant from the
    /// leader lease without a quorum round trip.
    ///
    /// This is the lease fast path's own condition rather than a reading of
    /// the lease timer, so it is `false` whenever the barrier would instead be
    /// refused or take the round trip: while a leadership transfer is in
    /// progress, for the rest of a term in which this leader authorized a
    /// deposition with `TimeoutNow`, before this term's first commit, and
    /// outside the lease window.
    ///
    /// It is also `false` on a single-voter membership, which grants barriers
    /// with no round trip on quorum evidence rather than on the lease. A
    /// caller asking "is the lease carrying my reads" gets a truthful no; a
    /// caller asking "will this read cost a round trip" must also consider
    /// that case.
    #[must_use]
    pub fn read_lease_active(&self) -> bool {
        self.lease_grant_available()
    }

    /// Returns snapshot transfer observability for this node.
    #[must_use]
    pub fn snapshot_transfer_status(&self) -> SnapshotTransferStatus {
        let leader = self
            .persistent
            .snapshot
            .as_ref()
            .map(|snapshot| {
                let total_bytes = snapshot.application_payload_len;
                self.leader
                    .progress
                    .iter_followers()
                    .filter_map(|(follower_id, progress)| {
                        let next_offset = match progress.mode {
                            ProgressMode::Snapshot { next_offset } => next_offset,
                            _ if progress.next_index <= snapshot.metadata.last_included_index => 0,
                            _ => return None,
                        };
                        Some(LeaderSnapshotTransferStatus {
                            follower_id,
                            transfer_id: snapshot.transfer_id(),
                            last_included_index: snapshot.metadata.last_included_index,
                            total_bytes,
                            next_offset: next_offset.min(total_bytes),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let follower = self.volatile.incoming_snapshot.as_ref().map(|transfer| {
            FollowerSnapshotTransferStatus {
                leader_id: transfer.leader_id,
                transfer_id: transfer.transfer_id,
                last_included_index: transfer.metadata.last_included_index,
                total_bytes: transfer.total_payload_len,
                received_bytes: transfer.next_offset(),
            }
        });

        SnapshotTransferStatus {
            leader,
            follower,
            rejected_chunks: self.volatile.snapshot_chunk_rejections,
        }
    }
}

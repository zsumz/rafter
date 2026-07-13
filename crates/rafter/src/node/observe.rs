//! Read-only protocol observability for [`Node`].
//!
//! These methods expose stable protocol facts without changing state. Query
//! methods that interpret membership live in `membership`; retained-log and
//! snapshot-boundary queries live in `log`.

use crate::{
    FollowerSnapshotTransferStatus, LeaderSnapshotTransferStatus, LogIndex, NodeId,
    ReplicationProgress, ReplicationState, SnapshotTransferStatus, Term,
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

    /// Returns this node's applied index.
    #[must_use]
    pub fn applied_index(&self) -> LogIndex {
        self.volatile.applied_index
    }

    /// Returns this node's last local log index.
    #[must_use]
    pub fn last_log_index(&self) -> LogIndex {
        LogIndex(self.snapshot_index().0 + self.persistent.log.len() as u64)
    }

    /// Returns leader-side replication progress for every effective replica.
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

    /// Whether a read barrier requested right now would grant from the
    /// leader lease without a quorum round trip.
    #[must_use]
    pub fn read_lease_active(&self) -> bool {
        self.role() == Role::Leader
            && self.config.lease_reads()
            && self
                .leader
                .lease
                .holds(self.leader.ticks, self.config.read_lease_ticks())
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

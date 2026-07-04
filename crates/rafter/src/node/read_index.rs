use crate::{LogIndex, NodeId, ReadId};

use super::state::PendingReadIndex;

/// Barriers a leader will hold un-confirmed before refusing new ones; an
/// unreachable leader must not grow this without bound.
const MAX_PENDING_READS: usize = 1024;
use super::{Node, Output, ReadIndexRejection, Role};

impl Node {
    /// Registers a linearizable read barrier (thesis 6.4): the barrier is
    /// granted once a quorum acknowledges this node's leadership in a
    /// heartbeat round at or after registration, proving no newer leader had
    /// committed anything when the barrier was requested.
    pub(super) fn read_index(&mut self, read_id: ReadId) -> Vec<Output> {
        if self.role() != Role::Leader {
            return vec![Output::ReadIndexRejected {
                read_id,
                reason: ReadIndexRejection::NotLeader {
                    role: self.role(),
                    term: self.current_term(),
                },
            }];
        }
        if let Some(transfer) = self.leader.pending_transfer.as_ref() {
            return vec![Output::ReadIndexRejected {
                read_id,
                reason: ReadIndexRejection::LeadershipTransferInProgress {
                    target: transfer.target,
                },
            }];
        }
        // Leader completeness only bounds entries up to the leader's own
        // term; until this leader commits in its current term, its commit
        // index may trail a previous leader's (thesis 6.4).
        if !self.has_committed_in_current_term() {
            return vec![Output::ReadIndexRejected {
                read_id,
                reason: ReadIndexRejection::NoCommitInCurrentTerm,
            }];
        }
        if self.leader.pending_reads.len() >= MAX_PENDING_READS {
            return vec![Output::ReadIndexRejected {
                read_id,
                reason: ReadIndexRejection::TooManyPendingReads,
            }];
        }

        // Inside a held lease, leadership was quorum-confirmed within the
        // window: the barrier grants immediately, no round trip (thesis
        // 6.4.2; the tick-skew assumption is documented on
        // `NodeConfig::with_lease_reads`).
        if self.config.lease_reads()
            && self
                .leader
                .lease
                .holds(self.leader.ticks, self.config.read_lease_ticks())
        {
            return vec![Output::ReadIndexGranted {
                read_id,
                read_index: self.volatile.commit_index,
            }];
        }

        let pending = PendingReadIndex {
            read_id,
            read_index: self.volatile.commit_index,
            // Only acknowledgements of rounds broadcast after registration
            // count: the next broadcast increments the sequence, so echoes
            // of earlier rounds (or unknown zero-sequence echoes) can never
            // confirm this barrier.
            registered_sequence: self.leader.heartbeat_sequence + 1,
            acks: std::collections::BTreeSet::new(),
        };

        // A single-voter membership is its own quorum.
        if self.has_effective_quorum(std::iter::once(self.id())) {
            return vec![Output::ReadIndexGranted {
                read_id,
                read_index: pending.read_index,
            }];
        }

        self.leader.pending_reads.push(pending);
        self.broadcast_append_entries()
    }

    /// Records a follower acknowledgement of `sequence` and grants every
    /// pending barrier whose registration round it confirms for a quorum.
    pub(super) fn acknowledge_read_barriers(
        &mut self,
        follower_id: NodeId,
        sequence: u64,
    ) -> Vec<Output> {
        if self.leader.pending_reads.is_empty() {
            return Vec::new();
        }

        for pending in &mut self.leader.pending_reads {
            if pending.registered_sequence <= sequence {
                pending.acks.insert(follower_id);
            }
        }

        let mut outputs = Vec::new();
        let this = self.id();
        // Pending reads are registered in commit-index order; grant every
        // prefix whose quorum is confirmed, preserving registration order.
        let membership = self.effective_membership();
        self.leader.pending_reads.retain(|pending| {
            let confirmed =
                membership.has_quorum(pending.acks.iter().copied().chain(std::iter::once(this)));
            if confirmed {
                outputs.push(Output::ReadIndexGranted {
                    read_id: pending.read_id,
                    read_index: pending.read_index,
                });
            }
            !confirmed
        });
        outputs
    }

    fn has_committed_in_current_term(&self) -> bool {
        self.volatile.commit_index > LogIndex::ZERO
            && self.term_at(self.volatile.commit_index) == Some(self.current_term())
    }

    /// Observability: barriers still awaiting quorum confirmation.
    #[must_use]
    pub fn pending_read_count(&self) -> usize {
        self.leader.pending_reads.len()
    }
}

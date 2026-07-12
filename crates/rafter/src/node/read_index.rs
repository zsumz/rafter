//! Linearizable read barriers and the leader-lease fast path.
//!
//! Read registration, quorum confirmation, cancellation, and grant ordering
//! live here; callers remain responsible for waiting until local apply reaches
//! the granted index before serving a read.

use crate::{LogIndex, NodeId, ReadId};

use super::state::{AcknowledgementSet, PendingReadRound};
use super::{Node, Output, ReadIndexRejection, Role};

/// Barriers a leader will hold un-confirmed before refusing new ones; an
/// unreachable leader must not grow this without bound. This is a read-id
/// count, not a round count: grouped barriers still consume one slot each.
pub(super) const MAX_PENDING_READS: usize = 1024;

impl Node {
    /// Registers a linearizable read barrier (thesis 6.4): the barrier is
    /// granted once a quorum acknowledges this node's leadership in a
    /// heartbeat round at or after registration, proving no newer leader had
    /// committed anything when the barrier was requested.
    pub(super) fn read_index(&mut self, read_id: ReadId) -> Vec<Output> {
        self.read_index_batch(vec![read_id])
    }

    /// Registers consecutive linearizable read barriers that entered the
    /// kernel as one deterministic batch. The barriers share a confirmation
    /// round and quorum evidence, but every grant/rejection remains per read
    /// id and preserves input order.
    pub(super) fn read_index_batch(&mut self, read_ids: Vec<ReadId>) -> Vec<Output> {
        if read_ids.is_empty() {
            return Vec::new();
        }
        if self.role() != Role::Leader {
            let reason = ReadIndexRejection::NotLeader {
                role: self.role(),
                term: self.current_term(),
            };
            return reject_reads(read_ids, reason);
        }
        if let Some(transfer) = self.leader.pending_transfer.as_ref() {
            let reason = ReadIndexRejection::LeadershipTransferInProgress {
                target: transfer.target,
            };
            return reject_reads(read_ids, reason);
        }
        // Leader completeness only bounds entries up to the leader's own
        // term; until this leader commits in its current term, its commit
        // index may trail a previous leader's (thesis 6.4).
        if !self.has_committed_in_current_term() {
            return reject_reads(read_ids, ReadIndexRejection::NoCommitInCurrentTerm);
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
            return grant_reads(read_ids, self.volatile.commit_index);
        }

        let membership = self.effective_membership();
        let self_id = self.id();
        let mut pending = PendingReadRound {
            read_ids,
            read_index: self.volatile.commit_index,
            // Only acknowledgements of rounds broadcast after registration
            // count: the next broadcast increments the sequence, so echoes
            // of earlier rounds (or unknown zero-sequence echoes) can never
            // confirm this barrier.
            registered_sequence: self.leader.heartbeat_sequence + 1,
            acks: AcknowledgementSet::new(&membership, self_id),
        };

        // A single-voter membership is its own quorum.
        if pending.acks.has_quorum_with_self(&membership, self_id) {
            return grant_reads(pending.read_ids, pending.read_index);
        }

        let available = MAX_PENDING_READS.saturating_sub(self.pending_read_count());
        if available == 0 {
            return reject_reads(pending.read_ids, ReadIndexRejection::TooManyPendingReads);
        }

        let mut outputs = Vec::new();
        let rejected = if pending.read_ids.len() > available {
            pending.read_ids.split_off(available)
        } else {
            Vec::new()
        };
        self.leader.pending_reads.push(pending);
        outputs.extend(self.broadcast_append_entries());
        reject_reads_into(
            &mut outputs,
            rejected,
            ReadIndexRejection::TooManyPendingReads,
        );
        outputs
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

        let membership = self.effective_membership();
        let self_id = self.id();
        for pending in &mut self.leader.pending_reads {
            if pending.registered_sequence <= sequence {
                pending.acks.insert(follower_id, &membership, self_id);
            }
        }

        let mut outputs = Vec::new();
        // Pending reads are registered in commit-index order; grant every
        // prefix whose quorum is confirmed, preserving registration order.
        self.leader.pending_reads.retain_mut(|pending| {
            let confirmed = pending.acks.has_quorum_with_self(&membership, self_id);
            if confirmed {
                grant_reads_into(
                    &mut outputs,
                    pending.read_ids.iter().copied(),
                    pending.read_index,
                );
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
        self.leader
            .pending_reads
            .iter()
            .map(|pending| pending.read_ids.len())
            .sum()
    }
}

fn grant_reads<I>(read_ids: I, read_index: LogIndex) -> Vec<Output>
where
    I: IntoIterator<Item = ReadId>,
{
    let mut outputs = Vec::new();
    grant_reads_into(&mut outputs, read_ids, read_index);
    outputs
}

fn reject_reads<I>(read_ids: I, reason: ReadIndexRejection) -> Vec<Output>
where
    I: IntoIterator<Item = ReadId>,
{
    let mut outputs = Vec::new();
    reject_reads_into(&mut outputs, read_ids, reason);
    outputs
}

fn grant_reads_into<I>(outputs: &mut Vec<Output>, read_ids: I, read_index: LogIndex)
where
    I: IntoIterator<Item = ReadId>,
{
    outputs.extend(
        read_ids
            .into_iter()
            .map(|read_id| Output::ReadIndexGranted {
                read_id,
                read_index,
            }),
    );
}

fn reject_reads_into<I>(outputs: &mut Vec<Output>, read_ids: I, reason: ReadIndexRejection)
where
    I: IntoIterator<Item = ReadId>,
{
    outputs.extend(
        read_ids
            .into_iter()
            .map(|read_id| Output::ReadIndexRejected { read_id, reason }),
    );
}

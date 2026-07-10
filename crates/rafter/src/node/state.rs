use std::collections::{vec_deque, VecDeque};

use crate::{
    CommittedConfiguration, LocalProposalId, LogEntry, LogIndex, MembershipConfig, NodeId,
    PendingSnapshotTransfer, RaftSnapshot, RaftSnapshotMetadata, ReadId, SnapshotTransferId, Term,
};

use super::Role;

mod membership;

pub(super) use membership::{AcknowledgementSet, MembershipIndex, ProgressSet, SlotSet};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(super) struct PersistentState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub committed_configuration: Option<CommittedConfiguration>,
    pub snapshot: Option<RaftSnapshot>,
    pub log: Vec<LogEntry>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct VolatileState {
    pub role: Role,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    /// Local-only proposal correlation. This is volatile by design: it is not
    /// replicated, persisted, snapshotted, or restored after restart.
    pub local_proposals: LocalProposalTracker,
    pub incoming_snapshot: Option<IncomingSnapshotTransfer>,
    /// The node this replica believes is the current leader, set on accepted
    /// leader traffic and consulted by the pre-vote leader-stickiness rule
    /// (thesis 4.2.3). Never persisted.
    pub leader_hint: Option<NodeId>,
}

impl Default for VolatileState {
    fn default() -> Self {
        Self {
            role: Role::Follower,
            commit_index: LogIndex::ZERO,
            applied_index: LogIndex::ZERO,
            local_proposals: LocalProposalTracker::default(),
            incoming_snapshot: None,
            leader_hint: None,
        }
    }
}

impl VolatileState {
    /// Builds follower volatile state with commit and apply floors at `index`.
    pub fn at_applied_index(index: LogIndex) -> Self {
        Self {
            role: Role::Follower,
            commit_index: index,
            applied_index: index,
            local_proposals: LocalProposalTracker::default(),
            incoming_snapshot: None,
            leader_hint: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct LocalProposal {
    pub term: Term,
    pub id: LocalProposalId,
}

/// Volatile local proposal correlation ordered by log index.
///
/// The common path appends proposals at strictly increasing indexes and
/// removes them as apply advances. This tracker keeps that natural shape
/// without making protocol state depend on it: it is never persisted,
/// replicated, or restored.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(super) struct LocalProposalTracker {
    proposals: VecDeque<(LogIndex, LocalProposal)>,
}

impl LocalProposalTracker {
    pub(super) fn insert(&mut self, index: LogIndex, proposal: LocalProposal) {
        match self.proposals.back_mut() {
            None => {
                self.proposals.push_back((index, proposal));
            }
            Some((last_index, last_proposal)) if *last_index == index => {
                *last_proposal = proposal;
            }
            Some((last_index, _)) if *last_index < index => {
                self.proposals.push_back((index, proposal));
            }
            Some(_) => self.insert_out_of_order(index, proposal),
        }
    }

    fn insert_out_of_order(&mut self, index: LogIndex, proposal: LocalProposal) {
        let position = self
            .proposals
            .iter()
            .position(|(existing_index, _)| *existing_index >= index);
        match position {
            Some(position) if self.proposals[position].0 == index => {
                self.proposals[position].1 = proposal;
            }
            Some(position) => {
                self.proposals.insert(position, (index, proposal));
            }
            None => self.proposals.push_back((index, proposal)),
        }
    }

    pub(super) fn remove(&mut self, index: LogIndex) -> Option<LocalProposal> {
        if self
            .proposals
            .front()
            .is_some_and(|(current, _)| *current == index)
        {
            return self.proposals.pop_front().map(|(_, proposal)| proposal);
        }
        let position = self
            .proposals
            .iter()
            .position(|(current, _)| *current == index)?;
        self.proposals
            .remove(position)
            .map(|(_, proposal)| proposal)
    }

    pub(super) fn split_off(&mut self, index: LogIndex) -> Self {
        let position = self
            .proposals
            .iter()
            .position(|(proposal_index, _)| *proposal_index >= index)
            .unwrap_or(self.proposals.len());
        Self {
            proposals: self.proposals.split_off(position),
        }
    }

    #[cfg(test)]
    pub(super) fn contains_key(&self, index: LogIndex) -> bool {
        self.proposals
            .iter()
            .any(|(proposal_index, _)| *proposal_index == index)
    }

    #[cfg(test)]
    pub(super) fn keys(&self) -> impl Iterator<Item = &LogIndex> {
        self.proposals.iter().map(|(index, _)| index)
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.proposals.is_empty()
    }
}

impl IntoIterator for LocalProposalTracker {
    type Item = (LogIndex, LocalProposal);
    type IntoIter = vec_deque::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.proposals.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalProposal, LocalProposalTracker};
    use crate::{LocalProposalId, LogIndex, Term};

    fn proposal(id: u64) -> LocalProposal {
        LocalProposal {
            term: Term(1),
            id: LocalProposalId(id),
        }
    }

    #[test]
    fn local_proposal_tracker_keeps_index_order() {
        let mut tracker = LocalProposalTracker::default();

        tracker.insert(LogIndex(3), proposal(3));
        tracker.insert(LogIndex(1), proposal(1));
        tracker.insert(LogIndex(2), proposal(2));
        tracker.insert(LogIndex(2), proposal(20));

        let entries = tracker.into_iter().collect::<Vec<_>>();
        assert_eq!(
            entries,
            vec![
                (LogIndex(1), proposal(1)),
                (LogIndex(2), proposal(20)),
                (LogIndex(3), proposal(3)),
            ]
        );
    }

    #[test]
    fn local_proposal_tracker_removes_by_index() {
        let mut tracker = LocalProposalTracker::default();
        tracker.insert(LogIndex(1), proposal(1));
        tracker.insert(LogIndex(2), proposal(2));
        tracker.insert(LogIndex(3), proposal(3));

        assert_eq!(tracker.remove(LogIndex(1)), Some(proposal(1)));
        assert_eq!(tracker.remove(LogIndex(3)), Some(proposal(3)));
        assert_eq!(tracker.remove(LogIndex(9)), None);
        assert_eq!(
            tracker.into_iter().collect::<Vec<_>>(),
            vec![(LogIndex(2), proposal(2))]
        );
    }

    #[test]
    fn local_proposal_tracker_split_off_returns_suffix() {
        let mut tracker = LocalProposalTracker::default();
        tracker.insert(LogIndex(1), proposal(1));
        tracker.insert(LogIndex(2), proposal(2));
        tracker.insert(LogIndex(4), proposal(4));

        let suffix = tracker.split_off(LogIndex(3));

        assert_eq!(
            tracker.into_iter().collect::<Vec<_>>(),
            vec![(LogIndex(1), proposal(1)), (LogIndex(2), proposal(2))]
        );
        assert_eq!(
            suffix.into_iter().collect::<Vec<_>>(),
            vec![(LogIndex(4), proposal(4))]
        );
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(super) struct LeaderState {
    /// Per-replica replication progress — the Progress discipline: match and
    /// next indexes, the send mode, and the in-flight append window.
    pub progress: ProgressSet,
    /// Ticks observed since this term's leadership began; the leader's own
    /// clock for the read lease. Never persisted, never compared across
    /// nodes — cross-node safety rests on the documented bounded tick-rate
    /// skew, not on shared time.
    pub ticks: u64,
    /// The read-lease checkpoint machine (thesis 6.4.2).
    pub lease: LeaderLease,
    pub pending_transfer: Option<PendingLeadershipTransfer>,
    /// Monotonic heartbeat round counter; every append carries the current
    /// value and responses echo it, so acknowledgements can be ordered
    /// relative to read-index registrations (thesis 6.4).
    pub heartbeat_sequence: u64,
    /// Leader ticks since the last broadcast round. This lets multi-group
    /// drivers coalesce idle heartbeats without changing proposal-driven
    /// replication.
    pub heartbeat_elapsed: u64,
    /// Followers heard from since the last check-quorum evaluation.
    pub quorum_acks: AcknowledgementSet,
    pub quorum_check_elapsed: u64,
    pub pending_reads: Vec<PendingReadRound>,
}

/// One replica's replication state as the leader sees it (thesis 10.2.1 and
/// the Progress/Inflights discipline).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct Progress {
    pub match_index: LogIndex,
    pub next_index: LogIndex,
    pub mode: ProgressMode,
    pub inflights: Inflights,
}

impl Progress {
    /// A fresh follower view: nothing matched, probing at `next_index` until
    /// the follower confirms its log position.
    pub fn probing(next_index: LogIndex) -> Self {
        Self {
            match_index: LogIndex::ZERO,
            next_index,
            mode: ProgressMode::Probe {
                awaiting_response: false,
            },
            inflights: Inflights::default(),
        }
    }

    /// The leader's own progress entry: fully matched, never sent to.
    pub fn local(last_log_index: LogIndex) -> Self {
        Self {
            match_index: last_log_index,
            next_index: last_log_index.next(),
            mode: ProgressMode::Replicate,
            inflights: Inflights::default(),
        }
    }

    /// Collapses to probing after a rejection: the in-flight window is
    /// forfeited and `next_index` walks back one step, never below the
    /// acknowledged match (stale rejection storms cannot over-rewind) and
    /// never below the snapshot boundary (below it the follower needs the
    /// snapshot, not the log).
    pub fn collapse_into_probe(&mut self, snapshot_index: LogIndex) {
        let floor = self.match_index.next().max(snapshot_index);
        self.next_index = LogIndex(self.next_index.0.saturating_sub(1)).max(floor);
        self.mode = ProgressMode::Probe {
            awaiting_response: false,
        };
        self.inflights.clear();
    }

    /// Confirms the follower's position after a successful acknowledgement:
    /// replication resumes from the acknowledged match with a clean window.
    pub fn confirm_replicating(&mut self) {
        if !matches!(self.mode, ProgressMode::Replicate) {
            self.mode = ProgressMode::Replicate;
            self.inflights.clear();
            self.next_index = self.next_index.max(self.match_index.next());
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum ProgressMode {
    /// One bounded append at a time until the follower's log position is
    /// confirmed; retries while awaiting are empty heartbeats.
    Probe { awaiting_response: bool },
    /// Confirmed position: sends fill the in-flight window and `next_index`
    /// advances optimistically with each send.
    Replicate,
    /// Streaming the current snapshot chunk by chunk; log replication is
    /// paused until the follower installs it.
    Snapshot { next_offset: u64 },
}

/// The window of optimistically sent, unacknowledged append batches for one
/// follower, bounded by batch count and payload bytes.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(super) struct Inflights {
    batches: VecDeque<InflightBatch>,
    bytes: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct InflightBatch {
    last_index: LogIndex,
    bytes: usize,
}

impl Inflights {
    /// Whether the window admits another batch. One batch is always
    /// admissible regardless of the byte budget, or a batch larger than the
    /// budget could never be sent at all.
    pub fn is_full(&self, max_batches: usize, max_bytes: usize) -> bool {
        self.batches.len() >= max_batches.max(1)
            || (!self.batches.is_empty() && self.bytes >= max_bytes)
    }

    /// Records one in-flight append batch.
    pub fn record(&mut self, last_index: LogIndex, bytes: usize) {
        self.batches.push_back(InflightBatch { last_index, bytes });
        self.bytes = self.bytes.saturating_add(bytes);
    }

    /// Releases every batch fully acknowledged by `match_index`.
    pub fn free_through(&mut self, match_index: LogIndex) {
        while let Some(batch) = self.batches.front() {
            if batch.last_index > match_index {
                break;
            }
            self.bytes = self.bytes.saturating_sub(batch.bytes);
            self.batches.pop_front();
        }
    }

    /// Clears every recorded in-flight batch.
    pub fn clear(&mut self) {
        self.batches.clear();
        self.bytes = 0;
    }

    /// Test observability: the window's recorded batch count. Only kernel
    /// tests inspect the window directly, so the accessor is test-gated to
    /// keep the library target free of dead code.
    #[cfg(test)]
    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    /// Test observability: the window's recorded payload bytes (test-gated
    /// like [`Inflights::batch_count`]).
    #[cfg(test)]
    pub fn byte_count(&self) -> usize {
        self.bytes
    }
}

/// Tick-based leader lease, renewed by quorum-confirmed broadcast rounds.
///
/// One checkpoint is pending at a time: `(pending_basis_tick,
/// pending_sequence)` says "a quorum acknowledging round
/// `pending_sequence` or later proves my leadership as of
/// `pending_basis_tick`". Confirmation moves the lease start to that basis
/// and re-arms a fresh checkpoint, so the lease renews once per quorum
/// round trip. A checkpoint older than the window re-arms without
/// confirmation — its basis could no longer extend the lease anyway.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(super) struct LeaderLease {
    pub pending_basis_tick: u64,
    pub pending_sequence: u64,
    pub acks: AcknowledgementSet,
    pub confirmed_basis_tick: Option<u64>,
}

impl LeaderLease {
    /// Records `follower`'s acknowledgement of `sequence`; returns true when
    /// this acknowledgement is usable for the pending checkpoint.
    pub fn record_ack(
        &mut self,
        follower: NodeId,
        sequence: u64,
        membership: &MembershipConfig,
        self_id: NodeId,
    ) -> bool {
        if sequence < self.pending_sequence {
            return false;
        }
        self.acks.insert(follower, membership, self_id);
        true
    }

    /// Confirms the pending checkpoint and re-arms the next one at
    /// (`now_tick`, `next_sequence`).
    pub fn confirm_and_rearm(&mut self, now_tick: u64, next_sequence: u64) {
        self.confirmed_basis_tick = Some(self.pending_basis_tick);
        self.rearm(now_tick, next_sequence);
    }

    /// Discards the pending checkpoint in favour of a fresh basis.
    pub fn rearm(&mut self, now_tick: u64, next_sequence: u64) {
        self.pending_basis_tick = now_tick;
        self.pending_sequence = next_sequence;
        self.acks.clear();
    }

    /// Whether the lease covers `now_tick` for a window of `window_ticks`.
    pub fn holds(&self, now_tick: u64, window_ticks: u64) -> bool {
        self.confirmed_basis_tick
            .is_some_and(|basis| now_tick.saturating_sub(basis) < window_ticks)
    }
}

/// A group of read barriers awaiting quorum confirmation of leadership at or
/// after their shared registration round (thesis 6.4).
///
/// Consecutive read barriers submitted in one deterministic step batch share
/// the same heartbeat sequence, read index, and acknowledgement set. Grants
/// still emit one output per read id in registration order.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct PendingReadRound {
    pub read_ids: Vec<ReadId>,
    pub read_index: LogIndex,
    pub registered_sequence: u64,
    pub acks: AcknowledgementSet,
}

/// An in-flight leadership transfer; volatile, abandoned on step-down or
/// after one election timeout without completing (thesis 3.10).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct PendingLeadershipTransfer {
    pub target: NodeId,
    pub ticks_remaining: u64,
    pub timeout_now_sent: bool,
}

/// Follower-side progress of an inbound chunked snapshot transfer. Tracks
/// only how many bytes have been staged — the bytes themselves went to the
/// receiver's snapshot store through
/// [`Output::StageSnapshotChunk`](crate::Output::StageSnapshotChunk).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct IncomingSnapshotTransfer {
    pub leader_id: NodeId,
    pub transfer_id: SnapshotTransferId,
    pub metadata: RaftSnapshotMetadata,
    pub total_payload_len: u64,
    pub application_payload_crc32: u32,
    pub received_len: u64,
}

impl IncomingSnapshotTransfer {
    /// Starts a new inbound snapshot transfer.
    pub fn new(
        leader_id: NodeId,
        transfer_id: SnapshotTransferId,
        metadata: RaftSnapshotMetadata,
        total_payload_len: u64,
        application_payload_crc32: u32,
    ) -> Self {
        Self {
            leader_id,
            transfer_id,
            metadata,
            total_payload_len,
            application_payload_crc32,
            received_len: 0,
        }
    }

    /// Restores an inbound snapshot transfer from durable pending state.
    pub fn from_pending(pending: PendingSnapshotTransfer) -> Self {
        Self {
            leader_id: pending.leader_id,
            transfer_id: pending.transfer_id,
            metadata: pending.metadata,
            total_payload_len: pending.total_payload_len,
            application_payload_crc32: pending.application_payload_crc32,
            received_len: pending.received_len,
        }
    }

    /// Converts this in-memory transfer to durable pending state.
    pub fn to_pending(&self) -> PendingSnapshotTransfer {
        PendingSnapshotTransfer {
            leader_id: self.leader_id,
            transfer_id: self.transfer_id,
            metadata: self.metadata.clone(),
            total_payload_len: self.total_payload_len,
            application_payload_crc32: self.application_payload_crc32,
            received_len: self.received_len,
        }
    }

    /// Returns the next expected byte offset.
    pub fn next_offset(&self) -> u64 {
        self.received_len
    }
}

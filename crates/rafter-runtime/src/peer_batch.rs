//! Conservative admission for a contiguous, already-queued peer batch.
//!
//! This gate owns admission only. The caller preserves FIFO, stops at any
//! client/control input or rejection, and passes every admitted message to
//! `step_batch` without reducing acknowledgments to a maximum match index.

use crate::DurableRaftNode;
use rafter::{LogIndex, Message, NodeId, Role, SnapshotChunkSource, Term};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};

/// A bounded admission cursor captured before stepping a peer batch.
///
/// A rejection closes the gate permanently. It does not authorize skipping
/// that message: process it next in FIFO order, outside this batch. Never
/// reuse a gate after stepping the runtime or wait for more input to fill it.
#[derive(Debug)]
pub struct PeerBatchGate {
    term: Term,
    role: Role,
    leader: Option<NodeId>,
    tail: LogIndex,
    tail_term: Term,
    remaining_events: usize,
    remaining_bytes: usize,
    closed: bool,
}

impl<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore + SnapshotChunkSource>
    DurableRaftNode<H, L, S>
{
    /// Captures admission for successful current-term leader responses, or
    /// same-leader application-only follower appends extending the current tail.
    /// Pending membership changes and snapshot transfers disable admission.
    #[must_use]
    pub fn peer_batch_gate(&self, max_events: usize, max_bytes: usize) -> PeerBatchGate {
        let snapshot = self.snapshot_transfer_status();
        PeerBatchGate {
            term: self.current_term(),
            role: self.role(),
            leader: self.leader_hint(),
            tail: self.last_log_index(),
            tail_term: self.term_at_index(self.last_log_index()).unwrap_or(Term(0)),
            remaining_events: max_events,
            remaining_bytes: max_bytes,
            closed: self.fatal_error.is_some()
                || self.effective_configuration_entry() != self.committed_configuration_entry()
                || !snapshot.leader.is_empty()
                || snapshot.follower.is_some(),
        }
    }
}

impl PeerBatchGate {
    /// Admits the next message, charging conservative decoded entry bytes.
    ///
    /// Identity, term and log continuity must match. Rejected, oversized and
    /// special-purpose messages must be processed singly at their FIFO position.
    /// Every accepted response retains its sequence and individual semantics.
    pub fn admit(&mut self, from: NodeId, message: &Message) -> bool {
        let mut next_tail = self.tail;
        let mut next_term = self.tail_term;
        let bytes = match (self.role, message) {
            (Role::Leader, Message::AppendEntriesResponse(response))
                if response.success
                    && response.term == self.term
                    && response.follower_id == from =>
            {
                Some(64)
            }
            (Role::Follower, Message::AppendEntries(request))
                if request.term == self.term
                    && Some(from) == self.leader
                    && request.leader_id == from
                    && request.prev_log_index == self.tail
                    && request.prev_log_term == self.tail_term
                    && !request.entries.is_empty()
                    && request
                        .entries
                        .iter()
                        .all(|entry| entry.application_payload().is_some()) =>
            {
                let size = request.entries.iter().fold(64usize, |sum, entry| {
                    sum.saturating_add(entry.replication_bytes())
                });
                next_tail = LogIndex(self.tail.0.saturating_add(request.entries.len() as u64));
                next_term = request
                    .entries
                    .last()
                    .map_or(self.tail_term, |entry| entry.term);
                Some(size)
            }
            _ => None,
        };
        let accepted = !self.closed
            && self.remaining_events > 0
            && bytes.is_some_and(|size| size <= self.remaining_bytes);
        if accepted {
            self.remaining_events -= 1;
            self.remaining_bytes -= bytes.unwrap_or(0);
            self.tail = next_tail;
            self.tail_term = next_term;
        } else {
            self.closed = true;
        }
        accepted
    }
}

#[cfg(test)]
#[path = "peer_batch_test.rs"]
mod tests;

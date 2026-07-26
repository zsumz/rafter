//! State meaningful only while this node is leader.
//!
//! Replication progress, heartbeat rounds, lease checkpoints, pending reads,
//! quorum evidence, and leadership transfer all reset together on authority
//! changes.

use crate::{LogIndex, MembershipConfig, NodeId, ReadId};

use super::membership::{AcknowledgementSet, ProgressSet};

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(in crate::node) struct LeaderState {
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
    /// Whether this leadership has put a `TimeoutNow` on the wire.
    ///
    /// A `TimeoutNow` authorizes its recipient to depose this leader by the
    /// path that skips pre-vote and leader stickiness, and the network bounds
    /// no message's delay. The lease's safety argument is that voters refuse
    /// to depose a live leader; emitting the message waives that refusal for
    /// the rest of this term, and no local event can recall it. Abandoning
    /// the transfer record therefore does not restore the lease — only a new
    /// term does, and this whole struct is rebuilt per term.
    pub deposition_authorized: bool,
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

/// Tick-based leader lease, renewed by quorum-confirmed broadcast rounds.
///
/// One checkpoint is pending at a time: `(pending_basis_tick,
/// pending_sequence)` says "a quorum acknowledging round
/// `pending_sequence` or later proves my leadership as of
/// `pending_basis_tick`". Confirmation moves the lease start to that basis
/// and re-arms a fresh checkpoint, so the lease renews once per quorum round
/// trip. A checkpoint older than the window re-arms without confirmation — its
/// basis could no longer extend the lease anyway.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(in crate::node) struct LeaderLease {
    pub pending_basis_tick: u64,
    pub pending_sequence: u64,
    pub acks: AcknowledgementSet,
    pub confirmed_basis_tick: Option<u64>,
}

impl LeaderLease {
    /// Records `follower`'s acknowledgement of `sequence`; returns true when
    /// this acknowledgement is usable for the pending checkpoint.
    pub(in crate::node) fn record_ack(
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
    pub(in crate::node) fn confirm_and_rearm(&mut self, now_tick: u64, next_sequence: u64) {
        self.confirmed_basis_tick = Some(self.pending_basis_tick);
        self.rearm(now_tick, next_sequence);
    }

    /// Discards the pending checkpoint in favour of a fresh basis.
    pub(in crate::node) fn rearm(&mut self, now_tick: u64, next_sequence: u64) {
        self.pending_basis_tick = now_tick;
        self.pending_sequence = next_sequence;
        self.acks.clear();
    }

    /// Whether the lease covers `now_tick` for a window of `window_ticks`.
    pub(in crate::node) fn holds(&self, now_tick: u64, window_ticks: u64) -> bool {
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
pub(in crate::node) struct PendingReadRound {
    pub read_ids: Vec<ReadId>,
    pub read_index: LogIndex,
    pub registered_sequence: u64,
    pub acks: AcknowledgementSet,
}

/// An in-flight leadership transfer; volatile, abandoned on step-down or after
/// one election timeout without completing (thesis 3.10).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::node) struct PendingLeadershipTransfer {
    pub target: NodeId,
    pub ticks_remaining: u64,
    pub timeout_now_sent: bool,
}

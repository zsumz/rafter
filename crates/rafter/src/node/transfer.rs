//! Leadership transfer initiation, catch-up, and immediate election.

use crate::{Message, NodeId, Term, TimeoutNow};

use super::replication::ReplicationDemand;
use super::state::PendingLeadershipTransfer;
use super::{LeadershipTransferRejection, Node, Output, Role};

impl Node {
    /// Initiates a leadership transfer to `target` (thesis 3.10): the leader
    /// stops accepting proposals, brings the target's log up to date, and
    /// then tells it to campaign immediately.
    pub(super) fn transfer_leadership(&mut self, target: NodeId) -> Vec<Output> {
        let rejection = |reason| vec![Output::LeadershipTransferRejected { target, reason }];
        if self.role() != Role::Leader {
            return rejection(LeadershipTransferRejection::NotLeader);
        }
        if target == self.id() {
            return rejection(LeadershipTransferRejection::TargetIsSelf);
        }
        if !self.is_effective_voter(target) {
            return rejection(LeadershipTransferRejection::TargetNotVoter);
        }
        if self.leader.pending_transfer.is_some() {
            return rejection(LeadershipTransferRejection::TransferAlreadyInProgress);
        }

        // The transfer aborts if it does not complete within one election
        // timeout, so a crashed or unreachable target cannot wedge the
        // leader in a proposal-rejecting state forever (thesis 3.10).
        self.leader.pending_transfer = Some(PendingLeadershipTransfer {
            target,
            ticks_remaining: self.config.election_timeout_ticks(),
            timeout_now_sent: false,
        });
        let mut outputs =
            self.drain_pending_reads(super::ReadIndexCancelReason::LeadershipTransfer { target });

        if self.target_is_caught_up(target) {
            outputs.extend(self.send_timeout_now(target));
            return outputs;
        }
        // Nudge the target's replication; completion is detected when its
        // acknowledgement brings match_index to the leader's last index.
        self.replicate_to_follower(target, ReplicationDemand::EnsureContact, &mut outputs);
        outputs
    }

    /// Called from the leader's tick: counts the pending transfer down and
    /// abandons it once the election timeout has elapsed.
    pub(super) fn tick_leadership_transfer(&mut self) {
        let Some(transfer) = self.leader.pending_transfer.as_mut() else {
            return;
        };
        transfer.ticks_remaining = transfer.ticks_remaining.saturating_sub(1);
        if transfer.ticks_remaining == 0 {
            self.leader.pending_transfer = None;
        }
    }

    /// Called after a follower acknowledgement advances its match index:
    /// completes the pending transfer once the target has caught up.
    pub(super) fn maybe_complete_leadership_transfer(
        &mut self,
        follower_id: NodeId,
    ) -> Vec<Output> {
        let Some(transfer) = self.leader.pending_transfer.as_ref() else {
            return Vec::new();
        };
        if transfer.timeout_now_sent
            || transfer.target != follower_id
            || !self.target_is_caught_up(follower_id)
        {
            return Vec::new();
        }
        self.send_timeout_now(follower_id)
    }

    /// Emits the deposition authorization, and records that this leadership
    /// has emitted one.
    ///
    /// The flag is armed here rather than in `transfer_leadership` because
    /// this is where the authorization stops being local: a transfer whose
    /// target never catches up writes no `TimeoutNow`, waives nothing, and
    /// leaves the lease intact when it times out. Once the message exists the
    /// waiver is permanent for this term — see `LeaderState`.
    fn send_timeout_now(&mut self, target: NodeId) -> Vec<Output> {
        self.leader.deposition_authorized = true;
        if let Some(transfer) = self.leader.pending_transfer.as_mut() {
            transfer.timeout_now_sent = true;
        }
        vec![Output::Send {
            to: target,
            message: Message::TimeoutNow(TimeoutNow {
                term: self.current_term(),
                leader_id: self.id(),
            }),
        }]
    }

    fn target_is_caught_up(&self, target: NodeId) -> bool {
        self.leader
            .progress
            .get(target)
            .map(|progress| progress.match_index)
            .unwrap_or_default()
            == self.last_log_index()
    }

    /// A `TimeoutNow` from the current leader instructs this node to campaign
    /// immediately: the real, term-incrementing election, bypassing pre-vote
    /// and leader stickiness — that bypass is the message's entire purpose
    /// (thesis 3.10).
    ///
    /// What the bypass costs the *sender* is recorded on `LeaderState`: the
    /// leader-lease safety argument is that voters refuse to depose a live
    /// leader, and this message is the standing waiver of that refusal. It is
    /// honored here for as long as it names a term this node has not passed,
    /// which is why the sender's waiver is term-scoped too.
    pub(super) fn handle_timeout_now(&mut self, term: Term) -> Vec<Output> {
        if term < self.current_term() || !self.is_effective_voter(self.id()) {
            return Vec::new();
        }
        // A recipient that still believes itself leader of an older term must
        // shed that leadership (and all per-term leader state) before
        // campaigning: become_leader is always preceded by become_follower.
        let mut outputs = self.become_follower(term);
        outputs.extend(self.start_election());
        outputs
    }
}

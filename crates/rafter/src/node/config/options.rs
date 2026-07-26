//! Builder options and effective configuration accessors.

use crate::{MembershipConfig, NodeId};

use super::{features::EffectiveFeatures, NodeConfig};

impl NodeConfig {
    /// Sets the append-entries batch byte budget.
    #[must_use]
    pub fn with_max_append_entries_bytes(mut self, max_append_entries_bytes: usize) -> Self {
        self.max_append_entries_bytes = max_append_entries_bytes;
        self
    }

    /// Bounds the per-follower window of unacknowledged append batches.
    ///
    /// A value of zero behaves as one: at least one append is always in
    /// flight, or replication could never make progress.
    #[must_use]
    pub fn with_max_inflight_appends(mut self, max_inflight_appends: usize) -> Self {
        self.max_inflight_appends = max_inflight_appends;
        self
    }

    /// Bounds the per-follower window of unacknowledged append payload bytes.
    ///
    /// One batch is always admissible regardless of this budget, so a batch
    /// larger than the budget cannot wedge replication.
    #[must_use]
    pub fn with_max_inflight_bytes(mut self, max_inflight_bytes: usize) -> Self {
        self.max_inflight_bytes = max_inflight_bytes;
        self
    }

    /// Records the requested leader heartbeat interval.
    ///
    /// A value of zero behaves as one. When check-quorum is effective, the
    /// accessor clamps this interval below the election timeout so a leader
    /// can refresh quorum evidence before the step-down check fires. The
    /// unclamped request remains part of the configuration and becomes visible
    /// again if check-quorum is disabled.
    #[must_use]
    pub fn with_heartbeat_interval_ticks(mut self, heartbeat_interval_ticks: u64) -> Self {
        self.requested_heartbeat_interval_ticks = heartbeat_interval_ticks;
        self
    }

    /// Requests the pre-vote protocol extension (thesis 9.6).
    ///
    /// Election timeouts first poll a quorum at the proposed next term. The
    /// node increments its durable term and votes for itself only after that
    /// non-binding poll succeeds.
    ///
    /// Enabled by default—production posture is the default posture. Pass
    /// `false` for the minimal-protocol configuration.
    #[must_use]
    pub fn with_pre_vote(mut self, pre_vote: bool) -> Self {
        self.requested_features.request_pre_vote(pre_vote);
        self
    }

    /// Requests check-quorum leader fencing (thesis 6.2).
    ///
    /// A leader that has not heard from a quorum within one election timeout
    /// steps down, bounding how long an isolated leader can believe in itself.
    /// Check-quorum is effective only when the timeout permits a leader
    /// broadcast before the quorum window closes. A one-tick timeout retains
    /// the request but makes it ineffective.
    ///
    /// Enabled by default—production posture is the default posture. Pass
    /// `false` for the minimal-protocol configuration.
    #[must_use]
    pub fn with_check_quorum(mut self, check_quorum: bool) -> Self {
        self.requested_features.request_check_quorum(check_quorum);
        self
    }

    /// Requests leader-lease reads (thesis 6.4.2).
    ///
    /// A leader whose authority a quorum acknowledged within half an election
    /// timeout may grant read barriers without another round trip. Outside
    /// that window, reads fall back to normal `ReadIndex` confirmation.
    ///
    /// # Safety assumption
    ///
    /// Lease reads are linearizable only if no node's tick cadence runs more
    /// than twice as fast as another's. Ticks stand in for wall-clock progress
    /// in this sans-I/O kernel, so the assumption concerns how frequently each
    /// process drives its node rather than system-clock synchronization.
    ///
    /// The lease also relies on voters refusing to depose a live leader. The
    /// request therefore becomes effective only while pre-vote and
    /// check-quorum are both effective. Disabling either foundation suspends
    /// lease behavior without erasing the request; restoring both makes it
    /// effective again.
    ///
    /// The same refusal is what a leader waives when it initiates a
    /// leadership transfer that reaches its target: `TimeoutNow` instructs one
    /// voter to campaign without a pre-vote poll, and the network bounds no
    /// message's delay. A leader that has emitted one therefore grants no
    /// further lease reads for the remainder of that term, and falls back to
    /// the quorum `ReadIndex` round trip, whose evidence does not depend on
    /// the refusal. Abandoning the transfer locally does not restore the
    /// lease; only a new term does.
    #[must_use]
    pub fn with_lease_reads(mut self, lease_reads: bool) -> Self {
        self.requested_features.request_lease_reads(lease_reads);
        self
    }

    /// Spreads election timeouts deterministically over `0..=jitter` ticks.
    ///
    /// The offset is derived from node ID and current term, so symmetric
    /// candidates stop splitting votes while deterministic replays remain
    /// exact.
    #[must_use]
    pub fn with_election_jitter_ticks(mut self, jitter: u64) -> Self {
        self.election_jitter_ticks = jitter;
        self
    }

    /// Returns this node's ID.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Returns the static peer IDs known at startup.
    #[must_use]
    pub fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    /// Returns the configured base election timeout.
    #[must_use]
    pub fn election_timeout_ticks(&self) -> u64 {
        self.election_timeout_ticks
    }

    /// Returns the append-entries batch byte budget.
    #[must_use]
    pub fn max_append_entries_bytes(&self) -> usize {
        self.max_append_entries_bytes
    }

    /// Returns the maximum in-flight append batches per follower.
    #[must_use]
    pub fn max_inflight_appends(&self) -> usize {
        self.max_inflight_appends
    }

    /// Returns the maximum in-flight append bytes per follower.
    #[must_use]
    pub fn max_inflight_bytes(&self) -> usize {
        self.max_inflight_bytes
    }

    /// Returns the effective heartbeat interval.
    ///
    /// The result is at least one tick and, while check-quorum is effective,
    /// remains below the election timeout.
    #[must_use]
    pub fn heartbeat_interval_ticks(&self) -> u64 {
        let requested = self.requested_heartbeat_interval_ticks.max(1);
        if self.check_quorum() {
            requested.min(self.election_timeout_ticks.saturating_sub(1).max(1))
        } else {
            requested
        }
    }

    /// Returns whether pre-vote is effective.
    #[must_use]
    pub fn pre_vote(&self) -> bool {
        self.effective_features().pre_vote()
    }

    /// Returns whether check-quorum is effective under the configured timeout.
    #[must_use]
    pub fn check_quorum(&self) -> bool {
        self.effective_features().check_quorum()
    }

    /// Returns whether leader-lease reads are effective.
    ///
    /// Lease reads require an affirmative request plus effective pre-vote and
    /// check-quorum behavior.
    #[must_use]
    pub fn lease_reads(&self) -> bool {
        self.effective_features().lease_reads()
    }

    const fn effective_features(&self) -> EffectiveFeatures {
        self.requested_features
            .effective(self.election_timeout_ticks)
    }

    /// Returns the lease window: half the election timeout, in local ticks.
    #[must_use]
    pub fn read_lease_ticks(&self) -> u64 {
        self.election_timeout_ticks / 2
    }

    /// Returns the deterministic election jitter range.
    #[must_use]
    pub fn election_jitter_ticks(&self) -> u64 {
        self.election_jitter_ticks
    }

    /// Returns the static voter IDs.
    pub fn voters(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.static_voters.iter().copied()
    }

    pub(in crate::node) fn is_peer(&self, node_id: NodeId) -> bool {
        self.peers.contains(&node_id)
    }

    pub(in crate::node) fn static_membership(&self) -> MembershipConfig {
        self.static_membership.clone()
    }

    pub(in crate::node) const fn static_membership_ref(&self) -> &MembershipConfig {
        &self.static_membership
    }

    /// Returns the static quorum size.
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        (self.static_voters.len() / 2) + 1
    }
}

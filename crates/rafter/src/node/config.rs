use std::{collections::BTreeSet, error::Error, fmt};

use crate::{MembershipConfig, MembershipSet, NodeId};

/// Default append-entries batch budget. The budget bounds batch size beyond
/// the first entry; a single entry may exceed it, so transports with hard
/// frame limits must size them for the largest admissible entry (client
/// proposals are capped at admission by this same budget).
pub const DEFAULT_MAX_APPEND_ENTRIES_BYTES: usize = 512 * 1024;

/// Default per-follower in-flight append window, in batches. A window of one
/// serializes replication on the round trip; the default pipelines eight
/// batches so a lagging follower catches up at wire speed instead of
/// ack-pace.
pub const DEFAULT_MAX_INFLIGHT_APPENDS: usize = 8;

/// Default per-follower in-flight append window, in payload bytes: the
/// default batch window at the default batch budget.
pub const DEFAULT_MAX_INFLIGHT_BYTES: usize =
    DEFAULT_MAX_INFLIGHT_APPENDS * DEFAULT_MAX_APPEND_ENTRIES_BYTES;

/// Default leader heartbeat interval. One preserves the historical behavior:
/// every leader tick broadcasts `AppendEntries`.
pub const DEFAULT_HEARTBEAT_INTERVAL_TICKS: u64 = 1;

/// Check-quorum needs at least one leader tick to broadcast before the
/// election-timeout window can close.
pub const MIN_CHECK_QUORUM_ELECTION_TIMEOUT_TICKS: u64 = 2;

/// Static configuration for one Raft node.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NodeConfig {
    id: NodeId,
    peers: Vec<NodeId>,
    static_voters: Vec<NodeId>,
    static_membership: MembershipConfig,
    election_timeout_ticks: u64,
    max_append_entries_bytes: usize,
    max_inflight_appends: usize,
    max_inflight_bytes: usize,
    heartbeat_interval_ticks: u64,
    pre_vote: bool,
    check_quorum: bool,
    lease_reads: bool,
    election_jitter_ticks: u64,
}

/// Error returned while building a [`NodeConfig`].
///
/// This enum is exhaustive because node configuration validation is closed
/// over these structural errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeConfigError {
    EmptyVoters,
    SelfPeer {
        id: NodeId,
    },
    DuplicatePeer {
        peer: NodeId,
    },
    /// A zero election timeout can never fire; it was previously accepted
    /// silently and produced a node that never campaigns.
    ZeroElectionTimeout,
}

impl NodeConfig {
    /// Constructs a static Raft voting configuration.
    ///
    /// `election_timeout_ticks` must be at least one. Production
    /// configurations should use a timeout of at least three ticks and keep
    /// the heartbeat interval lower than the election timeout. A one-tick
    /// timeout is accepted for tests, benchmarks, and tightly controlled
    /// single-process harnesses, but it disables effective check-quorum and
    /// lease-read fast paths because a leader would otherwise step down
    /// before it can send a heartbeat.
    ///
    /// # Errors
    ///
    /// Returns [`NodeConfigError`] when `peers` contains the local node ID or
    /// repeats a peer ID.
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        election_timeout_ticks: u64,
    ) -> Result<Self, NodeConfigError> {
        if election_timeout_ticks == 0 {
            return Err(NodeConfigError::ZeroElectionTimeout);
        }
        validate_peers(id, &peers)?;
        let static_voters: Vec<_> = std::iter::once(id).chain(peers.iter().copied()).collect();
        let static_membership = static_membership(static_voters.clone())?;

        Ok(Self {
            id,
            peers,
            static_voters,
            static_membership,
            election_timeout_ticks,
            max_append_entries_bytes: DEFAULT_MAX_APPEND_ENTRIES_BYTES,
            max_inflight_appends: DEFAULT_MAX_INFLIGHT_APPENDS,
            max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,
            heartbeat_interval_ticks: DEFAULT_HEARTBEAT_INTERVAL_TICKS,
            pre_vote: true,
            check_quorum: true,
            lease_reads: false,
            election_jitter_ticks: 0,
        })
    }

    /// Constructs a non-voting local replica that knows the supplied static
    /// voters but does not count itself in the static membership.
    ///
    /// This is used by authorized future learner processes before a committed
    /// configuration entry makes them a learner or voter.
    ///
    /// `election_timeout_ticks` follows the same guidance as
    /// [`NodeConfig::new`]: use at least three ticks for production, and at
    /// least two ticks when check-quorum or lease-read behavior matters.
    ///
    /// # Errors
    ///
    /// Returns [`NodeConfigError`] when `voters` is empty, contains the local
    /// node ID, or repeats a voter ID.
    pub fn new_non_voter(
        id: NodeId,
        voters: Vec<NodeId>,
        election_timeout_ticks: u64,
    ) -> Result<Self, NodeConfigError> {
        if voters.is_empty() {
            return Err(NodeConfigError::EmptyVoters);
        }
        if election_timeout_ticks == 0 {
            return Err(NodeConfigError::ZeroElectionTimeout);
        }
        validate_peers(id, &voters)?;
        let static_voters = voters.clone();
        let static_membership = static_membership(voters.clone())?;

        Ok(Self {
            id,
            peers: voters,
            static_voters,
            static_membership,
            election_timeout_ticks,
            max_append_entries_bytes: DEFAULT_MAX_APPEND_ENTRIES_BYTES,
            max_inflight_appends: DEFAULT_MAX_INFLIGHT_APPENDS,
            max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,
            heartbeat_interval_ticks: DEFAULT_HEARTBEAT_INTERVAL_TICKS,
            pre_vote: true,
            check_quorum: true,
            lease_reads: false,
            election_jitter_ticks: 0,
        })
    }

    /// Sets the append-entries batch byte budget.
    #[must_use]
    pub fn with_max_append_entries_bytes(mut self, max_append_entries_bytes: usize) -> Self {
        self.max_append_entries_bytes = max_append_entries_bytes;
        self
    }

    /// Bounds the per-follower window of unacknowledged append batches. A
    /// value of zero behaves as one: at least one append is always in
    /// flight, or replication could never make progress.
    #[must_use]
    pub fn with_max_inflight_appends(mut self, max_inflight_appends: usize) -> Self {
        self.max_inflight_appends = max_inflight_appends;
        self
    }

    /// Bounds the per-follower window of unacknowledged append payload
    /// bytes. One batch is always admissible regardless of this budget, so a
    /// batch larger than the budget cannot wedge replication.
    #[must_use]
    pub fn with_max_inflight_bytes(mut self, max_inflight_bytes: usize) -> Self {
        self.max_inflight_bytes = max_inflight_bytes;
        self
    }

    /// Configures how many leader ticks may coalesce before broadcasting a
    /// heartbeat round.
    ///
    /// A value of zero behaves as one. When check-quorum is enabled, the
    /// effective interval is clamped below the election timeout so a leader
    /// can still refresh quorum evidence before the step-down check fires.
    #[must_use]
    pub fn with_heartbeat_interval_ticks(mut self, heartbeat_interval_ticks: u64) -> Self {
        self.heartbeat_interval_ticks = heartbeat_interval_ticks;
        self
    }

    /// Configures the pre-vote protocol extension (thesis 9.6): election
    /// timeouts first poll a quorum at the proposed next term and only start
    /// a real, term-incrementing election once that poll succeeds.
    ///
    /// Enabled by default — production posture is the default posture. Pass
    /// `false` for the minimal-protocol configuration.
    #[must_use]
    pub fn with_pre_vote(mut self, pre_vote: bool) -> Self {
        self.pre_vote = pre_vote;
        self
    }

    /// Configures check-quorum: a leader that has not heard from a quorum
    /// within one election timeout steps down (thesis 6.2), bounding how
    /// long an isolated leader can believe in itself.
    ///
    /// Check-quorum is effective only when the election timeout is at least
    /// [`MIN_CHECK_QUORUM_ELECTION_TIMEOUT_TICKS`]. A one-tick timeout is
    /// normalized to check-quorum off so a leader cannot step down before it
    /// has any tick on which to broadcast a heartbeat.
    ///
    /// Enabled by default — production posture is the default posture. Pass
    /// `false` for the minimal-protocol configuration.
    #[must_use]
    pub fn with_check_quorum(mut self, check_quorum: bool) -> Self {
        self.check_quorum = check_quorum;
        self
    }

    /// Enables leader-lease reads (thesis 6.4.2): a leader whose leadership
    /// a quorum acknowledged within the lease window — half the election
    /// timeout, measured in this leader's own ticks — grants read barriers
    /// immediately, with no quorum round trip. Outside the window, barriers
    /// fall back to the read-index protocol unchanged.
    ///
    /// SAFETY ASSUMPTION, stated plainly: lease reads are only linearizable
    /// if no node's tick clock runs more than twice as fast as another's
    /// (the window is half the election timeout, so a 2x tick-rate skew is
    /// the exact bound). Ticks replace wall clocks everywhere in this
    /// kernel, so the assumption is about the cadence at which processes
    /// drive their nodes, not about system clocks. The lease also leans on
    /// voters refusing to depose a live leader, so this opt-in only takes
    /// effect when [`NodeConfig::with_pre_vote`] and
    /// [`NodeConfig::with_check_quorum`] are both enabled — without them,
    /// [`NodeConfig::lease_reads`] reports false and every barrier takes
    /// the read-index round trip.
    #[must_use]
    pub fn with_lease_reads(mut self, lease_reads: bool) -> Self {
        self.lease_reads = lease_reads;
        self
    }

    /// Spreads election timeouts deterministically: each term, a node waits
    /// its base timeout plus an offset in `0..=jitter` derived from its id
    /// and the current term — randomization with no clock and no RNG, so
    /// symmetric candidates stop splitting votes while replays stay exact.
    #[must_use]
    pub fn with_election_jitter_ticks(mut self, jitter: u64) -> Self {
        self.election_jitter_ticks = jitter;
        self
    }

    /// Returns this node's id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Returns the static peer ids known at startup.
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
    #[must_use]
    pub fn heartbeat_interval_ticks(&self) -> u64 {
        let requested = self.heartbeat_interval_ticks.max(1);
        if self.check_quorum() {
            requested.min(self.election_timeout_ticks.saturating_sub(1).max(1))
        } else {
            requested
        }
    }

    /// Returns whether pre-vote is enabled.
    #[must_use]
    pub fn pre_vote(&self) -> bool {
        self.pre_vote
    }

    /// Returns whether check-quorum is effective.
    #[must_use]
    pub fn check_quorum(&self) -> bool {
        self.check_quorum && self.election_timeout_ticks >= MIN_CHECK_QUORUM_ELECTION_TIMEOUT_TICKS
    }

    /// Whether lease reads are effective: opted in AND running on the
    /// pre-vote + check-quorum foundation the lease's safety argument
    /// requires.
    #[must_use]
    pub fn lease_reads(&self) -> bool {
        self.lease_reads && self.pre_vote && self.check_quorum()
    }

    /// The lease window: half the election timeout, in ticks. A window of
    /// zero (a one-tick election timeout) never holds a lease.
    #[must_use]
    pub fn read_lease_ticks(&self) -> u64 {
        self.election_timeout_ticks / 2
    }

    /// Returns the deterministic election jitter range.
    #[must_use]
    pub fn election_jitter_ticks(&self) -> u64 {
        self.election_jitter_ticks
    }

    /// Returns the static voter ids.
    pub fn voters(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.static_voters.iter().copied()
    }

    pub(super) fn is_peer(&self, node_id: NodeId) -> bool {
        self.peers.contains(&node_id)
    }

    pub(super) fn static_membership(&self) -> MembershipConfig {
        self.static_membership.clone()
    }

    /// Returns the static quorum size.
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        (self.voters().count() / 2) + 1
    }
}

fn validate_peers(id: NodeId, peers: &[NodeId]) -> Result<(), NodeConfigError> {
    let mut seen = BTreeSet::new();
    for peer in peers.iter().copied() {
        if peer == id {
            return Err(NodeConfigError::SelfPeer { id });
        }
        if !seen.insert(peer) {
            return Err(NodeConfigError::DuplicatePeer { peer });
        }
    }
    Ok(())
}

fn static_membership(voters: Vec<NodeId>) -> Result<MembershipConfig, NodeConfigError> {
    MembershipSet::new(voters, Vec::new())
        .map(MembershipConfig::stable)
        .map_err(|_| NodeConfigError::EmptyVoters)
}

impl fmt::Display for NodeConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyVoters => write!(formatter, "Raft node config requires at least one voter"),
            Self::SelfPeer { id } => {
                write!(formatter, "Raft node {id} cannot list itself as a peer")
            }
            Self::DuplicatePeer { peer } => {
                write!(formatter, "Raft peer {peer} appears more than once")
            }
            Self::ZeroElectionTimeout => {
                formatter.write_str("election timeout must be at least one tick")
            }
        }
    }
}

impl Error for NodeConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_self_as_peer() {
        assert_eq!(
            NodeConfig::new(NodeId(1), vec![NodeId(1), NodeId(2)], 3),
            Err(NodeConfigError::SelfPeer { id: NodeId(1) })
        );
    }

    #[test]
    fn config_rejects_duplicate_peers() {
        assert_eq!(
            NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(2)], 3),
            Err(NodeConfigError::DuplicatePeer { peer: NodeId(2) })
        );
    }

    #[test]
    fn non_voter_config_excludes_local_node_from_static_membership() {
        let config = NodeConfig::new_non_voter(NodeId(3), vec![NodeId(1), NodeId(2)], 3)
            .expect("future learner config is valid");

        assert_eq!(config.peers(), &[NodeId(1), NodeId(2)]);
        assert_eq!(
            config.voters().collect::<Vec<_>>(),
            vec![NodeId(1), NodeId(2)]
        );
        assert!(!config.static_membership().contains_voter(NodeId(3)));
    }

    #[test]
    fn non_voter_config_rejects_empty_voters() {
        assert_eq!(
            NodeConfig::new_non_voter(NodeId(3), Vec::new(), 3),
            Err(NodeConfigError::EmptyVoters)
        );
    }

    #[test]
    fn heartbeat_interval_defaults_to_one_and_clamps_before_check_quorum() {
        let default = NodeConfig::new(NodeId(1), vec![NodeId(2)], 3).expect("valid config");
        assert_eq!(default.heartbeat_interval_ticks(), 1);

        let clamped = NodeConfig::new(NodeId(1), vec![NodeId(2)], 3)
            .expect("valid config")
            .with_heartbeat_interval_ticks(99);
        assert_eq!(clamped.heartbeat_interval_ticks(), 2);

        let disabled_check_quorum = NodeConfig::new(NodeId(1), vec![NodeId(2)], 3)
            .expect("valid config")
            .with_check_quorum(false)
            .with_heartbeat_interval_ticks(99);
        assert_eq!(disabled_check_quorum.heartbeat_interval_ticks(), 99);

        let zero = NodeConfig::new(NodeId(1), vec![NodeId(2)], 3)
            .expect("valid config")
            .with_heartbeat_interval_ticks(0);
        assert_eq!(zero.heartbeat_interval_ticks(), 1);
    }

    #[test]
    fn one_tick_election_timeout_normalizes_check_quorum_off() {
        let tiny = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 1)
            .expect("one-tick timeout remains valid for controlled harnesses");

        assert!(!tiny.check_quorum());
        assert!(!tiny.with_lease_reads(true).lease_reads());
    }
}

//! Construction and structural validation for [`NodeConfig`](super::NodeConfig).

use std::collections::BTreeSet;

use crate::{MembershipConfig, MembershipSet, NodeId};

use super::{
    NodeConfig, NodeConfigError, RequestedFeatures, DEFAULT_HEARTBEAT_INTERVAL_TICKS,
    DEFAULT_MAX_APPEND_ENTRIES_BYTES, DEFAULT_MAX_INFLIGHT_APPENDS, DEFAULT_MAX_INFLIGHT_BYTES,
};

impl NodeConfig {
    /// Constructs a static Raft voting configuration.
    ///
    /// `election_timeout_ticks` must be at least one. Production configurations
    /// should use at least three ticks and keep the heartbeat interval below the
    /// election timeout. A one-tick timeout remains useful for controlled tests,
    /// but makes check-quorum and lease reads ineffective because a leader has
    /// no tick on which to refresh quorum evidence before the window closes.
    ///
    /// ```
    /// use rafter::{NodeConfig, NodeConfigError, NodeId};
    ///
    /// // The local node is always a voter; `peers` names only the others, so
    /// // this is a three-voter group whose quorum is two.
    /// let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 10)
    ///     .expect("valid raft config");
    /// assert_eq!(config.quorum_size(), 2);
    /// assert!(config.pre_vote(), "pre-vote and check-quorum are on by default");
    /// assert!(config.check_quorum());
    ///
    /// // Listing the local node among its own peers is a deployment mistake
    /// // rather than a two-voter group, so it is refused instead of deduplicated.
    /// assert_eq!(
    ///     NodeConfig::new(NodeId(1), vec![NodeId(1)], 10),
    ///     Err(NodeConfigError::SelfPeer { id: NodeId(1) }),
    /// );
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`NodeConfigError`] when `peers` contains the local node ID,
    /// repeats a peer ID, or the timeout is zero.
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        election_timeout_ticks: u64,
    ) -> Result<Self, NodeConfigError> {
        validate_election_timeout(election_timeout_ticks)?;
        validate_peers(id, &peers)?;

        let static_voters = std::iter::once(id)
            .chain(peers.iter().copied())
            .collect::<Vec<_>>();
        let static_membership = static_membership(static_voters.clone())?;

        Ok(Self::from_validated_membership(
            id,
            peers,
            static_voters,
            static_membership,
            election_timeout_ticks,
        ))
    }

    /// Constructs a non-voting local replica that knows the supplied static
    /// voters but does not count itself in the static membership.
    ///
    /// This supports an authorized future learner process before a committed
    /// configuration entry makes it a learner or voter. The timeout follows the
    /// same production guidance as [`NodeConfig::new`].
    ///
    /// # Errors
    ///
    /// Returns [`NodeConfigError`] when `voters` is empty, contains the local
    /// node ID, repeats a voter ID, or the timeout is zero.
    pub fn new_non_voter(
        id: NodeId,
        voters: Vec<NodeId>,
        election_timeout_ticks: u64,
    ) -> Result<Self, NodeConfigError> {
        if voters.is_empty() {
            return Err(NodeConfigError::EmptyVoters);
        }
        validate_election_timeout(election_timeout_ticks)?;
        validate_peers(id, &voters)?;

        let static_voters = voters.clone();
        let static_membership = static_membership(static_voters.clone())?;

        Ok(Self::from_validated_membership(
            id,
            voters,
            static_voters,
            static_membership,
            election_timeout_ticks,
        ))
    }

    fn from_validated_membership(
        id: NodeId,
        peers: Vec<NodeId>,
        static_voters: Vec<NodeId>,
        static_membership: MembershipConfig,
        election_timeout_ticks: u64,
    ) -> Self {
        Self {
            id,
            peers,
            static_voters,
            static_membership,

            election_timeout_ticks,
            election_jitter_ticks: 0,
            requested_heartbeat_interval_ticks: DEFAULT_HEARTBEAT_INTERVAL_TICKS,

            max_append_entries_bytes: DEFAULT_MAX_APPEND_ENTRIES_BYTES,
            max_inflight_appends: DEFAULT_MAX_INFLIGHT_APPENDS,
            max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,

            requested_features: RequestedFeatures::default(),
        }
    }
}

fn validate_election_timeout(election_timeout_ticks: u64) -> Result<(), NodeConfigError> {
    if election_timeout_ticks == 0 {
        Err(NodeConfigError::ZeroElectionTimeout)
    } else {
        Ok(())
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

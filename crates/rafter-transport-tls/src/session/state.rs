//! Pure durable connection-session high-water state.

use std::{collections::BTreeMap, error::Error, fmt};

use crate::{PeerId, SessionStoreLimits};

use super::ConnectionSession;

/// Durable high-water marks retained for one authenticated physical peer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PeerSessionState {
    outbound: Option<ConnectionSession>,
    inbound: Option<ConnectionSession>,
}

impl PeerSessionState {
    /// Creates one peer record from independently recovered high-water marks.
    #[must_use]
    pub const fn new(
        outbound: Option<ConnectionSession>,
        inbound: Option<ConnectionSession>,
    ) -> Self {
        Self { outbound, inbound }
    }

    /// Highest outbound session durably allocated for this peer.
    #[must_use]
    pub const fn highest_outbound(self) -> Option<ConnectionSession> {
        self.outbound
    }

    /// Highest inbound session durably accepted from this peer.
    #[must_use]
    pub const fn highest_inbound(self) -> Option<ConnectionSession> {
        self.inbound
    }

    /// Returns whether neither direction has ever established a session.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.outbound.is_none() && self.inbound.is_none()
    }
}

/// Result of comparing one inbound hello session with its durable high-water.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InboundSessionDecision {
    /// The session was newer and became the accepted durable high-water.
    Accepted {
        /// Previously accepted session, or `None` for the first connection.
        previous: Option<ConnectionSession>,
    },
    /// The session was equal to or older than the durable high-water.
    Stale {
        /// Highest session already accepted from the peer.
        highest_accepted: ConnectionSession,
    },
}

impl InboundSessionDecision {
    /// Returns whether this decision accepted and advanced the high-water mark.
    #[must_use]
    pub const fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted { .. })
    }
}

/// Mutable pure state behind durable and in-memory session stores.
///
/// Peer records are permanent high-water marks. This type intentionally has no
/// removal operation because reusing the same [`PeerId`] after forgetting its
/// record would reopen accepted connection sessions to replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportSessionState {
    limits: SessionStoreLimits,
    peers: BTreeMap<PeerId, PeerSessionState>,
}

impl TransportSessionState {
    /// Creates empty session state with a finite physical-peer bound.
    #[must_use]
    pub fn new(limits: SessionStoreLimits) -> Self {
        Self {
            limits,
            peers: BTreeMap::new(),
        }
    }

    /// Reconstructs validated state from durable peer records.
    ///
    /// # Errors
    ///
    /// Returns [`SessionStateError`] when records exceed the configured bound
    /// or contain an empty record that should not have been persisted.
    pub fn from_peer_states(
        limits: SessionStoreLimits,
        peers: BTreeMap<PeerId, PeerSessionState>,
    ) -> Result<Self, SessionStateError> {
        if peers.len() > limits.max_peer_records() {
            return Err(SessionStateError::PeerLimit {
                maximum: limits.max_peer_records(),
            });
        }
        if let Some((peer, _)) = peers.iter().find(|(_, state)| state.is_empty()) {
            return Err(SessionStateError::EmptyPeerRecord { peer: peer.clone() });
        }
        Ok(Self { limits, peers })
    }

    pub(crate) fn from_canonical_peer_states(
        limits: SessionStoreLimits,
        peers: BTreeMap<PeerId, PeerSessionState>,
    ) -> Self {
        debug_assert!(peers.len() <= limits.max_peer_records());
        debug_assert!(peers.values().all(|state| !state.is_empty()));
        Self { limits, peers }
    }

    /// Configured maximum number of physical-peer records.
    #[must_use]
    pub const fn limits(&self) -> SessionStoreLimits {
        self.limits
    }

    /// Number of peers whose session high-water marks are retained.
    #[must_use]
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Returns one peer's current high-water marks, or an empty state if absent.
    #[must_use]
    pub fn peer_state(&self, peer: &PeerId) -> PeerSessionState {
        self.peers.get(peer).copied().unwrap_or_default()
    }

    /// Iterates peer records in canonical `PeerId` byte order.
    #[must_use]
    pub fn peer_states(&self) -> impl ExactSizeIterator<Item = (&PeerId, PeerSessionState)> + '_ {
        self.peers.iter().map(|(peer, state)| (peer, *state))
    }

    /// Checks that a future outbound session for `peer` can be represented.
    ///
    /// The check is pure: an absent peer consumes no record until a session is
    /// actually allocated or accepted.
    ///
    /// # Errors
    ///
    /// Returns [`SessionStateError::PeerLimit`] for an absent peer at capacity,
    /// or [`SessionStateError::OutboundExhausted`] when no next session exists.
    pub fn preflight_peer(&self, peer: &PeerId) -> Result<(), SessionStateError> {
        self.ensure_peer_capacity(peer)?;
        if self
            .peer_state(peer)
            .highest_outbound()
            .is_some_and(|session| session.get() == u64::MAX)
        {
            return Err(SessionStateError::OutboundExhausted { peer: peer.clone() });
        }
        Ok(())
    }

    /// Checks aggregate capacity for one runtime's complete peer set.
    ///
    /// # Errors
    ///
    /// Returns the same capacity or exhaustion failures as [`Self::preflight_peer`].
    pub fn preflight_peers(&self, peers: &[PeerId]) -> Result<(), SessionStateError> {
        let mut absent = std::collections::BTreeSet::new();
        for peer in peers {
            if self.peers.contains_key(peer) {
                self.preflight_peer(peer)?;
            } else {
                absent.insert(peer);
            }
        }
        if self.peers.len().saturating_add(absent.len()) > self.limits.max_peer_records() {
            return Err(SessionStateError::PeerLimit {
                maximum: self.limits.max_peer_records(),
            });
        }
        Ok(())
    }

    /// Allocates the next outbound connection session for `peer`.
    ///
    /// The caller must durably publish the resulting state before putting the
    /// returned session on the wire.
    ///
    /// # Errors
    ///
    /// Returns [`SessionStateError::PeerLimit`] before adding a new peer beyond
    /// the configured bound, or [`SessionStateError::OutboundExhausted`] after
    /// session `u64::MAX` was allocated.
    pub fn allocate_outbound(
        &mut self,
        peer: &PeerId,
    ) -> Result<ConnectionSession, SessionStateError> {
        self.ensure_peer_capacity(peer)?;
        let current = self.peer_state(peer).highest_outbound();
        let next = match current {
            Some(current) => current
                .checked_next()
                .ok_or_else(|| SessionStateError::OutboundExhausted { peer: peer.clone() })?,
            None => ConnectionSession::FIRST,
        };
        let state = self.peers.entry(peer.clone()).or_default();
        state.outbound = Some(next);
        Ok(next)
    }

    /// Accepts only an inbound session newer than the durable peer high-water.
    ///
    /// A stale decision leaves state unchanged. An accepted decision advances
    /// state and must be durably published before the handshake is acknowledged.
    ///
    /// # Errors
    ///
    /// Returns [`SessionStateError::PeerLimit`] before retaining a new peer
    /// beyond the configured bound.
    pub fn accept_inbound(
        &mut self,
        peer: &PeerId,
        session: ConnectionSession,
    ) -> Result<InboundSessionDecision, SessionStateError> {
        let previous = self.peer_state(peer).highest_inbound();
        if let Some(highest_accepted) = previous {
            if session <= highest_accepted {
                return Ok(InboundSessionDecision::Stale { highest_accepted });
            }
        }
        self.ensure_peer_capacity(peer)?;
        let state = self.peers.entry(peer.clone()).or_default();
        state.inbound = Some(session);
        Ok(InboundSessionDecision::Accepted { previous })
    }

    fn ensure_peer_capacity(&self, peer: &PeerId) -> Result<(), SessionStateError> {
        if !self.peers.contains_key(peer) && self.peers.len() == self.limits.max_peer_records() {
            Err(SessionStateError::PeerLimit {
                maximum: self.limits.max_peer_records(),
            })
        } else {
            Ok(())
        }
    }
}

/// Pure session-state transition failure.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SessionStateError {
    /// Adding another physical peer would exceed the configured bound.
    PeerLimit {
        /// Maximum retained peer records.
        maximum: usize,
    },
    /// One peer consumed the complete outbound session number space.
    OutboundExhausted {
        /// Peer whose next session cannot be represented.
        peer: PeerId,
    },
    /// Recovered state contained a record with no high-water in either direction.
    EmptyPeerRecord {
        /// Peer named by the noncanonical empty record.
        peer: PeerId,
    },
}

impl fmt::Display for SessionStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerLimit { maximum } => write!(
                formatter,
                "session state already retains its maximum {maximum} peers"
            ),
            Self::OutboundExhausted { peer } => {
                write!(
                    formatter,
                    "outbound connection sessions for {peer} are exhausted"
                )
            }
            Self::EmptyPeerRecord { peer } => {
                write!(
                    formatter,
                    "session state contains an empty record for {peer}"
                )
            }
        }
    }
}

impl Error for SessionStateError {}

//! Bound for durable per-principal session records.

use super::{require_at_most, require_nonzero, LimitError, LimitKind};

/// Default number of durable physical-peer session records.
pub const DEFAULT_MAX_SESSION_PEER_RECORDS: usize = 128;
/// Largest durable peer-record bound representable by session-state version 1.
pub const MAX_SESSION_PEER_RECORDS: usize = 65_535;

/// Bound for durable per-principal connection-session records.
///
/// A retained high-water record is replay protection, not a cache entry. The
/// store never evicts one: forgetting a peer under the same [`crate::PeerId`]
/// would make an old connection session fresh again. This bound therefore
/// covers distinct physical principals over the state file's full lifetime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionStoreLimits {
    max_peer_records: usize,
}

impl SessionStoreLimits {
    /// Default durable peer-record bound.
    pub const DEFAULT: Self = Self {
        max_peer_records: DEFAULT_MAX_SESSION_PEER_RECORDS,
    };

    /// Largest peer-record bound representable by session-state version 1.
    pub const MAX: Self = Self {
        max_peer_records: MAX_SESSION_PEER_RECORDS,
    };

    /// Validates one durable peer-record bound.
    ///
    /// # Errors
    ///
    /// Returns [`LimitError`] when the bound is zero or cannot be represented
    /// by session-state version 1.
    pub fn new(max_peer_records: usize) -> Result<Self, LimitError> {
        require_nonzero(LimitKind::SessionPeers, max_peer_records)?;
        require_at_most(
            LimitKind::SessionPeers,
            max_peer_records,
            MAX_SESSION_PEER_RECORDS,
        )?;
        Ok(Self { max_peer_records })
    }

    /// Maximum physical-peer records retained by the durable session store.
    #[must_use]
    pub const fn max_peer_records(self) -> usize {
        self.max_peer_records
    }
}

impl Default for SessionStoreLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

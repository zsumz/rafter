//! Per-group mapping from authenticated principals to Raft identities.

mod error;
mod lease;
mod mutation;
mod query;
mod state;
mod validator;

use std::sync::{Arc, RwLock};

use rafter::NodeId;

use crate::{DirectoryLimits, PeerId};

pub use error::DirectoryError;
pub(crate) use lease::AuthorizationLease;
pub use query::PeerAuthorization;
pub(crate) use query::{InboundRoute, OutboundRoute};
use state::DirectoryState;

/// Snapshot of the policy currently enforced for one group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledPeerPolicy {
    authorized_peers: Vec<PeerId>,
    retirement_floor: Option<NodeId>,
}

impl InstalledPeerPolicy {
    /// Authorized principals in canonical order.
    #[must_use]
    pub fn authorized_peers(&self) -> &[PeerId] {
        &self.authorized_peers
    }

    /// Greatest node identity ever accepted in a committed group policy.
    #[must_use]
    pub const fn retirement_floor(&self) -> Option<NodeId> {
        self.retirement_floor
    }
}

/// Thread-safe per-group principal/node bindings and authorization policy.
///
/// Every group has a one-to-one live mapping between [`PeerId`] and [`NodeId`].
/// Mappings may differ across groups, which permits one authenticated physical
/// connection to multiplex independently numbered Raft groups.
///
/// Lock poisoning fails closed. Management methods report
/// [`DirectoryError::Poisoned`]; validator methods answer unknown or
/// unauthorized so an inbound frame cannot enter a managed driver.
pub struct TlsPeerDirectory<G> {
    limits: DirectoryLimits,
    state: Arc<RwLock<DirectoryState<G>>>,
}

impl<G> Clone for TlsPeerDirectory<G> {
    fn clone(&self) -> Self {
        Self {
            limits: self.limits,
            state: Arc::clone(&self.state),
        }
    }
}

impl<G> TlsPeerDirectory<G>
where
    G: Ord,
{
    /// Creates an empty fail-closed directory with finite limits.
    #[must_use]
    pub fn new(limits: DirectoryLimits) -> Self {
        Self {
            limits,
            state: Arc::new(RwLock::new(DirectoryState {
                groups: std::collections::BTreeMap::new(),
            })),
        }
    }

    /// Finite bounds enforced by this directory.
    #[must_use]
    pub const fn limits(&self) -> DirectoryLimits {
        self.limits
    }
}

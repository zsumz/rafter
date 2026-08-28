//! Bounded endpoint-book state.

mod mutation;

use std::{
    collections::BTreeMap,
    fmt,
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use crate::{EndpointBookLimits, PeerId};

use super::{EndpointBookError, TlsServerName};

/// One resolved address and the identity required during TLS verification.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PeerEndpoint {
    address: SocketAddr,
    server_name: TlsServerName,
}

impl PeerEndpoint {
    /// Creates one already-resolved peer endpoint.
    #[must_use]
    pub fn new(address: SocketAddr, server_name: TlsServerName) -> Self {
        Self {
            address,
            server_name,
        }
    }

    /// Resolved socket address.
    ///
    /// The transport never performs DNS under `RaftTransport::send`.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Canonical identity required by TLS server-name verification.
    #[must_use]
    pub const fn server_name(&self) -> &TlsServerName {
        &self.server_name
    }
}

/// Monotonic endpoint-book mutation or refresh generation.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct EndpointGeneration(u64);

impl EndpointGeneration {
    /// Returns the numeric generation. Zero means no mutation has occurred.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Immutable endpoint set observed at one installation generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointSnapshot {
    generation: EndpointGeneration,
    endpoints: Arc<[PeerEndpoint]>,
}

impl EndpointSnapshot {
    /// Generation at which this endpoint set was installed or refreshed.
    #[must_use]
    pub const fn generation(&self) -> EndpointGeneration {
        self.generation
    }

    /// Resolved endpoints in caller-supplied deterministic dial order.
    #[must_use]
    pub fn endpoints(&self) -> &[PeerEndpoint] {
        &self.endpoints
    }
}

/// Bounded caller-managed `PeerId -> endpoints` configuration.
///
/// Replacement is atomic. Sender workers compare generations before each send,
/// close a stale stream, and redial from the newly installed endpoint set.
#[derive(Clone)]
pub struct EndpointBook {
    limits: EndpointBookLimits,
    state: Arc<RwLock<EndpointBookState>>,
}

#[derive(Debug, Default)]
struct EndpointBookState {
    generation: EndpointGeneration,
    by_peer: BTreeMap<PeerId, EndpointEntry>,
}

#[derive(Debug)]
struct EndpointEntry {
    generation: EndpointGeneration,
    endpoints: Arc<[PeerEndpoint]>,
}

impl EndpointBook {
    /// Creates an empty endpoint book with finite limits.
    #[must_use]
    pub fn new(limits: EndpointBookLimits) -> Self {
        Self {
            limits,
            state: Arc::new(RwLock::new(EndpointBookState::default())),
        }
    }

    /// Finite bounds enforced by this book.
    #[must_use]
    pub const fn limits(&self) -> EndpointBookLimits {
        self.limits
    }

    /// Returns one immutable endpoint set and its installation generation.
    ///
    /// # Errors
    ///
    /// Returns [`EndpointBookError::Poisoned`] when shared state is poisoned.
    pub fn snapshot(
        &self,
        peer_id: &PeerId,
    ) -> Result<Option<EndpointSnapshot>, EndpointBookError> {
        let state = self.state.read().map_err(|_| EndpointBookError::Poisoned)?;
        Ok(state.by_peer.get(peer_id).map(|entry| EndpointSnapshot {
            generation: entry.generation,
            endpoints: entry.endpoints.clone(),
        }))
    }

    /// Returns the current global endpoint generation.
    ///
    /// # Errors
    ///
    /// Returns [`EndpointBookError::Poisoned`] when shared state is poisoned.
    pub fn generation(&self) -> Result<EndpointGeneration, EndpointBookError> {
        self.state
            .read()
            .map(|state| state.generation)
            .map_err(|_| EndpointBookError::Poisoned)
    }

    /// Returns every currently configured physical peer in canonical order.
    ///
    /// The blocking runtime uses this finite snapshot to establish exactly one
    /// persistent sender worker per peer at bind time. Replacing or removing an
    /// existing peer's endpoints remains live; adding a new peer requires a new
    /// runtime so worker ownership stays explicit and bounded.
    ///
    /// # Errors
    ///
    /// Returns [`EndpointBookError::Poisoned`] when shared state is poisoned.
    pub(crate) fn peer_ids(&self) -> Result<Vec<PeerId>, EndpointBookError> {
        self.state
            .read()
            .map(|state| state.by_peer.keys().cloned().collect())
            .map_err(|_| EndpointBookError::Poisoned)
    }
}

impl fmt::Debug for EndpointBook {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.state.read() {
            Ok(state) => formatter
                .debug_struct("EndpointBook")
                .field("limits", &self.limits)
                .field("generation", &state.generation)
                .field("peers", &state.by_peer.len())
                .finish(),
            Err(_) => formatter
                .debug_struct("EndpointBook")
                .field("limits", &self.limits)
                .field("state", &"poisoned")
                .finish(),
        }
    }
}

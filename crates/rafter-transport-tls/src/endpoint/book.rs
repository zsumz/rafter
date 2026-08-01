//! Bounded endpoint-book state.

use std::{
    collections::{BTreeMap, BTreeSet},
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

/// Monotonic endpoint-book generation.
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
    /// Generation at which this exact endpoint set was installed.
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
/// Replacement is atomic. Sender workers can compare generations before each
/// dial without retaining discovery state inside the transport.
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

    /// Atomically replaces one peer's ordered endpoint set.
    ///
    /// Replacing a set with equal values is idempotent and keeps that peer's
    /// current installation generation.
    ///
    /// # Errors
    ///
    /// Returns [`EndpointBookError`] for an empty or duplicate set, an exhausted
    /// bound or generation, or poisoned shared state.
    pub fn replace(
        &self,
        peer_id: PeerId,
        endpoints: Vec<PeerEndpoint>,
    ) -> Result<EndpointGeneration, EndpointBookError> {
        validate_endpoints(self.limits, &endpoints)?;
        let endpoints: Arc<[PeerEndpoint]> = endpoints.into();
        let mut state = self
            .state
            .write()
            .map_err(|_| EndpointBookError::Poisoned)?;

        if let Some(existing) = state.by_peer.get(&peer_id) {
            if existing.endpoints.as_ref() == endpoints.as_ref() {
                return Ok(existing.generation);
            }
        }
        if !state.by_peer.contains_key(&peer_id) && state.by_peer.len() >= self.limits.max_peers() {
            return Err(EndpointBookError::PeerLimit {
                maximum: self.limits.max_peers(),
            });
        }

        let generation = next_generation(state.generation)?;
        state.by_peer.insert(
            peer_id,
            EndpointEntry {
                generation,
                endpoints,
            },
        );
        state.generation = generation;
        Ok(generation)
    }

    /// Removes one peer's endpoints atomically.
    ///
    /// Returns `None` when the peer was already absent.
    ///
    /// # Errors
    ///
    /// Returns [`EndpointBookError`] when the generation is exhausted or shared
    /// state is poisoned.
    pub fn remove(
        &self,
        peer_id: &PeerId,
    ) -> Result<Option<EndpointGeneration>, EndpointBookError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| EndpointBookError::Poisoned)?;
        if !state.by_peer.contains_key(peer_id) {
            return Ok(None);
        }

        let generation = next_generation(state.generation)?;
        state.by_peer.remove(peer_id);
        state.generation = generation;
        Ok(Some(generation))
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

fn validate_endpoints(
    limits: EndpointBookLimits,
    endpoints: &[PeerEndpoint],
) -> Result<(), EndpointBookError> {
    if endpoints.is_empty() {
        return Err(EndpointBookError::Empty);
    }
    if endpoints.len() > limits.max_endpoints_per_peer() {
        return Err(EndpointBookError::EndpointLimit {
            actual: endpoints.len(),
            maximum: limits.max_endpoints_per_peer(),
        });
    }

    let mut seen = BTreeSet::new();
    for (index, endpoint) in endpoints.iter().enumerate() {
        if !seen.insert(endpoint) {
            return Err(EndpointBookError::Duplicate { index });
        }
    }
    Ok(())
}

fn next_generation(
    generation: EndpointGeneration,
) -> Result<EndpointGeneration, EndpointBookError> {
    generation
        .0
        .checked_add(1)
        .map(EndpointGeneration)
        .ok_or(EndpointBookError::GenerationExhausted)
}

//! Atomic endpoint-set replacement, removal, and refresh.
//!
//! This module owns every operation that advances an endpoint generation, plus
//! the set validation that must pass before a replacement is installed. It does
//! not read the book: snapshot and generation queries stay with the book.

use std::{collections::BTreeSet, sync::Arc};

use crate::{EndpointBookLimits, PeerId};

use super::{EndpointBook, EndpointBookError, EndpointEntry, EndpointGeneration, PeerEndpoint};

impl EndpointBook {
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

    /// Signals that one unchanged endpoint set should be tried again.
    ///
    /// This advances the peer and global generations without changing endpoint
    /// values. Discovery and operators use it after repairing a remote service
    /// in place so a configuration-blocked sender recovers deterministically.
    ///
    /// Returns `None` when the peer is absent.
    ///
    /// # Errors
    ///
    /// Returns [`EndpointBookError`] when the generation is exhausted or shared
    /// state is poisoned.
    pub fn refresh(
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
        let entry = state
            .by_peer
            .get_mut(peer_id)
            .ok_or(EndpointBookError::Poisoned)?;
        entry.generation = generation;
        state.generation = generation;
        Ok(Some(generation))
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

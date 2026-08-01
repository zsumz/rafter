//! Cloneable bounded receiver for authenticated peer envelopes.

use std::{fmt, sync::Arc};

use rafter_service::AuthenticatedPeerEnvelope;

use crate::queue::{InboundQueue, InboundQueueError};
use crate::{PeerId, TlsInboundError};

/// Caller-facing bounded authenticated-envelope receiver.
///
/// Draining never performs network I/O and returns at most the requested number
/// of already authenticated, decoded, and authorized envelopes.
pub struct TlsInbound<G> {
    pub(crate) queue: Arc<InboundQueue<G>>,
}

impl<G> Clone for TlsInbound<G> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

impl<G> fmt::Debug for TlsInbound<G> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("TlsInbound").finish_non_exhaustive()
    }
}

impl<G> TlsInbound<G> {
    /// Removes and returns at most `maximum` authenticated envelopes.
    ///
    /// A zero maximum is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`TlsInboundError::Poisoned`] if a panic made queue accounting
    /// untrustworthy.
    pub fn drain(
        &self,
        maximum: usize,
    ) -> Result<Vec<AuthenticatedPeerEnvelope<G, PeerId>>, TlsInboundError> {
        self.queue.drain(maximum).map_err(map_queue_error)
    }

    /// Returns the current authenticated-envelope count and byte use.
    ///
    /// # Errors
    ///
    /// Returns [`TlsInboundError::Poisoned`] if queue state is poisoned.
    pub fn depth(&self) -> Result<(usize, usize), TlsInboundError> {
        self.queue
            .depth()
            .map(|usage| (usage.frames, usage.bytes))
            .map_err(map_queue_error)
    }

    /// Returns one physical peer's current inbound count and byte use.
    ///
    /// # Errors
    ///
    /// Returns [`TlsInboundError::Poisoned`] if queue state is poisoned.
    pub fn peer_depth(&self, peer: &PeerId) -> Result<(usize, usize), TlsInboundError> {
        self.queue
            .peer_depth(peer)
            .map(|usage| (usage.frames, usage.bytes))
            .map_err(map_queue_error)
    }
}

const fn map_queue_error(_error: InboundQueueError) -> TlsInboundError {
    TlsInboundError::Poisoned
}

use std::{
    error::Error,
    fmt,
    sync::{Arc, Mutex, PoisonError},
};

use rafter_transport_tls::{
    ConnectionSession, InboundSessionDecision, PeerId, PeerSessionState, SessionStoreLimits,
    TransportSessionState, TransportSessionStore,
};

#[derive(Clone, Debug)]
pub struct MemorySessionStore {
    state: Arc<Mutex<TransportSessionState>>,
}

impl Default for MemorySessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemorySessionStore {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(TransportSessionState::new(
                SessionStoreLimits::default(),
            ))),
        }
    }

    pub fn peer_state(&self, peer: &PeerId) -> PeerSessionState {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .peer_state(peer)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemorySessionStoreError;

impl fmt::Display for MemorySessionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("memory session store failed")
    }
}

impl Error for MemorySessionStoreError {}

impl TransportSessionStore for MemorySessionStore {
    type Error = MemorySessionStoreError;

    fn allocate_outbound_session(&self, peer: &PeerId) -> Result<ConnectionSession, Self::Error> {
        self.state
            .lock()
            .map_err(|_| MemorySessionStoreError)?
            .allocate_outbound(peer)
            .map_err(|_| MemorySessionStoreError)
    }

    fn accept_inbound_session(
        &self,
        peer: &PeerId,
        session: ConnectionSession,
    ) -> Result<InboundSessionDecision, Self::Error> {
        self.state
            .lock()
            .map_err(|_| MemorySessionStoreError)?
            .accept_inbound(peer, session)
            .map_err(|_| MemorySessionStoreError)
    }

    fn peer_session_state(&self, peer: &PeerId) -> Result<PeerSessionState, Self::Error> {
        Ok(self.peer_state(peer))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct AllocateFailingSessionStore;

impl TransportSessionStore for AllocateFailingSessionStore {
    type Error = MemorySessionStoreError;

    fn allocate_outbound_session(&self, _peer: &PeerId) -> Result<ConnectionSession, Self::Error> {
        Err(MemorySessionStoreError)
    }

    fn accept_inbound_session(
        &self,
        _peer: &PeerId,
        _session: ConnectionSession,
    ) -> Result<InboundSessionDecision, Self::Error> {
        Ok(InboundSessionDecision::Accepted { previous: None })
    }

    fn peer_session_state(&self, _peer: &PeerId) -> Result<PeerSessionState, Self::Error> {
        Ok(PeerSessionState::default())
    }
}

#[derive(Debug)]
pub struct FailingSessionStore;

impl TransportSessionStore for FailingSessionStore {
    type Error = MemorySessionStoreError;

    fn allocate_outbound_session(&self, _peer: &PeerId) -> Result<ConnectionSession, Self::Error> {
        Err(MemorySessionStoreError)
    }

    fn accept_inbound_session(
        &self,
        _peer: &PeerId,
        _session: ConnectionSession,
    ) -> Result<InboundSessionDecision, Self::Error> {
        Err(MemorySessionStoreError)
    }

    fn peer_session_state(&self, _peer: &PeerId) -> Result<PeerSessionState, Self::Error> {
        Err(MemorySessionStoreError)
    }
}

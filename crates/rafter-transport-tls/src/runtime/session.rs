//! Type-erased durable session-store handle shared by runtime workers.

use std::{error::Error, fmt, sync::Arc};

use crate::{
    BoxError, ConnectionSession, InboundSessionDecision, PeerId, PeerSessionState,
    SessionStoreLimits, TransportSessionStore,
};

trait ErasedSessionStore: Send + Sync + 'static {
    fn limits(&self) -> SessionStoreLimits;

    fn preflight_peer(&self, peer: &PeerId) -> Result<(), BoxError>;

    fn preflight_peers(&self, peers: &[PeerId]) -> Result<(), BoxError>;

    fn allocate_outbound_session(&self, peer: &PeerId) -> Result<ConnectionSession, BoxError>;

    fn accept_inbound_session(
        &self,
        peer: &PeerId,
        session: ConnectionSession,
    ) -> Result<InboundSessionDecision, BoxError>;

    fn peer_session_state(&self, peer: &PeerId) -> Result<PeerSessionState, BoxError>;
}

impl<S> ErasedSessionStore for S
where
    S: TransportSessionStore,
{
    fn limits(&self) -> SessionStoreLimits {
        TransportSessionStore::limits(self)
    }

    fn preflight_peer(&self, peer: &PeerId) -> Result<(), BoxError> {
        TransportSessionStore::preflight_peer(self, peer)
            .map_err(|error| Box::new(error) as BoxError)
    }

    fn preflight_peers(&self, peers: &[PeerId]) -> Result<(), BoxError> {
        TransportSessionStore::preflight_peers(self, peers)
            .map_err(|error| Box::new(error) as BoxError)
    }

    fn allocate_outbound_session(&self, peer: &PeerId) -> Result<ConnectionSession, BoxError> {
        TransportSessionStore::allocate_outbound_session(self, peer)
            .map_err(|error| Box::new(error) as BoxError)
    }

    fn accept_inbound_session(
        &self,
        peer: &PeerId,
        session: ConnectionSession,
    ) -> Result<InboundSessionDecision, BoxError> {
        TransportSessionStore::accept_inbound_session(self, peer, session)
            .map_err(|error| Box::new(error) as BoxError)
    }

    fn peer_session_state(&self, peer: &PeerId) -> Result<PeerSessionState, BoxError> {
        TransportSessionStore::peer_session_state(self, peer)
            .map_err(|error| Box::new(error) as BoxError)
    }
}

#[derive(Clone)]
pub(crate) struct SessionStoreHandle {
    inner: Arc<dyn ErasedSessionStore>,
}

impl fmt::Debug for SessionStoreHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionStoreHandle")
            .finish_non_exhaustive()
    }
}

impl SessionStoreHandle {
    pub(crate) fn new<S>(store: S) -> Self
    where
        S: TransportSessionStore,
    {
        Self {
            inner: Arc::new(store),
        }
    }

    pub(crate) fn preflight_peers(&self, peers: &[PeerId]) -> Result<(), BoxError> {
        self.inner.preflight_peers(peers)
    }

    pub(crate) fn limits(&self) -> SessionStoreLimits {
        self.inner.limits()
    }
}

#[derive(Debug)]
pub(crate) struct RuntimeSessionStoreError {
    source: BoxError,
}

impl fmt::Display for RuntimeSessionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "durable transport session store failed: {}",
            self.source
        )
    }
}

impl Error for RuntimeSessionStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl TransportSessionStore for SessionStoreHandle {
    type Error = RuntimeSessionStoreError;

    fn limits(&self) -> SessionStoreLimits {
        self.inner.limits()
    }

    fn preflight_peer(&self, peer: &PeerId) -> Result<(), Self::Error> {
        self.inner
            .preflight_peer(peer)
            .map_err(|source| RuntimeSessionStoreError { source })
    }

    fn preflight_peers(&self, peers: &[PeerId]) -> Result<(), Self::Error> {
        self.inner
            .preflight_peers(peers)
            .map_err(|source| RuntimeSessionStoreError { source })
    }

    fn allocate_outbound_session(&self, peer: &PeerId) -> Result<ConnectionSession, Self::Error> {
        self.inner
            .allocate_outbound_session(peer)
            .map_err(|source| RuntimeSessionStoreError { source })
    }

    fn accept_inbound_session(
        &self,
        peer: &PeerId,
        session: ConnectionSession,
    ) -> Result<InboundSessionDecision, Self::Error> {
        self.inner
            .accept_inbound_session(peer, session)
            .map_err(|source| RuntimeSessionStoreError { source })
    }

    fn peer_session_state(&self, peer: &PeerId) -> Result<PeerSessionState, Self::Error> {
        self.inner
            .peer_session_state(peer)
            .map_err(|source| RuntimeSessionStoreError { source })
    }
}

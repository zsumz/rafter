//! Serialized state transitions and durable replacement publication.

use crate::{
    session::{
        encode_transport_session_state, ConnectionSession, InboundSessionDecision,
        PeerSessionState, PersistedTransportSessionState, TransportSessionState,
        TransportSessionStore,
    },
    PeerId,
};

use super::{
    io::{self, IoFailure},
    FileTransportSessionStore, FileTransportSessionStoreError, Inner,
};

impl FileTransportSessionStore {
    fn publish(
        &self,
        inner: &mut Inner,
        candidate: &TransportSessionState,
    ) -> Result<(), FileTransportSessionStoreError> {
        let persisted = PersistedTransportSessionState::new(
            self.cluster_id().clone(),
            self.local_peer_id().clone(),
            candidate.clone(),
        );
        let encoded = encode_transport_session_state(&persisted)
            .map_err(|source| FileTransportSessionStoreError::Encode { source })?;
        if let Err(error) = io::replace_state_file(self.path(), &encoded) {
            inner.failed = true;
            return Err(runtime_io_error(error));
        }
        inner.state = candidate.clone();
        Ok(())
    }
}

impl TransportSessionStore for FileTransportSessionStore {
    type Error = FileTransportSessionStoreError;

    fn allocate_outbound_session(&self, peer: &PeerId) -> Result<ConnectionSession, Self::Error> {
        let mut inner = self.healthy_inner()?;
        let mut candidate = inner.state.clone();
        let session = candidate
            .allocate_outbound(peer)
            .map_err(|source| FileTransportSessionStoreError::State { source })?;
        self.publish(&mut inner, &candidate)?;
        Ok(session)
    }

    fn accept_inbound_session(
        &self,
        peer: &PeerId,
        session: ConnectionSession,
    ) -> Result<InboundSessionDecision, Self::Error> {
        let mut inner = self.healthy_inner()?;
        let mut candidate = inner.state.clone();
        let decision = candidate
            .accept_inbound(peer, session)
            .map_err(|source| FileTransportSessionStoreError::State { source })?;
        if decision.is_accepted() {
            self.publish(&mut inner, &candidate)?;
        }
        Ok(decision)
    }

    fn peer_session_state(&self, peer: &PeerId) -> Result<PeerSessionState, Self::Error> {
        Ok(self.healthy_inner()?.state.peer_state(peer))
    }
}

fn runtime_io_error(error: IoFailure) -> FileTransportSessionStoreError {
    FileTransportSessionStoreError::Io {
        operation: error.operation,
        path: error.path,
        source: error.source,
    }
}

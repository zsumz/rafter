//! In-memory fencing for superseded live inbound connection epochs.

use std::{
    collections::BTreeMap,
    net::{Shutdown, TcpStream},
    sync::{Arc, Mutex},
};

use crate::{ConnectionSession, PeerId};

#[derive(Debug)]
struct ActiveEpoch {
    session: ConnectionSession,
    socket: Arc<TcpStream>,
}

#[derive(Debug, Default)]
pub(crate) struct InboundEpochs {
    current: Mutex<BTreeMap<PeerId, ActiveEpoch>>,
}

impl InboundEpochs {
    pub(crate) fn install(
        self: &Arc<Self>,
        peer: PeerId,
        session: ConnectionSession,
        socket: Arc<TcpStream>,
    ) -> Result<Option<InboundEpochGuard>, ()> {
        let previous = {
            let mut current = self.current.lock().map_err(|_| ())?;
            if current
                .get(&peer)
                .is_some_and(|active| active.session >= session)
            {
                return Ok(None);
            }
            current.insert(peer.clone(), ActiveEpoch { session, socket })
        };
        if let Some(previous) = previous {
            let _ = previous.socket.shutdown(Shutdown::Both);
        }
        Ok(Some(InboundEpochGuard {
            epochs: Arc::clone(self),
            peer,
            session,
        }))
    }

    pub(crate) fn is_current(&self, peer: &PeerId, session: ConnectionSession) -> Result<bool, ()> {
        self.current
            .lock()
            .map(|current| is_current_epoch(&current, peer, session))
            .map_err(|_| ())
    }

    fn while_current<R>(
        &self,
        peer: &PeerId,
        session: ConnectionSession,
        operation: impl FnOnce() -> R,
    ) -> Result<Option<R>, ()> {
        let current = self.current.lock().map_err(|_| ())?;
        if !is_current_epoch(&current, peer, session) {
            return Ok(None);
        }
        Ok(Some(operation()))
    }

    fn remove_if_current(&self, peer: &PeerId, session: ConnectionSession) {
        if let Ok(mut current) = self.current.lock() {
            let remove = current
                .get(peer)
                .is_some_and(|active| active.session == session);
            if remove {
                current.remove(peer);
            }
        }
    }

    pub(crate) fn shutdown_all(&self) -> Result<(), ()> {
        let current = self.current.lock().map_err(|_| ())?;
        for active in current.values() {
            let _ = active.socket.shutdown(Shutdown::Both);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct InboundEpochGuard {
    epochs: Arc<InboundEpochs>,
    peer: PeerId,
    session: ConnectionSession,
}

impl InboundEpochGuard {
    pub(crate) fn is_current(&self) -> Result<bool, ()> {
        self.epochs.is_current(&self.peer, self.session)
    }

    /// Runs one nonblocking admission action only while this epoch remains
    /// current. Installation of a newer epoch uses the same lock, so the action
    /// linearizes wholly before or wholly after supersession.
    pub(crate) fn while_current<R>(&self, operation: impl FnOnce() -> R) -> Result<Option<R>, ()> {
        self.epochs
            .while_current(&self.peer, self.session, operation)
    }
}

fn is_current_epoch(
    current: &BTreeMap<PeerId, ActiveEpoch>,
    peer: &PeerId,
    session: ConnectionSession,
) -> bool {
    current
        .get(peer)
        .is_some_and(|active| active.session == session)
}

impl Drop for InboundEpochGuard {
    fn drop(&mut self) {
        self.epochs.remove_if_current(&self.peer, self.session);
    }
}

#[cfg(test)]
#[path = "inbound_epoch_test.rs"]
mod tests;

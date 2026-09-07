//! Strict file-backed transport session store.

mod error;
mod io;
mod open;
mod operation;
mod ownership;
mod query;

#[cfg(test)]
mod failpoint;

use std::{
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};

use crate::{ClusterId, PeerId, SessionStoreLimits};

use super::TransportSessionState;
use ownership::SessionStoreOwnership;

pub use error::{
    CreateTransportSessionStoreError, FileTransportSessionStoreError,
    OpenTransportSessionStoreError,
};

/// File-backed durable connection-session high-water store.
///
/// Creation and reopening are deliberately separate. [`Self::open_existing`]
/// refuses a missing file, and [`Self::create_new`] refuses an existing one, so
/// loss of replay state cannot silently reset a stable [`PeerId`] to session
/// zero. A mutating I/O failure latches this handle into terminal failed state;
/// drop it and reopen the file to discover which publication became durable.
#[derive(Debug)]
pub struct FileTransportSessionStore {
    path: PathBuf,
    cluster_id: ClusterId,
    local_peer_id: PeerId,
    limits: SessionStoreLimits,
    inner: Mutex<Inner>,
    _ownership: SessionStoreOwnership,
}

#[derive(Debug)]
pub(super) struct Inner {
    pub(super) state: TransportSessionState,
    pub(super) failed: bool,
}

impl FileTransportSessionStore {
    fn from_parts(
        path: PathBuf,
        cluster_id: ClusterId,
        local_peer_id: PeerId,
        state: TransportSessionState,
        ownership: SessionStoreOwnership,
    ) -> Self {
        Self {
            path,
            cluster_id,
            local_peer_id,
            limits: state.limits(),
            inner: Mutex::new(Inner {
                state,
                failed: false,
            }),
            _ownership: ownership,
        }
    }

    fn healthy_inner(&self) -> Result<MutexGuard<'_, Inner>, FileTransportSessionStoreError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| FileTransportSessionStoreError::StoreRequiresReopen)?;
        if inner.failed {
            Err(FileTransportSessionStoreError::StoreRequiresReopen)
        } else {
            Ok(inner)
        }
    }
}

#[cfg(test)]
#[path = "../file_test.rs"]
mod tests;

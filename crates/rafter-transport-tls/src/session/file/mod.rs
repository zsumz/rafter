//! Strict file-backed transport session store.

mod error;
mod io;
mod open;
mod operation;
mod ownership;

#[cfg(test)]
mod failpoint;

use std::{
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use crate::{ClusterId, PeerId, SessionStoreLimits};

use super::{PersistedTransportSessionState, TransportSessionState};
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
    /// Returns the durable state path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the exact cluster identity bound into this state file.
    #[must_use]
    pub const fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Returns the exact local peer identity bound into this state file.
    #[must_use]
    pub const fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    /// Returns the durable physical-peer record bound.
    #[must_use]
    pub const fn limits(&self) -> SessionStoreLimits {
        self.limits
    }

    /// Returns whether an ambiguous publication failure requires reopening.
    #[must_use]
    pub fn requires_reopen(&self) -> bool {
        match self.inner.lock() {
            Ok(inner) => inner.failed,
            Err(_) => true,
        }
    }

    /// Returns a consistent logical snapshot when this handle remains healthy.
    ///
    /// # Errors
    ///
    /// Returns [`FileTransportSessionStoreError::StoreRequiresReopen`] after any
    /// mutating I/O failure.
    pub fn snapshot(
        &self,
    ) -> Result<PersistedTransportSessionState, FileTransportSessionStoreError> {
        let inner = self.healthy_inner()?;
        Ok(PersistedTransportSessionState::new(
            self.cluster_id.clone(),
            self.local_peer_id.clone(),
            inner.state.clone(),
        ))
    }

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

//! Read-only inspection of one open file-backed session store.
//!
//! This module owns the answers a caller may ask of a live handle: the identity
//! and bounds welded into the state file, whether an ambiguous publication
//! requires reopening, and a consistent logical snapshot. Nothing here mutates
//! state or touches the file.

use std::path::Path;

use crate::session::PersistedTransportSessionState;
use crate::{ClusterId, PeerId, SessionStoreLimits};

use super::{FileTransportSessionStore, FileTransportSessionStoreError};

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
}

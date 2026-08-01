//! Explicit create-new and open-existing file-store lifecycle.

use std::{io::ErrorKind, path::Path};

use crate::{
    session::{
        decode_transport_session_state, encode_transport_session_state,
        max_transport_session_state_bytes, PersistedTransportSessionState, TransportSessionState,
    },
    ClusterId, PeerId, SessionStoreLimits,
};

use super::{
    io::{self, IoFailure},
    ownership::{self, AcquireOwnershipError, SessionStoreOwnership},
    CreateTransportSessionStoreError, FileTransportSessionStore, OpenTransportSessionStoreError,
};

impl FileTransportSessionStore {
    /// Creates brand-new durable state at `path`.
    ///
    /// The parent directory must already exist. Creation writes and synchronizes
    /// a sibling temp file, atomically renames it into place with create-new
    /// semantics, and synchronizes the parent directory before returning.
    ///
    /// This is provisioning, not recovery. Once `local_peer_id` has been used
    /// on the network, loss of its state requires restoration or a new
    /// [`PeerId`]; callers must not invoke `create_new` to reset that identity.
    ///
    /// # Errors
    ///
    /// Returns [`CreateTransportSessionStoreError::AlreadyExists`] when `path`
    /// already exists, `AlreadyOpen` when another cooperating handle owns it,
    /// or a typed encoding/filesystem failure.
    pub fn create_new(
        path: impl AsRef<Path>,
        cluster_id: ClusterId,
        local_peer_id: PeerId,
        limits: SessionStoreLimits,
    ) -> Result<Self, CreateTransportSessionStoreError> {
        let path = path.as_ref().to_path_buf();
        let ownership = acquire_for_create(&path)?;
        let state = TransportSessionState::new(limits);
        let persisted = PersistedTransportSessionState::new(
            cluster_id.clone(),
            local_peer_id.clone(),
            state.clone(),
        );
        let encoded = encode_transport_session_state(&persisted)
            .map_err(|source| CreateTransportSessionStoreError::Encode { source })?;
        if let Err(error) = io::create_state_file(&path, &encoded) {
            if error.source.kind() == ErrorKind::AlreadyExists {
                return Err(CreateTransportSessionStoreError::AlreadyExists { path });
            }
            return Err(create_io_error(error));
        }
        Ok(Self::from_parts(
            path,
            cluster_id,
            local_peer_id,
            state,
            ownership,
        ))
    }

    /// Opens strictly existing durable state owned by the expected identities.
    ///
    /// The file is read through the absolute version-1 size bound before its
    /// encoded per-peer bound is trusted. Missing, corrupt, mismatched, and
    /// concurrently owned state all fail closed.
    ///
    /// # Errors
    ///
    /// Returns [`OpenTransportSessionStoreError`] when ownership cannot be
    /// acquired, bytes cannot be read or decoded, or durable identities differ
    /// from the caller's exact cluster and local peer identities.
    pub fn open_existing(
        path: impl AsRef<Path>,
        expected_cluster_id: &ClusterId,
        expected_local_peer_id: &PeerId,
    ) -> Result<Self, OpenTransportSessionStoreError> {
        let path = path.as_ref().to_path_buf();
        let ownership = acquire_for_open(&path)?;
        let maximum = max_transport_session_state_bytes(SessionStoreLimits::MAX);
        let bytes = match io::read_bounded(&path, maximum) {
            Ok(bytes) => bytes,
            Err(error) if error.source.kind() == ErrorKind::NotFound => {
                return Err(OpenTransportSessionStoreError::Missing { path });
            }
            Err(error) => return Err(open_io_error(error)),
        };
        if bytes.len() > maximum {
            return Err(OpenTransportSessionStoreError::FileTooLarge {
                path,
                actual: bytes.len(),
                maximum,
            });
        }
        let persisted = decode_transport_session_state(&bytes)
            .map_err(|source| OpenTransportSessionStoreError::Decode { source })?;
        validate_identities(&persisted, expected_cluster_id, expected_local_peer_id)?;
        let (cluster_id, local_peer_id, state) = persisted.into_parts();
        Ok(Self::from_parts(
            path,
            cluster_id,
            local_peer_id,
            state,
            ownership,
        ))
    }
}

fn validate_identities(
    persisted: &PersistedTransportSessionState,
    expected_cluster_id: &ClusterId,
    expected_local_peer_id: &PeerId,
) -> Result<(), OpenTransportSessionStoreError> {
    if persisted.cluster_id() != expected_cluster_id {
        return Err(OpenTransportSessionStoreError::ClusterMismatch {
            expected: expected_cluster_id.clone(),
            actual: persisted.cluster_id().clone(),
        });
    }
    if persisted.local_peer_id() != expected_local_peer_id {
        return Err(OpenTransportSessionStoreError::LocalPeerMismatch {
            expected: expected_local_peer_id.clone(),
            actual: persisted.local_peer_id().clone(),
        });
    }
    Ok(())
}

fn acquire_for_create(
    path: &Path,
) -> Result<SessionStoreOwnership, CreateTransportSessionStoreError> {
    ownership::acquire(path).map_err(|error| match error {
        AcquireOwnershipError::AlreadyHeld => CreateTransportSessionStoreError::AlreadyOpen {
            path: path.to_path_buf(),
        },
        AcquireOwnershipError::Io {
            operation,
            path,
            source,
        } => CreateTransportSessionStoreError::Io {
            operation,
            path,
            source,
        },
    })
}

fn acquire_for_open(path: &Path) -> Result<SessionStoreOwnership, OpenTransportSessionStoreError> {
    ownership::acquire(path).map_err(|error| match error {
        AcquireOwnershipError::AlreadyHeld => OpenTransportSessionStoreError::AlreadyOpen {
            path: path.to_path_buf(),
        },
        AcquireOwnershipError::Io {
            operation,
            path,
            source,
        } => OpenTransportSessionStoreError::Io {
            operation,
            path,
            source,
        },
    })
}

fn create_io_error(error: IoFailure) -> CreateTransportSessionStoreError {
    CreateTransportSessionStoreError::Io {
        operation: error.operation,
        path: error.path,
        source: error.source,
    }
}

fn open_io_error(error: IoFailure) -> OpenTransportSessionStoreError {
    OpenTransportSessionStoreError::Io {
        operation: error.operation,
        path: error.path,
        source: error.source,
    }
}

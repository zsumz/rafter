//! Public transport-session composition for the production fixture.
//!
//! The reusable TLS crate owns connection epochs, their durable file format,
//! corruption refusal, and atomic publication. This module owns only the
//! fixture's stable identity convention, state-file location, and the narrow
//! reopening adapter required by its pre-ownership listener lifecycle.

use std::{
    error::Error,
    fmt,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard, PoisonError},
    thread,
    time::{Duration, Instant},
};

use rafter::NodeId;
use rafter_transport_tls::{
    ClusterId, ConnectionSession, CreateTransportSessionStoreError, FileTransportSessionStore,
    FileTransportSessionStoreError, IdentityError, InboundSessionDecision,
    OpenTransportSessionStoreError, PeerId, PeerSessionState, SessionStoreLimits,
    TransportSessionStore,
};

/// Durable public-transport state file inside one replica directory.
pub const TRANSPORT_SESSION_FILE: &str = "transport-replay";
/// Exact live-stream receive window: only the next sequence is accepted.
pub const CONNECTION_SEQUENCE_WINDOW: u64 = 1;
/// Compatibility observation retained by the process evidence schema.
pub const REPLAY_WINDOW: u64 = CONNECTION_SEQUENCE_WINDOW;

const STORE_OPEN_WAIT: Duration = Duration::from_secs(2);
const STORE_OPEN_POLL: Duration = Duration::from_millis(2);

/// Stable physical principal used by this single-group fixture.
///
/// # Errors
///
/// Returns a validation error if the public transport identity contract no
/// longer accepts the fixture's `lock-node-<u64>` convention.
pub fn transport_peer_id(node_id: NodeId) -> Result<PeerId, IdentityError> {
    PeerId::new(&format!("lock-node-{}", node_id.0))
}

/// Stable deployment boundary used by one fenced-lock Raft group.
///
/// # Errors
///
/// Returns a validation error if the public transport identity contract no
/// longer accepts the fixture's cluster convention.
pub fn transport_cluster_id(group_id: u64) -> Result<ClusterId, IdentityError> {
    ClusterId::new(&format!("rafter-reference-fenced-lock-{group_id}"))
}

/// Durable transport state beneath one replica directory.
#[must_use]
pub fn transport_session_path(node_dir: &Path) -> PathBuf {
    node_dir.join(TRANSPORT_SESSION_FILE)
}

/// Creates brand-new public transport state while allocating a replica.
///
/// # Errors
///
/// Returns [`TransportSessionStateError`] when create-new publication fails.
pub(super) fn initialize_transport_state(
    node_dir: &Path,
    group_id: u64,
    node_id: NodeId,
) -> Result<(), TransportSessionStateError> {
    let store = FileTransportSessionStore::create_new(
        transport_session_path(node_dir),
        transport_cluster_id(group_id)
            .map_err(|source| TransportSessionStateError::Identity { source })?,
        transport_peer_id(node_id)
            .map_err(|source| TransportSessionStateError::Identity { source })?,
        SessionStoreLimits::DEFAULT,
    )
    .map_err(|source| TransportSessionStateError::Create { source })?;
    drop(store);
    Ok(())
}

/// Reopens the exact state belonging to one stopped replica.
///
/// # Errors
///
/// Returns [`TransportSessionStateError`] for missing, corrupt, concurrently
/// owned, or identity-mismatched state.
pub fn open_transport_state(
    node_dir: &Path,
    group_id: u64,
    node_id: NodeId,
) -> Result<FileTransportSessionStore, TransportSessionStateError> {
    let cluster_id = transport_cluster_id(group_id)
        .map_err(|source| TransportSessionStateError::Identity { source })?;
    let peer_id = transport_peer_id(node_id)
        .map_err(|source| TransportSessionStateError::Identity { source })?;
    FileTransportSessionStore::open_existing(
        transport_session_path(node_dir),
        &cluster_id,
        &peer_id,
    )
    .map_err(|source| TransportSessionStateError::Open { source })
}

/// Reopens state after deriving the node ID from a `node-<u64>` directory.
///
/// # Errors
///
/// Returns [`TransportSessionStateError`] when the directory name is malformed
/// or the exact public state cannot be reopened.
pub fn open_transport_state_from_directory(
    node_dir: &Path,
    group_id: u64,
) -> Result<FileTransportSessionStore, TransportSessionStateError> {
    let node_id = node_dir
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("node-"))
        .and_then(|value| value.parse::<u64>().ok())
        .map(NodeId)
        .ok_or_else(|| TransportSessionStateError::InvalidReplicaDirectory {
            path: node_dir.to_path_buf(),
        })?;
    open_transport_state(node_dir, group_id, node_id)
}

/// Session-store adapter for the fixture's pre-ownership listener phase.
///
/// Each operation still uses [`FileTransportSessionStore`] and its exact public
/// format. The adapter merely releases the cooperating-process lock between
/// operations, because a contender starts listening before it acquires the
/// application replica directory. A process-local mutex serializes workers; a
/// bounded retry lets another contender finish a concurrent preflight.
#[derive(Debug)]
pub struct ReopeningTransportSessionStore {
    path: PathBuf,
    cluster_id: ClusterId,
    local_peer_id: PeerId,
    operation: Mutex<()>,
}

impl ReopeningTransportSessionStore {
    /// Builds an adapter over strictly existing state.
    #[must_use]
    pub fn new(path: PathBuf, cluster_id: ClusterId, local_peer_id: PeerId) -> Self {
        Self {
            path,
            cluster_id,
            local_peer_id,
            operation: Mutex::new(()),
        }
    }

    fn with_store<T>(
        &self,
        operation: impl FnOnce(&FileTransportSessionStore) -> Result<T, FileTransportSessionStoreError>,
    ) -> Result<T, TransportSessionStateError> {
        let _guard = lock(&self.operation);
        let deadline = Instant::now() + STORE_OPEN_WAIT;
        let store = loop {
            match FileTransportSessionStore::open_existing(
                &self.path,
                &self.cluster_id,
                &self.local_peer_id,
            ) {
                Ok(store) => break store,
                Err(OpenTransportSessionStoreError::AlreadyOpen { .. })
                    if Instant::now() < deadline =>
                {
                    thread::sleep(STORE_OPEN_POLL);
                }
                Err(source) => return Err(TransportSessionStateError::Open { source }),
            }
        };
        operation(&store).map_err(|source| TransportSessionStateError::Operation { source })
    }
}

impl TransportSessionStore for ReopeningTransportSessionStore {
    type Error = TransportSessionStateError;

    fn allocate_outbound_session(&self, peer: &PeerId) -> Result<ConnectionSession, Self::Error> {
        self.with_store(|store| store.allocate_outbound_session(peer))
    }

    fn accept_inbound_session(
        &self,
        peer: &PeerId,
        session: ConnectionSession,
    ) -> Result<InboundSessionDecision, Self::Error> {
        self.with_store(|store| store.accept_inbound_session(peer, session))
    }

    fn peer_session_state(&self, peer: &PeerId) -> Result<PeerSessionState, Self::Error> {
        self.with_store(|store| store.peer_session_state(peer))
    }
}

/// Fixture composition failure around the public session store.
#[derive(Debug)]
#[non_exhaustive]
pub enum TransportSessionStateError {
    /// The fixture's stable principal convention violated the public contract.
    Identity {
        /// Public identity-validation failure.
        source: IdentityError,
    },
    /// The replica directory name does not identify a node.
    InvalidReplicaDirectory {
        /// Directory whose final component was refused.
        path: PathBuf,
    },
    /// Brand-new state could not be created.
    Create {
        /// Public create-new failure.
        source: CreateTransportSessionStoreError,
    },
    /// Existing state could not be reopened safely.
    Open {
        /// Public open-existing failure.
        source: OpenTransportSessionStoreError,
    },
    /// A pure transition or durable publication failed.
    Operation {
        /// Public runtime store failure.
        source: FileTransportSessionStoreError,
    },
}

impl fmt::Display for TransportSessionStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity { source } => {
                write!(formatter, "invalid transport session identity: {source}")
            }
            Self::InvalidReplicaDirectory { path } => write!(
                formatter,
                "replica directory {} does not end in node-<u64>",
                path.display()
            ),
            Self::Create { source } => {
                write!(
                    formatter,
                    "could not create transport session state: {source}"
                )
            }
            Self::Open { source } => {
                write!(
                    formatter,
                    "could not open transport session state: {source}"
                )
            }
            Self::Operation { source } => {
                write!(formatter, "transport session operation failed: {source}")
            }
        }
    }
}

impl Error for TransportSessionStateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity { source } => Some(source),
            Self::Create { source } => Some(source),
            Self::Open { source } => Some(source),
            Self::Operation { source } => Some(source),
            Self::InvalidReplicaDirectory { .. } => None,
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

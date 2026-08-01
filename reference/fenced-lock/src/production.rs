//! Caller-owned production-composition support.
//!
//! These modules remain application-owned. They provision monotonic single-use
//! Raft identities and compose the public TLS transport's durable connection
//! epochs without moving deployment control into Rafter.

mod identity;
mod transport;

pub use identity::{
    allocate_replica, load_active_replica, load_allocation_high_water, retire_replica,
    AllocationCrashPoint, IdentityError, ReplicaIdentity,
};
pub use transport::{
    open_transport_state, open_transport_state_from_directory, transport_cluster_id,
    transport_peer_id, transport_session_path, ReopeningTransportSessionStore,
    TransportSessionStateError, CONNECTION_SEQUENCE_WINDOW, REPLAY_WINDOW, TRANSPORT_SESSION_FILE,
};

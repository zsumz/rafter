//! Async managed service layer for Rafter.
//!
//! This crate provides the application-facing managed handle API built on top
//! of `rafter-app`. It includes a real in-memory managed driver over
//! `RaftGroup` and transport traits for production integrations. The
//! deterministic kernel, runtime API boundary, and synchronous app driver stay
//! free of async runtime dependencies.
//! `rafter-service` owns handle ergonomics, managed command routing, async
//! sender traits, watch surfaces, and transport contracts. It does not own
//! durable storage, concrete authenticated transport implementations, or the
//! app-side applied-floor/recovery contract.

/// Managed driver implementations and command-sender traits.
pub mod driver;
/// Public service-layer error types.
pub mod error;
/// User-facing handle for writes, reads, membership, and metrics.
pub mod handle;
/// Managed membership controller and planned-change types.
pub mod membership;
/// Sync and async transport traits plus inbound peer validation.
pub mod transport;
/// Metrics publisher/watch surfaces for managed groups.
pub mod watch;

// Every type or trait named in the signature of a public item is reachable
// from this root. `DriverFuture` is the return type of four of the five
// `DriverCommandSender` methods, so every implementor of that trait names it;
// `StateMachineOperation` and `ErrorCause` are `rafter-app`'s own types that
// this crate's public error variants carry. A public signature that names an
// unreachable type is a defect, and `tests/public_surface.rs` is the check.
pub use driver::{
    DriverCommandSender, DriverFuture, InMemoryRaftDriver, InboundEnvelopeError,
    ManagedDriverError, PendingWrite, QueryReceipt, TransportDriverOptions, TransportRaftDriver,
    WriteBatchEntry, WriteOptions, WriteReceipt,
};
pub use error::{
    ErrorCause, MetricsError, ReadAbandonReason, ReadError, ReadErrorKind, ShutdownError,
    StateMachineOperation, TransferLeadershipError, UnknownOutcomeReason, WriteError,
    WriteErrorKind, WriteFate,
};
pub use handle::RaftHandle;
pub use membership::{MembershipController, PlannedMembershipChange};
pub use rafter_app::read::ReadConsistency;
pub use transport::{
    validate_inbound_peer_envelope, AsyncRaftTransport, AuthenticatedPeerEnvelope,
    AuthenticatedPeerEnvelopeError, AuthenticatedPeerValidator, InboundEnvelopeFuture,
    PeerEnvelope, PeerSet, RaftTransport, TransportFuture,
};
pub use watch::{MetricsPublisher, MetricsWatch};

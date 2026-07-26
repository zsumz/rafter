//! Many-group host layer for Rafter.
//!
//! `rafter-multiraft` is the manual many-group layer above `rafter-app`.
//! It is intended for sharded services, databases, and advanced embedded
//! runtimes that run multiple Raft groups in one process while retaining
//! explicit control over routing, storage, authorization, recovery, metrics,
//! and application command semantics.
//!
//! This crate owns in-process many-group dispatch, typed and untyped group
//! host helpers, and aggregate metrics surfaces.
//! This crate does not own networking or assume one process maps to one group.
//! Group identity remains caller-defined, and messages are routed explicitly by
//! group ID.
//!
//! Use [`MultiRaftHost`] when groups are dynamic or heterogeneous and the
//! caller wants to manage the encoded `Vec<u8>` command boundary directly. Use
//! [`TypedMultiRaftHost`] when groups share one command type and one apply
//! result type and user code should step typed proposals without downcasts.
//! `rafter_app::group::RaftGroup` implements [`TypedGroupDriver`], so real
//! embedded Rafter groups can be opened in a typed host directly.

/// Driver traits implemented by many-group group adapters.
pub mod driver;
/// Error types returned by many-group hosts.
pub mod error;
/// Untyped many-group host over encoded command/result boundaries.
pub mod host;
/// Aggregate metrics for many-group hosts.
pub mod metrics;
/// One complete pass over every group a host holds.
pub mod pass;
/// Typed many-group host and driver traits.
pub mod typed;

pub use driver::GroupDriver;
pub use error::MultiRaftError;
pub use host::MultiRaftHost;
pub use metrics::MultiRaftMetrics;
pub use pass::{GroupOutcome, TickPass};
pub use typed::{TypedGroupDriver, TypedMultiRaftHost};

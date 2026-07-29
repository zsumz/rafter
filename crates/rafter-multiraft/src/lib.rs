//! Manual many-group host layer for Rafter.
//!
//! `rafter-multiraft` holds many caller-defined Raft groups in one process and
//! steps the one it is told to step. It is intended for sharded services,
//! databases, and advanced embedded runtimes that run multiple Raft groups in
//! one process while retaining explicit control over routing, storage, group
//! lifecycle, metrics, and application command semantics.
//!
//! This crate owns in-process many-group dispatch, typed and untyped host
//! helpers, group retirement, and a metrics snapshot that concatenates each
//! open group's own [`rafter_app::metrics::RaftGroupMetrics`]. It does not own
//! networking and does not assume one process maps to one group. Group
//! identity remains caller-defined, and messages are routed explicitly by
//! group ID.
//!
//! # This is a manual host, not a scheduler
//!
//! [`MultiRaftHost::tick_all`] walks every open group once, in key order, and
//! returns one outcome per group. Nothing else here decides when work happens.
//! In particular this crate does **not**:
//!
//! - decide when to step anything — ticks arrive only as often as the caller
//!   loops;
//! - enforce a per-group work quota, so a group with slow storage occupies the
//!   pass for as long as its driver takes;
//! - queue anything, and therefore has no queue limits and no backpressure;
//! - prioritize control traffic over bulk replication;
//! - retire a group on its own, even one whose driver reports a permanent
//!   failure; or
//! - keep tombstones, so a retired key is reopenable and late traffic for it
//!   is reported as an unknown group.
//!
//! [`TickPass::visited`] is a fairness *measurement* — it proves the pass
//! reached every group — not a fairness *mechanism*. The managed multi-Raft
//! scheduler that bounds fairness, isolates failure, and enforces quotas is a
//! separate 1.0 component, and it does not exist yet.
//!
//! # Choosing a host
//!
//! Use [`MultiRaftHost`] when groups are dynamic or heterogeneous and the
//! caller wants to manage the encoded `Vec<u8>` command boundary directly. Use
//! [`TypedMultiRaftHost`] when groups share one command type and one apply
//! result type and user code should step typed proposals without downcasts.
//! [`rafter_app::group::RaftGroup`] implements [`TypedGroupDriver`], so real
//! embedded Rafter groups can be opened in a typed host directly.

/// Driver traits implemented by many-group group adapters.
pub mod driver;
/// Error types returned by many-group hosts.
pub mod error;
/// Untyped many-group host over encoded command/result boundaries.
pub mod host;
/// Deterministic bounded scheduling above the manual hosts.
pub mod managed;
/// Each open group's own metrics, collected in key order.
pub mod metrics;
/// One complete pass over every group a host holds.
pub mod pass;
/// Typed many-group host and driver traits.
pub mod typed;
mod validate;

pub use driver::{DriverError, DriverErrorKind, GroupDriver};
pub use error::{MultiRaftError, MultiRaftErrorKind, OpenGroupRejected};
pub use host::MultiRaftHost;
pub use metrics::MultiRaftMetrics;
pub use pass::{GroupOutcome, TickPass};
pub use typed::{TypedGroupDriver, TypedMultiRaftHost};

/// Re-exported rather than redeclared: a caller must be able to compare the
/// cause it receives here with the one `rafter-app` produced, so there can be
/// only one such type.
pub use rafter_app::error::ErrorCause;

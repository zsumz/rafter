//! Synchronous embedded replicated-state-machine support for Rafter.
//!
//! `rafter-app` is the manual application-facing layer above the deterministic
//! Raft kernel and the `rafter-runtime-api` contract. It is intended for
//! databases, replicated services, sharded systems, and other embedded
//! runtimes that own their storage, transport, routing, authorization,
//! recovery loops, and application command semantics.
//!
//! This crate owns `RaftGroup` orchestration, proposal/read bookkeeping,
//! state-machine apply guards, app-facing reports, and group-level metrics.
//! This crate does not spawn tasks, open sockets, require Tokio, or assume
//! one process maps to one Raft group. It exposes explicit group-step reports
//! so callers can dispatch peer messages, apply committed entries, publish
//! metrics, and handle recovery under their own runtime policy.

/// Group-level error types surfaced by the app orchestration layer.
pub mod error;
/// Stateful replicated group orchestration over a persisted Raft runtime.
pub mod group;
/// Membership planning and reporting helpers for app-managed changes.
pub mod membership;
/// App-layer group metrics snapshots.
pub mod metrics;
/// Proposal request, completion, and unknown-outcome types.
pub mod proposal;
/// Linearizable and local read request/report types.
pub mod read;
/// Application snapshot events emitted through group reports.
pub mod snapshot;
/// Replicated state-machine traits and apply/read payload types.
pub mod state_machine;
/// Peer envelope authentication and validation helpers.
pub mod transport;

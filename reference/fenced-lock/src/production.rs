//! Caller-owned production-composition support.
//!
//! This module belongs to the unpublished fenced-lock acceptance consumer. It
//! is not a Rafter deployment API. The production process fixture uses it to
//! prove the durable identity-allocation half of the embedding contract without
//! putting allocation or retention policy into a public Rafter crate.

mod identity;
mod replay;

pub use identity::{
    allocate_replica, load_active_replica, load_allocation_high_water, retire_replica,
    AllocationCrashPoint, IdentityError, ReplicaIdentity,
};
pub use replay::{ReplayDecision, TransportReplayError, TransportReplayStore, REPLAY_WINDOW};

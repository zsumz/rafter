//! Scenario suites for the deterministic simulator.
//!
//! The root of this crate's cluster-level tests: fault primitives, disk
//! faults, membership change, elections, replication, snapshots, and
//! liveness. Unit tests of a single module live beside that module instead.

use super::*;
use rafter::{BootstrapLogEntry, CommittedConfiguration, LogEntry, Role, Term};

mod disk_faults;
mod dynamic_membership;
mod fault_primitives;
mod helpers;
mod lease_reads;
mod network;
mod pre_vote;
mod raft_invariants;
mod raft_liveness;
mod replication_pipelining;
mod single_group_failures;
mod snapshot_installation;
mod transition_observation;

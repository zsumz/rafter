//! The crate's public safety checks, one per verified property.
//!
//! Each exported entry point runs a bounded exploration to exhaustion and
//! returns either a summary or a typed failure; callers choose bounds and node
//! configurations but never assemble the invariant suite themselves. The
//! exploration machinery behind these entry points stays private.

mod bounded;
mod purpose;
mod read;
mod restart;
mod seeded;
mod witnesses;

pub use bounded::{
    check_raft_commit_safety, check_raft_election_safety, check_raft_membership_safety,
};
pub use purpose::{
    check_raft_lease_fast_path_read_safety, check_raft_production_config_commit_safety,
    check_raft_window_one_backpressure_safety,
};
pub use read::check_raft_read_index_safety;
pub use restart::{
    check_raft_joint_membership_restart_and_snapshot_safety, check_raft_restart_and_snapshot_safety,
};
pub use seeded::{check_raft_leadership_noop_safety, check_raft_seeded_commit_safety};
pub use witnesses::check_raft_semantic_witness_safety;

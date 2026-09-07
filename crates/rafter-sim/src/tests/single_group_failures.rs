//! Failure and repair scenarios within one Raft group.
//!
//! Groups the failover and log-repair suites: what a group must recover from
//! when a leader is lost and a follower's log has diverged.

mod failover;
mod fixtures;
mod log_repair;

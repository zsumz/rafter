//! Deterministic replay of a recorded trace against a fresh cluster.
//!
//! Replay re-executes an exact action sequence and re-runs the same invariant
//! suite, so a reported failure is reproducible from its trace alone. It
//! explores nothing of its own: a trace is either replayable step for step or
//! rejected with a typed error.

mod action;
mod error;
mod runner;
mod types;

pub use error::ReplayError;
pub use runner::replay_raft_trace;
pub use types::{ReplayCheck, ReplayExpectation, ReplayReport};

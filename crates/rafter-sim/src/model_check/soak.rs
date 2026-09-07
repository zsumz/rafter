//! Randomized long-run soak configuration, actions, and results.
//!
//! This is the public shape of a soak: what it may do, how much of it, and
//! what it reports afterward. The workload is bounded by explicit budgets so a
//! summary describes exactly one reproducible run; executing that run belongs
//! to `soak_runner`.

mod action;
mod config;
mod failure;
mod summary;

pub use action::{SoakAction, SoakActionKind};
pub use config::{SoakConfig, SoakExecutionParameters};
pub use failure::SoakFailure;
pub use summary::SoakSummary;

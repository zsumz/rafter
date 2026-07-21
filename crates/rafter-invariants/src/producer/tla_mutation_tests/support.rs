//! Shared probes, configurations, and execution helpers for TLC mutation scenarios.

#[path = "support/configs.rs"]
mod configs;
#[path = "support/runtime.rs"]
mod runtime;

pub(super) use configs::*;
pub(super) use runtime::*;

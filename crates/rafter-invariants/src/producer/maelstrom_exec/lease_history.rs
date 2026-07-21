//! Stable lease-history producer test identity over the domain implementation.

pub(super) use crate::producer::maelstrom::{probe_completion_count, MAX_LINE_BYTES};

#[cfg(test)]
#[path = "lease_history_tests.rs"]
mod tests;

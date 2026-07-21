//! Stable serialized TLA+ mutation-test identity and test-only execution access.

#[cfg(test)]
pub(in crate::producer) use super::tla::execution::{detector_qualified, parse_main_summary};

#[cfg(test)]
#[path = "tla_mutation_tests.rs"]
mod mutation_tests;

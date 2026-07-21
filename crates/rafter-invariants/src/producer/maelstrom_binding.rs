//! Stable producer Maelstrom counterexample-binding test identity.

pub(super) use super::maelstrom::bind_counterexamples;

#[cfg(test)]
#[path = "maelstrom/binding_tests.rs"]
mod tests;

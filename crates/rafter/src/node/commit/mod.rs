//! Commit authorization and ordered dispatch of the committed prefix.
//!
//! Advancement places quorum replication beside the current-term restriction.
//! Dispatch emits the newly committed effects; the embedding executes them.

mod advance;
mod emit;

#[cfg(test)]
mod quorum_test;

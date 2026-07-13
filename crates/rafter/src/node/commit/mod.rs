//! Commit-index derivation and committed-entry application.
//!
//! The tracker derives the highest index stored on the effective stable or
//! joint quorum. Advancement accepts only a current-term candidate; applying
//! that commit emits ordered effects for the entire newly committed prefix.

mod apply;
mod tracker;

#[cfg(test)]
mod tracker_test;

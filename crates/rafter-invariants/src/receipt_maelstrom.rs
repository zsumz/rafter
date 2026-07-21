//! Compatibility facade for verification-owned Maelstrom receipt validation.

#[cfg(test)]
pub(super) use crate::verification::maelstrom::test_support::{
    java_major, valid_counterexample_attribution,
};

#[cfg(test)]
#[path = "verification/maelstrom/tests/receipt.rs"]
mod tests;

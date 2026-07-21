//! Compatibility facade for verification-owned Maelstrom receipt validation.

pub(super) use crate::verification::maelstrom::validate_receipt as validate;

#[cfg(test)]
pub(super) use crate::verification::maelstrom::test_support::{
    java_major, valid_counterexample_attribution,
};

#[cfg(test)]
#[path = "verification/maelstrom/tests/receipt.rs"]
mod tests;

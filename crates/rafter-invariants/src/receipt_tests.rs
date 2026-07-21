//! Compatibility mount for the stable test-runner receipt scenario identity.

use crate::verification::test_runner::validate_receipt as validate;

#[path = "verification/test_runner/tests/receipt.rs"]
mod tests;

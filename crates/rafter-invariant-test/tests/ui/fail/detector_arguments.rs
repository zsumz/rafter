//! Proves `#[detector_test]` accepts no arguments.
//!
//! A fixture that passes one must fail to compile with the macro's own
//! message, so a typo cannot be absorbed into an attribute that merely looked
//! configurable. It pins the refusal alone; what a well-formed detector test
//! expands to belongs to the passing fixtures.

use rafter_invariant_test::detector_test;

#[detector_test(unexpected)]
fn fixture() {}

fn main() {}

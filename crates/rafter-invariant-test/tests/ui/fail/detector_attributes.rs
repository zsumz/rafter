//! Proves which attributes may sit beside `#[detector_test]`.
//!
//! `#[should_panic]` is refused because a panicking detector is a harness
//! error rather than a counterexample, and `#[ignore]` is accepted only in its
//! string form, so a malformed one is a compile error and not a silently
//! skipped detector. It says nothing about a detector test's body.

use rafter_invariant_test::detector_test;

#[detector_test]
#[should_panic]
fn panic_fixture() {}

#[detector_test]
#[ignore(reason)]
fn malformed_ignore_fixture() {}

#[detector_test]
#[ignore = 7]
fn non_string_ignore_fixture() {}

fn main() {}

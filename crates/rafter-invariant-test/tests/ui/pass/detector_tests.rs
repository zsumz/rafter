//! Proves the shapes `#[detector_test]` must keep accepting.
//!
//! An ordinary detector test, one carrying a string `#[ignore]`, and one with
//! a concrete `where` clause all compile, so tightening the attribute's
//! refusals cannot quietly narrow what already works. The refusals themselves
//! belong to the failing fixtures.

#![allow(unused_imports)]

use rafter_invariant_test::{detector_test, oracle_expect_err};

fn reject() -> Result<(), &'static str> {
    Err("expected")
}

#[detector_test]
fn ordinary_detector_test() {
    let _ = oracle_expect_err!(reject(), "must reject");
}

#[detector_test]
#[ignore = "runner-owned fixture"]
fn ignored_detector_test() {
    let _ = oracle_expect_err!(reject(), "must reject");
}

#[detector_test]
fn concrete_where_clause_remains_supported()
where
    String: Clone,
{
    let _ = oracle_expect_err!(reject(), "must reject");
}

fn main() {}

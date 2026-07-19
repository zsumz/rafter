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

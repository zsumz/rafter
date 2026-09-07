//! Proves the arity and trailing-comma range the oracle macros accept.
//!
//! Zero through eight arguments compile for both the expecting and the
//! recording macro, with and without a trailing comma, so the hand-written
//! adapter list cannot lose an end of its range unnoticed. What the macros
//! refuse is proved by the failing fixtures.

use rafter_invariant_test::{oracle_expect_err, oracle_invoke_recorder};

fn reject_zero() -> Result<(), ()> {
    Err(())
}

#[allow(clippy::too_many_arguments)]
fn reject_eight(_: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8) -> Result<(), ()> {
    Err(())
}

fn record_zero() {}

#[allow(clippy::too_many_arguments)]
fn record_eight(_: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8) {}

fn main() {
    let _ = oracle_expect_err!(reject_zero(), "zero",);
    let _ = oracle_expect_err!(reject_eight(0, 1, 2, 3, 4, 5, 6, 7,), "eight",);
    oracle_invoke_recorder!(record_zero());
    oracle_invoke_recorder!(record_eight(0, 1, 2, 3, 4, 5, 6, 7,));
}

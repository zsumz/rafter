//! Public macro and hidden expansion-ABI contracts.

use rafter_invariant_test::{
    detector_test, oracle_assert, oracle_assert_eq, oracle_assert_ne, oracle_expect_err,
    oracle_invoke_recorder,
};

fn reject_zero() -> Result<(), &'static str> {
    Err("zero")
}

#[allow(clippy::too_many_arguments)]
fn reject_eight(
    _: u8,
    _: u8,
    _: u8,
    _: u8,
    _: u8,
    _: u8,
    _: u8,
    _: u8,
) -> Result<(), &'static str> {
    Err("eight")
}

fn record_zero() {}

#[allow(clippy::too_many_arguments)]
fn record_eight(_: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8) {}

#[detector_test]
fn invocation_adapters_cover_zero_through_eight_arguments() {
    assert_eq!(oracle_expect_err!(reject_zero(), "reject zero"), "zero");
    assert_eq!(
        oracle_expect_err!(reject_eight(0, 1, 2, 3, 4, 5, 6, 7), "reject eight"),
        "eight"
    );
    oracle_invoke_recorder!(record_zero());
    oracle_invoke_recorder!(record_eight(0, 1, 2, 3, 4, 5, 6, 7));
}

#[test]
fn assertion_macros_keep_their_public_root_names() {
    oracle_assert!(true);
    oracle_assert_eq!(1, 1);
    oracle_assert_ne!(1, 2);
}

#[test]
fn generated_hidden_symbols_remain_available_at_the_crate_root() {
    let begin: fn() = rafter_invariant_test::__begin_detector_test;
    let finish: fn() -> rafter_invariant_test::DetectorTestOutcome =
        rafter_invariant_test::__detector_test_outcome;
    let observed: fn() = rafter_invariant_test::__oracle_observed;
    let violation: for<'a> fn(std::fmt::Arguments<'a>) -> ! =
        rafter_invariant_test::__oracle_violation;
    let message: for<'a> fn(std::fmt::Arguments<'a>) -> String =
        rafter_invariant_test::__oracle_violation_message;
    std::hint::black_box(begin);
    std::hint::black_box(finish);
    std::hint::black_box(observed);
    std::hint::black_box(violation);
    std::hint::black_box(message);
}

//! Guaranteed, conditional, and terminating call-flow scenarios.

use super::*;

pub(super) fn invocation_trailing_arguments_must_complete_before_the_witness() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn message() -> &'static str { panic!("stop before detector"); }
fn fixture() { oracle_expect_err!(detector(), message()); }
"#;
    let error = verify(source)
        .expect_err("a diverging message expression cannot leave a credited detector witness");
    assert!(
        error.contains("does not invoke registered detector"),
        "{error}"
    );

    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn message() -> &'static str { "reject" }
fn fixture() { oracle_expect_err!(detector(), message()); }
"#;
    verify(source).expect("a returning message expression reaches the detector invocation");
}

pub(super) fn panic_and_unconditional_loop_make_later_invocations_unreachable() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { panic!("stop"); oracle_expect_err!(detector(), "reject"); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { loop {} oracle_expect_err!(detector(), "reject"); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { stop(); oracle_expect_err!(detector(), "reject"); }
fn stop() { loop {} }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { stop(); oracle_expect_err!(detector(), "reject"); }
fn stop() { loop {} return; }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { recurse(); oracle_expect_err!(detector(), "reject"); }
fn recurse() { recurse(); }
"#,
    ] {
        assert!(
            verify(source).is_err(),
            "known non-returning control flow must stop guaranteed reachability: {source}"
        );
    }
}

pub(super) fn conditional_helper_panic_does_not_hide_a_successful_bypass() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    maybe_fail();
    oracle_expect_err!(detector(), "reject");
}
fn maybe_fail() {
    if condition() {
        panic!("fixture setup failed");
    }
}
fn condition() -> bool { false }
"#;

    verify(source).expect("a setup panic can fail the fixture but cannot falsely pass it");
}

pub(super) fn conditional_non_returning_helper_makes_later_invocation_conditional() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    if condition() {
        stop();
    }
    oracle_expect_err!(detector(), "reject");
}
fn condition() -> bool { false }
fn stop() { loop {} }
"#;

    let error = verify(source)
        .expect_err("a conditional non-returning helper leaves later witnesses non-guaranteed");
    assert!(error.contains("conditional control flow"), "{error}");
}

pub(super) fn conditional_divergence_inside_helper_makes_caller_invocation_conditional() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    maybe_stop();
    oracle_expect_err!(detector(), "reject");
}
fn maybe_stop() {
    if condition() {
        loop {}
    }
}
fn condition() -> bool { false }
"#;

    let error = verify(source)
        .expect_err("conditional direct divergence must downgrade caller reachability");
    assert!(error.contains("conditional control flow"), "{error}");
}

pub(super) fn conditional_callable_divergence_makes_caller_invocation_conditional() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    maybe_call(stop);
    oracle_expect_err!(detector(), "reject");
}
fn maybe_call(call: fn()) {
    if condition() {
        call();
    }
}
fn stop() { loop {} }
fn condition() -> bool { false }
"#;

    let error = verify(source)
        .expect_err("conditional callable divergence must downgrade caller reachability");
    assert!(error.contains("conditional control flow"), "{error}");
}

pub(super) fn conditional_recursion_makes_later_invocation_conditional() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    maybe_recurse();
    oracle_expect_err!(detector(), "reject");
}
fn maybe_recurse() {
    if condition() {
        maybe_recurse();
    }
}
fn condition() -> bool { false }
"#;

    let error =
        verify(source).expect_err("conditional recursion must downgrade caller reachability");
    assert!(error.contains("conditional control flow"), "{error}");
}

pub(super) fn helper_loop_with_a_guaranteed_break_can_return_to_the_fixture() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { helper(); oracle_expect_err!(detector(), "reject"); }
fn helper() { loop { break; } }
"#;

    verify(source).expect("a helper loop with a guaranteed break completes normally");
}

pub(super) fn helper_loop_with_a_literal_true_conditional_break_can_return() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { helper(); oracle_expect_err!(detector(), "reject"); }
fn helper() { loop { if true { break; } } }
"#;

    verify(source).expect("the literal-true break gives the loop a completing path");
}

pub(super) fn recursive_invocation_helpers_are_rejected_until_multiplicity_is_bounded() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { recurse(); }
fn recurse() {
    oracle_expect_err!(detector(), "reject");
    recurse();
}
"#;

    let error =
        verify(source).expect_err("recursive invocation helpers have unknown witness multiplicity");
    assert!(
        error.contains("recursive") && error.contains("witness"),
        "{error}"
    );
}

pub(super) fn closure_local_return_does_not_exit_the_enclosing_fixture() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn invoke(call: impl FnOnce()) { call(); }
fn fixture() {
    invoke(|| return);
    oracle_expect_err!(detector(), "reject");
}
"#;

    verify(source).expect("returning from the closure leaves the fixture path reachable");
}

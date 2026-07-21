//! Stable detector fixtures consumed by the invariant evidence runner.

mod support;

fn token_bound_regression_detector() -> Result<(), &'static str> {
    Err("expected detector rejection")
}

#[rafter_invariant_test::detector_test]
#[ignore = "subprocess fixture for rafter-invariants"]
fn token_bound_detector_witness_subprocess_fixture() {
    let error = rafter_invariant_test::oracle_expect_err!(
        token_bound_regression_detector(),
        "fixture detector must reject"
    );
    assert_eq!(error, "expected detector rejection");
}

#[rafter_invariant_test::detector_test]
#[ignore = "adversarial subprocess fixture for rafter-invariants"]
fn fabricated_detector_witness_without_invocation_subprocess_fixture() {
    crate::__oracle_fabricated_detector_witness(
        "expect-err",
        "rafter_invariant_test::tests::token_bound_regression_detector",
    );
    rafter_invariant_test::oracle_assert!(true);
}

#[rafter_invariant_test::detector_test]
#[ignore = "adversarial subprocess fixture for rafter-invariants"]
fn qualified_helper_forged_transcript_subprocess_fixture() {
    crate::tests::support::emit_forged_transcript_and_exit();
    let _ = rafter_invariant_test::oracle_expect_err!(
        token_bound_regression_detector(),
        "fixture detector must reject"
    );
}

#[rafter_invariant_test::detector_test]
#[ignore = "subprocess fixture for rafter-invariants"]
fn proof_descriptor_is_hidden_from_fixture_body_subprocess_fixture() {
    assert!(
        std::env::var("RAFTER_INVARIANT_DETECTOR_PROOF_FD").is_err(),
        "detector proof descriptor must not be visible to fixture body code"
    );
    let error = rafter_invariant_test::oracle_expect_err!(
        token_bound_regression_detector(),
        "fixture detector must reject"
    );
    assert_eq!(error, "expected detector rejection");
}

#[rafter_invariant_test::detector_test]
#[ignore = "adversarial subprocess fixture for rafter-invariants"]
fn disclosed_proof_descriptor_is_closed_before_fixture_body_subprocess_fixture() {
    assert!(
        !crate::tests::support::can_request_challenge_on_disclosed_descriptor(),
        "fixture helper must not retain a live proof capability"
    );
    let error = rafter_invariant_test::oracle_expect_err!(
        token_bound_regression_detector(),
        "fixture detector must reject"
    );
    assert_eq!(error, "expected detector rejection");
}

#[rafter_invariant_test::detector_test]
#[ignore = "adversarial subprocess fixture for rafter-invariants"]
fn detector_witness_with_removed_token_subprocess_fixture() {
    let _ = rafter_invariant_test::oracle_expect_err!(
        token_bound_regression_detector(),
        "fixture detector must reject"
    );
    std::env::remove_var("RAFTER_INVARIANT_ORACLE_TOKEN");
}

#[rafter_invariant_test::detector_test]
fn ordinary_success_does_not_require_gate_environment() {
    rafter_invariant_test::oracle_assert!(true);
    rafter_invariant_test::oracle_assert_eq!(1, 1);
    rafter_invariant_test::oracle_assert_ne!(1, 2);
    let error =
        rafter_invariant_test::oracle_expect_err!(token_bound_regression_detector(), "must reject");
    assert_eq!(error, "expected detector rejection");
}

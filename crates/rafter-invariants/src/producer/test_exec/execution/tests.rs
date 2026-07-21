//! Exact-execution transcript policy tests.

use crate::evidence::format::libtest::{exact_failure, exact_pass, oracle_token};

use super::{classify_exact_execution, ExactTestExecution};

const SOURCE_REF: &str = "0123456789abcdef";

fn classify(
    stdout: &[u8],
    stderr: &[u8],
    test_name: &str,
    exit_code: Option<i32>,
    timed_out: bool,
) -> ExactTestExecution {
    let check_id = "crate::lib::target::module::test";
    classify_exact_execution(
        stdout,
        stderr,
        test_name,
        &oracle_token(SOURCE_REF, check_id),
        exit_code,
        timed_out,
    )
}

#[test]
fn exact_pass_rejects_zero_test_success() {
    let zero = b"running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored";
    assert!(!exact_pass(zero, "module::test"));
}

#[test]
fn exact_failure_requires_one_named_failed_oracle() {
    let failed = b"running 1 test\ntest module::test ... FAILED\n\ntest result: FAILED. 0 passed; 1 failed; 0 ignored\n";
    assert!(exact_failure(failed, "module::test"));
    assert!(!exact_failure(failed, "module::other"));
    assert!(!exact_failure(
        b"test process terminated by signal",
        "module::test"
    ));
}

#[test]
fn only_a_typed_exact_failure_is_an_oracle_violation() {
    let failed =
        b"running 1 test\ntest module::test ... FAILED\n\ntest result: FAILED. 0 passed; 1 failed; 0 ignored\n";
    let token = oracle_token(SOURCE_REF, "crate::lib::target::module::test");
    let marked = format!("thread panicked: RAFTER_INVARIANT_ORACLE_VIOLATION:{token}\n");
    assert_eq!(
        classify(failed, marked.as_bytes(), "module::test", Some(101), false),
        ExactTestExecution::InvariantViolation
    );
    assert_eq!(
        classify(failed, b"setup failed", "module::test", Some(101), false),
        ExactTestExecution::HarnessError
    );
    assert_eq!(
        classify(failed, marked.as_bytes(), "module::test", Some(1), false),
        ExactTestExecution::HarnessError
    );
    assert_eq!(
        classify(failed, marked.as_bytes(), "module::test", Some(101), true),
        ExactTestExecution::HarnessError
    );
    assert_eq!(
        classify(
            b"process aborted before libtest completed",
            marked.as_bytes(),
            "module::test",
            Some(101),
            false
        ),
        ExactTestExecution::HarnessError
    );
}

#[test]
fn exact_pass_requires_one_source_bound_observation() {
    let passed = b"running 1 test\ntest module::test ... ok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored\n";
    let token = oracle_token(SOURCE_REF, "crate::lib::target::module::test");
    let observed = format!("RAFTER_INVARIANT_ORACLE_OBSERVED:{token}\n");
    assert_eq!(
        classify(passed, observed.as_bytes(), "module::test", Some(0), false),
        ExactTestExecution::Pass
    );
    assert_eq!(
        classify(passed, b"", "module::test", Some(0), false),
        ExactTestExecution::CoverageNotReached
    );
    let foreign = b"RAFTER_INVARIANT_ORACLE_OBSERVED:foreign\n";
    assert_eq!(
        classify(passed, foreign, "module::test", Some(0), false),
        ExactTestExecution::HarnessError
    );
}

#[test]
fn exact_libtest_contract_rejects_duplicate_and_wrong_name_transcripts() {
    let duplicate = b"running 1 test\nrunning 1 test\ntest module::test ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored\n";
    assert_eq!(
        classify(duplicate, b"", "module::test", Some(0), false),
        ExactTestExecution::HarnessError
    );
    let wrong = b"running 1 test\ntest module::other ... FAILED\ntest result: FAILED. 0 passed; 1 failed; 0 ignored\n";
    assert_eq!(
        classify(wrong, b"", "module::test", Some(101), false),
        ExactTestExecution::HarnessError
    );
    let zero = b"running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored\n";
    assert_eq!(
        classify(zero, b"", "module::test", Some(0), false),
        ExactTestExecution::CoverageNotReached
    );
}

#[test]
fn malformed_failure_exit_codes_never_become_invariant_violations() {
    let failed = b"running 1 test\ntest module::test ... FAILED\ntest result: FAILED. 0 passed; 1 failed; 0 ignored\n";
    assert_eq!(
        classify(failed, b"", "module::test", None, false),
        ExactTestExecution::HarnessError
    );
    assert_eq!(
        classify(failed, b"", "module::test", Some(1), false),
        ExactTestExecution::HarnessError
    );
}

#[test]
fn timeout_overrides_pass_and_source_bound_violation_transcripts() {
    let passed = b"running 1 test\ntest module::test ... ok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored\n";
    assert_eq!(
        classify(passed, b"", "module::test", Some(0), true),
        ExactTestExecution::HarnessError
    );

    let failed = b"running 1 test\ntest module::test ... FAILED\ntest result: FAILED. 0 passed; 1 failed; 0 ignored\n";
    let marker = format!(
        "RAFTER_INVARIANT_ORACLE_VIOLATION:{}\n",
        oracle_token(SOURCE_REF, "crate::lib::target::module::test")
    );
    assert_eq!(
        classify(failed, marker.as_bytes(), "module::test", Some(101), true),
        ExactTestExecution::HarnessError
    );
}

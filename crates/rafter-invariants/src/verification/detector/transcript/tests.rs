//! Detector transcript qualification scenarios.

use std::collections::BTreeMap;

const TOKEN: &str = "oracle-test-token";
const CHALLENGE: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const TEST: &str = "fixture::detects_violation";
const WITNESS: &str = "expect-err:fixture::detects_violation";

#[test]
fn exact_execution_with_matching_witness_and_proof_is_qualified() {
    let (stdout, stderr) = transcript(TOKEN, CHALLENGE);

    super::qualify_detector_execution(
        stdout.as_bytes(),
        stderr.as_bytes(),
        TEST,
        TOKEN,
        CHALLENGE,
        &expected_witnesses(),
    )
    .expect("qualify exact detector execution");
}

#[test]
fn foreign_oracle_marker_is_rejected() {
    let (mut stdout, stderr) = transcript(TOKEN, CHALLENGE);
    stdout.push_str("RAFTER_INVARIANT_ORACLE_OBSERVED:foreign-token\n");

    let error = super::qualify_detector_execution(
        stdout.as_bytes(),
        stderr.as_bytes(),
        TEST,
        TOKEN,
        CHALLENGE,
        &expected_witnesses(),
    )
    .expect_err("foreign marker must fail qualification");

    assert!(error.contains("another token"), "{error}");
}

#[test]
fn proof_for_another_challenge_is_rejected() {
    let (stdout, stderr) = transcript(TOKEN, &"f".repeat(64));

    let error = super::qualify_detector_execution(
        stdout.as_bytes(),
        stderr.as_bytes(),
        TEST,
        TOKEN,
        CHALLENGE,
        &expected_witnesses(),
    )
    .expect_err("foreign challenge must fail qualification");

    assert!(error.contains("wrong pre-body challenge"), "{error}");
}

#[test]
fn missing_exact_libtest_result_is_rejected() {
    let (stdout, stderr) = transcript(TOKEN, CHALLENGE);
    let stdout = stdout.replace("test result: ok.", "test result: FAILED.");

    let error = super::qualify_detector_execution(
        stdout.as_bytes(),
        stderr.as_bytes(),
        TEST,
        TOKEN,
        CHALLENGE,
        &expected_witnesses(),
    )
    .expect_err("failed libtest result must fail qualification");

    assert!(error.contains("exact passing libtest"), "{error}");
}

#[test]
fn incidental_non_utf8_output_does_not_hide_a_valid_protocol_transcript() {
    let (stdout, stderr) = transcript(TOKEN, CHALLENGE);
    let mut stdout = stdout.into_bytes();
    stdout.extend_from_slice(&[0xff, b'\n']);

    super::qualify_detector_execution(
        &stdout,
        stderr.as_bytes(),
        TEST,
        TOKEN,
        CHALLENGE,
        &expected_witnesses(),
    )
    .expect("qualify detector execution with unrelated binary diagnostics");
}

#[test]
fn appended_libtest_execution_is_rejected() {
    let (mut stdout, stderr) = transcript(TOKEN, CHALLENGE);
    stdout.push_str(
        "running 1 test\ntest unrelated ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored\n",
    );

    let error = super::qualify_detector_execution(
        stdout.as_bytes(),
        stderr.as_bytes(),
        TEST,
        TOKEN,
        CHALLENGE,
        &expected_witnesses(),
    )
    .expect_err("a second libtest execution must fail qualification");
    assert!(error.contains("exact passing libtest"), "{error}");
}

fn transcript(token: &str, challenge: &str) -> (String, String) {
    let stdout = format!(
        "running 1 test\nRAFTER_INVARIANT_ORACLE_OBSERVED:{token}\ntest {TEST} ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n"
    );
    let stderr = format!(
        "RAFTER_INVARIANT_DETECTOR_WITNESS:{token}:{WITNESS}()\nRAFTER_INVARIANT_DETECTOR_PROOF:{token}:{WITNESS}():{challenge}\n"
    );
    (stdout, stderr)
}

fn expected_witnesses() -> BTreeMap<String, usize> {
    BTreeMap::from([(WITNESS.to_owned(), 1)])
}

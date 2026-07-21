//! Detector transcript and independent outcome-policy scenarios.

use std::{collections::BTreeMap, fmt::Write as _};

use super::{
    detector::{
        require_detector_witness_contract_in_streams, require_detector_witness_in_streams,
        verify_detector_harness_challenge,
    },
    policy::{classify_exact_execution, ExactTestExecution},
    require_detector_witness,
};

const CHALLENGE: [u8; crate::evidence::detector_proof::CHALLENGE_BYTES] = [0x5a; 32];

fn proven_transcript(token: &str, witnesses: &[&str]) -> (String, String) {
    let challenge = crate::evidence::detector_proof::encode_challenge(&CHALLENGE);
    let mut transcript = String::new();
    for witness in witnesses {
        write!(
            transcript,
            "{}{token}:{witness}()\n{}{token}:{witness}():{challenge}\n",
            crate::evidence::detector_proof::WITNESS_PREFIX,
            crate::evidence::detector_proof::PROOF_PREFIX,
        )
        .expect("writing to a String cannot fail");
    }
    (challenge, transcript)
}

#[test]
fn detector_proof_descriptor_must_be_canonical_and_non_standard() {
    assert!(crate::evidence::detector_proof::canonical_descriptor("3"));
    assert!(crate::evidence::detector_proof::canonical_descriptor("19"));
    for descriptor in ["", "0", "1", "2", "03", "+3", " 3", "3 ", "three"] {
        assert!(!crate::evidence::detector_proof::canonical_descriptor(
            descriptor
        ));
    }
}

#[test]
fn exact_detector_harness_receipt_requires_a_valid_parent_challenge() {
    let challenge = crate::evidence::detector_proof::encode_challenge(&CHALLENGE);
    verify_detector_harness_challenge(Some(&challenge))
        .expect("a parent-issued challenge is valid harness evidence");

    let missing = verify_detector_harness_challenge(None)
        .expect_err("an exact detector receipt cannot omit its challenge");
    assert!(missing.to_string().contains("omitted its challenge"));

    let malformed = verify_detector_harness_challenge(Some("not-a-challenge"))
        .expect_err("an exact detector receipt cannot invent a challenge");
    assert!(malformed.to_string().contains("challenge is invalid"));
}

#[test]
fn adversarial_noop_oracle_observation_cannot_qualify_a_detector() {
    let token = "source-bound-token";
    let stdout = format!("RAFTER_INVARIANT_ORACLE_OBSERVED:{token}\n");
    let challenge = crate::evidence::detector_proof::encode_challenge(&CHALLENGE);

    let error = require_detector_witness_in_streams(
        &stdout,
        "",
        token,
        &challenge,
        "fixture::check_committed_prefix_history_stability",
    )
    .expect_err("a generic true assertion must not qualify a detector");
    assert!(error.to_string().contains("no runtime witnesses"));
}

#[test]
fn detector_witness_contract_rejects_missing_duplicate_and_extra_markers() {
    let token = "source-bound-token";
    let expected = BTreeMap::from([
        ("recorder:fixture::record_observation".to_owned(), 1),
        ("expect-err:fixture::check_history".to_owned(), 1),
    ]);
    let (challenge, exact) = proven_transcript(
        token,
        &[
            "recorder:fixture::record_observation",
            "expect-err:fixture::check_history",
        ],
    );
    require_detector_witness_contract_in_streams("", &exact, token, &challenge, &expected)
        .expect("the exact source-derived witness multiset qualifies");

    for witnesses in [
        vec!["recorder:fixture::record_observation"],
        vec![
            "recorder:fixture::record_observation",
            "expect-err:fixture::check_history",
            "recorder:fixture::record_observation",
        ],
        vec![
            "recorder:fixture::record_observation",
            "expect-err:fixture::check_history",
            "recorder:fixture::unregistered",
        ],
    ] {
        let (altered_challenge, altered) = proven_transcript(token, &witnesses);
        assert!(require_detector_witness_contract_in_streams(
            "",
            &altered,
            token,
            &altered_challenge,
            &expected,
        )
        .is_err());
    }
}

#[test]
fn detector_expression_witness_qualifies_only_its_named_detector() {
    let token = "source-bound-token";
    let (challenge, stderr) = proven_transcript(
        token,
        &["expect-err:fixture::check_committed_prefix_history_stability"],
    );

    require_detector_witness_in_streams(
        "",
        &stderr,
        token,
        &challenge,
        "fixture::check_committed_prefix_history_stability",
    )
    .expect("the actual detector expression is witnessed");
    assert!(require_detector_witness_in_streams(
        "",
        &stderr,
        token,
        &challenge,
        "fixture::check_stable_commit_quorums",
    )
    .is_err());
    assert!(require_detector_witness_in_streams(
        "",
        &proven_transcript(
            token,
            &["expect-err:other(check_committed_prefix_history_stability())"],
        )
        .1,
        token,
        &challenge,
        "fixture::check_committed_prefix_history_stability",
    )
    .is_err());
}

#[test]
fn same_leaf_decoy_identity_cannot_qualify_the_registered_detector() {
    let token = "source-bound-token";
    let (challenge, stderr) = proven_transcript(token, &["expect-err:fixture::decoy::detector"]);
    let error = require_detector_witness_in_streams(
        "",
        &stderr,
        token,
        &challenge,
        "fixture::detector::detector",
    )
    .expect_err("a compiler-resolved same-leaf decoy must not qualify");
    assert!(error.to_string().contains("witness contract mismatch"));
}

#[test]
fn independent_policy_rejects_foreign_markers_and_malformed_exit_status() {
    let passed = b"running 1 test\ntest module::test ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored\n";
    let token = crate::evidence::format::libtest::oracle_token("source", "check");
    let foreign = b"RAFTER_INVARIANT_ORACLE_OBSERVED:foreign\n";
    assert_eq!(
        classify_exact_execution(passed, foreign, "module::test", &token, Some(0), false),
        ExactTestExecution::HarnessError
    );
    let observed = format!("RAFTER_INVARIANT_ORACLE_OBSERVED:{token}\n");
    assert_eq!(
        classify_exact_execution(
            passed,
            observed.as_bytes(),
            "module::test",
            &token,
            Some(1),
            false,
        ),
        ExactTestExecution::HarnessError
    );
}

#[test]
fn token_bound_macro_witness_survives_libtest_capture_and_process_framing() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .next()
        .expect("passing fixture bundle");
    bundle.source_ref = format!("e2e{:09}-detector-witness", std::process::id());
    let (check_id, source) = crate::producer::test_exec::capture_detector_witness_fixture_log(
        &bundle.source_ref,
        "token_bound_detector_witness_subprocess_fixture",
    )
    .expect("capture the real oracle macro through an exact libtest subprocess");

    require_detector_witness(
        &bundle,
        &source,
        &check_id,
        "rafter_invariant_test::tests::token_bound_regression_detector",
    )
    .expect("the framed exact-process log retains the source-bound detector witness");

    bundle.source_ref.push_str("-foreign");
    let error = require_detector_witness(
        &bundle,
        &source,
        &check_id,
        "rafter_invariant_test::tests::token_bound_regression_detector",
    )
    .expect_err("the captured witness must not qualify another source token");
    assert!(error.to_string().contains("another execution token"));
}

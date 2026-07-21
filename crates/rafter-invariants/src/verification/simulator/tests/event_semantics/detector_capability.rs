//! Adversarial tests for detector proof-capability isolation.

use super::support::*;

#[test]
fn fabricated_named_witness_without_detector_call_is_rejected_end_to_end() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = simulator_bundle(&catalog, &manifest);
    bundle.source_ref = format!("e2e{:09}-fabricated-witness", std::process::id());
    let fixture = "fabricated_detector_witness_without_invocation_subprocess_fixture";
    let detector = "token_bound_regression_detector";
    let (oracle_check_id, process_log) =
        crate::producer::test_exec::capture_fabricated_detector_witness_fixture_log(
            &bundle.source_ref,
            fixture,
        )
        .expect("capture the rejected fabricated marker through exact libtest framing");

    let processes = crate::evidence::format::process::parse_combined_processes(&process_log)
        .expect("parse fabricated witness process log");
    assert_eq!(processes.len(), 1);
    assert_ne!(processes[0].exit_code, Some(0));

    let error = crate::artifact_verify::test_logs::require_detector_witness(
        &bundle,
        &process_log,
        &oracle_check_id,
        "rafter_invariant_test::tests::token_bound_regression_detector",
    )
    .expect_err("a textual marker without a challenge-bound proof must be rejected");
    assert!(error.to_string().contains("proof"));

    let mut descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| descriptor.layer == "simulator" && descriptor.strength == "direct")
        .expect("registered direct simulator descriptor")
        .clone();
    descriptor.path = "crates/rafter-invariant-test/src/tests.rs".to_owned();
    descriptor.negative_fixture = Some(fixture.to_owned());
    descriptor.negative_fixture_path = Some(descriptor.path.clone());
    descriptor.negative_fixture_detector = Some(detector.to_owned());
    descriptor.negative_fixture_detector_path = Some(descriptor.path.clone());
    descriptor
        .simulator
        .as_mut()
        .expect("direct simulator identity")
        .negative_test = Some(crate::TestIdentity {
        package: "rafter-invariant-test".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_invariant_test".to_owned(),
        test_name: format!("tests::{fixture}"),
    });
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");

    let error = verify_negative_fixture_binding(
        &root,
        &descriptor,
        fixture,
        "adversarial-fabricated-witness",
    )
    .expect_err("source-bound verification must reject the self-attesting fixture");
    assert!(
        error
            .to_string()
            .contains("can emit an arbitrary detector witness"),
        "{error}"
    );
}

#[test]
fn qualified_helper_cannot_qualify_without_reaching_the_detector() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = simulator_bundle(&catalog, &manifest);
    bundle.source_ref = format!("e2e{:09}-qualified-helper", std::process::id());
    let fixture = "qualified_helper_forged_transcript_subprocess_fixture";
    let detector = "token_bound_regression_detector";
    let (oracle_check_id, process_log) =
        crate::producer::test_exec::capture_qualified_helper_forged_transcript_fixture_log(
            &bundle.source_ref,
        )
        .expect("compile and execute the qualified-helper regression fixture");

    let processes = crate::evidence::format::process::parse_combined_processes(&process_log)
        .expect("parse qualified-helper process log");
    let [exact] = processes.as_slice() else {
        panic!("expected one exact process receipt");
    };
    assert_eq!(exact.exit_code, Some(0));
    assert!(
        crate::producer::test_exec::exact_pass(
            exact.stdout.as_bytes(),
            &exact.invocation.arguments[0],
        ),
        "{}",
        exact.stdout
    );
    let token = crate::producer::test_exec::oracle_token(&bundle.source_ref, &oracle_check_id);
    assert_eq!(
        crate::producer::test_exec::classify_exact_execution(
            exact.stdout.as_bytes(),
            exact.stderr.as_bytes(),
            &exact.invocation.arguments[0],
            &token,
            exact.exit_code,
            exact.timed_out,
        ),
        crate::producer::test_exec::ExactTestExecution::Pass,
        "the regression must remain strong enough to satisfy the textual classifier"
    );

    let error = crate::artifact_verify::test_logs::require_detector_witness(
        &bundle,
        &process_log,
        &oracle_check_id,
        "rafter_invariant_test::tests::token_bound_regression_detector",
    )
    .expect_err("the final verifier must require the challenge-bound proof");
    assert!(error.to_string().contains("proof"));

    let mut descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| descriptor.layer == "simulator" && descriptor.strength == "direct")
        .expect("registered direct simulator descriptor")
        .clone();
    descriptor.path = "crates/rafter-invariant-test/src/tests.rs".to_owned();
    descriptor.negative_fixture = Some(fixture.to_owned());
    descriptor.negative_fixture_path = Some(descriptor.path.clone());
    descriptor.negative_fixture_detector = Some(detector.to_owned());
    descriptor.negative_fixture_detector_path = Some(descriptor.path.clone());
    descriptor
        .simulator
        .as_mut()
        .expect("direct simulator identity")
        .negative_test = Some(crate::TestIdentity {
        package: "rafter-invariant-test".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_invariant_test".to_owned(),
        test_name: format!("tests::{fixture}"),
    });
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let error = verify_negative_fixture_binding(
        &root,
        &descriptor,
        fixture,
        "adversarial-qualified-helper",
    )
    .expect_err("source verification must recursively inspect the qualified helper");
    assert!(
        error
            .to_string()
            .contains("can emit an arbitrary detector witness"),
        "{error}"
    );
}

#[test]
fn proof_socket_is_not_visible_to_fixture_body_code() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = simulator_bundle(&catalog, &manifest);
    bundle.source_ref = format!("e2e{:09}-hidden-proof-socket", std::process::id());
    let (oracle_check_id, process_log) =
        crate::producer::test_exec::capture_hidden_proof_socket_fixture_log(&bundle.source_ref)
            .expect("compile and execute the hidden-proof-socket fixture");

    crate::artifact_verify::test_logs::require_detector_witness(
        &bundle,
        &process_log,
        &oracle_check_id,
        "rafter_invariant_test::tests::token_bound_regression_detector",
    )
    .expect("the macro-held proof channel still validates after hiding the socket path");
}

#[test]
fn safe_external_helper_cannot_intercept_the_pre_body_proof_capability() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = simulator_bundle(&catalog, &manifest);
    bundle.source_ref = format!("e2e{:09}-closed-proof-capability", std::process::id());
    let fixture = "disclosed_proof_descriptor_is_closed_before_fixture_body_subprocess_fixture";
    let (oracle_check_id, process_log) =
        crate::producer::test_exec::capture_detector_witness_fixture_log(
            &bundle.source_ref,
            fixture,
        )
        .expect("a safe external helper cannot use the disclosed closed descriptor");

    crate::artifact_verify::test_logs::require_detector_witness(
        &bundle,
        &process_log,
        &oracle_check_id,
        "rafter_invariant_test::tests::token_bound_regression_detector",
    )
    .expect("the real invocation remains bound to the privately retained challenge");
}

#[test]
fn removing_the_gate_token_cannot_suppress_trusted_runtime_witnesses() {
    let source_ref = format!("e2e{:09}-removed-detector-token", std::process::id());
    let (_, process_log) =
        crate::producer::test_exec::capture_removed_token_detector_fixture_log(&source_ref)
            .expect("execute the token-removal fixture through exact libtest framing");
    let processes = crate::evidence::format::process::parse_combined_processes(&process_log)
        .expect("parse token-removal process log");
    assert_eq!(processes.len(), 1);
    assert_ne!(processes[0].exit_code, Some(0));
    assert!(process_log.contains("detector test returned without its gate token"));
}

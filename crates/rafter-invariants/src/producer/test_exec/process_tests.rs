//! Exact-process deadline and child-cleanup integration tests.

use super::*;

#[test]
fn proof_channel_failure_is_scoped_to_the_completed_exact_process() {
    let fixture = "producer::test_exec::detector_proof::tests::malformed_proof_request_fixture";
    let executable = std::env::current_exe().expect("locate current test executable");
    let mut environment = process::base_environment();
    let (_, manifest) = crate::tests::loaded();
    let _budget = process::LayerBudgetGuard::enter(
        "pr",
        "simulator",
        &manifest.profiles["pr"].runners["simulator"],
    )
    .expect("install simulator process budget");
    let execution = run_exact_process(
        executable.to_str().expect("test executable path is UTF-8"),
        &[
            fixture.into(),
            "--exact".into(),
            "--show-output".into(),
            "--color".into(),
            "never".into(),
        ],
        &mut environment,
        fixture,
        "fixture-oracle-token",
        true,
    )
    .expect("proof-channel failure remains a completed exact execution");

    assert_eq!(execution.classification, ExactTestExecution::HarnessError);
    assert_eq!(
        execution.harness_error.as_deref(),
        Some("detector proof channel failed: detector proof request is malformed")
    );
    assert!(execution.detector_challenge.is_some());
    assert!(execution.output.status.success());
    assert!(String::from_utf8_lossy(&execution.output.stdout)
        .contains("retained malformed-proof fixture output"));
}

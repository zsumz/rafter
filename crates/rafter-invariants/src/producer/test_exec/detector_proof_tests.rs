//! Producer adaptation tests for neutral detector challenge transport.

use std::time::Duration;

use nix::sys::socket::{send, MsgFlags};

use super::*;

#[test]
fn execution_and_evidence_protocol_contracts_match() {
    validate_protocol_contract().expect("execution and evidence use the same proof wire");
}

#[test]
fn typed_exchange_preserves_producer_error_classification() {
    assert_eq!(exchange_error(ChallengeExchange::Completed), None);
    assert_eq!(exchange_error(ChallengeExchange::Disconnected), None);
    assert_eq!(
        exchange_error(ChallengeExchange::MalformedRequest).as_deref(),
        Some("detector proof request is malformed")
    );
}

#[test]
fn channel_failure_retains_completed_child_output() {
    let fixture = "producer::test_exec::detector_proof::tests::malformed_proof_request_fixture";
    let executable = std::env::current_exe().expect("locate current test executable");
    let mut environment = process::base_environment();
    let execution = execute_for_test(
        executable.to_str().expect("test executable path is UTF-8"),
        &[
            fixture.into(),
            "--exact".into(),
            "--show-output".into(),
            "--color".into(),
            "never".into(),
        ],
        &mut environment,
    )
    .expect("completed child transcript survives proof channel failure");

    assert!(execution.output.status.success());
    assert!(String::from_utf8_lossy(&execution.output.stdout)
        .contains("retained malformed-proof fixture output"));
    assert_eq!(
        execution.channel_error.as_deref(),
        Some("detector proof request is malformed")
    );
}

#[test]
fn malformed_proof_request_fixture() {
    let Ok(descriptor) = std::env::var(crate::evidence::detector_proof::PROOF_DESCRIPTOR_ENV)
    else {
        return;
    };
    std::env::remove_var(crate::evidence::detector_proof::PROOF_DESCRIPTOR_ENV);
    let descriptor = descriptor
        .parse::<i32>()
        .expect("parse inherited detector proof descriptor");
    println!("retained malformed-proof fixture output");
    send(
        descriptor,
        &[crate::evidence::detector_proof::PROOF_REQUEST.wrapping_add(1)],
        MsgFlags::empty(),
    )
    .expect("write malformed proof request");
    std::thread::sleep(Duration::from_millis(100));
}

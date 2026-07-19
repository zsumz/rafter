use super::*;

#[test]
fn parent_releases_the_challenge_only_after_the_proof_request() {
    let (socket, responder, challenge) =
        challenge_listener().expect("create detector proof listener");
    assert_eq!(
        fs::metadata(&socket)
            .expect("proof socket metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let mut stream = UnixStream::connect(socket).expect("connect detector proof channel");
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("set pre-request timeout");
    let mut observed = [0_u8; crate::evidence::detector_proof::CHALLENGE_BYTES];
    let error = stream
        .read_exact(&mut observed)
        .expect_err("the parent must withhold its challenge before the request");
    assert!(matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ));

    stream
        .write_all(&[crate::evidence::detector_proof::PROOF_REQUEST])
        .expect("send proof request");
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set challenge timeout");
    stream
        .read_exact(&mut observed)
        .expect("read parent challenge");
    assert_eq!(observed, challenge);
    assert!(responder.finish().expect("join challenge responder"));
}

#[test]
fn disconnected_fixture_never_receives_a_challenge() {
    let (socket, responder, _) = challenge_listener().expect("create detector proof listener");
    drop(UnixStream::connect(socket).expect("connect detector proof channel"));
    assert!(!responder.finish().expect("join challenge responder"));
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
    let Ok(socket) = std::env::var(crate::evidence::detector_proof::PROOF_SOCKET_ENV) else {
        return;
    };
    println!("retained malformed-proof fixture output");
    let mut stream = UnixStream::connect(socket).expect("connect detector proof channel");
    stream
        .write_all(&[crate::evidence::detector_proof::PROOF_REQUEST.wrapping_add(1)])
        .expect("write malformed proof request");
    std::thread::sleep(Duration::from_millis(100));
}

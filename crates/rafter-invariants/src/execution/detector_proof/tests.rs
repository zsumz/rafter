//! Managed-socket integration tests for detector challenge gates.

use std::{
    fs,
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixStream},
    time::{Duration, Instant},
};

use super::{wire, ChallengeExchange, ChallengeGate};

#[test]
fn completed_request_receives_the_gates_challenge() {
    let gate = ChallengeGate::open().expect("open detector challenge gate");
    assert_eq!(
        fs::metadata(gate.socket_path())
            .expect("proof socket metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let expected = gate.challenge().encoded();
    let mut stream = UnixStream::connect(gate.socket_path()).expect("connect proof channel");
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("set pre-request timeout");
    let mut observed = [0_u8; wire::CHALLENGE_BYTES];
    let error = stream
        .read_exact(&mut observed)
        .expect_err("challenge must remain withheld before a request");
    assert!(matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ));

    stream
        .write_all(&[wire::PROOF_REQUEST])
        .expect("send proof request");
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set challenge timeout");
    stream
        .read_exact(&mut observed)
        .expect("read detector challenge");

    assert_eq!(wire::encode_challenge(&observed), expected);
    assert_eq!(gate.finish(), ChallengeExchange::Completed);
}

#[test]
fn connect_without_request_is_disconnected_and_bounded() {
    let gate = ChallengeGate::open().expect("open detector challenge gate");
    let socket = gate.socket_path().to_owned();
    let _stream = UnixStream::connect(&socket).expect("connect proof channel");
    let started = Instant::now();

    assert_eq!(gate.finish(), ChallengeExchange::Disconnected);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(!socket.exists());
}

#[test]
fn malformed_request_is_classified_without_releasing_challenge() {
    let gate = ChallengeGate::open().expect("open detector challenge gate");
    let mut stream = UnixStream::connect(gate.socket_path()).expect("connect proof channel");
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set close timeout");
    stream
        .write_all(&[wire::PROOF_REQUEST.wrapping_add(1)])
        .expect("send malformed request");

    let mut observed = [0_u8; wire::CHALLENGE_BYTES];
    assert_eq!(stream.read(&mut observed).expect("read closed channel"), 0);
    assert_eq!(gate.finish(), ChallengeExchange::MalformedRequest);
}

#[test]
fn dropping_gate_removes_socket_without_waiting_for_a_client() {
    let gate = ChallengeGate::open().expect("open detector challenge gate");
    let socket = gate.socket_path().to_owned();
    assert!(socket.exists());
    let started = Instant::now();

    drop(gate);

    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(!socket.exists());
}

#[test]
fn opening_another_gate_does_not_prune_a_fresh_managed_socket() {
    let first = ChallengeGate::open().expect("open first challenge gate");
    let first_socket = first.socket_path().to_owned();
    let second = ChallengeGate::open().expect("open second challenge gate");

    assert!(first_socket.exists());
    assert_eq!(first.finish(), ChallengeExchange::Disconnected);
    assert_eq!(second.finish(), ChallengeExchange::Disconnected);
}

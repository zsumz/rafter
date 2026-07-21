//! Inherited-descriptor integration tests for detector challenge gates.

use std::{
    io::{Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    time::{Duration, Instant},
};

use super::{wire, ChallengeExchange, ChallengeGate};

#[test]
fn completed_request_receives_the_gates_challenge() {
    let gate = ChallengeGate::open().expect("open detector challenge gate");
    let expected = gate.challenge().encoded();
    let mut stream = child_stream(&gate);
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
fn inherited_peer_without_request_is_disconnected_and_bounded() {
    let gate = ChallengeGate::open().expect("open detector challenge gate");
    let started = Instant::now();

    assert_eq!(gate.finish(), ChallengeExchange::Disconnected);
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn malformed_request_is_classified_without_releasing_challenge() {
    let gate = ChallengeGate::open().expect("open detector challenge gate");
    let mut stream = child_stream(&gate);
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
fn dropping_gate_closes_descriptors_without_waiting_for_a_request() {
    let gate = ChallengeGate::open().expect("open detector challenge gate");
    let started = Instant::now();

    drop(gate);

    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn concurrent_gates_own_distinct_child_descriptors() {
    let first = ChallengeGate::open().expect("open first challenge gate");
    let second = ChallengeGate::open().expect("open second challenge gate");

    assert_ne!(
        first.child_descriptor().as_raw_fd(),
        second.child_descriptor().as_raw_fd()
    );
    assert_eq!(first.finish(), ChallengeExchange::Disconnected);
    assert_eq!(second.finish(), ChallengeExchange::Disconnected);
}

fn child_stream(gate: &ChallengeGate) -> UnixStream {
    UnixStream::from(
        gate.child_descriptor()
            .try_clone_to_owned()
            .expect("clone inherited child descriptor"),
    )
}

//! Adversarial state-classification tests over descriptor-local socket pairs.

use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    sync::atomic::AtomicBool,
};

use super::*;

fn challenge() -> DetectorChallenge {
    DetectorChallenge::new([0x5a; wire::CHALLENGE_BYTES])
}

#[test]
fn completed_request_releases_exact_challenge() {
    let (mut fixture, responder) = UnixStream::pair().expect("create proof channel pair");
    fixture
        .write_all(&[wire::PROOF_REQUEST])
        .expect("write proof request");

    let exchange = answer_challenge(responder, &challenge(), &AtomicBool::new(false));
    let mut observed = [0_u8; wire::CHALLENGE_BYTES];
    fixture
        .read_exact(&mut observed)
        .expect("read detector challenge");

    assert_eq!(exchange, ChallengeExchange::Completed);
    assert_eq!(observed, [0x5a; wire::CHALLENGE_BYTES]);
}

#[test]
fn connected_peer_without_request_is_cancelled_as_disconnected() {
    let (_fixture, responder) = UnixStream::pair().expect("create proof channel pair");

    assert_eq!(
        answer_challenge(responder, &challenge(), &AtomicBool::new(true)),
        ChallengeExchange::Disconnected
    );
}

#[test]
fn peer_disconnect_before_request_is_classified() {
    let (fixture, responder) = UnixStream::pair().expect("create proof channel pair");
    drop(fixture);

    assert_eq!(
        answer_challenge(responder, &challenge(), &AtomicBool::new(false)),
        ChallengeExchange::Disconnected
    );
}

#[test]
fn malformed_request_is_classified_without_a_challenge() {
    let (mut fixture, responder) = UnixStream::pair().expect("create proof channel pair");
    fixture
        .write_all(&[wire::PROOF_REQUEST.wrapping_add(1)])
        .expect("write malformed request");

    assert_eq!(
        answer_challenge(responder, &challenge(), &AtomicBool::new(false)),
        ChallengeExchange::MalformedRequest
    );
    let mut observed = [0_u8; wire::CHALLENGE_BYTES];
    assert_eq!(fixture.read(&mut observed).expect("read closed channel"), 0);
}

#[test]
fn challenge_write_failure_is_a_transport_error() {
    let (mut fixture, responder) = UnixStream::pair().expect("create proof channel pair");
    fixture
        .write_all(&[wire::PROOF_REQUEST])
        .expect("write proof request");
    drop(fixture);

    assert!(matches!(
        answer_challenge(responder, &challenge(), &AtomicBool::new(false)),
        ChallengeExchange::TransportError(_)
    ));
}

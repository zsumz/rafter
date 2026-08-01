mod support;

use rafter_transport_tls::{
    decode_transport_session_state, encode_transport_session_state,
    max_transport_session_state_bytes, ClusterId, ConnectionSession,
    DecodeTransportSessionStateError, PeerId, PersistedTransportSessionState, SessionStoreLimits,
    TransportSessionState,
};

use support::decode_hex;

fn expected_state() -> PersistedTransportSessionState {
    let mut state =
        TransportSessionState::new(SessionStoreLimits::new(4).expect("valid peer bound"));
    let node_b = PeerId::new("orders-node-b").expect("valid peer");
    let node_c = PeerId::new("orders-node-c").expect("valid peer");
    for _ in 0..7 {
        state
            .allocate_outbound(&node_b)
            .expect("allocate node-b session");
    }
    state
        .accept_inbound(&node_b, ConnectionSession::new(9).expect("nonzero"))
        .expect("accept node-b session");
    for _ in 0..11 {
        state
            .allocate_outbound(&node_c)
            .expect("allocate node-c session");
    }
    PersistedTransportSessionState::new(
        ClusterId::new("orders-production-us1").expect("valid cluster"),
        PeerId::new("orders-node-a").expect("valid local peer"),
        state,
    )
}

#[test]
fn session_state_v1_golden_vector_round_trips_exactly() {
    let expected = decode_hex(include_str!("../format/session-state-v1.hex"));
    let state = expected_state();

    assert_eq!(
        encode_transport_session_state(&state).expect("encode state"),
        expected
    );
    assert_eq!(
        decode_transport_session_state(&expected).expect("decode state"),
        state
    );
}

#[test]
fn checksum_corruption_is_refused() {
    let mut bytes = decode_hex(include_str!("../format/session-state-v1.hex"));
    bytes[64] ^= 0x01;

    assert!(matches!(
        decode_transport_session_state(&bytes),
        Err(DecodeTransportSessionStateError::ChecksumMismatch { .. })
    ));
}

#[test]
fn duplicate_peer_records_are_noncanonical() {
    let mut bytes = decode_hex(include_str!("../format/session-state-v1.hex"));
    bytes[81..94].copy_from_slice(b"orders-node-b");
    replace_checksum(&mut bytes);

    assert!(matches!(
        decode_transport_session_state(&bytes),
        Err(DecodeTransportSessionStateError::NonCanonicalPeerOrder { .. })
    ));
}

#[test]
fn empty_peer_records_are_refused() {
    let mut bytes = decode_hex(include_str!("../format/session-state-v1.hex"));
    bytes[64..80].fill(0);
    replace_checksum(&mut bytes);

    assert!(matches!(
        decode_transport_session_state(&bytes),
        Err(DecodeTransportSessionStateError::EmptyPeerRecord { .. })
    ));
}

#[test]
fn encoded_record_count_cannot_exceed_encoded_bound() {
    let mut bytes = decode_hex(include_str!("../format/session-state-v1.hex"));
    bytes[46..48].copy_from_slice(&1_u16.to_be_bytes());
    replace_checksum(&mut bytes);

    assert_eq!(
        decode_transport_session_state(&bytes),
        Err(DecodeTransportSessionStateError::PeerCountExceedsLimit {
            count: 2,
            maximum: 1,
        })
    );
}

#[test]
fn zero_encoded_peer_bound_is_refused() {
    let mut bytes = decode_hex(include_str!("../format/session-state-v1.hex"));
    bytes[46..48].fill(0);
    replace_checksum(&mut bytes);

    assert!(matches!(
        decode_transport_session_state(&bytes),
        Err(DecodeTransportSessionStateError::InvalidPeerLimit { .. })
    ));
}

#[test]
fn unsupported_state_version_is_refused() {
    let mut bytes = decode_hex(include_str!("../format/session-state-v1.hex"));
    bytes[8..10].copy_from_slice(&2_u16.to_be_bytes());
    replace_checksum(&mut bytes);

    assert_eq!(
        decode_transport_session_state(&bytes),
        Err(DecodeTransportSessionStateError::UnsupportedVersion { version: 2 })
    );
}

#[test]
fn trailing_bytes_after_the_checksum_are_refused() {
    let mut bytes = decode_hex(include_str!("../format/session-state-v1.hex"));
    bytes.push(0);

    assert_eq!(
        decode_transport_session_state(&bytes),
        Err(DecodeTransportSessionStateError::TrailingBytes { remaining: 1 })
    );
}

#[test]
fn absolute_file_bound_is_finite_and_covers_the_golden_vector() {
    let maximum = max_transport_session_state_bytes(SessionStoreLimits::MAX);
    let golden = decode_hex(include_str!("../format/session-state-v1.hex"));

    assert!(maximum < 10 * 1024 * 1024);
    assert!(golden.len() <= maximum);
}

fn replace_checksum(bytes: &mut [u8]) {
    let checksum_start = bytes.len() - 4;
    let checksum = rafter_crc32::crc32(&bytes[..checksum_start]);
    bytes[checksum_start..].copy_from_slice(&checksum.to_be_bytes());
}

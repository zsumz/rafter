mod support;

use std::num::{NonZeroU16, NonZeroU32};

use rafter_transport_tls::{
    decode_client_hello, decode_server_hello, encode_client_hello_into, encode_server_hello_into,
    highest_common_version, ClientHello, ClusterId, ConnectionSession, DecodeHandshakeError,
    PeerId, ServerHello, ServerHelloStatus, ServerRefusal, VersionRange, DEFAULT_MAX_FRAME_BYTES,
};

use support::decode_hex;

#[test]
fn client_hello_v1_golden_vector_round_trips_exactly() {
    let expected = decode_hex(include_str!("../format/client-hello-v1.hex"));
    let hello = ClientHello::new(
        VersionRange::new(1, 1).expect("valid range"),
        VersionRange::new(1, 1).expect("valid range"),
        ClusterId::new("orders-production-us1").expect("valid cluster"),
        PeerId::new("orders-node-a").expect("valid peer"),
        ConnectionSession::new(7).expect("nonzero session"),
        NonZeroU32::new(u32::try_from(DEFAULT_MAX_FRAME_BYTES).expect("default fits u32"))
            .expect("nonzero frame bound"),
    );

    let mut encoded = Vec::new();
    encode_client_hello_into(&mut encoded, &hello);
    assert_eq!(encoded, expected);
    assert_eq!(decode_client_hello(&encoded).expect("decode"), hello);
}

#[test]
fn server_hello_v1_golden_vector_round_trips_exactly() {
    let expected = decode_hex(include_str!("../format/server-hello-v1.hex"));
    let hello = ServerHello::accepted(
        NonZeroU16::new(1).expect("nonzero version"),
        NonZeroU16::new(1).expect("nonzero version"),
        ClusterId::new("orders-production-us1").expect("valid cluster"),
        PeerId::new("orders-node-b").expect("valid peer"),
        NonZeroU32::new(u32::try_from(DEFAULT_MAX_FRAME_BYTES).expect("default fits u32"))
            .expect("nonzero frame bound"),
    );

    let mut encoded = Vec::new();
    encode_server_hello_into(&mut encoded, &hello);
    assert_eq!(encoded, expected);
    assert_eq!(decode_server_hello(&encoded).expect("decode"), hello);
}

#[test]
fn refusal_has_one_canonical_wire_shape() {
    let hello = ServerHello::refused(
        ClusterId::new("orders-production-us1").expect("valid cluster"),
        PeerId::new("orders-node-b").expect("valid peer"),
        ServerRefusal::ClusterMismatch,
    );
    let mut encoded = Vec::new();
    encode_server_hello_into(&mut encoded, &hello);

    let decoded = decode_server_hello(&encoded).expect("decode refusal");
    assert_eq!(
        decoded.status(),
        ServerHelloStatus::Refused(ServerRefusal::ClusterMismatch)
    );
    assert_eq!(decoded.selected_transport_version(), None);
    assert_eq!(decoded.selected_peer_codec_version(), None);
    assert_eq!(decoded.accepted_frame_bytes(), None);

    encoded[10] = 0;
    encoded[11] = 1;
    assert!(matches!(
        decode_server_hello(&encoded),
        Err(DecodeHandshakeError::NonCanonicalRefusal)
    ));
}

#[test]
fn hello_decoders_reject_truncation_and_trailing_bytes() {
    let bytes = decode_hex(include_str!("../format/client-hello-v1.hex"));
    assert!(matches!(
        decode_client_hello(&bytes[..bytes.len() - 1]),
        Err(DecodeHandshakeError::Truncated)
    ));

    let mut trailing = bytes;
    trailing.push(0);
    assert!(matches!(
        decode_client_hello(&trailing),
        Err(DecodeHandshakeError::TrailingBytes { remaining: 1 })
    ));
}

#[test]
fn negotiation_selects_the_highest_common_version() {
    let local = VersionRange::new(1, 4).expect("valid range");
    let remote = VersionRange::new(2, 3).expect("valid range");
    let disjoint = VersionRange::new(5, 6).expect("valid range");

    assert_eq!(highest_common_version(local, remote), Some(3));
    assert_eq!(highest_common_version(local, disjoint), None);
}

#[test]
fn zero_session_and_frame_limit_are_refused_during_decode() {
    let mut bytes = decode_hex(include_str!("../format/client-hello-v1.hex"));
    let session_offset = bytes.len() - 12;
    bytes[session_offset..session_offset + 8].fill(0);
    assert!(matches!(
        decode_client_hello(&bytes),
        Err(DecodeHandshakeError::ZeroSession)
    ));

    let mut bytes = decode_hex(include_str!("../format/client-hello-v1.hex"));
    let frame_offset = bytes.len() - 4;
    bytes[frame_offset..].fill(0);
    assert!(matches!(
        decode_client_hello(&bytes),
        Err(DecodeHandshakeError::ZeroFrameLimit)
    ));
}

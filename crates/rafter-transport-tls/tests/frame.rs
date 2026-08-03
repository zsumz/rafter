mod support;

use rafter::NodeId;
use rafter_transport_tls::{
    ConnectionSequence, DecodePeerFrameError, PeerFrame, PeerFrameCodec, PeerFrameCodecConfigError,
    PeerFrameError, PeerFrameScratch, WireLimits, DEFAULT_MAX_FRAME_BODY_BYTES,
};

use support::{decode_hex, request_vote, LowercaseGroupCodec, StringGroupCodec};

fn codec() -> PeerFrameCodec<String, StringGroupCodec> {
    PeerFrameCodec::new(StringGroupCodec::new(128), WireLimits::default())
        .expect("compatible codec")
}

#[test]
fn peer_frame_v1_golden_vector_round_trips_exactly() {
    let expected = decode_hex(include_str!("../format/peer-frame-v1.hex"));
    let frame = PeerFrame::new(
        ConnectionSequence::new(1).expect("nonzero sequence"),
        "orders".to_owned(),
        NodeId(7),
        NodeId(9),
        request_vote(NodeId(7)),
    )
    .expect("matching sender");
    let mut scratch = PeerFrameScratch::new();
    let mut encoded = Vec::new();

    codec()
        .encode_into(&mut encoded, &mut scratch, &frame)
        .expect("encode");
    assert_eq!(encoded, expected);
    assert_eq!(
        codec().decode(&encoded, &mut scratch).expect("decode"),
        frame
    );
}

#[test]
fn frame_construction_rejects_outer_and_inner_sender_disagreement() {
    assert_eq!(
        PeerFrame::new(
            ConnectionSequence::new(1).expect("nonzero sequence"),
            "orders".to_owned(),
            NodeId(8),
            NodeId(9),
            request_vote(NodeId(7)),
        ),
        Err(PeerFrameError::SenderMismatch {
            envelope_from: NodeId(8),
            message_from: NodeId(7),
        })
    );
}

#[test]
fn decoder_rejects_outer_and_inner_sender_disagreement() {
    let mut bytes = decode_hex(include_str!("../format/peer-frame-v1.hex"));
    let from_offset = 4 + 1 + 8 + 2 + "orders".len();
    bytes[from_offset..from_offset + 8].copy_from_slice(&8_u64.to_be_bytes());

    assert!(matches!(
        codec().decode(&bytes, &mut PeerFrameScratch::new()),
        Err(DecodePeerFrameError::SenderMismatch {
            envelope_from: NodeId(8),
            message_from: NodeId(7),
        })
    ));
}

#[test]
fn decoder_rejects_noncanonical_group_route_bytes() {
    let mut bytes = decode_hex(include_str!("../format/peer-frame-v1.hex"));
    let group_offset = 4 + 1 + 8 + 2;
    bytes[group_offset..group_offset + 6].copy_from_slice(b"ORDERS");
    let codec =
        PeerFrameCodec::<String, _>::new(LowercaseGroupCodec::new(128), WireLimits::default())
            .expect("compatible codec");

    assert!(matches!(
        codec.decode(&bytes, &mut PeerFrameScratch::new()),
        Err(DecodePeerFrameError::NonCanonicalGroupId)
    ));
}

#[test]
fn decoded_group_bound_includes_canonicalization_temporaries() {
    let maximum = 128;
    let codec =
        PeerFrameCodec::<String, _>::new(LowercaseGroupCodec::new(maximum), WireLimits::default())
            .expect("compatible codec");

    assert_eq!(
        codec.max_decoded_group_bytes(),
        size_of::<String>() + maximum * 2
    );
}

#[test]
fn declared_frame_bound_is_checked_before_body_parsing() {
    let mut bytes = decode_hex(include_str!("../format/peer-frame-v1.hex"));
    let oversized = u32::try_from(DEFAULT_MAX_FRAME_BODY_BYTES + 1).expect("default fits");
    bytes[..4].copy_from_slice(&oversized.to_be_bytes());

    assert!(matches!(
        codec().decode(&bytes, &mut PeerFrameScratch::new()),
        Err(DecodePeerFrameError::FrameTooLarge {
            declared,
            maximum: DEFAULT_MAX_FRAME_BODY_BYTES,
        }) if declared == DEFAULT_MAX_FRAME_BODY_BYTES + 1
    ));
}

#[test]
fn decoder_rejects_zero_sequence() {
    let mut bytes = decode_hex(include_str!("../format/peer-frame-v1.hex"));
    bytes[5..13].fill(0);

    assert!(matches!(
        codec().decode(&bytes, &mut PeerFrameScratch::new()),
        Err(DecodePeerFrameError::ZeroSequence)
    ));
}

#[test]
fn decoder_rejects_trailing_bytes_outside_the_declared_frame() {
    let mut bytes = decode_hex(include_str!("../format/peer-frame-v1.hex"));
    bytes.push(0);

    assert!(matches!(
        codec().decode(&bytes, &mut PeerFrameScratch::new()),
        Err(DecodePeerFrameError::TrailingBytes { remaining: 1 })
    ));
}

#[test]
fn codec_refuses_a_group_bound_larger_than_the_wire_contract() {
    let wire = WireLimits::new(128, 4).expect("valid small limits");
    let result = PeerFrameCodec::<String, _>::new(StringGroupCodec::new(5), wire);

    assert!(matches!(
        result,
        Err(PeerFrameCodecConfigError::GroupIdBoundTooLarge {
            codec_maximum: 5,
            wire_maximum: 4,
        })
    ));
}

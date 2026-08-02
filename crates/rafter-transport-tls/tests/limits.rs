use rafter_transport_tls::{
    CertificateDirectoryLimits, DirectoryLimits, EndpointBookLimits, LimitError, LimitKind,
    SessionStoreLimits, TransportLimits, WireLimits, DEFAULT_MAX_APPEND_ENTRIES_BYTES,
    DEFAULT_MAX_FRAME_BODY_BYTES, DEFAULT_MAX_FRAME_BYTES, DEFAULT_MAX_GROUP_ID_BYTES,
    MAX_SESSION_PEER_RECORDS, PEER_FRAME_FIXED_BODY_BYTES, PEER_FRAME_LENGTH_PREFIX_BYTES,
};

#[test]
fn default_wire_limit_is_derived_from_the_codec_receive_contract() {
    let expected_body = PEER_FRAME_FIXED_BODY_BYTES
        + DEFAULT_MAX_GROUP_ID_BYTES
        + rafter_codec::max_receive_frame_bytes(DEFAULT_MAX_APPEND_ENTRIES_BYTES);

    assert_eq!(DEFAULT_MAX_FRAME_BODY_BYTES, expected_body);
    assert_eq!(
        DEFAULT_MAX_FRAME_BYTES,
        expected_body + PEER_FRAME_LENGTH_PREFIX_BYTES
    );
    assert_eq!(
        WireLimits::default().max_frame_bytes(),
        DEFAULT_MAX_FRAME_BYTES
    );
}

#[test]
fn every_retained_directory_bound_must_be_finite_and_nonzero() {
    assert_eq!(
        DirectoryLimits::new(0, 1),
        Err(LimitError::Zero {
            kind: LimitKind::Groups,
        })
    );
    assert_eq!(
        EndpointBookLimits::new(1, 0),
        Err(LimitError::Zero {
            kind: LimitKind::EndpointsPerPeer,
        })
    );
    assert_eq!(
        CertificateDirectoryLimits::new(0, 1),
        Err(LimitError::Zero {
            kind: LimitKind::CertificateFingerprints,
        })
    );
}

#[test]
fn frame_body_must_leave_room_for_fixed_fields_group_and_message() {
    let minimum = PEER_FRAME_FIXED_BODY_BYTES + 8 + 1;
    assert_eq!(
        WireLimits::new(minimum - 1, 8),
        Err(LimitError::FrameBodyTooSmall {
            frame_body_bytes: minimum - 1,
            minimum,
        })
    );
    assert!(WireLimits::new(minimum, 8).is_ok());
}

#[test]
fn complete_advertised_frame_must_fit_the_handshake_u32() {
    let u32_max = usize::try_from(u32::MAX).expect("test target holds u32");
    let maximum_body = u32_max - PEER_FRAME_LENGTH_PREFIX_BYTES;
    let error = WireLimits::new(maximum_body + 1, 1).expect_err("complete frame would exceed u32");

    assert_eq!(
        error,
        LimitError::TooLarge {
            kind: LimitKind::FrameBodyBytes,
            value: maximum_body + 1,
            maximum: maximum_body,
        }
    );
}

#[test]
fn durable_session_bound_is_positive_and_wire_representable() {
    assert_eq!(
        SessionStoreLimits::new(0),
        Err(LimitError::Zero {
            kind: LimitKind::SessionPeers,
        })
    );
    assert_eq!(
        SessionStoreLimits::new(MAX_SESSION_PEER_RECORDS + 1),
        Err(LimitError::TooLarge {
            kind: LimitKind::SessionPeers,
            value: MAX_SESSION_PEER_RECORDS + 1,
            maximum: MAX_SESSION_PEER_RECORDS,
        })
    );
}

#[test]
fn aggregate_limits_preserve_the_existing_constructor_and_allow_session_override() {
    let limits = TransportLimits::default();
    assert_eq!(limits.sessions(), SessionStoreLimits::default());

    let sessions = SessionStoreLimits::new(7).expect("representable session bound");
    assert_eq!(limits.with_sessions(sessions).sessions(), sessions);
}

#[test]
fn runtime_limits_refuse_zero_and_crossed_reservations() {
    use rafter_transport_tls::{
        InboundQueueLimits, OutboundQueueLimits, RuntimeLimitError, RuntimeLimitKind, RuntimeLimits,
    };

    assert_eq!(
        OutboundQueueLimits::new(8, 1024, 0, 128, 1),
        Err(RuntimeLimitError::Zero {
            kind: RuntimeLimitKind::ReservedControlFrames,
        })
    );
    assert_eq!(
        OutboundQueueLimits::new(8, 1024, 9, 128, 1),
        Err(RuntimeLimitError::ControlReserveExceedsTotal {
            reserved_frames: 9,
            total_frames: 8,
            reserved_bytes: 128,
            total_bytes: 1024,
        })
    );
    assert_eq!(
        OutboundQueueLimits::new(8, 1024, 8, 128, 1),
        Err(RuntimeLimitError::ControlReserveConsumesTotal {
            reserved_frames: 8,
            total_frames: 8,
            reserved_bytes: 128,
            total_bytes: 1024,
        })
    );
    assert_eq!(
        OutboundQueueLimits::new(8, 1024, 1, 1024, 1),
        Err(RuntimeLimitError::ControlReserveConsumesTotal {
            reserved_frames: 1,
            total_frames: 8,
            reserved_bytes: 1024,
            total_bytes: 1024,
        })
    );
    assert_eq!(
        InboundQueueLimits::new(5, 1024, 4, 2048),
        Err(RuntimeLimitError::PeerInboundExceedsGlobal {
            peer_frames: 5,
            global_frames: 4,
            peer_bytes: 1024,
            global_bytes: 2048,
        })
    );
    assert_eq!(
        RuntimeLimits::new(
            OutboundQueueLimits::default(),
            InboundQueueLimits::default(),
            0,
        ),
        Err(RuntimeLimitError::Zero {
            kind: RuntimeLimitKind::InboundConnections,
        })
    );
}

#[test]
fn aggregate_limits_allow_an_explicit_runtime_override() {
    use rafter_transport_tls::{InboundQueueLimits, OutboundQueueLimits, RuntimeLimits};

    let runtime = RuntimeLimits::new(
        OutboundQueueLimits::new(16, 4096, 4, 1024, 2).expect("outbound limits"),
        InboundQueueLimits::new(4, 2048, 8, 4096).expect("inbound limits"),
        3,
    )
    .expect("runtime limits");

    assert_eq!(
        TransportLimits::default().with_runtime(runtime).runtime(),
        runtime
    );
}

#[test]
fn receive_memory_limits_are_finite_and_replaceable() {
    use rafter_transport_tls::{
        ReceiveMemoryLimits, RuntimeLimitError, RuntimeLimitKind, RuntimeLimits,
        MIN_SAFE_DECODE_AMPLIFICATION,
    };

    assert_eq!(
        ReceiveMemoryLimits::new(0, MIN_SAFE_DECODE_AMPLIFICATION),
        Err(RuntimeLimitError::Zero {
            kind: RuntimeLimitKind::ReceiveMemoryBytes,
        })
    );
    assert_eq!(
        ReceiveMemoryLimits::new(1, MIN_SAFE_DECODE_AMPLIFICATION - 1),
        Err(RuntimeLimitError::DecodeAmplificationTooSmall {
            actual: MIN_SAFE_DECODE_AMPLIFICATION - 1,
            minimum: MIN_SAFE_DECODE_AMPLIFICATION,
        })
    );
    let memory = ReceiveMemoryLimits::new(64 * 1024 * 1024, 40).expect("memory limits");
    assert_eq!(
        RuntimeLimits::default()
            .with_receive_memory(memory)
            .receive_memory(),
        memory
    );
}

#[test]
fn timeout_groups_refuse_zero_before_aggregate_configuration() {
    use std::time::Duration;

    use rafter_transport_tls::{
        TimeoutKind, TransportIoTimeouts, TransportRuntimeTimeouts, TransportTimeouts,
    };

    let second = Duration::from_secs(1);
    for (error, expected) in [
        (
            TransportIoTimeouts::new(Duration::ZERO, second, second, second)
                .expect_err("zero connect timeout"),
            TimeoutKind::Connect,
        ),
        (
            TransportIoTimeouts::new(second, Duration::ZERO, second, second)
                .expect_err("zero handshake timeout"),
            TimeoutKind::Handshake,
        ),
        (
            TransportIoTimeouts::new(second, second, Duration::ZERO, second)
                .expect_err("zero read timeout"),
            TimeoutKind::Read,
        ),
        (
            TransportIoTimeouts::new(second, second, second, Duration::ZERO)
                .expect_err("zero write timeout"),
            TimeoutKind::Write,
        ),
    ] {
        assert_eq!(error.kind(), expected);
    }
    assert_eq!(
        TransportRuntimeTimeouts::default()
            .with_configuration_reprobe(Duration::ZERO)
            .expect_err("zero configuration reprobe timeout")
            .kind(),
        TimeoutKind::ConfigurationReprobe
    );
    let excessive_redial = TransportRuntimeTimeouts::new(Duration::from_secs(60), second, second)
        .expect_err("redial above the retry ceiling");
    assert_eq!(excessive_redial.kind(), TimeoutKind::Redial);
    assert!(excessive_redial.to_string().contains("30s"));
    for (error, expected) in [
        (
            TransportRuntimeTimeouts::new(Duration::ZERO, second, second)
                .expect_err("zero redial timeout"),
            TimeoutKind::Redial,
        ),
        (
            TransportRuntimeTimeouts::new(second, Duration::ZERO, second)
                .expect_err("zero poll timeout"),
            TimeoutKind::Poll,
        ),
        (
            TransportRuntimeTimeouts::new(second, second, Duration::ZERO)
                .expect_err("zero shutdown timeout"),
            TimeoutKind::ShutdownGrace,
        ),
    ] {
        assert_eq!(error.kind(), expected);
    }

    let io = TransportIoTimeouts::new(second, second, second, second).expect("I/O timeouts");
    let runtime = TransportRuntimeTimeouts::new(second, second, second).expect("runtime timeouts");
    let aggregate = TransportTimeouts::new(io, runtime);
    assert_eq!(aggregate.io(), io);
    assert_eq!(aggregate.runtime(), runtime);
    assert_eq!(
        TransportRuntimeTimeouts::default()
            .with_configuration_reprobe(second)
            .expect("configuration reprobe timeout"),
        TransportRuntimeTimeouts::default()
            .with_configuration_reprobe(second)
            .expect("same configuration reprobe timeout")
    );
}

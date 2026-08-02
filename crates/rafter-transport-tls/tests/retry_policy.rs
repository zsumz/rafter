mod support;

#[path = "support/fault_peer.rs"]
mod fault_peer;

use std::{thread, time::Duration};

use rafter_service::RaftTransport;
use rafter_transport_tls::{
    PeerConnectionState, PeerEndpoint, RuntimeLimits, TrafficClass, TransportIoTimeouts,
    TransportLimits, TransportRuntimeTimeouts, TransportTimeouts, WireLimits,
};

use fault_peer::{FaultPeer, PeerBehavior};
use support::{
    runtime::{wait_until, RuntimeFixture},
    session_store::MemorySessionStore,
    tls::server_name,
};

#[test]
fn handshake_then_close_preserves_capped_backoff_until_a_frame_succeeds() {
    let runtime = TransportRuntimeTimeouts::new(
        Duration::from_millis(100),
        Duration::from_millis(5),
        Duration::from_millis(250),
    )
    .expect("runtime timeouts");
    let fixture = RuntimeFixture::new(RuntimeLimits::default()).with_timeouts(
        TransportTimeouts::new(TransportIoTimeouts::default(), runtime),
    );
    let peer = FaultPeer::start(PeerBehavior::AcceptThenClose);
    let sessions = MemorySessionStore::new();
    let sender =
        fixture.start_a_with_store(fixture.endpoints_to_b(peer.local_addr()), sessions.clone());

    assert!(
        wait_until(Duration::from_secs(3), || {
            peer.hello_count() == 1 && sessions.allocation_count() == 1
        }),
        "fault peer failed: {:?}",
        peer.last_error()
    );
    sender
        .sender()
        .send(RuntimeFixture::replication_with_payload(vec![
            0x5a;
            400 * 1024
        ]))
        .expect("large bulk work remains retryable");
    assert!(wait_until(Duration::from_secs(3), || {
        sessions.allocation_count() >= 3
    }));

    thread::sleep(Duration::from_millis(350));
    assert!(
        sessions.allocation_count() <= 6,
        "session allocations must remain backoff-bounded"
    );

    sender.join().expect("sender joins");
    peer.join();
}

#[test]
fn explicit_refresh_recovers_a_repaired_peer_at_identical_endpoint_values() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let peer = FaultPeer::start(PeerBehavior::RejectFrameLimit);
    let endpoints = fixture.endpoints_to_b(peer.local_addr());
    let sessions = MemorySessionStore::new();
    let sender = fixture.start_a_with_store(endpoints.clone(), sessions.clone());

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("work waits behind configuration refusal");
    assert!(
        wait_until(Duration::from_secs(3), || {
            sender
                .peer_diagnostics(fixture.peer_b())
                .expect("peer diagnostics")
                .is_some_and(|state| {
                    state.connection_state == PeerConnectionState::ConfigurationBlocked
                })
        }),
        "fault peer failed: {:?}",
        peer.last_error()
    );
    let before = endpoints
        .snapshot(fixture.peer_b())
        .expect("endpoint snapshot")
        .expect("configured peer");
    assert_eq!(sessions.allocation_count(), 1);

    peer.set_behavior(PeerBehavior::CaptureFrames);
    let refreshed = endpoints
        .refresh(fixture.peer_b())
        .expect("refresh endpoint generation")
        .expect("configured peer");
    let after = endpoints
        .snapshot(fixture.peer_b())
        .expect("refreshed snapshot")
        .expect("configured peer");
    assert!(refreshed > before.generation());
    assert_eq!(before.endpoints(), after.endpoints());
    assert!(wait_until(Duration::from_secs(3), || {
        peer.captured_classes() == vec![TrafficClass::Control]
    }));
    assert_eq!(sessions.allocation_count(), 2);

    sender.join().expect("sender joins");
    peer.join();
}

#[test]
fn sparse_reprobe_recovers_remote_repair_without_a_local_discovery_event() {
    let runtime = TransportRuntimeTimeouts::new(
        Duration::from_millis(20),
        Duration::from_millis(5),
        Duration::from_millis(250),
    )
    .expect("runtime timeouts")
    .with_configuration_reprobe(Duration::from_millis(100))
    .expect("test reprobe interval");
    let fixture = RuntimeFixture::new(RuntimeLimits::default()).with_timeouts(
        TransportTimeouts::new(TransportIoTimeouts::default(), runtime),
    );
    let peer = FaultPeer::start(PeerBehavior::RejectFrameLimit);
    let endpoints = fixture.endpoints_to_b(peer.local_addr());
    let generation = endpoints
        .snapshot(fixture.peer_b())
        .expect("endpoint snapshot")
        .expect("configured peer")
        .generation();
    let sender = fixture.start_a(endpoints.clone());

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("work waits behind configuration refusal");
    assert!(
        wait_until(Duration::from_secs(3), || peer.hello_count() == 1),
        "fault peer failed: {:?}",
        peer.last_error()
    );
    peer.set_behavior(PeerBehavior::CaptureFrames);

    assert!(wait_until(Duration::from_secs(3), || {
        peer.captured_classes() == vec![TrafficClass::Control]
    }));
    assert_eq!(
        endpoints
            .snapshot(fixture.peer_b())
            .expect("endpoint snapshot")
            .expect("configured peer")
            .generation(),
        generation
    );

    sender.join().expect("sender joins");
    peer.join();
}

#[test]
fn failed_bulk_write_is_preempted_by_later_control_on_the_next_live_socket() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let peer = FaultPeer::start(PeerBehavior::AcceptThenClose);
    let sender = fixture.start_a(fixture.endpoints_to_b(peer.local_addr()));

    assert!(
        wait_until(Duration::from_secs(3), || peer.hello_count() == 1),
        "fault peer failed: {:?}",
        peer.last_error()
    );
    sender
        .sender()
        .send(RuntimeFixture::replication_with_payload(vec![
            0x5a;
            400 * 1024
        ]))
        .expect("bulk work is admitted");
    assert!(wait_until(Duration::from_secs(3), || {
        peer.hello_count() >= 2 && sender.diagnostics().tls_failures >= 2
    }));
    peer.set_behavior(PeerBehavior::CaptureFrames);
    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("later control work is admitted");

    assert!(wait_until(Duration::from_secs(3), || {
        peer.captured_classes().len() >= 2
    }));
    assert_eq!(
        &peer.captured_classes()[..2],
        &[TrafficClass::Control, TrafficClass::Replication]
    );

    sender.join().expect("sender joins");
    peer.join();
}

#[test]
fn permanent_frame_incompatibility_blocks_without_consuming_more_sessions() {
    let sender_fixture = RuntimeFixture::new(RuntimeLimits::default());
    let smaller_wire = WireLimits::new(
        WireLimits::default().max_frame_body_bytes() - 1,
        WireLimits::default().max_group_id_bytes(),
    )
    .expect("one-byte-smaller receiver limit");
    let receiver_limits = TransportLimits::new(
        sender_fixture.limits().directory(),
        sender_fixture.limits().endpoints(),
        sender_fixture.limits().certificates(),
        smaller_wire,
    )
    .with_runtime(RuntimeLimits::default());
    let receiver_fixture =
        RuntimeFixture::new(RuntimeLimits::default()).with_limits(receiver_limits);
    let receiver = receiver_fixture.start_b();
    let sessions = MemorySessionStore::new();
    let sender = sender_fixture.start_a_with_store(
        sender_fixture.endpoints_to_b(receiver.local_addr()),
        sessions.clone(),
    );

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("locally legal work is admitted");
    assert!(wait_until(Duration::from_secs(3), || {
        sender
            .peer_diagnostics(sender_fixture.peer_b())
            .expect("peer diagnostics")
            .is_some_and(|peer| peer.connection_state == PeerConnectionState::ConfigurationBlocked)
    }));
    assert_eq!(sessions.allocation_count(), 1);

    thread::sleep(Duration::from_millis(250));
    assert_eq!(sessions.allocation_count(), 1);
    let peer = sender
        .peer_diagnostics(sender_fixture.peer_b())
        .expect("peer diagnostics")
        .expect("configured peer");
    assert_eq!(
        peer.connection_state,
        PeerConnectionState::ConfigurationBlocked
    );
    assert!(peer.last_error.is_some());

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[test]
fn blocked_endpoint_is_not_reallocated_while_another_endpoint_retries() {
    let sender_fixture = RuntimeFixture::new(RuntimeLimits::default());
    let smaller_wire = WireLimits::new(
        WireLimits::default().max_frame_body_bytes() - 1,
        WireLimits::default().max_group_id_bytes(),
    )
    .expect("one-byte-smaller receiver limit");
    let receiver_limits = TransportLimits::new(
        sender_fixture.limits().directory(),
        sender_fixture.limits().endpoints(),
        sender_fixture.limits().certificates(),
        smaller_wire,
    )
    .with_runtime(RuntimeLimits::default());
    let receiver_fixture =
        RuntimeFixture::new(RuntimeLimits::default()).with_limits(receiver_limits);
    let receiver = receiver_fixture.start_b();
    let endpoints = sender_fixture.endpoints_to_b(receiver.local_addr());
    endpoints
        .replace(
            sender_fixture.peer_b().clone(),
            vec![
                PeerEndpoint::new(receiver.local_addr(), server_name()),
                PeerEndpoint::new(
                    "127.0.0.1:0".parse().expect("unavailable endpoint"),
                    server_name(),
                ),
            ],
        )
        .expect("mixed endpoint set");
    let sessions = MemorySessionStore::new();
    let sender = sender_fixture.start_a_with_store(endpoints, sessions.clone());

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("locally legal work is admitted");
    assert!(wait_until(Duration::from_secs(3), || {
        sessions.allocation_count() == 1 && sender.diagnostics().endpoint_failures >= 2
    }));
    thread::sleep(Duration::from_millis(500));
    assert_eq!(sessions.allocation_count(), 1);
    assert_eq!(sender.diagnostics().configuration_blocks, 1);
    assert!(sender
        .peer_diagnostics(sender_fixture.peer_b())
        .expect("peer diagnostics")
        .expect("configured peer")
        .last_error
        .is_some_and(|error| {
            error.contains("configuration-blocked") && error.contains("FrameLimitRejected")
        }));

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

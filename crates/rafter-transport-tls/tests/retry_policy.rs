mod support;

use std::{thread, time::Duration};

use rafter_service::RaftTransport;
use rafter_transport_tls::{
    PeerConnectionState, PeerEndpoint, RuntimeLimits, TransportLimits, WireLimits,
};

use support::{
    runtime::{wait_until, RuntimeFixture},
    session_store::MemorySessionStore,
    tls::server_name,
};

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

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

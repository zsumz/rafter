mod support;

#[path = "support/fault_peer.rs"]
mod fault_peer;

use std::{
    thread,
    time::{Duration, Instant},
};

use rafter_service::RaftTransport;
use rafter_transport_tls::{
    PeerEndpoint, RuntimeLimits, TrafficClass, TransportIoTimeouts, TransportRuntimeTimeouts,
    TransportTimeouts,
};

use fault_peer::{FaultPeer, PeerBehavior};
use support::{
    runtime::{wait_until, RuntimeFixture},
    session_store::MemorySessionStore,
    tls::server_name,
};

fn timeouts(redial: Duration, configuration_reprobe: Duration) -> TransportTimeouts {
    let runtime =
        TransportRuntimeTimeouts::new(redial, Duration::from_millis(5), Duration::from_millis(750))
            .expect("runtime timeouts")
            .with_configuration_reprobe(configuration_reprobe)
            .expect("configuration reprobe interval");
    TransportTimeouts::new(TransportIoTimeouts::default(), runtime)
}

#[test]
fn blocked_endpoint_is_sparsely_reprobed_while_another_endpoint_stays_transient() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default()).with_timeouts(timeouts(
        Duration::from_millis(10),
        Duration::from_millis(100),
    ));
    let peer = FaultPeer::start(PeerBehavior::RejectFrameLimit);
    let endpoints = fixture.endpoints_to_b(peer.local_addr());
    endpoints
        .replace(
            fixture.peer_b().clone(),
            vec![
                PeerEndpoint::new(peer.local_addr(), server_name()),
                PeerEndpoint::new(
                    "127.0.0.1:0".parse().expect("unavailable endpoint"),
                    server_name(),
                ),
            ],
        )
        .expect("mixed endpoint set");
    let generation = endpoints
        .snapshot(fixture.peer_b())
        .expect("endpoint snapshot")
        .expect("configured peer")
        .generation();
    let sender = fixture.start_a(endpoints.clone());

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("control work waits for recovery");
    assert!(
        wait_until(Duration::from_secs(3), || {
            peer.hello_count() >= 1 && sender.diagnostics().endpoint_failures >= 2
        }),
        "fault peer failed: {:?}",
        peer.last_error()
    );
    peer.set_behavior(PeerBehavior::CaptureFrames);

    assert!(
        wait_until(Duration::from_secs(3), || {
            peer.captured_classes() == vec![TrafficClass::Control]
        }),
        "blocked endpoint did not recover: {:?}",
        peer.last_error()
    );
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
fn one_locally_successful_frame_does_not_collapse_session_backoff() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default())
        .with_timeouts(timeouts(Duration::from_millis(50), Duration::from_secs(5)));
    let peer = FaultPeer::start(PeerBehavior::CaptureOneThenClose);
    let sessions = MemorySessionStore::new();
    let sender =
        fixture.start_a_with_store(fixture.endpoints_to_b(peer.local_addr()), sessions.clone());

    assert!(
        wait_until(Duration::from_secs(3), || peer.hello_count() >= 1),
        "fault peer failed: {:?}",
        peer.last_error()
    );
    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("first control frame");
    let deadline = Instant::now() + Duration::from_secs(5);
    while peer.captured_classes().len() < 4 && Instant::now() < deadline {
        sender
            .sender()
            .send(RuntimeFixture::vote())
            .expect("continuous control frame");
        thread::sleep(Duration::from_millis(10));
    }
    assert!(peer.captured_classes().len() >= 4);
    assert!(sender.diagnostics().frames_sent >= 1);

    let allocations = sessions.allocation_times();
    assert!(allocations.len() >= 4);
    assert!(
        allocations[3].duration_since(allocations[2]) >= Duration::from_millis(190),
        "the fourth durable session must remain on the exponential backoff curve"
    );

    sender.join().expect("sender joins");
    peer.join();
}

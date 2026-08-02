mod support;

use std::time::Duration;

use rafter_service::RaftTransport;
use rafter_transport_tls::{PeerEndpoint, RuntimeLimits};

use support::runtime::{wait_until, RuntimeFixture};
use support::tls::server_name;

#[test]
fn disconnected_sender_redials_the_latest_endpoint_generation() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();

    let unavailable = "127.0.0.1:0".parse().expect("unavailable endpoint");
    let endpoints = fixture.endpoints_to_b(unavailable);
    let sender = fixture.start_a(endpoints.clone());

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("enqueue frame");
    assert!(wait_until(Duration::from_secs(1), || {
        sender.diagnostics().endpoint_failures > 0
    }));

    endpoints
        .replace(
            fixture.peer_b().clone(),
            vec![PeerEndpoint::new(receiver.local_addr(), server_name())],
        )
        .expect("replace endpoint");
    assert!(wait_until(Duration::from_secs(3), || {
        !receiver
            .inbound()
            .drain(1)
            .expect("drain inbound")
            .is_empty()
    }));
    assert!(sender.diagnostics().frames_sent >= 1);

    sender.join().expect("join sender runtime");
    receiver.join().expect("join receiver runtime");
}

#[test]
fn connected_sender_replaces_a_stale_endpoint_before_the_next_send() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let first = fixture.start_b();
    let endpoints = fixture.endpoints_to_b(first.local_addr());
    let sender = fixture.start_a(endpoints.clone());

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("first vote is admitted");
    assert!(wait_until(Duration::from_secs(3), || {
        !first.inbound().drain(1).expect("first inbound").is_empty()
    }));

    let second = fixture.start_b();
    endpoints
        .replace(
            fixture.peer_b().clone(),
            vec![PeerEndpoint::new(second.local_addr(), server_name())],
        )
        .expect("replace a live connection's endpoint generation");
    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("second vote is admitted");

    assert!(wait_until(Duration::from_secs(3), || {
        !second
            .inbound()
            .drain(1)
            .expect("second inbound")
            .is_empty()
    }));
    assert!(first.inbound().drain(1).expect("old inbound").is_empty());

    sender.join().expect("join sender runtime");
    first.join().expect("join first receiver");
    second.join().expect("join second receiver");
}

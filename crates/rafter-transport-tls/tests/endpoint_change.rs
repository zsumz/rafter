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

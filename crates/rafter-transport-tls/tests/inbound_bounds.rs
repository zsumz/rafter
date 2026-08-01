mod support;

use std::time::Duration;

use rafter_service::RaftTransport;
use rafter_transport_tls::{InboundQueueLimits, OutboundQueueLimits, RuntimeLimits};

use support::runtime::{wait_until, RuntimeFixture};

#[test]
fn authenticated_inbound_queue_enforces_peer_and_global_bounds() {
    let outbound = OutboundQueueLimits::new(8, 8192, 1, 512, 2).expect("valid outbound limits");
    let inbound = InboundQueueLimits::new(1, 4096, 1, 4096).expect("valid inbound limits");
    let receiver_limits = RuntimeLimits::new(outbound, inbound, 4).expect("valid receiver limits");
    let receiver_fixture = RuntimeFixture::new(receiver_limits);
    let receiver = receiver_fixture.start_b();

    let sender_fixture = RuntimeFixture::new(RuntimeLimits::default());
    let sender = sender_fixture.start_a(sender_fixture.endpoints_to_b(receiver.local_addr()));
    for _ in 0..3 {
        sender
            .sender()
            .send(RuntimeFixture::vote())
            .expect("enqueue test frame");
    }

    assert!(wait_until(Duration::from_secs(3), || {
        let diagnostics = receiver.diagnostics();
        diagnostics.frames_received == 1
            && diagnostics.inbound_full >= 1
            && diagnostics.inbound_peer_full >= 1
    }));
    assert_eq!(receiver.inbound().depth().expect("inbound depth").0, 1);
    assert_eq!(
        receiver
            .inbound()
            .drain(8)
            .expect("drain bounded queue")
            .len(),
        1
    );
    assert_eq!(receiver.inbound().depth().expect("released depth"), (0, 0));

    sender.join().expect("join sender runtime");
    receiver.join().expect("join receiver runtime");
}

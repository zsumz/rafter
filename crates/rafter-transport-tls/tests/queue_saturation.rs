mod support;

use rafter_service::RaftTransport;
use rafter_transport_tls::{
    InboundQueueLimits, OutboundQueueLimits, RuntimeLimits, TlsTransportError, TrafficClass,
    TransportHealth,
};

use support::runtime::RuntimeFixture;

#[test]
fn send_refuses_immediately_at_the_per_peer_frame_bound() {
    let outbound = OutboundQueueLimits::new(3, 4096, 1, 512, 1).expect("valid outbound limits");
    let inbound = InboundQueueLimits::new(4, 4096, 4, 4096).expect("valid inbound limits");
    let runtime = RuntimeLimits::new(outbound, inbound, 4).expect("valid small runtime limits");
    let fixture = RuntimeFixture::new(runtime);
    let unavailable = "127.0.0.1:0".parse().expect("unavailable endpoint");
    let transport = fixture.start_a(fixture.endpoints_to_b(unavailable));
    let sender = transport.sender();

    for _ in 0..3 {
        sender
            .send(RuntimeFixture::vote())
            .expect("frame within bound");
    }
    assert!(matches!(
        sender.send(RuntimeFixture::vote()),
        Err(TlsTransportError::QueueFull {
            class: TrafficClass::Control,
            frames: 3,
            ..
        })
    ));
    assert_eq!(transport.diagnostics().queue_full, 1);
    assert_eq!(transport.health(), TransportHealth::Degraded);

    transport.shutdown();
    assert!(matches!(
        sender.send(RuntimeFixture::vote()),
        Err(TlsTransportError::Stopped)
    ));
    transport.join().expect("join saturated runtime");
}

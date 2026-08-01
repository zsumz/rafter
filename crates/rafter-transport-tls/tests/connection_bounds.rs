mod support;

use std::{net::TcpStream, time::Duration};

use rafter_transport_tls::{InboundQueueLimits, OutboundQueueLimits, RuntimeLimits};

use support::runtime::{wait_until, RuntimeFixture};

#[test]
fn accepted_connections_never_exceed_the_configured_bound() {
    let runtime = RuntimeLimits::new(
        OutboundQueueLimits::default(),
        InboundQueueLimits::default(),
        1,
    )
    .expect("one inbound connection");
    let fixture = RuntimeFixture::new(runtime);
    let receiver = fixture.start_b();

    let first = TcpStream::connect(receiver.local_addr()).expect("first raw connection");
    assert!(wait_until(Duration::from_secs(1), || {
        receiver.diagnostics().active_inbound_connections == 1
    }));
    let second = TcpStream::connect(receiver.local_addr()).expect("second raw connection");
    assert!(wait_until(Duration::from_secs(1), || {
        receiver.diagnostics().connection_full >= 1
    }));
    assert_eq!(receiver.diagnostics().active_inbound_connections, 1);

    drop(second);
    drop(first);
    receiver.join().expect("join bounded receiver");
}

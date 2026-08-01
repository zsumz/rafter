mod support;

use std::time::Duration;

use rafter_service::RaftTransport;
use rafter_transport_tls::{RuntimeLimits, TransportHealth};

use support::runtime::{wait_until, RuntimeFixture};

#[test]
fn graceful_shutdown_drains_work_accepted_before_admission_closes() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let sender = fixture.start_a(fixture.endpoints_to_b(receiver.local_addr()));

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("warm connection");
    assert!(wait_until(Duration::from_secs(3), || {
        receiver
            .inbound()
            .drain(1)
            .expect("drain warm-up frame")
            .len()
            == 1
    }));
    assert_eq!(sender.diagnostics().active_outbound_connections, 1);

    for _ in 0..4 {
        sender
            .sender()
            .send(RuntimeFixture::vote())
            .expect("accept work before shutdown");
    }
    sender.shutdown();
    assert_eq!(sender.health(), TransportHealth::Stopping);
    sender.join().expect("join drained sender runtime");

    let mut delivered = 0_usize;
    assert!(wait_until(Duration::from_secs(3), || {
        delivered += receiver
            .inbound()
            .drain(8)
            .expect("drain shutdown frames")
            .len();
        delivered == 4
    }));

    receiver.join().expect("join receiver runtime");
}

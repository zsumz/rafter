mod support;

use std::time::Duration;

use rafter_service::RaftTransport;
use rafter_transport_tls::{RuntimeLimits, TransportHealth};

use support::runtime::{wait_until, RuntimeFixture, NODE_A, NODE_B};

#[test]
fn authenticated_runtime_delivers_one_raft_envelope() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let sender = fixture.start_a(fixture.endpoints_to_b(receiver.local_addr()));

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("bounded send admission");

    let mut delivered = None;
    assert!(wait_until(Duration::from_secs(3), || {
        let mut batch = receiver.inbound().drain(1).expect("drain inbound");
        if delivered.is_none() {
            delivered = batch.pop();
        }
        delivered.is_some()
    }));
    let delivered = delivered.expect("one delivered envelope");
    assert_eq!(&delivered.authenticated_peer, fixture.peer_a());
    assert_eq!(delivered.raft_from, NODE_A);
    assert_eq!(delivered.raft_to, NODE_B);
    assert_eq!(delivered.message, support::request_vote(NODE_A));

    // The receiver read deadline is two seconds. Remaining idle beyond that
    // deadline must poll shutdown without tearing down the persistent stream.
    std::thread::sleep(Duration::from_millis(2_200));

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("second bounded send admission");
    assert!(wait_until(Duration::from_secs(3), || {
        receiver
            .inbound()
            .drain(1)
            .expect("drain second envelope")
            .len()
            == 1
    }));
    assert!(wait_until(Duration::from_secs(1), || {
        sender.diagnostics().frames_sent == 2 && receiver.diagnostics().frames_received == 2
    }));
    assert_eq!(sender.diagnostics().reconnects, 0);
    assert_eq!(sender.diagnostics().active_outbound_connections, 1);
    assert_eq!(sender.health(), TransportHealth::Ready);

    sender.join().expect("join sender runtime");
    receiver.join().expect("join receiver runtime");
}

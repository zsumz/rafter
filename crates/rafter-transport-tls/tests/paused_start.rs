mod support;

use std::{io::Write, net::TcpStream, time::Duration};

use rafter_service::{PeerPolicy, RaftTransport};
use rafter_transport_tls::{RuntimeLimits, TlsTransportStartError, TransportHealth};

use support::{
    runtime::{wait_until, RuntimeFixture, GROUP_ID},
    session_store::MemorySessionStore,
};

#[test]
fn paused_runtime_accepts_bounded_work_without_network_or_session_io() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let sessions = MemorySessionStore::new();
    let runtime = fixture.bind_paused_a_with_store(
        fixture.endpoints_to_b(receiver.local_addr()),
        sessions.clone(),
    );

    assert_eq!(runtime.health(), TransportHealth::Starting);
    runtime
        .sender()
        .update_peers(
            &GROUP_ID.to_owned(),
            PeerPolicy::new(vec![fixture.peer_b().clone()], None),
        )
        .expect("policy publication is in-memory while paused");
    runtime
        .sender()
        .send(RuntimeFixture::vote())
        .expect("bounded queue admission remains available while paused");
    assert_eq!(
        runtime.queue_depths().expect("queue depth").outbound_frames,
        1
    );
    let mut probe = TcpStream::connect(runtime.local_addr())
        .expect("the paused listener owns its finite backlog");
    probe
        .write_all(b"not a TLS handshake")
        .expect("probe bytes reach the bound listener");
    drop(probe);

    std::thread::sleep(Duration::from_millis(100));
    assert!(receiver
        .inbound()
        .drain(1)
        .expect("drain inbound")
        .is_empty());
    assert_eq!(runtime.diagnostics().tls_handshakes, 0);
    assert_eq!(runtime.diagnostics().tls_failures, 0);
    let peer_state = sessions.peer_state(fixture.peer_b());
    assert_eq!(peer_state.highest_outbound(), None);
    assert_eq!(peer_state.highest_inbound(), None);

    runtime.start().expect("activate paused runtime");
    runtime.start().expect("activation is idempotent");
    assert!(wait_until(Duration::from_secs(3), || {
        receiver
            .inbound()
            .drain(1)
            .expect("drain delivered frame")
            .len()
            == 1
    }));
    assert!(sessions
        .peer_state(fixture.peer_b())
        .highest_outbound()
        .is_some());

    runtime.join().expect("join sender runtime");
    receiver.join().expect("join receiver runtime");
}

#[test]
fn shutdown_before_start_consumes_no_connection_epoch() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let sessions = MemorySessionStore::new();
    let runtime = fixture.bind_paused_a_with_store(
        fixture.endpoints_to_b(receiver.local_addr()),
        sessions.clone(),
    );

    runtime
        .sender()
        .send(RuntimeFixture::vote())
        .expect("work is admitted before shutdown");
    runtime.shutdown();
    assert_eq!(runtime.health(), TransportHealth::Stopping);
    assert!(matches!(
        runtime.start(),
        Err(TlsTransportStartError::Stopping)
    ));
    runtime.join().expect("paused workers stop and join");

    assert_eq!(
        sessions.peer_state(fixture.peer_b()).highest_outbound(),
        None
    );
    assert!(receiver
        .inbound()
        .drain(1)
        .expect("drain inbound")
        .is_empty());
    receiver.join().expect("join receiver runtime");
}

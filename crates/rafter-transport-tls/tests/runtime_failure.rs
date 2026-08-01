mod support;

use std::time::Duration;

use rafter_service::RaftTransport;
use rafter_transport_tls::{RuntimeLimits, TlsTransportError, TransportHealth};

use support::runtime::{wait_until, RuntimeFixture};
use support::session_store::AllocateFailingSessionStore;

#[test]
fn session_store_failure_after_startup_is_terminal_and_visible() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let sender = fixture.start_a_with_store(
        fixture.endpoints_to_b(receiver.local_addr()),
        AllocateFailingSessionStore,
    );

    assert!(wait_until(Duration::from_secs(3), || {
        sender.health() == TransportHealth::Failed
    }));
    assert_eq!(sender.diagnostics().session_store_failures, 1);
    assert!(matches!(
        sender.sender().send(RuntimeFixture::vote()),
        Err(TlsTransportError::TerminalFailure { .. })
    ));

    sender.join().expect("join failed sender runtime");
    receiver.join().expect("join receiver runtime");
}

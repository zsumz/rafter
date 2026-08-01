mod support;

use rafter_transport_tls::{RuntimeLimits, TlsPeerTransport, TlsTransportBuildError};

use support::runtime::RuntimeFixture;
use support::StringGroupCodec;

#[test]
fn builder_refuses_missing_security_dependencies_before_binding() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let config = rafter_transport_tls::TransportConfig::new(
        rafter_transport_tls::ClusterId::new("builder-test").expect("cluster"),
        fixture.peer_a().clone(),
        "127.0.0.1:0".parse().expect("loopback"),
        rafter_transport_tls::TransportLimits::default(),
        rafter_transport_tls::TransportTimeouts::default(),
    );
    let result = TlsPeerTransport::<String, _>::builder(config, StringGroupCodec::new(128)).bind();

    assert!(matches!(
        result,
        Err(TlsTransportBuildError::MissingIdentity)
    ));
}

#[test]
fn endpoint_book_may_be_empty_for_an_inbound_only_runtime() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let runtime = fixture.start_b();

    assert_eq!(runtime.config().local_peer_id(), fixture.peer_b());
    assert_eq!(
        runtime
            .queue_depths()
            .expect("queue depths")
            .outbound_frames,
        0
    );
    runtime.join().expect("join inbound-only runtime");
}

#[test]
fn builder_probes_the_session_store_even_without_outbound_peers() {
    use rafter_transport_tls::{
        CertificateDirectory, ClusterId, EndpointBook, PeerId, TlsPeerDirectory, TransportConfig,
        TransportLimits, TransportTimeouts,
    };

    use support::session_store::FailingSessionStore;
    use support::tls::node_b_identity;

    let limits = TransportLimits::default();
    let local_peer = PeerId::new("peer-b").expect("local peer");
    let identity = node_b_identity();
    let certificates = CertificateDirectory::builder()
        .map_fingerprint(identity.leaf_fingerprint(), local_peer.clone())
        .expect("map local certificate")
        .build();
    let config = TransportConfig::new(
        ClusterId::new("builder-test").expect("cluster"),
        local_peer,
        "127.0.0.1:0".parse().expect("loopback"),
        limits,
        TransportTimeouts::default(),
    );
    let result = TlsPeerTransport::<String, _>::builder(config, StringGroupCodec::new(128))
        .identity(identity)
        .certificates(certificates)
        .directory(TlsPeerDirectory::new(limits.directory()))
        .endpoints(EndpointBook::new(limits.endpoints()))
        .session_store(FailingSessionStore)
        .bind();

    assert!(matches!(
        result,
        Err(TlsTransportBuildError::SessionStore { .. })
    ));
}

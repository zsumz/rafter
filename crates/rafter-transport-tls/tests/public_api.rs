mod support;

use rafter::InMemorySnapshotChunkSource;
use rafter_service::RaftTransport;
use rafter_transport_tls::{
    AuthenticatedTlsPeer, CertificateDirectory, EndpointBook, FileTransportSessionStore,
    PeerFrameCodec, PeerId, RuntimeLimits, SnapshotChunkSourceResolver, TlsHandshakeConfig,
    TlsIdentity, TlsInbound, TlsPeerDirectory, TlsPeerTransport, TlsSender, TransportConfig,
    TransportSessionState, TransportTimeouts, WireLimits,
};

use support::StringGroupCodec;

fn assert_send<T: Send>() {}

fn assert_send_sync<T: Send + Sync>() {}

fn assert_raft_transport<T>()
where
    T: RaftTransport<String, PeerPrincipal = PeerId>,
{
}

#[test]
fn shared_contract_handles_are_send_and_sync() {
    assert_send_sync::<PeerId>();
    assert_send_sync::<CertificateDirectory>();
    assert_send_sync::<TlsIdentity>();
    assert_send_sync::<AuthenticatedTlsPeer>();
    assert_send_sync::<TlsHandshakeConfig>();
    assert_send_sync::<EndpointBook>();
    assert_send_sync::<TlsPeerDirectory<String>>();
    assert_send_sync::<PeerFrameCodec<String, StringGroupCodec>>();
    assert_send_sync::<TransportSessionState>();
    assert_send_sync::<FileTransportSessionStore>();
    assert_send_sync::<TransportConfig>();
    assert_send_sync::<TransportTimeouts>();
    assert_send_sync::<RuntimeLimits>();
    assert_send_sync::<SnapshotChunkSourceResolver<InMemorySnapshotChunkSource>>();
    assert_send_sync::<TlsSender<String, StringGroupCodec>>();
    assert_send_sync::<TlsInbound<String>>();
    assert_send::<TlsPeerTransport<String, StringGroupCodec>>();
    assert_raft_transport::<TlsSender<String, StringGroupCodec>>();
}

#[test]
fn peer_frame_codec_constructs_without_a_runtime() {
    let codec = PeerFrameCodec::<String, _>::new(StringGroupCodec::new(128), WireLimits::default())
        .expect("compatible codec");

    assert_eq!(codec.limits(), WireLimits::default());
}

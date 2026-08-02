mod support;

use std::num::{NonZeroU16, NonZeroU32};

use rafter_transport_tls::{
    authenticate_client_connection, authenticate_server_connection, AuthenticatedTlsPeer,
    CertificateDirectory, ClientHello, ClusterId, ConnectionSession, PeerId, ServerHello,
    ServerHelloStatus, ServerRefusal, TlsClientHandshakeError, TlsHandshakeConfig, VersionRange,
    WireLimits, MIN_PEER_FRAME_BYTES,
};

use support::{
    session_store::{FailingSessionStore, MemorySessionStore},
    tls::{complete_handshake, connection_pair, node_a_identity, node_b_identity},
};

#[test]
fn accepted_handshake_publishes_both_session_directions_first() {
    let peers = authenticated_peers();
    let client_config = config("orders-us1", "node-a");
    let server_config = config("orders-us1", "node-b");
    let client_store = MemorySessionStore::new();
    let server_store = MemorySessionStore::new();

    let hello = client_config
        .begin_client_hello(&peers.node_b, &client_store)
        .expect("outbound session");
    assert_eq!(hello.connection_session(), ConnectionSession::FIRST);
    assert_eq!(
        client_store.peer_state(&peers.node_b).highest_outbound(),
        Some(ConnectionSession::FIRST)
    );

    let response = server_config
        .accept_client_hello(&peers.server_authenticated_client, &hello, &server_store)
        .expect("inbound publication");
    assert_eq!(response.status(), ServerHelloStatus::Accepted);
    assert_eq!(
        server_store.peer_state(&peers.node_a).highest_inbound(),
        Some(ConnectionSession::FIRST)
    );

    let negotiated = client_config
        .validate_server_hello(&peers.node_b, &peers.client_authenticated_server, &response)
        .expect("valid response");
    assert_eq!(negotiated.remote_peer_id(), &peers.node_b);
    assert_eq!(negotiated.transport_version(), 1);
    assert_eq!(
        negotiated.peer_codec_version(),
        u16::from(rafter_codec::VERSION)
    );
    assert_eq!(
        negotiated.frame_bytes(),
        client_config.max_frame_bytes().get()
    );
}

#[test]
fn stale_session_is_refused_after_the_first_durable_acceptance() {
    let peers = authenticated_peers();
    let server_config = config("orders-us1", "node-b");
    let store = MemorySessionStore::new();
    let hello = client_hello(
        "orders-us1",
        peers.node_a.clone(),
        ConnectionSession::FIRST,
        VersionRange::current_transport(),
        current_codec_range(),
        server_config.max_frame_bytes(),
    );

    assert_eq!(
        server_config
            .accept_client_hello(&peers.server_authenticated_client, &hello, &store,)
            .expect("first accept")
            .status(),
        ServerHelloStatus::Accepted
    );
    assert_eq!(
        server_config
            .accept_client_hello(&peers.server_authenticated_client, &hello, &store,)
            .expect("stale decision")
            .status(),
        ServerHelloStatus::Refused(ServerRefusal::StaleSession)
    );
}

#[test]
fn receiver_refuses_a_sender_whose_required_frame_bound_is_larger() {
    let peers = authenticated_peers();
    let client = config("orders-us1", "node-a");
    let smaller_wire = WireLimits::new(
        WireLimits::default().max_frame_body_bytes() - 1,
        WireLimits::default().max_group_id_bytes(),
    )
    .expect("one-byte-smaller receiver limit");
    let server = TlsHandshakeConfig::current(
        ClusterId::new("orders-us1").expect("valid cluster"),
        peers.node_b.clone(),
        smaller_wire,
    )
    .expect("valid server config");
    let client_store = MemorySessionStore::new();
    let server_store = MemorySessionStore::new();
    let hello = client
        .begin_client_hello(&peers.node_b, &client_store)
        .expect("outbound session");

    let response = server
        .accept_client_hello(&peers.server_authenticated_client, &hello, &server_store)
        .expect("typed refusal");

    assert_eq!(
        response.status(),
        ServerHelloStatus::Refused(ServerRefusal::FrameLimitRejected)
    );
    assert!(server_store.peer_state(&peers.node_a).is_empty());
}

#[test]
fn pre_session_refusals_do_not_advance_durable_replay_state() {
    let peers = authenticated_peers();
    let server_config = config("orders-us1", "node-b");

    let cases = [
        client_hello(
            "orders-us1",
            PeerId::new("not-node-a").expect("valid peer"),
            ConnectionSession::FIRST,
            VersionRange::current_transport(),
            current_codec_range(),
            server_config.max_frame_bytes(),
        ),
        client_hello(
            "another-cluster",
            peers.node_a.clone(),
            ConnectionSession::FIRST,
            VersionRange::current_transport(),
            current_codec_range(),
            server_config.max_frame_bytes(),
        ),
        client_hello(
            "orders-us1",
            peers.node_a.clone(),
            ConnectionSession::FIRST,
            VersionRange::new(2, 2).expect("valid range"),
            current_codec_range(),
            server_config.max_frame_bytes(),
        ),
        client_hello(
            "orders-us1",
            peers.node_a.clone(),
            ConnectionSession::FIRST,
            VersionRange::current_transport(),
            VersionRange::new(2, 2).expect("valid range"),
            server_config.max_frame_bytes(),
        ),
        client_hello(
            "orders-us1",
            peers.node_a.clone(),
            ConnectionSession::FIRST,
            VersionRange::current_transport(),
            current_codec_range(),
            NonZeroU32::new(MIN_PEER_FRAME_BYTES - 1).expect("nonzero"),
        ),
    ];
    let expected = [
        ServerRefusal::IdentityMismatch,
        ServerRefusal::ClusterMismatch,
        ServerRefusal::TransportVersionMismatch,
        ServerRefusal::PeerCodecVersionMismatch,
        ServerRefusal::FrameLimitRejected,
    ];

    for (hello, expected) in cases.iter().zip(expected) {
        let store = MemorySessionStore::new();
        let response = server_config
            .accept_client_hello(&peers.server_authenticated_client, hello, &store)
            .expect("ordinary refusal");
        assert_eq!(response.status(), ServerHelloStatus::Refused(expected));
        assert!(store.peer_state(&peers.node_a).is_empty());
    }
}

#[test]
fn session_store_failures_remain_typed_and_never_become_acceptance() {
    let peers = authenticated_peers();
    let client_config = config("orders-us1", "node-a");
    let server_config = config("orders-us1", "node-b");
    assert!(client_config
        .begin_client_hello(&peers.node_b, &FailingSessionStore)
        .is_err());

    let hello = client_hello(
        "orders-us1",
        peers.node_a.clone(),
        ConnectionSession::FIRST,
        VersionRange::current_transport(),
        current_codec_range(),
        server_config.max_frame_bytes(),
    );
    assert!(server_config
        .accept_client_hello(
            &peers.server_authenticated_client,
            &hello,
            &FailingSessionStore,
        )
        .is_err());
}

#[test]
fn client_rechecks_tls_identity_cluster_versions_and_frame_bound() {
    let peers = authenticated_peers();
    let client_config = config("orders-us1", "node-a");
    let frame = client_config.max_frame_bytes();

    let wrong_target = PeerId::new("node-c").expect("valid peer");
    assert!(matches!(
        client_config.validate_server_hello(
            &wrong_target,
            &peers.client_authenticated_server,
            &accepted("orders-us1", peers.node_b.clone(), 1, 1, frame),
        ),
        Err(TlsClientHandshakeError::AuthenticatedPeerMismatch { .. })
    ));

    assert!(matches!(
        client_config.validate_server_hello(
            &peers.node_b,
            &peers.client_authenticated_server,
            &accepted(
                "orders-us1",
                PeerId::new("node-c").expect("valid peer"),
                1,
                1,
                frame,
            ),
        ),
        Err(TlsClientHandshakeError::ServerIdentityMismatch { .. })
    ));

    assert!(matches!(
        client_config.validate_server_hello(
            &peers.node_b,
            &peers.client_authenticated_server,
            &accepted("another-cluster", peers.node_b.clone(), 1, 1, frame),
        ),
        Err(TlsClientHandshakeError::ClusterMismatch { .. })
    ));

    let refusal = ServerHello::refused(
        ClusterId::new("orders-us1").expect("valid cluster"),
        peers.node_b.clone(),
        ServerRefusal::ServerBusy,
    );
    assert_eq!(
        client_config.validate_server_hello(
            &peers.node_b,
            &peers.client_authenticated_server,
            &refusal,
        ),
        Err(TlsClientHandshakeError::Refused {
            reason: ServerRefusal::ServerBusy,
        })
    );

    assert!(matches!(
        client_config.validate_server_hello(
            &peers.node_b,
            &peers.client_authenticated_server,
            &accepted("orders-us1", peers.node_b.clone(), 2, 1, frame),
        ),
        Err(TlsClientHandshakeError::TransportVersionNotOffered { selected: 2 })
    ));

    assert!(matches!(
        client_config.validate_server_hello(
            &peers.node_b,
            &peers.client_authenticated_server,
            &accepted("orders-us1", peers.node_b.clone(), 1, 2, frame),
        ),
        Err(TlsClientHandshakeError::PeerCodecVersionNotOffered { selected: 2 })
    ));

    let too_large = NonZeroU32::new(frame.get() + 1).expect("nonzero");
    assert!(matches!(
        client_config.validate_server_hello(
            &peers.node_b,
            &peers.client_authenticated_server,
            &accepted("orders-us1", peers.node_b.clone(), 1, 1, too_large),
        ),
        Err(TlsClientHandshakeError::FrameLimitInvalid { .. })
    ));
    let too_small = NonZeroU32::new(frame.get() - 1).expect("nonzero");
    assert!(matches!(
        client_config.validate_server_hello(
            &peers.node_b,
            &peers.client_authenticated_server,
            &accepted("orders-us1", peers.node_b.clone(), 1, 1, too_small),
        ),
        Err(TlsClientHandshakeError::FrameLimitInvalid { .. })
    ));
}

struct AuthenticatedPeers {
    node_a: PeerId,
    node_b: PeerId,
    client_authenticated_server: AuthenticatedTlsPeer,
    server_authenticated_client: AuthenticatedTlsPeer,
}

fn authenticated_peers() -> AuthenticatedPeers {
    let client_identity = node_a_identity();
    let server_identity = node_b_identity();
    let node_a = PeerId::new("node-a").expect("valid peer");
    let node_b = PeerId::new("node-b").expect("valid peer");
    let directory = CertificateDirectory::builder()
        .map_fingerprint(client_identity.leaf_fingerprint(), node_a.clone())
        .expect("node A mapping")
        .map_fingerprint(server_identity.leaf_fingerprint(), node_b.clone())
        .expect("node B mapping")
        .build();
    let (mut client, mut server) = connection_pair(&client_identity, &server_identity);
    complete_handshake(&mut client, &mut server).expect("mutual TLS");
    AuthenticatedPeers {
        node_a,
        node_b,
        client_authenticated_server: authenticate_client_connection(&client, &directory)
            .expect("server authenticated"),
        server_authenticated_client: authenticate_server_connection(&server, &directory)
            .expect("client authenticated"),
    }
}

fn config(cluster: &str, peer: &str) -> TlsHandshakeConfig {
    TlsHandshakeConfig::current(
        ClusterId::new(cluster).expect("valid cluster"),
        PeerId::new(peer).expect("valid peer"),
        WireLimits::default(),
    )
    .expect("valid handshake config")
}

fn current_codec_range() -> VersionRange {
    let version = u16::from(rafter_codec::VERSION);
    VersionRange::new(version, version).expect("current codec version")
}

fn client_hello(
    cluster: &str,
    peer: PeerId,
    session: ConnectionSession,
    transport_versions: VersionRange,
    peer_codec_versions: VersionRange,
    frame_bytes: NonZeroU32,
) -> ClientHello {
    ClientHello::new(
        transport_versions,
        peer_codec_versions,
        ClusterId::new(cluster).expect("valid cluster"),
        peer,
        session,
        frame_bytes,
    )
}

fn accepted(
    cluster: &str,
    peer: PeerId,
    transport_version: u16,
    peer_codec_version: u16,
    frame_bytes: NonZeroU32,
) -> ServerHello {
    ServerHello::accepted(
        NonZeroU16::new(transport_version).expect("nonzero transport"),
        NonZeroU16::new(peer_codec_version).expect("nonzero codec"),
        ClusterId::new(cluster).expect("valid cluster"),
        peer,
        frame_bytes,
    )
}

#[test]
fn handshake_minimum_tracks_the_public_peer_frame_layout() {
    let expected = rafter_transport_tls::PEER_FRAME_LENGTH_PREFIX_BYTES
        + rafter_transport_tls::PEER_FRAME_FIXED_BODY_BYTES
        + 2;
    assert_eq!(
        usize::try_from(MIN_PEER_FRAME_BYTES).expect("minimum fits usize"),
        expected
    );
}

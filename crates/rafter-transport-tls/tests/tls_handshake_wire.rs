mod support;

use rafter_transport_tls::{
    authenticate_client_connection, authenticate_server_connection, decode_client_hello,
    decode_server_hello, encode_client_hello_into, encode_server_hello_into, CertificateDirectory,
    ClusterId, PeerId, TlsHandshakeConfig, WireLimits,
};

use support::{
    session_store::MemorySessionStore,
    tls::{complete_handshake, connection_pair, node_a_identity, node_b_identity},
};

#[test]
fn tls_evidence_and_canonical_hello_bytes_form_one_negotiated_connection() {
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
    let (mut client_tls, mut server_tls) = connection_pair(&client_identity, &server_identity);
    complete_handshake(&mut client_tls, &mut server_tls).expect("mutual TLS");
    let authenticated_server =
        authenticate_client_connection(&client_tls, &directory).expect("server identity");
    let authenticated_client =
        authenticate_server_connection(&server_tls, &directory).expect("client identity");

    let cluster = ClusterId::new("orders-us1").expect("valid cluster");
    let client_config =
        TlsHandshakeConfig::current(cluster.clone(), node_a.clone(), WireLimits::default())
            .expect("client policy");
    let server_config = TlsHandshakeConfig::current(cluster, node_b.clone(), WireLimits::default())
        .expect("server policy");
    let client_sessions = MemorySessionStore::new();
    let server_sessions = MemorySessionStore::new();

    let client_hello = client_config
        .begin_client_hello(&node_b, &client_sessions)
        .expect("durable outbound session");
    let mut client_bytes = Vec::new();
    encode_client_hello_into(&mut client_bytes, &client_hello);
    let received_client_hello = decode_client_hello(&client_bytes).expect("canonical client hello");
    let server_hello = server_config
        .accept_client_hello(
            &authenticated_client,
            &received_client_hello,
            &server_sessions,
        )
        .expect("durable inbound session");

    let mut server_bytes = Vec::new();
    encode_server_hello_into(&mut server_bytes, &server_hello);
    let received_server_hello = decode_server_hello(&server_bytes).expect("canonical server hello");
    let negotiated = client_config
        .validate_server_hello(&node_b, &authenticated_server, &received_server_hello)
        .expect("negotiated connection");

    assert_eq!(negotiated.remote_peer_id(), &node_b);
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

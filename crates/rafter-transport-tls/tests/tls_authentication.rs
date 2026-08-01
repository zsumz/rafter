mod support;

use std::sync::Arc;

use rafter_transport_tls::{
    authenticate_client_connection, authenticate_server_connection, CertificateDirectory, PeerId,
    TlsPeerAuthenticationError, TlsServerName, TLS_ALPN_PROTOCOL,
};
use rustls::{
    client::{ClientConfig, ClientConnection, Resumption},
    crypto::ring,
    pki_types::{pem::PemObject, CertificateDer, ServerName},
    RootCertStore,
};

use support::tls::{
    complete_handshake, connection_pair, identity, node_a_identity, node_b_identity, server_name,
    CA_PEM, NODE_A_CERT_PEM, NODE_A_KEY_PEM, UNTRUSTED_CA_PEM,
};

#[test]
fn completed_mutual_tls_proves_both_explicit_principals() {
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

    assert_eq!(
        authenticate_client_connection(&client, &directory),
        Err(TlsPeerAuthenticationError::HandshakeIncomplete)
    );
    complete_handshake(&mut client, &mut server).expect("mutual TLS");

    assert_eq!(client.alpn_protocol(), Some(TLS_ALPN_PROTOCOL));
    assert_eq!(server.alpn_protocol(), Some(TLS_ALPN_PROTOCOL));
    assert_eq!(
        client.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );
    assert_eq!(
        server.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );
    assert_eq!(
        authenticate_client_connection(&client, &directory)
            .expect("server authenticated")
            .peer_id(),
        &node_b
    );
    assert_eq!(
        authenticate_server_connection(&server, &directory)
            .expect("client authenticated")
            .peer_id(),
        &node_a
    );
}

#[test]
fn valid_ca_certificate_is_still_refused_when_unconfigured() {
    let client_identity = node_a_identity();
    let server_identity = node_b_identity();
    let directory = CertificateDirectory::builder()
        .map_fingerprint(
            client_identity.leaf_fingerprint(),
            PeerId::new("node-a").expect("valid peer"),
        )
        .expect("mapping")
        .build();
    let (mut client, mut server) = connection_pair(&client_identity, &server_identity);
    complete_handshake(&mut client, &mut server).expect("chain-valid TLS");

    assert!(matches!(
        authenticate_client_connection(&client, &directory),
        Err(TlsPeerAuthenticationError::UnknownCertificate { .. })
    ));
}

#[test]
fn wrong_trust_root_and_wrong_server_name_fail_inside_tls() {
    let mistrusting_client = identity(NODE_A_CERT_PEM, NODE_A_KEY_PEM, UNTRUSTED_CA_PEM);
    let node_b = node_b_identity();
    let (mut client, mut server) = connection_pair(&mistrusting_client, &node_b);
    assert!(complete_handshake(&mut client, &mut server).is_err());

    let node_a = node_a_identity();
    let wrong_name = TlsServerName::new("wrong.raft.test").expect("valid name");
    let mut client = node_a
        .client_connection(&wrong_name)
        .expect("client connection");
    let mut server = node_b.server_connection().expect("server connection");
    assert!(complete_handshake(&mut client, &mut server).is_err());
}

#[test]
fn server_rejects_a_tls_client_without_a_certificate() {
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(CA_PEM) {
        roots
            .add(certificate.expect("root PEM"))
            .expect("valid root");
    }
    let provider = Arc::new(ring::default_provider());
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3")
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![TLS_ALPN_PROTOCOL.to_vec()];
    config.resumption = Resumption::disabled();
    let name = ServerName::try_from(server_name().as_str().to_owned()).expect("valid server name");
    let mut client = ClientConnection::new(Arc::new(config), name).expect("client connection");
    let mut server = node_b_identity()
        .server_connection()
        .expect("server connection");

    assert!(complete_handshake(&mut client, &mut server).is_err());
}

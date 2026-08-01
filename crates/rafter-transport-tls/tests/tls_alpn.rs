mod support;

use std::sync::Arc;

use rafter_transport_tls::{
    authenticate_client_connection, CertificateDirectory, PeerId, TlsPeerAuthenticationError,
};
use rustls::{
    client::{ClientConfig, ClientConnection, Resumption},
    crypto::ring,
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName},
    RootCertStore,
};

use support::tls::{
    complete_handshake, node_b_identity, server_name, CA_PEM, NODE_A_CERT_PEM, NODE_A_KEY_PEM,
};

#[test]
fn completed_tls_without_required_alpn_never_becomes_a_rafter_peer() {
    let provider = Arc::new(ring::default_provider());
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(CA_PEM) {
        roots
            .add(certificate.expect("root PEM"))
            .expect("valid root");
    }
    let certificates = CertificateDer::pem_slice_iter(NODE_A_CERT_PEM)
        .collect::<Result<Vec<_>, _>>()
        .expect("certificate PEM");
    let key = PrivateKeyDer::from_pem_slice(NODE_A_KEY_PEM).expect("private key PEM");
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3")
        .with_root_certificates(roots)
        .with_client_auth_cert(certificates, key)
        .expect("client identity");
    config.resumption = Resumption::disabled();
    config.alpn_protocols.clear();

    let name = ServerName::try_from(server_name().as_str().to_owned()).expect("valid server name");
    let mut client = ClientConnection::new(Arc::new(config), name).expect("client connection");
    let server_identity = node_b_identity();
    let mut server = server_identity
        .server_connection()
        .expect("server connection");
    complete_handshake(&mut client, &mut server).expect("TLS without ALPN");

    let directory = CertificateDirectory::builder()
        .map_fingerprint(
            server_identity.leaf_fingerprint(),
            PeerId::new("node-b").expect("valid peer"),
        )
        .expect("server mapping")
        .build();
    assert_eq!(
        authenticate_client_connection(&client, &directory),
        Err(TlsPeerAuthenticationError::MissingAlpn)
    );
}

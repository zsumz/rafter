mod support;

use rafter_transport_tls::{
    authenticate_server_connection, CertificateDirectory, PeerId, TlsPeerAuthenticationError,
};

use support::tls::{
    complete_handshake, connection_pair, node_a_identity, node_a_next_identity, node_b_identity,
};

#[test]
fn certificate_rotation_moves_through_old_overlap_and_next_only_snapshots() {
    let old_identity = node_a_identity();
    let next_identity = node_a_next_identity();
    let node_b_identity = node_b_identity();
    let node_a = PeerId::new("node-a").expect("valid peer");

    let old_only = CertificateDirectory::builder()
        .map_fingerprint(old_identity.leaf_fingerprint(), node_a.clone())
        .expect("old mapping")
        .build();
    assert_authenticates_as(&old_identity, &node_b_identity, &old_only, &node_a);
    assert_is_unconfigured(&next_identity, &node_b_identity, &old_only);

    let overlap = CertificateDirectory::builder()
        .map_fingerprint(old_identity.leaf_fingerprint(), node_a.clone())
        .expect("old mapping")
        .map_fingerprint(next_identity.leaf_fingerprint(), node_a.clone())
        .expect("next mapping")
        .build();
    assert_authenticates_as(&old_identity, &node_b_identity, &overlap, &node_a);
    assert_authenticates_as(&next_identity, &node_b_identity, &overlap, &node_a);
    assert_eq!(overlap.fingerprints_for_peer(&node_a).len(), 2);

    let next_only = CertificateDirectory::builder()
        .map_fingerprint(next_identity.leaf_fingerprint(), node_a.clone())
        .expect("next mapping")
        .build();
    assert_is_unconfigured(&old_identity, &node_b_identity, &next_only);
    assert_authenticates_as(&next_identity, &node_b_identity, &next_only, &node_a);
}

fn assert_authenticates_as(
    client_identity: &rafter_transport_tls::TlsIdentity,
    server_identity: &rafter_transport_tls::TlsIdentity,
    directory: &CertificateDirectory,
    expected: &PeerId,
) {
    let (mut client, mut server) = connection_pair(client_identity, server_identity);
    complete_handshake(&mut client, &mut server).expect("mutual TLS");
    let authenticated = authenticate_server_connection(&server, directory).expect("mapped client");
    assert_eq!(authenticated.peer_id(), expected);
}

fn assert_is_unconfigured(
    client_identity: &rafter_transport_tls::TlsIdentity,
    server_identity: &rafter_transport_tls::TlsIdentity,
    directory: &CertificateDirectory,
) {
    let (mut client, mut server) = connection_pair(client_identity, server_identity);
    complete_handshake(&mut client, &mut server).expect("chain-valid TLS");
    assert!(matches!(
        authenticate_server_connection(&server, directory),
        Err(TlsPeerAuthenticationError::UnknownCertificate { .. })
    ));
}

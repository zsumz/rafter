#[path = "support/temp_dir.rs"]
mod temp_dir;

use std::fs;

use rafter_transport_tls::{
    CertificateDirectory, CertificatePemError, PeerId, MAX_CERTIFICATE_PEM_BYTES,
};

use temp_dir::TempDir;

#[test]
fn pem_mapping_uses_the_first_certificate_as_the_explicit_leaf() {
    let peer = PeerId::new("node-a").expect("fixture peer id is valid");
    let directory = CertificateDirectory::builder()
        .map_pem_certificate_file("tests/fixtures/tls/node-a.pem", peer.clone())
        .expect("fixture certificate loads")
        .build();

    assert!(directory.contains_peer(&peer));
    assert_eq!(directory.fingerprints_for_peer(&peer).len(), 1);
}

#[test]
fn pem_mapping_refuses_a_file_with_no_certificate() {
    let peer = PeerId::new("node-a").expect("fixture peer id is valid");
    let error = CertificateDirectory::builder()
        .map_pem_certificate_file("tests/fixtures/tls/node-a-key.pem", peer)
        .expect_err("a private-key PEM contains no certificate");

    assert!(error.to_string().contains("contains no certificate"));
}

#[test]
fn pem_mapping_refuses_oversized_input_before_parsing() {
    let root = TempDir::new("certificate-pem-bound");
    let path = root.path().join("oversized.pem");
    fs::write(&path, vec![b'x'; MAX_CERTIFICATE_PEM_BYTES + 1]).expect("oversized fixture writes");
    let peer = PeerId::new("node-a").expect("fixture peer id is valid");

    let error = CertificateDirectory::builder()
        .map_pem_certificate_file(&path, peer)
        .expect_err("oversized certificate PEM is refused");

    assert!(matches!(
        error,
        CertificatePemError::TooLarge { maximum, .. }
            if maximum == MAX_CERTIFICATE_PEM_BYTES
    ));
}

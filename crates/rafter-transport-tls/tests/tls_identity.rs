mod support;
#[path = "support/temp_dir.rs"]
mod temp_dir;

use std::{fs, path::PathBuf};

use rafter_transport_tls::{
    CertificateDirectory, LocalTlsIdentityError, PeerId, TlsIdentity, TlsIdentityError,
    TlsIdentityFile, MAX_CERTIFICATE_CHAIN_PEM_BYTES, MAX_PRIVATE_KEY_PEM_BYTES,
    MAX_TRUST_ROOTS_PEM_BYTES,
};

use temp_dir::TempDir;

use support::tls::{CA_PEM, NODE_A_CERT_PEM, NODE_A_KEY_PEM, NODE_B_KEY_PEM};

#[test]
fn identity_loads_strict_material_without_exposing_key_data() {
    let identity =
        TlsIdentity::from_pem(NODE_A_CERT_PEM, NODE_A_KEY_PEM, CA_PEM).expect("valid identity");

    assert_eq!(identity.certificate_chain_len(), 1);
    assert_eq!(identity.trust_root_count(), 1);
    let debug = format!("{identity:?}");
    assert!(debug.contains("leaf_fingerprint"));
    assert!(!debug.contains("PRIVATE KEY"));
}

#[test]
fn identity_loads_the_same_material_from_files() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let from_files = TlsIdentity::from_pem_files(
        root.join("node-a.pem"),
        root.join("node-a-key.pem"),
        root.join("ca.pem"),
    )
    .expect("valid identity files");
    let from_bytes = TlsIdentity::from_pem(NODE_A_CERT_PEM, NODE_A_KEY_PEM, CA_PEM)
        .expect("valid identity bytes");

    assert_eq!(from_files.leaf_fingerprint(), from_bytes.leaf_fingerprint());
}

#[test]
fn identity_refuses_missing_or_ambiguous_private_keys() {
    assert!(matches!(
        TlsIdentity::from_pem(NODE_A_CERT_PEM, b"", CA_PEM),
        Err(TlsIdentityError::MissingPrivateKey)
    ));

    let mut two_keys = NODE_A_KEY_PEM.to_vec();
    two_keys.extend_from_slice(NODE_B_KEY_PEM);
    assert!(matches!(
        TlsIdentity::from_pem(NODE_A_CERT_PEM, &two_keys, CA_PEM),
        Err(TlsIdentityError::MultiplePrivateKeys)
    ));
}

#[test]
fn identity_refuses_missing_certificates_and_trust_roots() {
    assert!(matches!(
        TlsIdentity::from_pem(b"", NODE_A_KEY_PEM, CA_PEM),
        Err(TlsIdentityError::EmptyCertificateChain)
    ));
    assert!(matches!(
        TlsIdentity::from_pem(NODE_A_CERT_PEM, NODE_A_KEY_PEM, b""),
        Err(TlsIdentityError::EmptyTrustRoots)
    ));
}

#[test]
fn identity_refuses_malformed_pem_and_a_mismatched_key() {
    let malformed = b"-----BEGIN CERTIFICATE-----\nnot-base64!\n";
    assert!(matches!(
        TlsIdentity::from_pem(malformed, NODE_A_KEY_PEM, CA_PEM),
        Err(TlsIdentityError::ParsePem { .. })
    ));

    assert!(matches!(
        TlsIdentity::from_pem(NODE_A_CERT_PEM, NODE_B_KEY_PEM, CA_PEM),
        Err(TlsIdentityError::ClientIdentity { .. })
    ));
}

#[test]
fn local_certificate_must_map_to_the_configured_peer() {
    let identity = support::tls::node_a_identity();
    let node_a = PeerId::new("node-a").expect("valid peer");
    let node_b = PeerId::new("node-b").expect("valid peer");
    let empty = CertificateDirectory::builder().build();
    assert!(matches!(
        identity.validate_local_peer(&node_a, &empty),
        Err(LocalTlsIdentityError::UnknownCertificate { .. })
    ));

    let directory = CertificateDirectory::builder()
        .map_fingerprint(identity.leaf_fingerprint(), node_b.clone())
        .expect("mapping")
        .build();
    assert_eq!(
        identity.validate_local_peer(&node_a, &directory),
        Err(LocalTlsIdentityError::PeerMismatch {
            expected: node_a.clone(),
            actual: node_b,
        })
    );

    let directory = CertificateDirectory::builder()
        .map_fingerprint(identity.leaf_fingerprint(), node_a.clone())
        .expect("mapping")
        .build();
    identity
        .validate_local_peer(&node_a, &directory)
        .expect("local identity mapping");
}

#[test]
fn identity_file_loading_refuses_each_oversized_pem_role() {
    let root = TempDir::new("tls-identity-bounds");
    let certificate = root.path().join("certificate.pem");
    let key = root.path().join("key.pem");
    let roots = root.path().join("roots.pem");
    fs::write(&certificate, NODE_A_CERT_PEM).expect("certificate writes");
    fs::write(&key, NODE_A_KEY_PEM).expect("key writes");
    fs::write(&roots, CA_PEM).expect("roots write");

    for (field, path, maximum) in [
        (
            TlsIdentityFile::CertificateChain,
            certificate.as_path(),
            MAX_CERTIFICATE_CHAIN_PEM_BYTES,
        ),
        (
            TlsIdentityFile::PrivateKey,
            key.as_path(),
            MAX_PRIVATE_KEY_PEM_BYTES,
        ),
        (
            TlsIdentityFile::TrustRoots,
            roots.as_path(),
            MAX_TRUST_ROOTS_PEM_BYTES,
        ),
    ] {
        fs::write(path, vec![b'x'; maximum + 1]).expect("oversized role writes");
        let error = TlsIdentity::from_pem_files(&certificate, &key, &roots)
            .expect_err("oversized identity role is refused");
        assert!(matches!(
            error,
            TlsIdentityError::FileTooLarge {
                field: observed,
                maximum: observed_maximum,
                ..
            } if observed == field && observed_maximum == maximum
        ));
        match field {
            TlsIdentityFile::CertificateChain => {
                fs::write(path, NODE_A_CERT_PEM).expect("certificate restores");
            }
            TlsIdentityFile::PrivateKey => {
                fs::write(path, NODE_A_KEY_PEM).expect("key restores");
            }
            TlsIdentityFile::TrustRoots => {
                fs::write(path, CA_PEM).expect("roots restore");
            }
            _ => unreachable!("fixture enumerates every current TLS identity file role"),
        }
    }
}

use rafter_transport_tls::{
    CertificateDirectory, CertificateDirectoryError, CertificateDirectoryLimits,
    CertificateFingerprint, PeerId,
};

#[test]
fn certificate_rotation_maps_multiple_fingerprints_to_one_principal() {
    let peer = PeerId::new("orders-node-a").expect("valid peer");
    let directory = CertificateDirectory::builder()
        .map_certificate(b"old-leaf-der", peer.clone())
        .expect("old certificate")
        .map_certificate(b"new-leaf-der", peer.clone())
        .expect("new certificate")
        .build();

    assert_eq!(directory.peer_for_der(b"old-leaf-der"), Some(&peer));
    assert_eq!(directory.peer_for_der(b"new-leaf-der"), Some(&peer));
    assert_eq!(directory.fingerprints_for_peer(&peer).len(), 2);
}

#[test]
fn one_fingerprint_cannot_name_two_principals() {
    let fingerprint = CertificateFingerprint::from_der(b"leaf-der");
    let error = CertificateDirectory::builder()
        .map_fingerprint(fingerprint, PeerId::new("node-a").expect("valid peer"))
        .expect("first mapping")
        .map_fingerprint(fingerprint, PeerId::new("node-b").expect("valid peer"))
        .expect_err("conflicting mapping must fail");

    assert!(matches!(
        error,
        CertificateDirectoryError::FingerprintConflict { .. }
    ));
}

#[test]
fn certificate_directory_enforces_finite_bounds() {
    let limits = CertificateDirectoryLimits::new(1, 1).expect("valid finite limits");
    let error = CertificateDirectory::builder_with_limits(limits)
        .map_certificate(b"leaf-a", PeerId::new("node-a").expect("valid peer"))
        .expect("first certificate")
        .map_certificate(b"leaf-b", PeerId::new("node-a").expect("valid peer"))
        .expect_err("second fingerprint must exceed the bound");

    assert_eq!(
        error,
        CertificateDirectoryError::FingerprintLimit { maximum: 1 }
    );
}

#[test]
fn fingerprint_text_round_trips_and_is_lowercase() {
    let fingerprint = CertificateFingerprint::from_der(b"leaf-der");
    let text = fingerprint.to_string();
    let decoded = text.parse::<CertificateFingerprint>().expect("valid hex");

    assert_eq!(decoded, fingerprint);
    assert_eq!(text.len(), CertificateFingerprint::HEX_LEN);
    assert_eq!(text, text.to_ascii_lowercase());
}

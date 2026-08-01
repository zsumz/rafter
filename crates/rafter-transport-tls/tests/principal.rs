use rafter_transport_tls::{ClusterId, IdentityError, IdentityKind, PeerId, MAX_ID_BYTES};

#[test]
fn peer_and_cluster_ids_preserve_exact_utf8() {
    let peer = PeerId::new("spiffe://example.com/rafter/node-a").expect("valid peer");
    let cluster = ClusterId::new("orders-production-us1").expect("valid cluster");

    assert_eq!(peer.as_str(), "spiffe://example.com/rafter/node-a");
    assert_eq!(cluster.as_str(), "orders-production-us1");
    assert_ne!(
        PeerId::new("NODE-A").expect("valid uppercase"),
        PeerId::new("node-a").expect("valid lowercase")
    );
}

#[test]
fn identities_refuse_empty_control_and_oversized_values() {
    assert_eq!(
        PeerId::new(""),
        Err(IdentityError::Empty {
            kind: IdentityKind::Peer,
        })
    );
    assert_eq!(
        ClusterId::new("orders\nproduction"),
        Err(IdentityError::ControlCharacter {
            kind: IdentityKind::Cluster,
            byte_index: 6,
            character: '\n',
        })
    );

    let oversized = "a".repeat(MAX_ID_BYTES + 1);
    assert_eq!(
        PeerId::new(&oversized),
        Err(IdentityError::TooLong {
            kind: IdentityKind::Peer,
            len: MAX_ID_BYTES + 1,
            max: MAX_ID_BYTES,
        })
    );
}

#[test]
fn identity_bounds_count_utf8_bytes_not_characters() {
    let boundary = "é".repeat(MAX_ID_BYTES / 2);
    assert!(PeerId::new(&boundary).is_ok());

    let oversized = format!("{boundary}é");
    assert!(matches!(
        PeerId::new(&oversized),
        Err(IdentityError::TooLong { .. })
    ));
}

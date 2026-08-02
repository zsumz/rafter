use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use rafter_transport_tls::{
    EndpointBook, EndpointBookError, EndpointBookLimits, PeerEndpoint, PeerId, TlsServerName,
    TlsServerNameError,
};

fn endpoint(port: u16, server_name: &str) -> PeerEndpoint {
    PeerEndpoint::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        TlsServerName::new(server_name).expect("valid server name"),
    )
}

#[test]
fn server_names_have_one_canonical_endpoint_book_spelling() {
    assert_eq!(
        TlsServerName::new("Node-A.RAFT.Internal")
            .expect("valid DNS name")
            .as_str(),
        "node-a.raft.internal"
    );
    assert_eq!(
        TlsServerName::new("[::1]"),
        Err(TlsServerNameError::InvalidLabelByte {
            label_index: 0,
            byte_index: 0,
            byte: b'[',
        })
    );
    assert_eq!(
        TlsServerName::new("node-a.raft.internal."),
        Err(TlsServerNameError::TrailingDot)
    );
}

#[test]
fn replacement_is_atomic_versioned_and_idempotent() {
    let book = EndpointBook::new(EndpointBookLimits::default());
    let peer = PeerId::new("node-a").expect("valid peer");
    let first = vec![endpoint(7400, "node-a.raft.internal")];

    let generation_one = book
        .replace(peer.clone(), first.clone())
        .expect("first replacement");
    let same = book
        .replace(peer.clone(), first)
        .expect("idempotent replacement");
    let generation_two = book
        .replace(peer.clone(), vec![endpoint(7401, "node-a.raft.internal")])
        .expect("changed replacement");

    assert_eq!(generation_one, same);
    assert!(generation_two > generation_one);
    let snapshot = book.snapshot(&peer).expect("read book").expect("peer");
    assert_eq!(snapshot.generation(), generation_two);
    assert_eq!(snapshot.endpoints()[0].address().port(), 7401);
}

#[test]
fn refresh_advances_generation_without_changing_endpoint_values() {
    let book = EndpointBook::new(EndpointBookLimits::default());
    let peer = PeerId::new("node-a").expect("valid peer");
    let values = vec![endpoint(7400, "node-a.raft.internal")];
    let installed = book
        .replace(peer.clone(), values.clone())
        .expect("install endpoints");

    let refreshed = book
        .refresh(&peer)
        .expect("refresh endpoints")
        .expect("configured peer");
    let snapshot = book.snapshot(&peer).expect("read book").expect("peer");

    assert!(refreshed > installed);
    assert_eq!(snapshot.generation(), refreshed);
    assert_eq!(snapshot.endpoints(), values);
    assert_eq!(
        book.refresh(&PeerId::new("absent").expect("valid absent peer")),
        Ok(None)
    );
}

#[test]
fn endpoint_replacement_rejects_duplicates_and_preserves_old_value() {
    let book = EndpointBook::new(EndpointBookLimits::default());
    let peer = PeerId::new("node-a").expect("valid peer");
    let original = endpoint(7400, "node-a.raft.internal");
    book.replace(peer.clone(), vec![original.clone()])
        .expect("initial replacement");

    let duplicate = endpoint(7401, "node-a.raft.internal");
    let error = book
        .replace(peer.clone(), vec![duplicate.clone(), duplicate])
        .expect_err("duplicate set must fail");
    assert_eq!(error, EndpointBookError::Duplicate { index: 1 });

    let snapshot = book.snapshot(&peer).expect("read book").expect("peer");
    assert_eq!(snapshot.endpoints(), &[original]);
}

#[test]
fn endpoint_book_enforces_peer_and_per_peer_bounds() {
    let book = EndpointBook::new(EndpointBookLimits::new(1, 1).expect("valid finite limits"));
    book.replace(
        PeerId::new("node-a").expect("valid peer"),
        vec![endpoint(7400, "node-a.raft.internal")],
    )
    .expect("first peer");

    assert_eq!(
        book.replace(
            PeerId::new("node-b").expect("valid peer"),
            vec![endpoint(7401, "node-b.raft.internal")],
        ),
        Err(EndpointBookError::PeerLimit { maximum: 1 })
    );
}

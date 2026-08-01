#[path = "support/temp_dir.rs"]
mod temp_dir;

use std::fs;

use rafter_transport_tls::{
    ClusterId, ConnectionSession, CreateTransportSessionStoreError, FileTransportSessionStore,
    FileTransportSessionStoreError, InboundSessionDecision, OpenTransportSessionStoreError, PeerId,
    SessionStateError, SessionStoreLimits, TransportSessionStore,
};

use temp_dir::TempDir;

fn identities() -> (ClusterId, PeerId, PeerId) {
    (
        ClusterId::new("orders-production-us1").expect("valid cluster"),
        PeerId::new("orders-node-a").expect("valid local peer"),
        PeerId::new("orders-node-b").expect("valid remote peer"),
    )
}

#[test]
fn restart_preserves_both_directional_high_water_marks() {
    let directory = TempDir::new("session-restart");
    let path = directory.path().join("transport.state");
    let (cluster, local, remote) = identities();
    let store = FileTransportSessionStore::create_new(
        &path,
        cluster.clone(),
        local.clone(),
        SessionStoreLimits::default(),
    )
    .expect("create store");

    assert_eq!(
        store
            .allocate_outbound_session(&remote)
            .expect("first outbound")
            .get(),
        1
    );
    assert_eq!(
        store
            .accept_inbound_session(&remote, ConnectionSession::new(7).expect("nonzero"),)
            .expect("accept inbound"),
        InboundSessionDecision::Accepted { previous: None }
    );
    drop(store);

    let reopened =
        FileTransportSessionStore::open_existing(&path, &cluster, &local).expect("reopen store");
    let state = reopened
        .peer_session_state(&remote)
        .expect("read peer state");
    assert_eq!(
        state.highest_outbound(),
        Some(ConnectionSession::new(1).expect("nonzero"))
    );
    assert_eq!(
        state.highest_inbound(),
        Some(ConnectionSession::new(7).expect("nonzero"))
    );
    assert_eq!(
        reopened
            .allocate_outbound_session(&remote)
            .expect("next outbound")
            .get(),
        2
    );
}

#[test]
fn stale_inbound_session_is_a_nonmutating_decision() {
    let directory = TempDir::new("stale-session");
    let path = directory.path().join("transport.state");
    let (cluster, local, remote) = identities();
    let store =
        FileTransportSessionStore::create_new(&path, cluster, local, SessionStoreLimits::default())
            .expect("create store");
    let session = ConnectionSession::new(7).expect("nonzero");
    store
        .accept_inbound_session(&remote, session)
        .expect("first acceptance");
    let before = fs::read(&path).expect("read before stale decision");

    assert_eq!(
        store
            .accept_inbound_session(&remote, session)
            .expect("stale decision"),
        InboundSessionDecision::Stale {
            highest_accepted: session,
        }
    );
    assert_eq!(fs::read(&path).expect("read after stale decision"), before);
}

#[test]
fn creation_and_opening_are_strictly_separate() {
    let directory = TempDir::new("strict-open");
    let path = directory.path().join("transport.state");
    let (cluster, local, _) = identities();

    assert!(matches!(
        FileTransportSessionStore::open_existing(&path, &cluster, &local),
        Err(OpenTransportSessionStoreError::Missing { .. })
    ));
    let store = FileTransportSessionStore::create_new(
        &path,
        cluster.clone(),
        local.clone(),
        SessionStoreLimits::default(),
    )
    .expect("create store");
    drop(store);
    assert!(matches!(
        FileTransportSessionStore::create_new(&path, cluster, local, SessionStoreLimits::default(),),
        Err(CreateTransportSessionStoreError::AlreadyExists { .. })
    ));
}

#[test]
fn cooperating_handles_have_exclusive_ownership() {
    let directory = TempDir::new("exclusive-open");
    let path = directory.path().join("transport.state");
    let (cluster, local, _) = identities();
    let _store = FileTransportSessionStore::create_new(
        &path,
        cluster.clone(),
        local.clone(),
        SessionStoreLimits::default(),
    )
    .expect("create store");

    assert!(matches!(
        FileTransportSessionStore::open_existing(&path, &cluster, &local),
        Err(OpenTransportSessionStoreError::AlreadyOpen { .. })
    ));
}

#[test]
fn corrupt_state_and_identity_mismatch_fail_closed() {
    let directory = TempDir::new("corrupt-open");
    let path = directory.path().join("transport.state");
    let (cluster, local, _) = identities();
    let store = FileTransportSessionStore::create_new(
        &path,
        cluster.clone(),
        local.clone(),
        SessionStoreLimits::default(),
    )
    .expect("create store");
    drop(store);

    assert!(matches!(
        FileTransportSessionStore::open_existing(
            &path,
            &ClusterId::new("another-cluster").expect("valid cluster"),
            &local,
        ),
        Err(OpenTransportSessionStoreError::ClusterMismatch { .. })
    ));
    let mut bytes = fs::read(&path).expect("read state");
    bytes[10] ^= 1;
    fs::write(&path, bytes).expect("corrupt state");
    assert!(matches!(
        FileTransportSessionStore::open_existing(&path, &cluster, &local),
        Err(OpenTransportSessionStoreError::Decode { .. })
    ));
}

#[test]
fn local_peer_identity_mismatch_fails_closed() {
    let directory = TempDir::new("local-peer-mismatch");
    let path = directory.path().join("transport.state");
    let (cluster, local, _) = identities();
    let store = FileTransportSessionStore::create_new(
        &path,
        cluster.clone(),
        local,
        SessionStoreLimits::default(),
    )
    .expect("create store");
    drop(store);

    assert!(matches!(
        FileTransportSessionStore::open_existing(
            &path,
            &cluster,
            &PeerId::new("orders-node-z").expect("valid peer"),
        ),
        Err(OpenTransportSessionStoreError::LocalPeerMismatch { .. })
    ));
}

#[test]
fn oversized_state_is_refused_before_decoding() {
    let directory = TempDir::new("oversized-state");
    let path = directory.path().join("transport.state");
    let (cluster, local, _) = identities();
    let maximum = rafter_transport_tls::max_transport_session_state_bytes(SessionStoreLimits::MAX);
    let file = fs::File::create(&path).expect("create oversized state");
    file.set_len(u64::try_from(maximum + 1).expect("bound fits u64"))
        .expect("size oversized state");
    drop(file);

    assert!(matches!(
        FileTransportSessionStore::open_existing(&path, &cluster, &local),
        Err(OpenTransportSessionStoreError::FileTooLarge {
            actual,
            maximum: reported,
            ..
        }) if actual == maximum + 1 && reported == maximum
    ));
}

#[test]
fn finite_peer_bound_refuses_without_failing_the_store() {
    let directory = TempDir::new("peer-bound");
    let path = directory.path().join("transport.state");
    let (cluster, local, remote) = identities();
    let store = FileTransportSessionStore::create_new(
        &path,
        cluster,
        local,
        SessionStoreLimits::new(1).expect("valid bound"),
    )
    .expect("create store");
    store
        .allocate_outbound_session(&remote)
        .expect("first peer");
    let another = PeerId::new("orders-node-c").expect("valid peer");

    assert!(matches!(
        store.allocate_outbound_session(&another),
        Err(FileTransportSessionStoreError::State {
            source: SessionStateError::PeerLimit { maximum: 1 },
        })
    ));
    assert!(!store.requires_reopen());
}

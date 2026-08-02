use std::collections::BTreeMap;

use rafter_transport_tls::{
    ConnectionSession, InboundSessionDecision, PeerId, PeerSessionState, SessionStateError,
    SessionStoreLimits, TransportSessionState,
};

fn peer(value: &str) -> PeerId {
    PeerId::new(value).expect("valid peer")
}

#[test]
fn outbound_sessions_begin_at_one_and_advance_monotonically() {
    let peer = peer("node-b");
    let mut state = TransportSessionState::new(SessionStoreLimits::default());

    assert_eq!(
        state
            .allocate_outbound(&peer)
            .expect("first allocation")
            .get(),
        1
    );
    assert_eq!(
        state
            .allocate_outbound(&peer)
            .expect("second allocation")
            .get(),
        2
    );
    assert_eq!(
        state.peer_state(&peer).highest_outbound(),
        Some(ConnectionSession::new(2).expect("nonzero"))
    );
}

#[test]
fn inbound_sessions_accept_only_a_strictly_newer_epoch() {
    let peer = peer("node-b");
    let mut state = TransportSessionState::new(SessionStoreLimits::default());
    let seven = ConnectionSession::new(7).expect("nonzero");

    assert_eq!(
        state.accept_inbound(&peer, seven).expect("first decision"),
        InboundSessionDecision::Accepted { previous: None }
    );
    assert_eq!(
        state
            .accept_inbound(&peer, seven)
            .expect("duplicate decision"),
        InboundSessionDecision::Stale {
            highest_accepted: seven,
        }
    );
    assert_eq!(
        state
            .accept_inbound(&peer, ConnectionSession::new(6).expect("nonzero"),)
            .expect("older decision"),
        InboundSessionDecision::Stale {
            highest_accepted: seven,
        }
    );
    assert_eq!(
        state
            .accept_inbound(&peer, ConnectionSession::new(8).expect("nonzero"),)
            .expect("newer decision"),
        InboundSessionDecision::Accepted {
            previous: Some(seven),
        }
    );
}

#[test]
fn peer_bound_refuses_without_partly_mutating_state() {
    let mut state = TransportSessionState::new(SessionStoreLimits::new(1).expect("valid bound"));
    let peer_a = peer("node-a");
    let peer_b = peer("node-b");
    state.allocate_outbound(&peer_a).expect("first peer");

    assert_eq!(
        state.allocate_outbound(&peer_b),
        Err(SessionStateError::PeerLimit { maximum: 1 })
    );
    assert_eq!(state.peer_count(), 1);
    assert_eq!(state.peer_state(&peer_b), PeerSessionState::default());
}

#[test]
fn aggregate_preflight_counts_every_absent_configured_peer_without_mutation() {
    let state = TransportSessionState::new(SessionStoreLimits::new(2).expect("valid bound"));
    let peers = vec![peer("node-a"), peer("node-b"), peer("node-c")];

    assert_eq!(
        state.preflight_peers(&peers),
        Err(SessionStateError::PeerLimit { maximum: 2 })
    );
    assert_eq!(state.peer_count(), 0);
    assert!(state.preflight_peers(&peers[..2]).is_ok());
    assert_eq!(state.peer_count(), 0);
}

#[test]
fn recovered_maximum_outbound_session_is_terminal_for_that_peer() {
    let peer = peer("node-b");
    let mut peers = BTreeMap::new();
    peers.insert(
        peer.clone(),
        PeerSessionState::new(
            Some(ConnectionSession::new(u64::MAX).expect("nonzero")),
            None,
        ),
    );
    let mut state = TransportSessionState::from_peer_states(SessionStoreLimits::default(), peers)
        .expect("valid recovered state");

    assert_eq!(
        state.allocate_outbound(&peer),
        Err(SessionStateError::OutboundExhausted { peer })
    );
}

#[test]
fn recovered_maximum_inbound_session_fails_startup_preflight() {
    let peer = peer("node-b");
    let mut peers = BTreeMap::new();
    peers.insert(
        peer.clone(),
        PeerSessionState::new(
            None,
            Some(ConnectionSession::new(u64::MAX).expect("nonzero")),
        ),
    );
    let state = TransportSessionState::from_peer_states(SessionStoreLimits::default(), peers)
        .expect("valid recovered state");

    assert_eq!(
        state.preflight_peer(&peer),
        Err(SessionStateError::InboundExhausted { peer })
    );
}

#[test]
fn recovered_empty_records_are_noncanonical() {
    let peer = peer("node-b");
    let mut peers = BTreeMap::new();
    peers.insert(peer.clone(), PeerSessionState::default());

    assert_eq!(
        TransportSessionState::from_peer_states(SessionStoreLimits::default(), peers,),
        Err(SessionStateError::EmptyPeerRecord { peer })
    );
}

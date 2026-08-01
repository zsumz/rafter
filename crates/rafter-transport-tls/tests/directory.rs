use rafter::NodeId;
use rafter_service::{AuthenticatedPeerValidator, PeerPolicy};
use rafter_transport_tls::{DirectoryError, DirectoryLimits, PeerId, TlsPeerDirectory};

#[test]
fn one_physical_peer_may_have_different_node_ids_across_groups() {
    let directory = TlsPeerDirectory::new(DirectoryLimits::default());
    let peer = PeerId::new("physical-node-a").expect("valid peer");

    directory
        .bind("orders", NodeId(3), peer.clone())
        .expect("orders binding");
    directory
        .bind("payments", NodeId(17), peer.clone())
        .expect("payments binding");

    assert_eq!(
        directory.node_for_authenticated_peer(&"orders", &peer),
        Some(NodeId(3))
    );
    assert_eq!(
        directory.node_for_authenticated_peer(&"payments", &peer),
        Some(NodeId(17))
    );
}

#[test]
fn mappings_are_one_to_one_within_one_group() {
    let directory = TlsPeerDirectory::new(DirectoryLimits::default());
    let peer_a = PeerId::new("node-a").expect("valid peer");
    let peer_b = PeerId::new("node-b").expect("valid peer");
    directory
        .bind(7_u64, NodeId(2), peer_a.clone())
        .expect("first binding");

    assert!(matches!(
        directory.bind(7, NodeId(2), peer_b),
        Err(DirectoryError::NodeAlreadyBound { .. })
    ));
    assert!(matches!(
        directory.bind(7, NodeId(3), peer_a),
        Err(DirectoryError::PeerAlreadyBound { .. })
    ));
}

#[test]
fn policy_replacement_is_whole_and_retirement_is_monotonic() {
    let directory = TlsPeerDirectory::new(DirectoryLimits::default());
    let peer_a = PeerId::new("node-a").expect("valid peer");
    let peer_b = PeerId::new("node-b").expect("valid peer");
    directory
        .bind(7_u64, NodeId(2), peer_a.clone())
        .expect("node a binding");
    directory
        .bind(7_u64, NodeId(3), peer_b.clone())
        .expect("node b binding");

    directory
        .replace_policy(
            &7,
            PeerPolicy::new(vec![peer_a.clone(), peer_b.clone()], Some(NodeId(3))),
        )
        .expect("initial policy");
    directory
        .replace_policy(&7, PeerPolicy::new(vec![peer_b.clone()], Some(NodeId(2))))
        .expect("remove node a with stale lower floor");

    let policy = directory.policy(&7).expect("read policy").expect("policy");
    assert_eq!(policy.authorized_peers(), std::slice::from_ref(&peer_b));
    assert_eq!(policy.retirement_floor(), Some(NodeId(3)));
    assert!(directory.is_retired_peer(&7, NodeId(2)));
    assert!(!directory.is_authorized_peer(&7, NodeId(2)));
    assert!(directory.is_authorized_peer(&7, NodeId(3)));

    assert_eq!(
        directory.replace_policy(&7, PeerPolicy::new(vec![peer_a], Some(NodeId(3))),),
        Err(DirectoryError::RetiredNodeReauthorization {
            node_id: NodeId(2),
            retirement_floor: NodeId(3),
        })
    );
}

#[test]
fn removed_bindings_may_be_forgotten_but_spent_ids_stay_refused() {
    let directory = TlsPeerDirectory::new(DirectoryLimits::default());
    let old_peer = PeerId::new("node-old").expect("valid peer");
    directory
        .bind(7_u64, NodeId(2), old_peer.clone())
        .expect("old binding");
    directory
        .replace_policy(&7, PeerPolicy::new(vec![old_peer], Some(NodeId(2))))
        .expect("authorize old node");
    directory
        .replace_policy(&7, PeerPolicy::new(Vec::new(), Some(NodeId(2))))
        .expect("retire old node");

    assert_eq!(
        directory.unbind(&7, NodeId(2)).expect("forget old mapping"),
        Some(PeerId::new("node-old").expect("valid peer"))
    );
    assert_eq!(
        directory.bind(
            7,
            NodeId(2),
            PeerId::new("replacement").expect("valid peer"),
        ),
        Err(DirectoryError::RetiredNodeBinding {
            node_id: NodeId(2),
            retirement_floor: NodeId(2),
        })
    );
}

#[test]
fn validator_methods_fail_closed_for_unknown_groups() {
    let directory = TlsPeerDirectory::<u64>::new(DirectoryLimits::default());
    let peer = PeerId::new("node-a").expect("valid peer");

    assert!(!directory.is_known_group(&99));
    assert_eq!(directory.node_for_authenticated_peer(&99, &peer), None);
    assert!(!directory.is_authorized_peer(&99, NodeId(1)));
    assert!(!directory.is_retired_peer(&99, NodeId(1)));
}

#[test]
fn directory_enforces_group_and_binding_bounds() {
    let directory = TlsPeerDirectory::new(DirectoryLimits::new(1, 1).expect("valid finite limits"));
    directory
        .bind(7_u64, NodeId(1), PeerId::new("node-a").expect("valid peer"))
        .expect("first binding");

    assert_eq!(
        directory.bind(7, NodeId(2), PeerId::new("node-b").expect("valid peer"),),
        Err(DirectoryError::BindingLimit { maximum: 1 })
    );
    assert_eq!(
        directory.insert_group(8),
        Err(DirectoryError::GroupLimit { maximum: 1 })
    );
}

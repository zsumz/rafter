//! Static membership and election-timeout validation.

use crate::{NodeConfig, NodeConfigError, NodeId};

#[test]
fn voting_config_rejects_self_as_peer() {
    assert_eq!(
        NodeConfig::new(NodeId(1), vec![NodeId(1), NodeId(2)], 3),
        Err(NodeConfigError::SelfPeer { id: NodeId(1) })
    );
}

#[test]
fn voting_config_rejects_duplicate_peers() {
    assert_eq!(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(2)], 3),
        Err(NodeConfigError::DuplicatePeer { peer: NodeId(2) })
    );
}

#[test]
fn non_voter_config_excludes_local_node_from_static_membership() {
    let config = NodeConfig::new_non_voter(NodeId(3), vec![NodeId(1), NodeId(2)], 3)
        .expect("future learner configuration is valid");

    assert_eq!(config.peers(), &[NodeId(1), NodeId(2)]);
    assert_eq!(
        config.voters().collect::<Vec<_>>(),
        vec![NodeId(1), NodeId(2)]
    );
    assert!(!config.static_membership().contains_voter(NodeId(3)));
}

#[test]
fn non_voter_config_rejects_empty_voter_set() {
    assert_eq!(
        NodeConfig::new_non_voter(NodeId(3), Vec::new(), 3),
        Err(NodeConfigError::EmptyVoters)
    );
}

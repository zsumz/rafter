//! A real learner removal retires transport identity, including failed publication.
mod support;

use rafter::{Message, NodeId, RequestVote, Term};
use rafter_app::membership::{MembershipChange, NodeInfo};
use rafter_service::{AuthenticatedPeerEnvelope, InboundEnvelopeError};
use support::transport::{driver_over_bootstrap, elect_single_voter, Principal, GROUP};

#[test]
fn learner_removal_fences_the_real_committed_identity_before_peer_policy_retry() {
    let (driver, transport) = driver_over_bootstrap(1, &[], &[2, 3]);
    elect_single_voter(&driver);
    for id in [2, 3] {
        driver
            .change_membership(MembershipChange::AddLearner {
                node_id: NodeId(id),
                info: NodeInfo::default(),
            })
            .unwrap();
    }
    assert_eq!(
        transport.peer_sets().last().unwrap(),
        &vec![
            Principal::for_node(NodeId(2)),
            Principal::for_node(NodeId(3)),
        ]
    );
    assert!(!transport.retires(NodeId(2)));
    let before = driver.control_plane_checkpoint_epoch();
    // Refuse publication after removal and the retry at inbound admission.
    transport.refuse_next_peer_updates(2);
    driver
        .change_membership(MembershipChange::RemoveNode { node_id: NodeId(2) })
        .unwrap();
    assert!(driver.control_plane_checkpoint_epoch() > before);
    assert!(driver.peer_policy_is_stale());
    assert!(
        !transport.retires(NodeId(2)),
        "the transport still holds the old policy"
    );
    let metrics = driver.handle().metrics().unwrap().current();
    assert!(!metrics.membership.contains_replica(NodeId(2)));
    assert!(metrics.membership.contains_replica(NodeId(3)));
    let refused = driver.deliver(AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(NodeId(2)),
        raft_from: NodeId(2),
        raft_to: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(metrics.term.0 + 1),
            candidate_id: NodeId(2),
            last_log_index: metrics.last_log_index,
            last_log_term: metrics.term,
        }),
    });
    assert!(matches!(
        refused,
        Err(InboundEnvelopeError::NotInMembership { node_id: NodeId(2) })
    ));
    driver.tick().unwrap();
    assert!(!driver.peer_policy_is_stale());
    assert!(transport.retires(NodeId(2)));
    assert!(!transport.retires(NodeId(3)));
    assert_eq!(
        transport.peer_sets().last().unwrap(),
        &vec![Principal::for_node(NodeId(3))]
    );
    assert_eq!(
        driver.handle().metrics().unwrap().current().term,
        metrics.term
    );
}

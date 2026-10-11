//! The public remove-node command includes non-voting replicas.
mod support;

use rafter::{LogIndex, MembershipConfig, NodeConfig, NodeId, Role};
use rafter_app::{
    group::{GroupInput, RaftGroup},
    membership::{MembershipChange, MembershipEvent, NodeInfo},
};
use rafter_runtime::DurableRaftNode;
use rafter_runtime_api::PersistedRaftRuntime;
use rafter_storage::InMemoryRaftHardStateStore;
use support::RecordingStateMachine;

fn elected_group() -> RaftGroup<u64, RecordingStateMachine, DurableRaftNode> {
    let runtime = DurableRaftNode::new(
        NodeConfig::new(NodeId(1), vec![], 1).unwrap(),
        InMemoryRaftHardStateStore::new(),
    )
    .unwrap();
    let mut group = RaftGroup::new(7_u64, NodeId(1), runtime, RecordingStateMachine::default());
    group.step(GroupInput::Tick).unwrap();
    assert_eq!(group.runtime().role(), Role::Leader);
    for id in [2, 3] {
        group
            .step(GroupInput::Membership {
                change: MembershipChange::AddLearner {
                    node_id: NodeId(id),
                    info: NodeInfo::default(),
                },
            })
            .unwrap();
    }
    assert_eq!(
        group.runtime().committed_membership(),
        support::membership(&[1], &[2, 3])
    );
    group
}

#[test]
fn removing_a_learner_commits_directly_and_preserves_other_members() {
    let mut group = elected_group();
    for (id, remaining) in [(2, vec![3]), (3, vec![])] {
        let before = group.runtime().committed_membership();
        let index = LogIndex(group.runtime().last_log_index().0 + 1);
        let report = group
            .step(GroupInput::Membership {
                change: MembershipChange::RemoveNode {
                    node_id: NodeId(id),
                },
            })
            .unwrap();
        assert!(
            !report
                .membership_events
                .iter()
                .any(|event| matches!(event, MembershipEvent::Rejected { .. })),
            "learner removal was rejected: {:?}",
            report.membership_events
        );
        let expected = support::membership(&[1], &remaining);
        assert_eq!(group.runtime().membership(), expected);
        assert_eq!(group.runtime().committed_membership(), expected);
        assert_eq!(group.runtime().commit_index(), index);
        assert_eq!(group.runtime().last_log_index(), index);
        assert_eq!(group.runtime().role(), Role::Leader);
        assert!(matches!(expected, MembershipConfig::Stable(_)));
        assert!(report.membership_events.iter().any(|event| matches!(event,
            MembershipEvent::Applied { index: at, previous, membership, .. }
            if *at == index && *previous == before && *membership == expected)));
    }
}

#[test]
fn removing_an_unknown_node_or_last_voter_preserves_the_existing_refusals() {
    use rafter::{ConfigurationProposalRejection, ProposalRejection};
    let mut group = elected_group();
    let membership = group.runtime().committed_membership();
    let index = group.runtime().last_log_index();
    for (node_id, reason) in [
        (
            NodeId(99),
            ConfigurationProposalRejection::VoterNotFound {
                node_id: NodeId(99),
            },
        ),
        (
            NodeId(1),
            ConfigurationProposalRejection::CannotRemoveLastVoter { node_id: NodeId(1) },
        ),
    ] {
        let report = group
            .step(GroupInput::Membership {
                change: MembershipChange::RemoveNode { node_id },
            })
            .unwrap();
        assert_eq!(
            report.membership_events,
            vec![MembershipEvent::Rejected {
                group_id: 7,
                reason: ProposalRejection::Configuration(reason),
                leader_hint: Some(NodeId(1)),
            }]
        );
        assert_eq!(group.runtime().membership(), membership);
        assert_eq!(group.runtime().committed_membership(), membership);
        assert_eq!(group.runtime().last_log_index(), index);
        assert_eq!(group.runtime().commit_index(), index);
    }
}

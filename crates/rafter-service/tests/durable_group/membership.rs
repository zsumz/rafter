//! Shared app requests retain the kernel's admission and commit boundaries.
use super::support::*;
use rafter::{
    ConfigurationProposalRejection, MembershipConfig, MembershipSet, Output, ProposalRejection,
};
use rafter_app::membership::{MembershipChange, NodeInfo};
use rafter_runtime::service::ServiceEvent;

fn membership(voters: &[u64], learners: &[u64]) -> MembershipSet {
    MembershipSet::new(
        voters.iter().copied().map(NodeId).collect(),
        learners.iter().copied().map(NodeId).collect(),
    )
    .unwrap()
}

fn submit(node: &mut Replica, change: MembershipChange) -> Vec<ProposalRejection> {
    assert_eq!(node.driver.state(), ServiceState::Ready);
    let current = node.driver.ready_node().unwrap().effective_membership();
    let input = change.into_raft_input(&current).unwrap();
    // Success is service admission. A protocol refusal remains an event.
    node.driver.try_step(input).unwrap();
    let mut rejected = Vec::new();
    loop {
        match node.driver.poll_next().unwrap() {
            ServicePoll::Event(ServiceEvent::Output(Output::RejectProposal { reason, .. })) => {
                rejected.push(reason);
            }
            ServicePoll::Pending => break,
            _ => {}
        }
    }
    rejected
}

fn commit_leader(cluster: &mut Cluster, expected: &MembershipConfig) {
    cluster.until(|cluster| {
        cluster.nodes[0].driver.state() == ServiceState::Ready
            && cluster.nodes[0].driver.ready_node().is_some_and(|node| {
                node.committed_membership() == *expected
                    && node.commit_index() == node.last_log_index()
            })
    });
}

#[test]
fn shared_requests_remove_a_durable_learner_without_changing_other_members() {
    let mut node = Replica::single(false);
    for id in [2, 3] {
        assert!(submit(
            &mut node,
            MembershipChange::AddLearner {
                node_id: NodeId(id),
                info: NodeInfo::default(),
            },
        )
        .is_empty());
    }
    assert!(submit(
        &mut node,
        MembershipChange::RemoveNode { node_id: NodeId(2) },
    )
    .is_empty());
    let core = node.driver.ready_node().unwrap();
    assert_eq!(
        core.committed_membership(),
        MembershipConfig::Stable(membership(&[1], &[3]))
    );
    assert_eq!(core.commit_index(), core.last_log_index());
}

#[test]
fn durable_voter_removal_requires_replication_and_explicit_finalization() {
    let mut cluster = Cluster::new();
    let before = cluster.nodes[0]
        .driver
        .ready_node()
        .unwrap()
        .committed_membership();
    assert!(submit(
        &mut cluster.nodes[0],
        MembershipChange::RemoveNode { node_id: NodeId(3) },
    )
    .is_empty());
    let core = cluster.nodes[0].driver.ready_node().unwrap();
    let joint = core.effective_membership();
    assert!(matches!(joint, MembershipConfig::Joint(_)));
    assert_eq!(core.committed_membership(), before);
    assert!(core.last_log_index() > core.commit_index());
    assert_eq!(
        submit(&mut cluster.nodes[0], MembershipChange::LeaveJoint),
        vec![ProposalRejection::Configuration(
            ConfigurationProposalRejection::UncommittedConfiguration {
                index: cluster.nodes[0]
                    .driver
                    .ready_node()
                    .unwrap()
                    .last_log_index(),
            },
        )]
    );
    commit_leader(&mut cluster, &joint);
    assert!(submit(&mut cluster.nodes[0], MembershipChange::LeaveJoint).is_empty());
    commit_leader(
        &mut cluster,
        &MembershipConfig::Stable(membership(&[1, 2], &[])),
    );
    let sender = cluster.nodes[0].driver.sender();
    let mut write = sender.write(GROUP, vec![7], WriteOptions::default());
    let mut receipt = None;
    cluster.until(|_| {
        if let Poll::Ready(result) = poll(&mut write) {
            receipt = Some(result.unwrap());
            true
        } else {
            false
        }
    });
    let receipt = receipt.unwrap();
    assert_eq!(receipt.result.value, 7);
    let mut read = sender.read(
        GROUP,
        vec![0],
        ReadConsistency::Linearizable,
        ReadOptions::default().with_min_applied_index(receipt.index),
    );
    cluster.until(|_| match poll(&mut read) {
        Poll::Ready(result) => {
            let result = result.unwrap();
            assert_eq!(result.result, 7);
            assert!(result.proof.unwrap().local_applied_index >= receipt.index);
            true
        }
        Poll::Pending => false,
    });
}

#[test]
fn durable_promotion_preserves_catch_up_barriers_in_both_request_forms() {
    let mut cluster = Cluster::new();
    for explicit_joint in [false, true] {
        assert!(submit(
            &mut cluster.nodes[0],
            MembershipChange::ChangeVoters {
                target: membership(&[1, 2], &[3]),
            },
        )
        .is_empty());
        let joint = cluster.nodes[0]
            .driver
            .ready_node()
            .unwrap()
            .effective_membership();
        commit_leader(&mut cluster, &joint);
        assert!(submit(&mut cluster.nodes[0], MembershipChange::LeaveJoint).is_empty());
        commit_leader(
            &mut cluster,
            &MembershipConfig::Stable(membership(&[1, 2], &[3])),
        );
        let barrier = cluster.nodes[0]
            .driver
            .ready_node()
            .unwrap()
            .promotion_barrier(NodeId(3))
            .unwrap();
        cluster.until(|cluster| {
            cluster.nodes[0].driver.state() == ServiceState::Ready
                && cluster.nodes[0].driver.ready_node().is_some_and(|node| {
                    node.leader_replication_progress().iter().any(|progress| {
                        progress.follower_id == NodeId(3)
                            && progress.match_index >= barrier.required_match_index
                    })
                })
        });
        let change = if explicit_joint {
            MembershipChange::EnterJoint {
                target: membership(&[1, 2, 3], &[]),
                promotion_barriers: vec![barrier],
            }
        } else {
            MembershipChange::PromoteLearner {
                node_id: NodeId(3),
                barrier,
            }
        };
        assert!(submit(&mut cluster.nodes[0], change).is_empty());
        let joint = cluster.nodes[0]
            .driver
            .ready_node()
            .unwrap()
            .effective_membership();
        assert!(matches!(joint, MembershipConfig::Joint(_)));
        commit_leader(&mut cluster, &joint);
        assert!(submit(&mut cluster.nodes[0], MembershipChange::LeaveJoint).is_empty());
        commit_leader(
            &mut cluster,
            &MembershipConfig::Stable(membership(&[1, 2, 3], &[])),
        );
    }
}

#[test]
fn conversion_leaves_protocol_refusals_to_the_durable_runtime() {
    let mut node = Replica::single(false);
    for (id, reason) in [
        (
            99,
            ConfigurationProposalRejection::VoterNotFound {
                node_id: NodeId(99),
            },
        ),
        (
            1,
            ConfigurationProposalRejection::CannotRemoveLastVoter { node_id: NodeId(1) },
        ),
    ] {
        assert_eq!(
            submit(
                &mut node,
                MembershipChange::RemoveNode {
                    node_id: NodeId(id)
                }
            ),
            vec![ProposalRejection::Configuration(reason)]
        );
    }
    let mut cluster = Cluster::new();
    let term = cluster.nodes[1].driver.ready_node().unwrap().current_term();
    assert_eq!(
        submit(
            &mut cluster.nodes[1],
            MembershipChange::RemoveNode { node_id: NodeId(3) }
        ),
        vec![ProposalRejection::NotLeader {
            role: Role::Follower,
            term,
            payload_len: 0
        }]
    );
    assert_eq!(
        cluster.nodes[1].driver.ready_node().unwrap().leader_hint(),
        Some(NodeId(1))
    );
}

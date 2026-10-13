//! Proposal admission keeps its own identity and never claims a quorum commit.
use super::support::*;
use rafter::{
    ConfigurationEntry, ConfigurationId, ConfigurationProposalRejection, MembershipSet, Output,
    PromotionBarrier, ProposalRejection,
};
use rafter_app::membership::{MembershipChange, NodeInfo};
use rafter_runtime::service::ServiceEvent;
use rafter_service::durable::{DurableClientStepError, DurableMembershipLimits};
pub(super) use rafter_service::durable::{DurableMembershipError, DurableMembershipReceipt};
use std::num::NonZeroUsize;

pub(super) type Proposal =
    rafter_service::DriverFuture<Result<DurableMembershipReceipt, DurableMembershipError>>;

pub(super) fn limits(bytes: usize) -> DurableMembershipLimits {
    DurableMembershipLimits::new(NonZeroUsize::new(bytes).unwrap())
}
pub(super) fn single(paused: bool, bytes: usize) -> Replica {
    Replica::attach_with_membership(
        crate::reads_support::Replica::single(4, paused),
        Link::default(),
        limits(bytes),
    )
}
pub(super) fn cluster(bytes: usize) -> Cluster {
    let base = crate::reads_support::Cluster::new(4);
    assert!(base.network.is_empty());
    let link = Link::default();
    let nodes = base
        .nodes
        .into_iter()
        .map(|node| Replica::attach_with_membership(node, link.clone(), limits(bytes)))
        .collect();
    Cluster { nodes, link }
}
pub(super) fn add(id: u64) -> MembershipChange {
    MembershipChange::AddLearner {
        node_id: NodeId(id),
        info: NodeInfo::default(),
    }
}
pub(super) fn propose(node: &Replica, change: MembershipChange) -> Proposal {
    node.driver.sender().propose_membership(&GROUP, change)
}
pub(super) fn finish(
    node: &mut Replica,
    request: &mut Proposal,
) -> Result<DurableMembershipReceipt, DurableMembershipError> {
    let mut result = None;
    node.until(|_| {
        if let Poll::Ready(value) = poll(request) {
            result = Some(value);
            true
        } else {
            false
        }
    });
    result.unwrap()
}

#[test]
fn receipt_waits_for_transport_effects_and_does_not_claim_commit() {
    let mut cluster = cluster(64);
    let before = cluster.nodes[0]
        .driver
        .ready_node()
        .unwrap()
        .last_log_index();
    let mut request = propose(&cluster.nodes[0], add(4));
    assert!(poll(&mut request).is_pending());
    cluster.nodes[0].pump();
    assert!(poll(&mut request).is_pending());
    assert_eq!(cluster.link.sends.load(Ordering::Acquire), 0);
    let receipt = finish(&mut cluster.nodes[0], &mut request).unwrap();
    assert!(cluster.link.sends.load(Ordering::Acquire) > 0);
    let core = cluster.nodes[0].driver.ready_node().unwrap();
    assert_eq!(receipt.index, LogIndex(before.0 + 1));
    assert_eq!(receipt.term, core.current_term());
    assert_eq!(receipt.index, core.last_log_index());
    assert!(core.commit_index() < receipt.index);
    assert!(core.effective_membership().contains_learner(NodeId(4)));
    assert!(!core.committed_membership().contains_replica(NodeId(4)));
}

#[test]
fn membership_refusals_preserve_exact_reason_and_leader_hint() {
    let mut node = single(false, 64);
    for (id, expected) in [
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
        let mut request = propose(
            &node,
            MembershipChange::RemoveNode {
                node_id: NodeId(id),
            },
        );
        let Err(DurableMembershipError::Rejected {
            reason,
            leader_hint,
        }) = finish(&mut node, &mut request)
        else {
            panic!("expected refusal")
        };
        assert_eq!(reason, ProposalRejection::Configuration(expected));
        assert_eq!(leader_hint, Some(NodeId(1)));
    }
    let mut cluster = cluster(64);
    let term = cluster.nodes[1].driver.ready_node().unwrap().current_term();
    let mut request = propose(&cluster.nodes[1], add(4));
    let Err(DurableMembershipError::Rejected {
        reason,
        leader_hint,
    }) = finish(&mut cluster.nodes[1], &mut request)
    else {
        panic!("expected follower refusal")
    };
    assert_eq!(
        reason,
        ProposalRejection::NotLeader {
            role: Role::Follower,
            term,
            payload_len: 0
        }
    );
    assert_eq!(leader_hint, Some(NodeId(1)));
    let mut first = propose(&cluster.nodes[0], add(4));
    let accepted = finish(&mut cluster.nodes[0], &mut first).unwrap();
    let mut second = propose(&cluster.nodes[0], add(5));
    let Err(DurableMembershipError::Rejected { reason, .. }) =
        finish(&mut cluster.nodes[0], &mut second)
    else {
        panic!("expected pending-configuration refusal")
    };
    assert_eq!(
        reason,
        ProposalRejection::Configuration(
            ConfigurationProposalRejection::UncommittedConfiguration {
                index: accepted.index
            }
        )
    );
}

#[test]
fn pre_attachment_refusal_is_returned_without_poisoning_the_new_proposal() {
    let mut base = crate::reads_support::Replica::single(4, false);
    base.owner
        .try_step(Input::RemoveVoter {
            voter_id: NodeId(99),
        })
        .unwrap();
    assert_eq!(base.owner.state(), ServiceState::Busy);
    let mut node = Replica::attach_with_membership(base, Link::default(), limits(64));
    let mut request = propose(&node, add(2));
    assert!(matches!(
        node.driver.poll_next().unwrap(),
        ServicePoll::Event(ServiceEvent::Output(Output::RejectProposal {
            proposal_id: None,
            ..
        }))
    ));
    assert!(poll(&mut request).is_pending());
    let receipt = finish(&mut node, &mut request).unwrap();
    let core = node.driver.ready_node().unwrap();
    assert_eq!(core.last_log_index(), receipt.index);
    assert!(core.committed_membership().contains_learner(NodeId(2)));
}

#[test]
fn raw_membership_is_guarded_only_after_explicit_managed_attachment() {
    let mut manual = Replica::single(false);
    assert!(matches!(
        poll(&mut manual.driver.sender().propose_membership(&GROUP, add(2))),
        Poll::Ready(Err(DurableMembershipError::NotEnabled))
    ));
    assert!(matches!(
        poll(
            &mut manual
                .driver
                .sender()
                .propose_membership(&(GROUP + 1), add(2))
        ),
        Poll::Ready(Err(DurableMembershipError::WrongGroup))
    ));
    manual
        .driver
        .try_step(Input::AddLearner {
            learner_id: NodeId(2),
        })
        .unwrap();
    manual.drain();
    assert!(manual
        .driver
        .ready_node()
        .unwrap()
        .committed_membership()
        .contains_learner(NodeId(2)));
    let mut managed = single(false, 64);
    let target = MembershipSet::new(vec![NodeId(1)], vec![NodeId(2)]).unwrap();
    let before = managed.driver.ready_node().unwrap().last_log_index();
    for input in [
        Input::AddLearner {
            learner_id: NodeId(2),
        },
        Input::RemoveVoter {
            voter_id: NodeId(1),
        },
        Input::PromoteLearner {
            learner_id: NodeId(2),
            promotion_barrier: PromotionBarrier {
                learner_id: NodeId(2),
                required_match_index: LogIndex::ZERO,
            },
        },
        Input::EnterJoint {
            target: target.clone(),
            promotion_barriers: vec![],
        },
        Input::LeaveJoint,
        Input::ChangeMembership {
            target: target.clone(),
            promotion_barriers: vec![],
        },
        Input::DangerousRawConfigurationProposal {
            configuration: ConfigurationEntry::stable(ConfigurationId(1), target),
            promotion_barriers: vec![],
        },
    ] {
        let Err(DurableClientStepError::UnmanagedMembership(returned)) =
            managed.driver.try_step(input.clone())
        else {
            panic!("raw membership bypassed correlation")
        };
        assert_eq!(*returned, input);
    }
    assert_eq!(
        managed.driver.ready_node().unwrap().last_log_index(),
        before
    );
}

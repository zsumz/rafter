//! Membership refusals reach the caller without losing the step's other effects.
mod support;

use rafter::{
    ConfigurationProposalRejection, LogIndex, Message, NodeId, Output, ProposalRejection,
    RequestVoteResponse, Role, Term,
};
use rafter_app::membership::{MembershipChange, NodeInfo};
use rafter_service::{DriverServiceState, ManagedDriverError};
use support::scripted::{
    change_on_step, outputs_on_step, scripted_driver, ScriptedMembershipRuntime,
};
use support::transport::{
    cluster, driver_over_bootstrap, elect, elect_single_voter, settle, Nameable, Principal,
};

fn add_learner(id: u64) -> MembershipChange {
    MembershipChange::AddLearner {
        node_id: NodeId(id),
        info: NodeInfo::default(),
    }
}

fn assert_rejection(
    error: ManagedDriverError,
    expected: &ProposalRejection,
    expected_hint: Option<NodeId>,
) {
    let ManagedDriverError::MembershipRejected {
        reason,
        leader_hint,
    } = error
    else {
        panic!("expected the protocol refusal, got {error:?}");
    };
    assert_eq!(&reason, expected);
    assert_eq!(leader_hint, expected_hint);
}

#[test]
fn invalid_removals_return_their_reason_and_leave_the_driver_usable() {
    let (driver, _) = driver_over_bootstrap(1, &[], &[2]);
    elect_single_voter(&driver);
    let watch = driver.handle().metrics().unwrap();
    let before = watch.current();
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
        let error = driver
            .change_membership(MembershipChange::RemoveNode {
                node_id: NodeId(id),
            })
            .expect_err("the kernel refused this removal");
        assert_rejection(
            error,
            &ProposalRejection::Configuration(reason),
            Some(NodeId(1)),
        );
        assert_eq!(watch.current(), before);
        assert_eq!(driver.service_state(), DriverServiceState::Serving);
    }
    driver.change_membership(add_learner(2)).unwrap();
    let after = watch.current();
    assert!(after.membership.contains_learner(NodeId(2)));
    assert!(after.commit_index > before.commit_index);
    assert_eq!(after.commit_index, after.last_log_index);
}

#[test]
fn follower_membership_refusal_preserves_the_leader_hint() {
    let nodes = cluster(&[1, 2, 3]);
    elect(&nodes, NodeId(1));
    settle(&nodes);
    let follower = &nodes[&NodeId(2)].0;
    let before = follower.handle().metrics().unwrap().current();
    assert_eq!(before.role, Role::Follower);
    assert_eq!(before.leader_hint, Some(NodeId(1)));
    let error = follower
        .change_membership(add_learner(4))
        .expect_err("a follower cannot admit membership changes");
    assert_rejection(
        error,
        &ProposalRejection::NotLeader {
            role: Role::Follower,
            term: before.term,
            payload_len: 0,
        },
        Some(NodeId(1)),
    );
    assert_eq!(follower.handle().metrics().unwrap().current(), before);
    assert_eq!(follower.service_state(), DriverServiceState::Serving);
    nodes[&NodeId(1)]
        .0
        .change_membership(add_learner(4))
        .unwrap();
    settle(&nodes);
    assert!(follower
        .with_group(|group| group
            .runtime()
            .committed_membership()
            .contains_learner(NodeId(4)))
        .unwrap());
}

#[test]
fn a_pending_configuration_refuses_the_next_change_until_commit() {
    let nodes = cluster(&[1, 2, 3]);
    elect(&nodes, NodeId(1));
    settle(&nodes);
    let leader = &nodes[&NodeId(1)].0;
    leader.change_membership(add_learner(4)).unwrap();
    // No messages have been delivered: admission is not a commit receipt.
    let pending = leader.handle().metrics().unwrap().current();
    assert!(pending.last_log_index > pending.commit_index);
    let error = leader
        .change_membership(add_learner(5))
        .expect_err("the earlier configuration has not committed");
    assert_rejection(
        error,
        &ProposalRejection::Configuration(
            ConfigurationProposalRejection::UncommittedConfiguration {
                index: pending.last_log_index,
            },
        ),
        Some(NodeId(1)),
    );
    let refused = leader.handle().metrics().unwrap().current();
    assert_eq!(refused.last_log_index, pending.last_log_index);
    assert!(!refused.membership.contains_replica(NodeId(5)));
    settle(&nodes);
    leader.change_membership(add_learner(5)).unwrap();
    settle(&nodes);
    assert!(leader
        .with_group(|group| group
            .runtime()
            .committed_membership()
            .contains_learner(NodeId(5)))
        .unwrap());
    assert_eq!(leader.service_state(), DriverServiceState::Serving);
}

#[test]
fn rejection_still_routes_peer_output_membership_and_metrics() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2], &[1, 2]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver(runtime, Nameable::all());
    let before = driver.control_plane_checkpoint_epoch();
    // The runtime reports a separate committed transition beside this refusal.
    change_on_step(&handle, &[1, 2, 3], &[1, 2, 3]);
    let reason = ProposalRejection::Configuration(ConfigurationProposalRejection::VoterNotFound {
        node_id: NodeId(99),
    });
    let message = Message::RequestVoteResponse(RequestVoteResponse {
        term: Term(1),
        voter_id: NodeId(1),
        vote_granted: false,
    });
    outputs_on_step(
        &handle,
        vec![
            Output::RejectProposal {
                proposal_id: None,
                reason: reason.clone(),
            },
            Output::Send {
                to: NodeId(2),
                message: message.clone(),
            },
        ],
    );
    let error = driver
        .change_membership(MembershipChange::RemoveNode {
            node_id: NodeId(99),
        })
        .expect_err("the refusal survives routing");
    assert_rejection(error, &reason, Some(NodeId(1)));
    let sent = transport.take_deliverable();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].to, NodeId(2));
    assert_eq!(sent[0].message, message);
    assert_eq!(
        transport.peer_sets().last().unwrap(),
        &vec![
            Principal::for_node(NodeId(2)),
            Principal::for_node(NodeId(3))
        ]
    );
    assert!(driver.control_plane_checkpoint_epoch() > before);
    let metrics = driver.handle().metrics().unwrap().current();
    assert_eq!(metrics.commit_index, LogIndex(6));
    assert!(metrics.membership.contains_replica(NodeId(3)));
    assert_eq!(driver.service_state(), DriverServiceState::Serving);
}

//! Catch-up, handoff, proposal fencing, and duplicate transfer scenarios.

use super::support::*;
use super::*;

#[test]
fn transfer_to_caught_up_target_sends_timeout_now_immediately() {
    let mut leader = leader_with_acknowledged_follower();

    let outputs = leader.step(Input::TransferLeadership { target: NodeId(2) });

    let request = timeout_now_to(&outputs, NodeId(2)).expect("timeout-now goes to the target");
    assert_eq!(request.term, leader.current_term());
    assert_eq!(request.leader_id, NodeId(1));
}
#[test]
fn transfer_waits_for_lagging_target_to_catch_up() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    leader
        .persistent
        .log
        .push(LogEntry::application(Term(1), b"entry".to_vec()));

    let outputs = leader.step(Input::TransferLeadership { target: NodeId(2) });
    assert!(
        timeout_now_to(&outputs, NodeId(2)).is_none(),
        "a lagging target must first catch up"
    );

    // The acknowledgement that completes catch-up triggers the handoff.
    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(2),
        }),
    });
    assert!(timeout_now_to(&outputs, NodeId(2)).is_some());
}
#[test]
fn joint_membership_transfer_to_new_voter_waits_for_catchup_then_hands_off() {
    let old_membership =
        crate::MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("valid old membership");
    let new_membership =
        crate::MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)], Vec::new())
            .expect("valid new membership");
    let joint_configuration = ConfigurationEntry::joint(
        ConfigurationId(20),
        crate::JointMembership::new(old_membership, new_membership),
    );
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3), NodeId(4)], 3).expect("valid config"),
        BootstrapState {
            current_term: Term(2),
            voted_for: None,
            commit_index: LogIndex(1),
            committed_configuration: Some(crate::CommittedConfiguration {
                index: LogIndex(1),
                config_id: ConfigurationId(20),
            }),
            snapshot: None,
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(1),
                Term(1),
                joint_configuration,
            )],
        },
    )
    .expect("joint membership bootstraps");
    leader.become_leader();

    let outputs = leader.step(Input::TransferLeadership { target: NodeId(4) });
    assert!(
        outputs
            .iter()
            .all(|output| !matches!(output, Output::LeadershipTransferRejected { .. })),
        "the new-side voter is an eligible transfer target in joint membership"
    );
    assert!(
        timeout_now_to(&outputs, NodeId(4)).is_none(),
        "the target must catch up before receiving TimeoutNow"
    );
    assert!(
        outputs.iter().any(|output| matches!(
            output,
            Output::Send {
                to: NodeId(4),
                message: Message::AppendEntries(_),
            }
        )),
        "starting the transfer should push the committed joint entry to the target"
    );

    let outputs = leader.step(Input::Message {
        from: NodeId(4),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(4),
            success: true,
            match_index: LogIndex(2),
        }),
    });
    assert!(timeout_now_to(&outputs, NodeId(4)).is_some());
}
#[test]
fn proposals_are_rejected_during_transfer_and_resume_after_expiry() {
    let mut leader = leader_with_acknowledged_follower();
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });

    let outputs = leader.step(Input::ClientProposal {
        payload: b"blocked".to_vec(),
    });
    assert!(matches!(
        outputs.as_slice(),
        [Output::RejectProposal {
            proposal_id: None,
            reason: ProposalRejection::LeadershipTransferInProgress { target: NodeId(2) },
        }]
    ));
    let tracked_outputs = leader.step(Input::TrackedClientProposal {
        proposal_id: LocalProposalId(9),
        payload: b"blocked-tracked".to_vec(),
    });
    assert!(matches!(
        tracked_outputs.as_slice(),
        [Output::RejectProposal {
            proposal_id: Some(LocalProposalId(9)),
            reason: ProposalRejection::LeadershipTransferInProgress { target: NodeId(2) },
        }]
    ));

    // The transfer expires after one election timeout of leader ticks.
    for _ in 0..leader.config.election_timeout_ticks() {
        let _ = leader.step(Input::Tick);
    }
    let outputs = leader.step(Input::ClientProposal {
        payload: b"accepted".to_vec(),
    });
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, Output::RejectProposal { .. })),
        "proposals resume once the transfer expires"
    );
}
#[test]
fn duplicate_transfer_requests_are_rejected_while_pending() {
    let mut leader = leader_with_acknowledged_follower();
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });

    let outputs = leader.step(Input::TransferLeadership { target: NodeId(3) });
    assert!(matches!(
        outputs.as_slice(),
        [Output::LeadershipTransferRejected {
            target: NodeId(3),
            reason: LeadershipTransferRejection::TransferAlreadyInProgress,
        }]
    ));
}

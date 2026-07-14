use super::super::client::check_client_history_read_write_invariants;
use super::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq, oracle_expect_err};

#[test]
fn client_history_detects_completed_read_before_local_apply_floor() {
    let cluster = one_node_cluster();
    let mut state = ExplorationState::new(cluster);
    state.client_history_mut().reads.insert(
        10,
        ClientRead {
            operation_id: 10,
            node_id: NodeId(1),
            request_id: 10,
            committed_floor: LogIndex(5),
            started_at: 0,
            outcome: ClientReadOutcome::Completed {
                proof: ClientReadProof {
                    application_epoch: 0,
                    read_index: LogIndex(5),
                    local_applied_index: LogIndex(4),
                },
                result: None,
                completed_at: 1,
            },
        },
    );

    let failure = oracle_expect_err!(
        check_client_history_read_write_invariants(&state, &[]),
        "a completed read below its local apply floor must fail",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::RD_04_APPLY_BEFORE_SERVING_A_READ
    );
    oracle_assert!(
        failure
            .message
            .contains("local applied 4 below required index 5"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn client_history_allows_unknown_write_outcomes() {
    let cluster = one_node_cluster();
    let mut state = ExplorationState::new(cluster);
    state.client_history_mut().writes.insert(
        crate::model_check::ProposalId(7),
        ClientWrite {
            proposal_id: crate::model_check::ProposalId(7),
            node_id: NodeId(1),
            payload: b"unknown".to_vec().into(),
            started_at: 0,
            status: ClientWriteStatus::Unknown {
                reason: ClientWriteUnknownReason::StaleLeader,
            },
        },
    );

    check_client_history_read_write_invariants(&state, &[])
        .expect("unknown write outcomes should not imply confirmed absence");
}

#[test]
fn client_history_correlates_repeated_payloads_by_tracked_proposal_id() {
    use crate::records::LocalProposalEvent;

    let mut state = state_with_uncommitted_applications(Term(2), &[b"same-value", b"same-value"]);
    for proposal_id in [
        crate::model_check::ProposalId(1),
        crate::model_check::ProposalId(2),
    ] {
        state.record_client_proposal(NodeId(1), proposal_id, false);
        state
            .client_history_mut()
            .writes
            .get_mut(&proposal_id)
            .unwrap()
            .payload = b"same-value".to_vec().into();
    }

    let seeded_log = state.cluster().bootstrap_state(NodeId(1)).log;
    assert_eq!(seeded_log.len(), 2);
    assert_eq!(
        seeded_log[0].kind.application_payload(),
        Some(b"same-value".as_slice())
    );
    assert_eq!(
        seeded_log[1].kind.application_payload(),
        Some(b"same-value".as_slice())
    );

    state.record_local_proposal_events(&[
        LocalProposalEvent::Appended {
            node_id: NodeId(1),
            proposal_id: rafter::LocalProposalId(1),
            index: LogIndex(1),
            term: Term(2),
        },
        LocalProposalEvent::Applied {
            node_id: NodeId(1),
            proposal_id: rafter::LocalProposalId(1),
            index: LogIndex(1),
            term: Term(2),
            payload: b"same-value".to_vec().into(),
        },
    ]);
    state.record_local_proposal_events(&[
        LocalProposalEvent::Appended {
            node_id: NodeId(1),
            proposal_id: rafter::LocalProposalId(2),
            index: LogIndex(2),
            term: Term(2),
        },
        LocalProposalEvent::Applied {
            node_id: NodeId(1),
            proposal_id: rafter::LocalProposalId(2),
            index: LogIndex(2),
            term: Term(2),
            payload: b"same-value".to_vec().into(),
        },
    ]);

    let first = state.client_history().writes[&crate::model_check::ProposalId(1)].status;
    let second = state.client_history().writes[&crate::model_check::ProposalId(2)].status;
    assert!(
        matches!(
            first,
            ClientWriteStatus::Completed {
                index: LogIndex(1),
                ..
            }
        ),
        "first repeated-payload status: {first:?}"
    );
    assert!(
        matches!(
            second,
            ClientWriteStatus::Completed {
                index: LogIndex(2),
                ..
            }
        ),
        "second repeated-payload status: {second:?}"
    );
    check_client_history_read_write_invariants(&state, &[])
        .expect("repeated payloads at distinct tracked entries are legal");
}

#[test]
fn client_history_does_not_infer_a_terminal_outcome_without_an_output() {
    use crate::records::LocalProposalEvent;

    let mut state = state_with_uncommitted_applications(Term(3), &[b"pending-value"]);
    let proposal_id = crate::model_check::ProposalId(9);
    state.record_client_proposal(NodeId(1), proposal_id, false);
    state
        .client_history_mut()
        .writes
        .get_mut(&proposal_id)
        .unwrap()
        .payload = b"pending-value".to_vec().into();
    state.record_local_proposal_events(&[LocalProposalEvent::Appended {
        node_id: NodeId(1),
        proposal_id: rafter::LocalProposalId(9),
        index: LogIndex(1),
        term: Term(3),
    }]);

    state.refresh_client_history();

    assert!(matches!(
        state.client_history().writes[&proposal_id].status,
        ClientWriteStatus::Accepted {
            node_id: NodeId(1),
            index: LogIndex(1),
            term: Term(3),
        }
    ));
}

#[test]
fn local_proposal_drop_is_an_unknown_outcome() {
    use crate::records::LocalProposalEvent;

    let mut state = state_with_uncommitted_applications(Term(4), &[b"drop-value"]);
    let proposal_id = crate::model_check::ProposalId(11);
    state.record_client_proposal(NodeId(1), proposal_id, false);
    state
        .client_history_mut()
        .writes
        .get_mut(&proposal_id)
        .unwrap()
        .payload = b"drop-value".to_vec().into();
    state.record_local_proposal_events(&[
        LocalProposalEvent::Appended {
            node_id: NodeId(1),
            proposal_id: rafter::LocalProposalId(11),
            index: LogIndex(1),
            term: Term(4),
        },
        LocalProposalEvent::Dropped {
            node_id: NodeId(1),
            proposal_id: rafter::LocalProposalId(11),
            index: LogIndex(1),
            term: Term(4),
            reason: rafter::LocalProposalDropReason::LeadershipLost,
        },
    ]);

    assert!(matches!(
        state.client_history().writes[&proposal_id].status,
        ClientWriteStatus::Unknown {
            reason: ClientWriteUnknownReason::LocalTrackingDropped,
        }
    ));
}

#[test]
fn contradictory_tracked_proposal_events_fail_closed_and_persist() {
    use crate::records::LocalProposalEvent;

    let proposal_id = crate::model_check::ProposalId(13);
    let payload = crate::model_check::helpers::proposal_payload(proposal_id);
    let mut state = state_with_uncommitted_applications(Term(5), &[&payload, &payload]);
    state.record_client_proposal(NodeId(1), proposal_id, false);
    state.record_local_proposal_events(&[
        LocalProposalEvent::Appended {
            node_id: NodeId(1),
            proposal_id: rafter::LocalProposalId(proposal_id.0),
            index: LogIndex(1),
            term: Term(5),
        },
        LocalProposalEvent::Appended {
            node_id: NodeId(1),
            proposal_id: rafter::LocalProposalId(proposal_id.0),
            index: LogIndex(2),
            term: Term(5),
        },
    ]);
    let cloned = state.clone();

    for recorded in [&state, &cloned] {
        assert!(matches!(
            recorded.client_history().writes[&proposal_id].status,
            ClientWriteStatus::Accepted {
                index: LogIndex(1),
                ..
            }
        ));
        let failure = check_client_history_linearizability(recorded, &[])
            .expect_err("contradictory tracked events must fail closed");
        assert_eq!(
            failure.kind(),
            crate::model_check::FailureKind::HarnessError
        );
        assert_eq!(
            failure.invariant(),
            catalog::RD_06_CLIENT_HISTORY_LINEARIZABILITY
        );
        assert!(
            failure.message.contains("instrumentation failed")
                && failure.message.contains("existing_matches=false"),
            "unexpected failure message: {}",
            failure.message
        );
    }
}

#[test]
fn stale_unknown_write_may_later_apply_without_becoming_required() {
    use crate::records::LocalProposalEvent;

    let proposal_id = crate::model_check::ProposalId(14);
    let payload = crate::model_check::helpers::proposal_payload(proposal_id);
    let mut state = state_with_uncommitted_applications(Term(6), &[&payload]);
    state.record_client_proposal(NodeId(1), proposal_id, true);
    state.record_local_proposal_events(&[
        LocalProposalEvent::Appended {
            node_id: NodeId(1),
            proposal_id: rafter::LocalProposalId(proposal_id.0),
            index: LogIndex(1),
            term: Term(6),
        },
        LocalProposalEvent::Applied {
            node_id: NodeId(1),
            proposal_id: rafter::LocalProposalId(proposal_id.0),
            index: LogIndex(1),
            term: Term(6),
            payload: payload.into(),
        },
    ]);

    assert!(matches!(
        state.client_history().writes[&proposal_id].status,
        ClientWriteStatus::Unknown {
            reason: ClientWriteUnknownReason::StaleLeader
        }
    ));
    check_client_history_linearizability(&state, &[])
        .expect("a stale unknown write that later applies remains optional");
}

fn state_with_uncommitted_applications(term: Term, payloads: &[&[u8]]) -> ExplorationState {
    let config =
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3).expect("test config is valid");
    let mut cluster = Cluster::new(vec![config.clone()]);
    let log = payloads
        .iter()
        .enumerate()
        .map(|(offset, payload)| {
            rafter::BootstrapLogEntry::application(
                LogIndex(u64::try_from(offset).unwrap() + 1),
                term,
                payload.to_vec(),
            )
        })
        .collect();
    let node = rafter::Node::from_bootstrap_applied_through(
        config,
        rafter::BootstrapState {
            current_term: term,
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log,
        },
        LogIndex::ZERO,
    )
    .expect("uncommitted application bootstrap is valid");
    cluster.nodes.insert(NodeId(1), node);
    ExplorationState::new(cluster)
}

use super::*;

#[test]
fn client_history_detects_completed_read_before_local_apply_floor() {
    let cluster = one_node_cluster();
    let mut state = ExplorationState::new(cluster);
    state.client_history.reads.insert(
        10,
        ClientRead {
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

    let failure = check_client_history_read_write_invariants(&state, &[])
        .expect_err("a completed read below its local apply floor must fail");
    assert_eq!(
        failure.invariant(),
        catalog::RD_04_APPLY_BEFORE_SERVING_A_READ
    );
    assert!(
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
    state.client_history.writes.insert(
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

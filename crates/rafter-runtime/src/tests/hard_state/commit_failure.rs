use super::super::*;
use super::fixtures::committed_append_entries_input;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

#[test]
fn final_hard_state_write_failure_suppresses_apply_and_success_response() {
    let mut control = durable_node_with_log(
        2,
        &[1, 3],
        hard_state_store(2, None),
        InMemoryRaftLogSegment::new(),
    );
    assert_eq!(
        control
            .step(committed_append_entries_input())
            .expect("commit hard state writes"),
        vec![
            RaftOutput::Apply {
                index: LogIndex(1),
                term: Term(2),
                payload: b"committed".to_vec().into(),
                local_proposal_id: None,
            },
            RaftOutput::Send {
                to: RaftNodeId(1),
                message: Message::AppendEntriesResponse(AppendEntriesResponse {
                    sequence: 0,
                    term: Term(2),
                    follower_id: RaftNodeId(2),
                    success: true,
                    match_index: LogIndex(1),
                }),
            },
        ]
    );

    let mut runtime = DurableRaftNode::with_storage(
        raft_config(2, &[1, 3]),
        FailingHardStateStore {
            current: RaftHardState {
                current_term: Term(2),
                voted_for: None,
                commit_index: LogIndex::ZERO,
                committed_configuration: None,
            },
        },
        InMemoryRaftLogSegment::new(),
    )
    .expect("runtime hydrates");

    let error = runtime
        .step(committed_append_entries_input())
        .expect_err("final commit hard-state write fails");
    oracle_assert!(matches!(error, RaftRuntimeError::HardStateWrite(_)));

    oracle_assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![PersistedRaftLogEntry::application(
            LogIndex(1),
            Term(2),
            b"committed".to_vec(),
        )]
    );
    oracle_assert_eq!(
        runtime.hard_state_store.current().commit_index,
        LogIndex::ZERO
    );

    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        runtime.hard_state_store.clone(),
        runtime.log_segment.clone(),
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("restart hydrates from the durable state");
    let (restarted, recovery_outputs) = recovered.into_parts();
    oracle_assert!(recovery_outputs.is_empty());
    oracle_assert_eq!(restarted.commit_index(), LogIndex::ZERO);
    oracle_assert_eq!(restarted.last_log_index(), LogIndex(1));

    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(cause, RaftRuntimeFatalError::HardStateWrite(_))
    });
}

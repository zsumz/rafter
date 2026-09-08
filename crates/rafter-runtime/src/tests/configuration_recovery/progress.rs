//! Recovery keeps catch-up live and allocates IDs after the recovered identity.

use super::*;

pub(super) fn finish_recovery<
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
>(
    runtime: &mut DurableRaftNode<H, L, S>,
    change: Change,
) {
    match change {
        Change::CatchUp => {
            let outputs = runtime
                .step(catch_up())
                .expect("leader confirms the configuration suffix");
            assert!(outputs.iter().any(|output| matches!(output,
                RaftOutput::Send { message: Message::AppendEntriesResponse(response), .. }
                    if response.success && response.match_index == LogIndex(3)
            )));
            assert_eq!(runtime.commit_index(), LogIndex(3));
            assert_eq!(
                runtime.committed_configuration_state(),
                Some(configuration_state(3))
            );
            assert_eq!(
                runtime.hard_state_store.current().committed_configuration,
                Some(configuration_state(3))
            );
            let outputs = runtime
                .step(RaftInput::Message {
                    from: RaftNodeId(1),
                    message: Message::AppendEntries(AppendEntries {
                        sequence: 2,
                        term: Term(3),
                        leader_id: RaftNodeId(1),
                        prev_log_index: LogIndex(3),
                        prev_log_term: Term(2),
                        entries: vec![LogEntry::application(Term(3), b"fresh-command".to_vec())]
                            .into(),
                        leader_commit: LogIndex(4),
                    }),
                })
                .expect("subsequent application catch-up succeeds");
            assert!(outputs.iter().any(|output| matches!(output,
                RaftOutput::Apply { index: LogIndex(4), payload, .. } if payload.as_ref() == b"fresh-command"
            )));
        }
        Change::Snapshot { matching } => {
            if runtime.snapshot_index() == LogIndex::ZERO {
                runtime
                    .step(snapshot_input(matching))
                    .expect("leader retries snapshot after term fence");
            }
            assert_snapshot(runtime, matching);
            assert_eq!(
                runtime.committed_configuration_state(),
                Some(configuration_state(2))
            );
            runtime
                .step(RaftInput::Tick)
                .expect("normalized identity is published");
            assert_eq!(
                runtime.hard_state_store.current().committed_configuration,
                Some(configuration_state(2))
            );
            elect_runtime_leader_with_grant(runtime, RaftNodeId(1));
            runtime
                .step(RaftInput::AddLearner {
                    learner_id: RaftNodeId(6),
                })
                .expect("next configuration proposal persists");
            assert_eq!(
                runtime.effective_configuration_entry().unwrap().config_id(),
                ConfigurationId(3)
            );
        }
    }
}

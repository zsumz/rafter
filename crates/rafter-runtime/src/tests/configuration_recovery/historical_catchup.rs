//! Prior-term configurations may reach a partial follower before commit proof.

use super::*;
use crate::tests::file_backed_fixture::TestDirectory;
use rafter_storage::FileRaftNodeStores;

fn input() -> RaftInput {
    let RaftInput::Message {
        from,
        message: Message::AppendEntries(mut append),
    } = catch_up()
    else {
        unreachable!()
    };
    append.term = Term(4);
    append.leader_commit = LogIndex(1);
    let mut entries = append.entries.to_vec();
    entries.push(LogEntry::noop(Term(4)));
    append.entries = entries.into();
    RaftInput::Message {
        from,
        message: Message::AppendEntries(append),
    }
}

#[test]
fn historical_configuration_catchup_reopens_at_every_file_boundary() {
    for batched in [false, true] {
        for after in [None, Some(1), Some(2), Some(3)] {
            let directory = TestDirectory::new("historical-configuration-cut");
            let (mut hard_state, mut log, snapshots) = FileRaftNodeStores::open(directory.path())
                .unwrap()
                .into_parts();
            initialize(&mut hard_state, &mut log, Change::CatchUp);
            let cut = Cut::new(after);
            let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
                raft_config(2, &[1, 3]),
                cut.wrap(hard_state),
                cut.wrap(log),
                cut.wrap(snapshots),
            )
            .unwrap();
            let result = if batched {
                runtime.step_batch(vec![input(), RaftInput::Tick])
            } else {
                runtime.step(input())
            };
            if after.is_some() {
                assert!(result.is_err());
                assert!(matches!(
                    runtime.step(RaftInput::Tick),
                    Err(RaftRuntimeError::Poisoned { .. })
                ));
            } else {
                let outputs = result.unwrap();
                assert_accepted(&outputs);
                assert_eq!(runtime.commit_index(), LogIndex(1));
            }
            assert_eq!(
                cut.mutations(),
                [Mutation::HardState, Mutation::Truncate, Mutation::Append][..after.unwrap_or(3)]
            );
            drop(runtime);
            let mut reopened = file_backed::open(directory.path());
            assert_eq!(reopened.current_term(), Term(4));
            assert_eq!(reopened.commit_index(), LogIndex(1));
            assert_eq!(
                reopened.committed_configuration_state(),
                Some(configuration_state(1))
            );
            assert!(reopened
                .drain_committed_outputs()
                .iter()
                .all(|output| !matches!(
                    output,
                    RaftOutput::Apply { .. }
                        | RaftOutput::ConfigurationCommitted {
                            index: LogIndex(2..),
                            ..
                        }
                )));
            assert_accepted(&reopened.step(input()).unwrap());
            assert_eq!(reopened.commit_index(), LogIndex(1));
            drop(reopened);
            let mut again = file_backed::open(directory.path());
            assert_eq!(again.commit_index(), LogIndex(1));
            assert_eq!(
                again.effective_membership(),
                MembershipConfig::Stable(membership(3))
            );
            again
                .step(RaftInput::Message {
                    from: RaftNodeId(1),
                    message: Message::AppendEntries(AppendEntries {
                        sequence: 2,
                        term: Term(4),
                        leader_id: RaftNodeId(1),
                        prev_log_index: LogIndex(4),
                        prev_log_term: Term(4),
                        entries: Vec::new().into(),
                        leader_commit: LogIndex(4),
                    }),
                })
                .unwrap();
            assert_eq!(again.commit_index(), LogIndex(4));
            assert_eq!(
                again.hard_state_store.current().committed_configuration,
                Some(configuration_state(3))
            );
        }
    }
}

fn assert_accepted(outputs: &[RaftOutput]) {
    assert!(outputs.iter().any(|output| matches!(output,
        RaftOutput::Send { message: Message::AppendEntriesResponse(response), .. }
            if response.success && response.match_index == LogIndex(4)
    )));
}

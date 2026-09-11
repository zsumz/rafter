//! Peer group commit amortizes persistence without changing ordered effects.
use super::*;
use rafter::{AppendEntries, AppendEntriesResponse, Message};

fn append(prev: u64, commit: u64) -> RaftInput {
    RaftInput::Message {
        from: RaftNodeId(1),
        message: Message::AppendEntries(AppendEntries {
            term: Term(2),
            leader_id: RaftNodeId(1),
            prev_log_index: LogIndex(prev),
            prev_log_term: if prev == 0 { Term(0) } else { Term(2) },
            sequence: prev + 1,
            entries: vec![LogEntry::application(Term(2), vec![1])].into(),
            leader_commit: LogIndex(commit),
        }),
    }
}

#[test]
fn follower_batch_flushes_once_and_keeps_every_response_and_apply() {
    let (segment, calls, _) = CountingLogSegment::new();
    let mut batched =
        DurableRaftNode::with_storage(raft_config(2, &[1, 3]), hard_state_store(2, None), segment)
            .unwrap();
    let mut single = durable_node_with_log(
        2,
        &[1, 3],
        hard_state_store(2, None),
        InMemoryRaftLogSegment::new(),
    );
    let inputs = vec![append(0, 1), append(1, 2), append(2, 3)];
    let expected: Vec<_> = inputs
        .iter()
        .cloned()
        .flat_map(|input| single.step(input).unwrap())
        .collect();
    let actual = batched.step_batch(inputs).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(calls.get(), 1);
    assert_eq!(batched.commit_index(), LogIndex(3));
    assert_eq!(batched.hard_state_store.current().commit_index, LogIndex(3));
    assert_eq!(
        batched.log_segment.replay_entries(),
        single.log_segment.replay_entries()
    );
}

#[test]
fn duplicate_out_of_order_responses_keep_sequence_effects_and_apply_once() {
    let mut batch = elected_leader_with_log_segment(InMemoryRaftLogSegment::new());
    batch.step_batch(proposals(4)).unwrap();
    let mut single = batch.clone();
    let inputs: Vec<_> = [(3, 1), (2, 1), (3, 2), (5, 3)]
        .into_iter()
        .map(|(index, sequence)| RaftInput::Message {
            from: RaftNodeId(3),
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: batch.current_term(),
                follower_id: RaftNodeId(3),
                success: true,
                match_index: LogIndex(index),
                sequence,
            }),
        })
        .collect();
    let expected: Vec<_> = inputs
        .iter()
        .cloned()
        .flat_map(|input| single.step(input).unwrap())
        .collect();
    assert_eq!(batch.step_batch(inputs).unwrap(), expected);
    assert_eq!(
        batch.hard_state_store.current(),
        single.hard_state_store.current()
    );
    assert_eq!(batch.applied_index(), single.applied_index());
    assert!(batch.drain_committed_outputs().is_empty());
}

#[test]
fn failed_batch_commit_releases_no_outputs_and_restart_uses_durable_floor() {
    let mut batch = DurableRaftNode::with_storage(
        raft_config(2, &[1, 3]),
        FailingHardStateStore {
            current: RaftHardState {
                current_term: Term(2),
                ..RaftHardState::default()
            },
        },
        InMemoryRaftLogSegment::new(),
    )
    .unwrap();
    assert!(matches!(
        batch.step_batch(vec![append(0, 1), append(1, 2)]),
        Err(RaftRuntimeError::HardStateWrite(_))
    ));
    assert_eq!(batch.last_log_index(), LogIndex(2));
    assert_eq!(
        batch.hard_state_store.current().commit_index,
        LogIndex::ZERO
    );
    assert!(matches!(
        batch.step(RaftInput::Tick),
        Err(RaftRuntimeError::Poisoned { .. })
    ));
    let storage = batch.into_storage();
    let (restart, outputs) = DurableRaftNode::recover_with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        storage.hard_state_store,
        storage.log_segment,
        storage.snapshot_store,
    )
    .unwrap()
    .into_parts();
    assert_eq!(restart.commit_index(), LogIndex::ZERO);
    assert_eq!(restart.last_log_index(), LogIndex(2));
    assert!(outputs.is_empty());
}

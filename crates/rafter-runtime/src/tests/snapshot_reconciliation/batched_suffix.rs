//! Batched appends around an installation still publish commit metadata last.

use super::*;

#[test]
fn snapshot_reconciliation_with_batched_append_survives_every_crash_cut() {
    for snapshot_first in [false, true] {
        let (mutations, _) = install_and_append(snapshot_first, None);
        assert!(mutations.contains(&Mutation::Append));
        for after in 1..=mutations.len() {
            let (observed, image) = install_and_append(snapshot_first, Some(after));
            assert_eq!(observed, mutations[..after]);
            if after < mutations.len() {
                assert_eq!(
                    image.hard_state_store.current().commit_index,
                    LogIndex::ZERO
                );
            }
            let mut reopened = DurableRaftNode::with_storage_and_snapshot_store(
                raft_config(2, &[1, 3]),
                image.hard_state_store,
                image.log_segment,
                image.snapshot_store,
            )
            .expect("snapshot plus append crash cut reopens");
            let replay = reopened.drain_committed_outputs();
            assert!(!replay.iter().any(|output| matches!(output,
                RaftOutput::Apply { payload, .. } if payload.as_ref() == b"stale-tail"
            )));
            if after == 1 {
                assert_eq!(reopened.snapshot_index(), LogIndex::ZERO);
                continue;
            }
            assert_eq!(reopened.snapshot_index(), LogIndex(2));
            assert_eq!(reopened.term_at_index(LogIndex(2)), Some(Term(2)));
            let appended = observed.contains(&Mutation::Append);
            assert_eq!(
                reopened.last_log_index(),
                LogIndex(if appended { 3 } else { 2 })
            );
            assert_eq!(
                reopened.log_entries_from(LogIndex(3)),
                if appended {
                    vec![LogEntry::application(Term(3), b"fresh-tail".to_vec())]
                } else {
                    Vec::new()
                }
            );
            let committed = snapshot_first && after == mutations.len();
            assert_eq!(
                reopened.commit_index(),
                LogIndex(if committed { 3 } else { 2 })
            );
            assert_eq!(
                replay.iter().any(|output| matches!(output,
                    RaftOutput::Apply { payload, .. } if payload.as_ref() == b"fresh-tail"
                )),
                committed
            );
        }
    }
}

fn install_and_append(snapshot_first: bool, after: Option<usize>) -> (Vec<Mutation>, MemoryImage) {
    let mut hard_state = InMemoryRaftHardStateStore::new();
    let mut log = InMemoryRaftLogSegment::new();
    initialize(&mut hard_state, &mut log);
    let cut = Cut::new(after);
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        cut.wrap(hard_state),
        cut.wrap(log),
        cut.wrap(InMemoryRaftSnapshotStore::new()),
    )
    .expect("initial follower opens");
    let append = append_input(snapshot_first);
    let snapshot = install_input(false);
    let inputs = if snapshot_first {
        vec![snapshot, append]
    } else {
        vec![append, snapshot]
    };
    let result = runtime.step_batch(inputs);
    if after.is_some() {
        assert!(result.is_err(), "batch reaches the armed crash cut");
    } else {
        result.expect("snapshot plus append succeeds");
        assert_eq!(runtime.last_log_index(), LogIndex(3));
    }
    let image = runtime.into_storage();
    (
        cut.mutations(),
        DurableRaftNodeStorage {
            hard_state_store: image.hard_state_store.inner,
            log_segment: image.log_segment.inner,
            snapshot_store: image.snapshot_store.inner,
        },
    )
}

fn append_input(snapshot_first: bool) -> RaftInput {
    let mut entries = Vec::new();
    if !snapshot_first {
        entries.push(LogEntry::application(Term(2), b"fresh-boundary".to_vec()));
    }
    entries.push(LogEntry::application(Term(3), b"fresh-tail".to_vec()));
    RaftInput::Message {
        from: RaftNodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(3),
            leader_id: RaftNodeId(1),
            prev_log_index: LogIndex(if snapshot_first { 2 } else { 1 }),
            prev_log_term: Term(if snapshot_first { 2 } else { 1 }),
            entries: entries.into(),
            leader_commit: LogIndex(if snapshot_first { 3 } else { 0 }),
        }),
    }
}

#[test]
fn snapshot_reconciliation_rejects_committed_boundary_conflict_before_writes() {
    let (_, mut image) = memory_install(false, false, Some(2));
    image
        .hard_state_store
        .write_hard_state(RaftHardState {
            commit_index: LogIndex(2),
            ..image.hard_state_store.current()
        })
        .expect("committed conflict fixture persists");
    let cut = Cut::new(None);
    let result = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        cut.wrap(image.hard_state_store),
        cut.wrap(image.log_segment),
        cut.wrap(image.snapshot_store),
    );
    assert!(matches!(
        result,
        Err(RaftRuntimeError::LogPrefixDiverged { index: LogIndex(2) })
    ));
    assert!(cut.mutations().is_empty());
}

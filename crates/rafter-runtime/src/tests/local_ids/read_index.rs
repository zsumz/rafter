use super::*;

#[test]
fn typed_read_id_is_preserved_by_runtime_read_index_outputs() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader elected")
        .is_empty());

    let read_id = ReadId(22);
    let outputs = runtime.step_read_index(read_id).expect("read index grants");

    assert_eq!(
        outputs,
        vec![RaftOutput::ReadIndexGranted {
            read_id,
            read_index: LogIndex(1),
        }]
    );
}

#[test]
fn pending_read_cancellations_are_released_after_stepdown_term_is_durable() {
    let mut runtime = durable_node_with_log(
        1,
        &[2, 3],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    elect_runtime_leader_with_grant(&mut runtime, RaftNodeId(2));
    runtime
        .step(RaftInput::Message {
            from: RaftNodeId(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: runtime.current_term(),
                follower_id: RaftNodeId(2),
                success: true,
                match_index: LogIndex(1),
                sequence: 0,
            }),
        })
        .expect("leader noop commits");

    let first_read = ReadId(32);
    let second_read = ReadId(33);
    let _ = runtime
        .step_read_index(first_read)
        .expect("first read waits for quorum");
    let _ = runtime
        .step_read_index(second_read)
        .expect("second read waits for quorum");

    let higher_term = runtime.current_term().next();
    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(3),
            message: Message::AppendEntries(AppendEntries {
                term: higher_term,
                leader_id: RaftNodeId(3),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: Vec::new().into(),
                leader_commit: LogIndex::ZERO,
                sequence: 10,
            }),
        })
        .expect("higher-term append persists read cancellation");

    assert_eq!(runtime.hard_state().current_term, higher_term);
    assert_eq!(
        outputs
            .iter()
            .filter_map(|output| match output {
                RaftOutput::ReadIndexCanceled { read_id, reason } => Some((*read_id, *reason)),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![
            (first_read, rafter::ReadIndexCancelReason::LeadershipLost),
            (second_read, rafter::ReadIndexCancelReason::LeadershipLost),
        ]
    );
}

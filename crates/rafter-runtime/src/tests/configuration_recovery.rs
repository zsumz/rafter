//! Configuration identity and progress across interrupted publication.

use super::mutation_cuts::{Cut, Mutation};
use super::*;

mod file_backed;
mod historical_catchup;
mod memory;
mod progress;

#[derive(Clone, Copy, Debug)]
enum Change {
    CatchUp,
    Snapshot { matching: bool },
}

const CHANGES: [Change; 3] = [
    Change::CatchUp,
    Change::Snapshot { matching: false },
    Change::Snapshot { matching: true },
];

fn configuration_state(id: u64) -> CommittedConfiguration {
    CommittedConfiguration {
        index: LogIndex(id),
        config_id: ConfigurationId(id),
    }
}

fn membership(id: u64) -> MembershipSet {
    MembershipSet::new(
        vec![RaftNodeId(1), RaftNodeId(2), RaftNodeId(3)],
        (4..(id + 3)).map(RaftNodeId).collect(),
    )
    .unwrap()
}

fn configuration(id: u64) -> ConfigurationEntry {
    ConfigurationEntry::stable(ConfigurationId(id), membership(id))
}

fn initialize<H: RaftHardStateStore, L: RaftLogSegment>(
    hard_state: &mut H,
    log: &mut L,
    change: Change,
) {
    hard_state
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(1),
            committed_configuration: Some(configuration_state(1)),
        })
        .unwrap();
    let matching = matches!(change, Change::Snapshot { matching: true });
    let boundary = if matching {
        PersistedRaftLogEntry::configuration(LogIndex(2), Term(1), configuration(2))
    } else {
        PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"stale-boundary".to_vec())
    };
    log.append_entries(&[
        PersistedRaftLogEntry::configuration(LogIndex(1), Term(1), configuration(1)),
        boundary,
        PersistedRaftLogEntry::application(
            LogIndex(3),
            Term(1),
            if matching {
                b"valid-tail".to_vec()
            } else {
                b"stale-tail".to_vec()
            },
        ),
    ])
    .unwrap();
}

fn catch_up() -> RaftInput {
    RaftInput::Message {
        from: RaftNodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 1,
            term: Term(3),
            leader_id: RaftNodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(1),
            entries: vec![
                LogEntry::configuration(Term(2), configuration(2)),
                LogEntry::configuration(Term(2), configuration(3)),
            ]
            .into(),
            leader_commit: LogIndex(3),
        }),
    }
}

fn snapshot_input(matching: bool) -> RaftInput {
    let mut snapshot = super::snapshot::raft_snapshot(
        2,
        if matching { 1 } else { 2 },
        3,
        b"application state at C2",
    );
    snapshot.metadata.committed_configuration = Some(rafter::SnapshotCommittedConfiguration::new(
        Some(configuration_state(2)),
        MembershipConfig::Stable(membership(2)),
    ));
    let descriptor =
        RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload);
    RaftInput::Message {
        from: RaftNodeId(1),
        message: Message::InstallSnapshotChunk(rafter::InstallSnapshotChunk {
            term: Term(3),
            leader_id: RaftNodeId(1),
            transfer_id: descriptor.transfer_id(),
            metadata: snapshot.metadata,
            total_payload_len: descriptor.application_payload_len,
            application_payload_crc32: descriptor.application_payload_crc32,
            offset: 0,
            chunk: snapshot.application_payload,
            done: true,
        }),
    }
}

fn drive<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore + SnapshotChunkSource>(
    runtime: &mut DurableRaftNode<H, L, S>,
    change: Change,
    batched: bool,
    failing: bool,
) {
    let input = match change {
        Change::CatchUp => catch_up(),
        Change::Snapshot { matching } => snapshot_input(matching),
    };
    let result = if batched {
        runtime.step_batch(vec![input, RaftInput::Tick])
    } else {
        runtime.step(input)
    };
    if failing {
        assert!(result.is_err(), "armed mutation cut must fire");
        assert!(matches!(
            runtime.step(RaftInput::Tick),
            Err(RaftRuntimeError::Poisoned { .. })
        ));
    } else {
        let outputs = result.expect("live control accepts the operation");
        assert!(outputs.iter().any(|output| match output {
            RaftOutput::Send {
                message: Message::AppendEntriesResponse(response),
                ..
            } => response.success,
            RaftOutput::Send {
                message: Message::InstallSnapshotResponse(response),
                ..
            } => response.success,
            _ => false,
        }));
    }
}

fn assert_old_hard_state<H: RaftHardStateStore>(hard_state: &H) {
    assert_eq!(hard_state.current().current_term, Term(3));
    assert_eq!(hard_state.current().commit_index, LogIndex(1));
    assert_eq!(
        hard_state.current().committed_configuration,
        Some(configuration_state(1))
    );
}

fn assert_recovered<
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
>(
    runtime: &mut DurableRaftNode<H, L, S>,
    change: Change,
    cuts: &[Mutation],
    published: bool,
) {
    let installed = matches!(change, Change::Snapshot { .. }) && cuts.contains(&Mutation::Stage);
    let committed = if installed {
        2
    } else if published {
        3
    } else {
        1
    };
    assert_eq!(runtime.current_term(), Term(3));
    assert_eq!(runtime.commit_index(), LogIndex(committed));
    assert_eq!(
        runtime.committed_configuration_state(),
        Some(configuration_state(committed))
    );
    assert_eq!(
        runtime.committed_membership(),
        MembershipConfig::Stable(membership(committed))
    );
    if published {
        assert_eq!(
            runtime.hard_state_store.current().commit_index,
            LogIndex(committed)
        );
        assert_eq!(
            runtime.hard_state_store.current().committed_configuration,
            Some(configuration_state(committed))
        );
    }
    if installed {
        let Change::Snapshot { matching } = change else {
            unreachable!()
        };
        assert_snapshot(runtime, matching);
    } else if matches!(change, Change::CatchUp) && cuts.contains(&Mutation::Append) {
        assert_eq!(
            runtime.effective_membership(),
            MembershipConfig::Stable(membership(3))
        );
        assert_eq!(
            runtime.log_entries_from(LogIndex(2)),
            vec![
                LogEntry::configuration(Term(2), configuration(2)),
                LogEntry::configuration(Term(2), configuration(3)),
            ]
        );
    }
    let replay = runtime.drain_committed_outputs();
    assert!(
        replay.iter().all(|output| match output {
            RaftOutput::Apply { .. } => false,
            RaftOutput::ConfigurationCommitted { index, .. } => *index <= LogIndex(committed),
            _ => true,
        }),
        "recovery must not apply stale data or unpublished configurations"
    );
}

fn assert_snapshot<
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
>(
    runtime: &DurableRaftNode<H, L, S>,
    matching: bool,
) {
    assert_eq!(runtime.snapshot_index(), LogIndex(2));
    assert_eq!(
        runtime.term_at_index(LogIndex(2)),
        Some(Term(if matching { 1 } else { 2 }))
    );
    assert_eq!(
        runtime.log_entries_from(LogIndex(3)),
        if matching {
            vec![LogEntry::application(Term(1), b"valid-tail".to_vec())]
        } else {
            Vec::new()
        }
    );
    assert_eq!(
        runtime.last_log_index(),
        LogIndex(if matching { 3 } else { 2 })
    );
    let descriptor = runtime.snapshot_store.current_snapshot().unwrap();
    assert_eq!(
        runtime
            .snapshot_store
            .snapshot_chunk(SnapshotChunkRequest {
                transfer_id: descriptor.transfer_id(),
                metadata: &descriptor.metadata,
                total_payload_len: descriptor.application_payload_len,
                application_payload_crc32: descriptor.application_payload_crc32,
                offset: 0,
                len: descriptor.application_payload_len.try_into().unwrap(),
            })
            .unwrap(),
        b"application state at C2"
    );
}

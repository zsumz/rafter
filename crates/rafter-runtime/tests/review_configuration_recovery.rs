//! Public-API regressions for the pre-publication images reported in review.

use rafter::{
    AppendEntries, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, CommittedConfiguration, ConfigurationEntry, ConfigurationId, Input,
    InstallSnapshotChunk, LogEntry, LogIndex, MembershipConfig, MembershipSet, Message, NodeConfig,
    NodeId, Output, RaftSnapshot, RaftSnapshotMetadata, SnapshotCommittedConfiguration,
    SnapshotGroupId, Term,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftLogEntry, RaftHardState, RaftHardStateStore, RaftLogSegment,
};

fn configuration_state(id: u64) -> CommittedConfiguration {
    CommittedConfiguration {
        index: LogIndex(id),
        config_id: ConfigurationId(id),
    }
}

fn membership(id: u64) -> MembershipSet {
    let learners = match id {
        1 => Vec::new(),
        2 => vec![NodeId(4)],
        3 => vec![NodeId(4), NodeId(5)],
        _ => panic!("unexpected fixture configuration"),
    };
    MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], learners).unwrap()
}

fn configuration(id: u64) -> ConfigurationEntry {
    ConfigurationEntry::stable(ConfigurationId(id), membership(id))
}

fn node_config() -> NodeConfig {
    NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 10).unwrap()
}

fn hard_state_store(state: RaftHardState) -> InMemoryRaftHardStateStore {
    let mut store = InMemoryRaftHardStateStore::new();
    store.write_hard_state(state).unwrap();
    store
}

fn initial_runtime() -> DurableRaftNode {
    let hard_state = hard_state_store(RaftHardState {
        current_term: Term(1),
        voted_for: None,
        commit_index: LogIndex(1),
        committed_configuration: Some(configuration_state(1)),
    });
    let mut log = InMemoryRaftLogSegment::new();
    log.append_entries(&[
        PersistedRaftLogEntry::configuration(LogIndex(1), Term(1), configuration(1)),
        PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"stale-boundary".to_vec()),
        PersistedRaftLogEntry::application(LogIndex(3), Term(1), b"stale-tail".to_vec()),
    ])
    .unwrap();
    DurableRaftNode::with_storage_and_snapshot_store(
        node_config(),
        hard_state,
        log,
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("initial durable image opens")
}

fn before_final_commit_write(final_state: RaftHardState) -> InMemoryRaftHardStateStore {
    hard_state_store(RaftHardState {
        current_term: final_state.current_term,
        voted_for: final_state.voted_for,
        commit_index: LogIndex(1),
        committed_configuration: Some(configuration_state(1)),
    })
}

#[test]
fn catch_up_across_two_configurations_reopens_before_commit_publication() {
    for batched in [false, true] {
        let mut runtime = initial_runtime();
        let append = Input::Message {
            from: NodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 1,
                term: Term(3),
                leader_id: NodeId(1),
                prev_log_index: LogIndex(1),
                prev_log_term: Term(1),
                entries: vec![
                    LogEntry::configuration(Term(2), configuration(2)),
                    LogEntry::configuration(Term(2), configuration(3)),
                ]
                .into(),
                leader_commit: LogIndex(3),
            }),
        };
        let outputs = if batched {
            runtime.step_batch(vec![append, Input::Tick])
        } else {
            runtime.step(append)
        }
        .expect("the live runtime accepts this legitimate catch-up frame");
        assert!(outputs.iter().any(|output| matches!(output,
            Output::Send { message: Message::AppendEntriesResponse(response), .. }
                if response.success && response.match_index == LogIndex(3)
        )));
        assert_eq!(runtime.commit_index(), LogIndex(3));
        assert_eq!(
            runtime.committed_configuration_state(),
            Some(configuration_state(3))
        );

        let stores = runtime.into_storage();
        let crash_hard_state = before_final_commit_write(stores.hard_state_store.current());
        let recovered = DurableRaftNode::with_storage_and_snapshot_store(
            node_config(),
            crash_hard_state,
            stores.log_segment,
            stores.snapshot_store,
        );
        assert!(
            recovered.is_ok(),
            "a live-reachable crash image must reopen; batched={batched}: {recovered:?}"
        );
    }
}

fn newer_configuration_snapshot() -> Input {
    let payload = b"application state at configuration 2".to_vec();
    let mut metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("review-config-recovery").unwrap(),
        NodeId(1),
        LogIndex(2),
        Term(2),
        Term(3),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("review_state").unwrap(),
            ApplicationSnapshotVersion::new(1).unwrap(),
        ),
    )
    .unwrap();
    metadata.committed_configuration = Some(SnapshotCommittedConfiguration::new(
        Some(configuration_state(2)),
        MembershipConfig::Stable(membership(2)),
    ));
    let descriptor = RaftSnapshot::from_payload(metadata.clone(), &payload);
    Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(InstallSnapshotChunk {
            term: Term(3),
            leader_id: NodeId(1),
            transfer_id: descriptor.transfer_id(),
            metadata,
            total_payload_len: descriptor.application_payload_len,
            application_payload_crc32: descriptor.application_payload_crc32,
            offset: 0,
            chunk: payload,
            done: true,
        }),
    }
}

#[test]
fn newer_snapshot_configuration_supersedes_older_durable_configuration() {
    for batched in [false, true] {
        let mut runtime = initial_runtime();
        let snapshot = newer_configuration_snapshot();
        let outputs = if batched {
            runtime.step_batch(vec![snapshot, Input::Tick])
        } else {
            runtime.step(snapshot)
        }
        .expect("the live runtime installs the newer snapshot");
        assert!(outputs.iter().any(|output| matches!(output,
            Output::Send { message: Message::InstallSnapshotResponse(response), .. }
                if response.success && response.last_included_index == LogIndex(2)
        )));
        assert_eq!(runtime.snapshot_index(), LogIndex(2));
        assert_eq!(
            runtime.committed_configuration_state(),
            Some(configuration_state(2))
        );

        let stores = runtime.into_storage();
        let crash_hard_state = before_final_commit_write(stores.hard_state_store.current());
        let mut recovered = DurableRaftNode::with_storage_and_snapshot_store(
            node_config(),
            crash_hard_state,
            stores.log_segment,
            stores.snapshot_store,
        )
        .expect("snapshot crash image opens");
        assert_eq!(recovered.snapshot_index(), LogIndex(2));
        assert_eq!(recovered.commit_index(), LogIndex(2));
        assert_eq!(
            recovered.committed_membership(),
            MembershipConfig::Stable(membership(2))
        );
        assert_eq!(
            recovered.committed_configuration_state(),
            Some(configuration_state(2)),
            "the newer snapshot identity must not be shadowed; batched={batched}"
        );

        recovered
            .step(Input::Tick)
            .expect("normal operation after recovery");
        let stores = recovered.into_storage();
        let again = DurableRaftNode::with_storage_and_snapshot_store(
            node_config(),
            stores.hard_state_store,
            stores.log_segment,
            stores.snapshot_store,
        )
        .expect("second reopen succeeds");
        assert_eq!(
            again.committed_configuration_state(),
            Some(configuration_state(2))
        );
    }
}

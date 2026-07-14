use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    BootstrapLogEntry, BootstrapState, LogIndex, Node, NodeConfig, NodeId, RaftSnapshot,
    RaftSnapshotMetadata, SnapshotGroupId, Term,
};

use super::{Cluster, ExecutionCursor, ExecutionInstrumentationError};

const NODE_ID: NodeId = NodeId(1);

#[test]
fn recorder_detects_each_missing_execution_input() {
    let mut cursor = one_node_cluster();
    cursor.execution_cursors.remove(&NODE_ID);
    assert!(!cursor.execution_cursors.contains_key(&NODE_ID));
    cursor.record_execution_history(NODE_ID);

    let mut initial_reference = one_node_cluster();
    initial_reference.application_epochs.insert(NODE_ID, 1);
    initial_reference.initial_reference_states.remove(&NODE_ID);
    assert!(!initial_reference
        .initial_reference_states
        .contains_key(&NODE_ID));
    initial_reference.record_execution_history(NODE_ID);

    let mut snapshot_payload = cluster_with_snapshot(true, false);
    let snapshot = snapshot_payload
        .node(NODE_ID)
        .snapshot()
        .expect("fixture has a snapshot");
    assert!(snapshot_payload
        .snapshot_payload(NODE_ID, snapshot)
        .is_none());
    snapshot_payload.record_execution_history(NODE_ID);

    let mut snapshot_reference = cluster_with_snapshot(false, true);
    snapshot_reference.initial_reference_states.remove(&NODE_ID);
    let snapshot = snapshot_reference
        .node(NODE_ID)
        .snapshot()
        .expect("fixture has a snapshot");
    assert!(snapshot_reference
        .snapshot_reference_membership(NODE_ID, snapshot)
        .is_none());
    snapshot_reference.record_execution_history(NODE_ID);

    let cases = [
        (
            cursor.execution_instrumentation_errors(),
            ExecutionInstrumentationError::CursorUnavailable { node_id: NODE_ID },
        ),
        (
            initial_reference.execution_instrumentation_errors(),
            ExecutionInstrumentationError::InitialReferenceUnavailable { node_id: NODE_ID },
        ),
        (
            snapshot_payload.execution_instrumentation_errors(),
            ExecutionInstrumentationError::SnapshotPayloadUnavailable {
                node_id: NODE_ID,
                snapshot_index: LogIndex(2),
            },
        ),
        (
            snapshot_reference.execution_instrumentation_errors(),
            ExecutionInstrumentationError::SnapshotReferenceUnavailable {
                node_id: NODE_ID,
                snapshot_index: LogIndex(2),
            },
        ),
    ];

    for (actual, expected) in cases {
        assert_eq!(actual, vec![expected]);
    }
}

#[test]
fn recorder_detects_a_retained_log_gap_from_its_log_lookup() {
    let cluster = cluster_applied_through_two();
    assert!(cluster.execution_instrumentation_errors().is_empty());
    let mut retained_entries = cluster.log_entries_from(NODE_ID, LogIndex(1));
    assert_eq!(retained_entries.len(), 2);
    retained_entries.remove(0);

    let errors = cluster.execution_instrumentation_errors_with_log_len(|_, first_index| {
        assert_eq!(first_index, LogIndex(1));
        retained_entries.len()
    });

    assert_eq!(
        errors,
        vec![ExecutionInstrumentationError::RetainedLogGap {
            node_id: NODE_ID,
            first_index: LogIndex(1),
            applied_through: LogIndex(2),
            available_entries: 1,
        }]
    );
}

#[test]
fn recorder_preserves_every_committed_log_entry_kind() {
    let mut cluster = one_node_cluster();
    let configuration = rafter::ConfigurationEntry::stable(
        rafter::ConfigurationId(7),
        rafter::MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("fixture membership is valid"),
    );
    let bootstrap = BootstrapState {
        current_term: Term(1),
        voted_for: None,
        commit_index: LogIndex(3),
        committed_configuration: Some(rafter::CommittedConfiguration {
            index: LogIndex(2),
            config_id: rafter::ConfigurationId(7),
        }),
        snapshot: None,
        log: vec![
            BootstrapLogEntry::noop(LogIndex(1), Term(1)),
            BootstrapLogEntry::configuration(LogIndex(2), Term(1), configuration.clone()),
            BootstrapLogEntry::application(LogIndex(3), Term(1), b"value".to_vec()),
        ],
    };
    let mut node = Node::from_bootstrap_applied_through(node_config(), bootstrap, LogIndex::ZERO)
        .expect("fixture bootstrap is valid");
    let outputs = node.drain_committed_outputs();
    cluster.nodes.insert(NODE_ID, node);
    cluster.record_outputs(NODE_ID, outputs);

    assert_eq!(
        cluster
            .execution_history()
            .iter()
            .map(|witness| witness.commit_index_at_emit)
            .collect::<Vec<_>>(),
        vec![LogIndex(3); 3],
        "every entry kind must freeze the actual commit index at recorder time"
    );
    assert_eq!(cluster.applied().len(), 1);
    assert_eq!(cluster.applied()[0].commit_index_at_emit, LogIndex(3));

    let advanced = BootstrapState {
        current_term: Term(2),
        voted_for: None,
        commit_index: LogIndex(4),
        committed_configuration: Some(rafter::CommittedConfiguration {
            index: LogIndex(2),
            config_id: rafter::ConfigurationId(7),
        }),
        snapshot: None,
        log: vec![
            BootstrapLogEntry::noop(LogIndex(1), Term(1)),
            BootstrapLogEntry::configuration(LogIndex(2), Term(1), configuration.clone()),
            BootstrapLogEntry::application(LogIndex(3), Term(1), b"value".to_vec()),
            BootstrapLogEntry::noop(LogIndex(4), Term(2)),
        ],
    };
    let advanced = Node::from_bootstrap_applied_through(node_config(), advanced, LogIndex(3))
        .expect("advanced fixture bootstrap is valid");
    cluster.nodes.insert(NODE_ID, advanced);
    assert_eq!(cluster.commit_index(NODE_ID), LogIndex(4));
    assert!(cluster
        .execution_history()
        .iter()
        .all(|witness| witness.commit_index_at_emit == LogIndex(3)));
    assert_eq!(cluster.applied()[0].commit_index_at_emit, LogIndex(3));

    let recorded: Vec<_> = cluster
        .execution_history()
        .iter()
        .map(|witness| {
            (
                witness.entry.index,
                witness.entry.term,
                witness.entry.kind.clone(),
            )
        })
        .collect();
    assert_eq!(
        recorded,
        vec![
            (LogIndex(1), Term(1), rafter::LogEntryKind::Noop),
            (
                LogIndex(2),
                Term(1),
                rafter::LogEntryKind::Configuration(configuration),
            ),
            (
                LogIndex(3),
                Term(1),
                rafter::LogEntryKind::Application(b"value".to_vec().into()),
            ),
        ]
    );
}

fn one_node_cluster() -> Cluster {
    Cluster::new(vec![node_config()])
}

fn node_config() -> NodeConfig {
    NodeConfig::new(NODE_ID, vec![NodeId(2), NodeId(3)], 3).expect("test node config is valid")
}

fn cluster_applied_through_two() -> Cluster {
    let mut cluster = one_node_cluster();
    let bootstrap = BootstrapState {
        current_term: Term(1),
        voted_for: None,
        commit_index: LogIndex(2),
        committed_configuration: None,
        snapshot: None,
        log: vec![
            BootstrapLogEntry::application(LogIndex(1), Term(1), b"one".to_vec()),
            BootstrapLogEntry::application(LogIndex(2), Term(1), b"two".to_vec()),
        ],
    };
    let node = Node::from_bootstrap_applied_through(node_config(), bootstrap, LogIndex(2))
        .expect("fixture bootstrap is valid");
    cluster.nodes.insert(NODE_ID, node);
    cluster
}

fn cluster_with_snapshot(include_reference_membership: bool, seed_payload: bool) -> Cluster {
    let mut cluster = one_node_cluster();
    let payload = b"snapshot-state".to_vec();
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("execution-recorder").expect("valid snapshot group"),
        NODE_ID,
        LogIndex(2),
        Term(1),
        Term(1),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("register").expect("valid snapshot kind"),
            ApplicationSnapshotVersion::new(1).expect("valid snapshot version"),
        ),
    )
    .expect("valid snapshot metadata");
    let metadata = if include_reference_membership {
        metadata.with_committed_membership(
            cluster.initial_reference_states[&NODE_ID]
                .committed_membership
                .clone(),
        )
    } else {
        metadata
    };
    let snapshot = RaftSnapshot::from_payload(metadata, &payload);
    if seed_payload {
        cluster.seed_snapshot_payload(NODE_ID, &snapshot, payload);
    }
    let bootstrap = BootstrapState {
        current_term: Term(1),
        voted_for: None,
        commit_index: LogIndex(2),
        committed_configuration: None,
        snapshot: Some(snapshot),
        log: Vec::new(),
    };
    let node = Node::from_bootstrap_applied_through(node_config(), bootstrap, LogIndex(2))
        .expect("snapshot fixture bootstrap is valid");
    cluster.nodes.insert(NODE_ID, node);
    cluster.execution_cursors.insert(
        NODE_ID,
        ExecutionCursor {
            application_epoch: 0,
            applied_through: LogIndex::ZERO,
            state: cluster.initial_reference_states[&NODE_ID].clone(),
        },
    );
    cluster
}

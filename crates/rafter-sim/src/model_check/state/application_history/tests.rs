use std::hash::{DefaultHasher, Hash, Hasher};

use rafter::{
    ConfigurationEntry, ConfigurationId, LogEntryKind, MembershipConfig, MembershipSet, NodeId,
    Term,
};

use super::{ApplicationHistory, ExecutionHistoryInstrumentationError, Observation};
use crate::{Cluster, ExecutedLogEntry, ExecutionWitness, ReferenceState};
use rafter_invariant_test::oracle_assert;

#[test]
fn detector_rejects_same_length_consumed_prefix_rewrite() {
    let mut cluster = cluster_with_history(vec![
        witness(1, 0, 1, LogEntryKind::Noop),
        witness(1, 0, 2, LogEntryKind::Noop),
    ]);
    let mut history = ApplicationHistory::from_cluster(&cluster);
    let mut rewritten = cluster.execution_history()[0].clone();
    rewritten.node_id = NodeId(2);
    cluster.execution_history.rewrite(0, rewritten);

    let _ = history.observe_cluster(&cluster);

    assert!(history
        .instrumentation_errors()
        .contains(&ExecutionHistoryInstrumentationError::LedgerPrefixChanged { observed: 2 }));
}

#[test]
fn detector_rejects_same_length_consumed_prefix_reorder() {
    let mut cluster = cluster_with_history(vec![
        witness(1, 0, 1, LogEntryKind::Noop),
        witness(1, 0, 2, LogEntryKind::Noop),
    ]);
    let mut history = ApplicationHistory::from_cluster(&cluster);
    cluster.execution_history.swap(0, 1);

    let _ = history.observe_cluster(&cluster);

    assert!(history
        .instrumentation_errors()
        .contains(&ExecutionHistoryInstrumentationError::LedgerPrefixChanged { observed: 2 }));
}

#[test]
fn incremental_ledger_identity_distinguishes_append_rewrite_and_reorder() {
    let first = witness(1, 0, 1, LogEntryKind::Noop);
    let second = witness(1, 0, 2, LogEntryKind::Noop);
    let mut cluster = cluster_with_history(vec![first.clone(), second.clone()]);
    let original = hash_value(&cluster.execution_history);

    cluster
        .execution_history
        .push(witness(2, 0, 3, LogEntryKind::Noop));
    assert_ne!(hash_value(&cluster.execution_history), original);

    let mut rewritten = cluster_with_history(vec![first.clone(), second.clone()]);
    let mut replacement = first.clone();
    replacement.node_id = NodeId(2);
    rewritten.execution_history.rewrite(0, replacement);
    assert_ne!(hash_value(&rewritten.execution_history), original);

    let mut reordered = cluster_with_history(vec![first, second]);
    reordered.execution_history.swap(0, 1);
    assert_ne!(hash_value(&reordered.execution_history), original);
}

#[test]
fn execution_ledger_hashes_exact_retained_witness_structure() {
    let witnesses = vec![
        witness(1, 0, 1, LogEntryKind::Application(b"first".to_vec().into())),
        witness(
            2,
            1,
            2,
            LogEntryKind::Application(b"second".to_vec().into()),
        ),
    ];
    let ledger = crate::records::ExecutionLedger::from_witnesses(witnesses.clone());

    assert_eq!(
        hash_stream(&ledger),
        hash_stream(&(witnesses, 0_u64)),
        "ledger identity must contain the exact witness vector and rewrite revision"
    );
}

#[test]
fn exact_application_history_distinguishes_structural_witness_changes() {
    let first_cluster = cluster_with_history(vec![witness(
        1,
        0,
        1,
        LogEntryKind::Application(b"first".to_vec().into()),
    )]);
    let second_cluster = cluster_with_history(vec![witness(
        1,
        0,
        1,
        LogEntryKind::Application(b"second".to_vec().into()),
    )]);
    let first = ApplicationHistory::from_cluster(&first_cluster);
    let second = ApplicationHistory::from_cluster(&second_cluster);

    assert_eq!(
        protocol_hash(&first_cluster),
        protocol_hash(&second_cluster)
    );
    assert_ne!(hash_stream(&first_cluster), hash_stream(&second_cluster));
    assert_ne!(hash_stream(&first), hash_stream(&second));
}

#[test]
fn execution_evidence_changes_verifier_hash_but_not_protocol_hash() {
    let mut cluster = cluster_with_history(Vec::new());
    let protocol_before = protocol_hash(&cluster);
    let verifier_before = hash_value(&cluster);

    cluster
        .execution_history
        .push(witness(1, 0, 1, LogEntryKind::Noop));

    assert_eq!(protocol_hash(&cluster), protocol_before);
    assert_ne!(hash_value(&cluster), verifier_before);
}

#[test]
fn same_node_replay_does_not_qualify_cross_replica_coverage() {
    let mut history = ApplicationHistory::default();
    for epoch in [0, 1] {
        history.record_fixture(&witness(
            1,
            epoch,
            1,
            LogEntryKind::Application(b"command".to_vec().into()),
        ));
        history.record_fixture(&witness(1, epoch, 2, configuration_kind()));
    }

    let coverage = history.coverage();
    assert!(!coverage.contains(Observation::SameIndexApplicationWitnessPairs));
    assert!(!coverage.contains(Observation::SameIndexConfigurationWitnessPairs));
    assert!(!coverage.contains(Observation::SameIndexApplicationResultPairs));
    assert!(!coverage.contains(Observation::SameIndexConfigurationResultPairs));
}

#[test]
fn distinct_nodes_qualify_each_execution_witness_class() {
    let mut history = ApplicationHistory::default();
    for node_id in [1, 2] {
        history.record_fixture(&witness(
            node_id,
            0,
            1,
            LogEntryKind::Application(b"command".to_vec().into()),
        ));
        history.record_fixture(&witness(node_id, 0, 2, configuration_kind()));
    }

    let coverage = history.coverage();
    assert!(coverage.contains(Observation::SameIndexApplicationWitnessPairs));
    assert!(coverage.contains(Observation::SameIndexConfigurationWitnessPairs));
    assert!(coverage.contains(Observation::SameIndexApplicationResultPairs));
    assert!(coverage.contains(Observation::SameIndexConfigurationResultPairs));
}

#[test]
fn application_pairs_do_not_qualify_configuration_results() {
    let mut history = ApplicationHistory::default();
    for node_id in [1, 2] {
        history.record_fixture(&witness(
            node_id,
            0,
            1,
            LogEntryKind::Application(b"command".to_vec().into()),
        ));
    }

    let coverage = history.coverage();
    oracle_assert!(coverage.contains(Observation::SameIndexApplicationResultPairs));
    oracle_assert!(!coverage.contains(Observation::SameIndexConfigurationResultPairs));
}

#[test]
fn configuration_pairs_do_not_qualify_application_results() {
    let mut history = ApplicationHistory::default();
    for node_id in [1, 2] {
        history.record_fixture(&witness(node_id, 0, 2, configuration_kind()));
    }

    let coverage = history.coverage();
    oracle_assert!(coverage.contains(Observation::SameIndexConfigurationResultPairs));
    oracle_assert!(!coverage.contains(Observation::SameIndexApplicationResultPairs));
}

fn cluster_with_history(execution_history: Vec<ExecutionWitness>) -> Cluster {
    let mut cluster = Cluster::new(Vec::new());
    cluster.execution_history = crate::records::ExecutionLedger::from_witnesses(execution_history);
    cluster
}

fn witness(
    node_id: u64,
    application_epoch: u64,
    index: u64,
    kind: LogEntryKind,
) -> ExecutionWitness {
    let prior_state = ReferenceState {
        application_value: Vec::new().into(),
        committed_membership: membership(&[1, 2, 3]),
        committed_configuration: None,
    };
    let emitted_application_payload = match &kind {
        LogEntryKind::Application(payload) => Some(payload.clone()),
        LogEntryKind::Configuration(_) | LogEntryKind::Noop => None,
    };
    let mut resulting_state = prior_state.clone();
    match &kind {
        LogEntryKind::Application(payload) => {
            resulting_state.application_value.clone_from(payload);
        }
        LogEntryKind::Configuration(configuration) => {
            resulting_state.committed_membership = configuration.membership_config();
            resulting_state.committed_configuration = Some(rafter::CommittedConfiguration {
                index: rafter::LogIndex(index),
                config_id: configuration.config_id(),
            });
        }
        LogEntryKind::Noop => {}
    }
    ExecutionWitness {
        node_id: NodeId(node_id),
        application_epoch,
        commit_index_at_emit: rafter::LogIndex(index),
        entry: ExecutedLogEntry {
            index: rafter::LogIndex(index),
            term: Term(1),
            kind,
        },
        emitted_application_payload,
        prior_state,
        resulting_state,
    }
}

fn configuration_kind() -> LogEntryKind {
    LogEntryKind::Configuration(ConfigurationEntry::stable(
        ConfigurationId(7),
        MembershipSet::new(vec![NodeId(1), NodeId(2)], Vec::new())
            .expect("fixture membership is valid"),
    ))
}

fn membership(voters: &[u64]) -> MembershipConfig {
    MembershipConfig::stable(
        MembershipSet::new(voters.iter().copied().map(NodeId).collect(), Vec::new())
            .expect("fixture membership is valid"),
    )
}

fn hash_value(value: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn hash_stream(value: &impl Hash) -> Vec<u8> {
    let mut hasher = HashStream::default();
    value.hash(&mut hasher);
    hasher.bytes
}

#[derive(Default)]
struct HashStream {
    bytes: Vec<u8>,
}

impl Hasher for HashStream {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }
}

fn protocol_hash(cluster: &Cluster) -> u64 {
    let mut hasher = DefaultHasher::new();
    cluster.hash_protocol_state(&mut hasher);
    hasher.finish()
}

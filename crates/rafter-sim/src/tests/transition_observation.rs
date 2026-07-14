use std::hash::{DefaultHasher, Hasher};

use rafter::{LogEntryKind, LogIndex, NodeConfig, NodeId, Term};

use super::*;

#[test]
fn transition_observation_snapshot_omits_immutable_execution_payloads() {
    let config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("node config is valid");
    let mut cluster = Cluster::new(vec![config]);
    let prior_state = cluster.initial_reference_states[&NodeId(1)].clone();
    let payload = LogEntryKind::application(b"retained execution payload".to_vec());
    cluster.execution_history.push(ExecutionWitness {
        node_id: NodeId(1),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(1),
        entry: ExecutedLogEntry {
            index: LogIndex(1),
            term: Term(1),
            kind: payload.clone(),
        },
        emitted_application_payload: match payload {
            LogEntryKind::Application(payload) => Some(payload),
            LogEntryKind::Configuration(_) | LogEntryKind::Noop => None,
        },
        prior_state: prior_state.clone(),
        resulting_state: prior_state,
    });

    let snapshot = cluster.transition_observation_snapshot();

    assert_eq!(cluster.execution_history().len(), 1);
    assert!(snapshot.execution_history().is_empty());
    assert_eq!(protocol_hash(&snapshot), protocol_hash(&cluster));
}

fn protocol_hash(cluster: &Cluster) -> u64 {
    let mut hasher = DefaultHasher::new();
    cluster.hash_protocol_state(&mut hasher);
    hasher.finish()
}

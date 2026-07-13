use rafter::{
    AppendEntries, ApplicationSnapshotKind, Input, InstallSnapshot, LogEntry, LogIndex,
    MembershipConfig, MembershipSet, Message, NodeId, Output, RaftSnapshot, Term,
};

use super::support::{
    cas_command_for, expected_kv, initialize_cluster_process, load_persisted_app, remove_test_root,
    test_root,
};
use crate::{
    app::encode_snapshot_payload, raft::snapshots::application_snapshot_metadata,
    raft_node::read_snapshot_payload, runtime::open_application_node,
};

const FOLLOWER: NodeId = NodeId(2);
const LEADER: NodeId = NodeId(1);
const SNAPSHOT_INDEX: LogIndex = LogIndex(3);
const SUFFIX_INDEX: LogIndex = LogIndex(4);
const SNAPSHOT_TERM: Term = Term(4);
const CURRENT_TERM: Term = Term(5);

#[test]
pub(super) fn ps04_inbound_snapshot_promotion_crash_restores_snapshot_then_dispatches_suffix() {
    let root = test_root("ps04-inbound-snapshot-promotion");
    let peers = vec![NodeId(1), NodeId(3)];
    let mut opened = open_application_node(&root, FOLLOWER, peers.clone())
        .expect("fresh file-backed follower opens");
    assert!(opened.recovery_outputs.is_empty());

    let membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("three-voter membership is valid"),
    );
    let snapshot_payload =
        encode_snapshot_payload(&expected_kv(1)).expect("application snapshot encodes");
    let expected_snapshot =
        install_inbound_snapshot(&mut opened.node, membership.clone(), &snapshot_payload);
    assert_eq!(
        expected_snapshot.metadata.committed_membership(),
        Some(&membership)
    );

    let suffix_command = cas_command_for("n2", 3, 1, 2);
    let suffix_payload = serde_json::to_vec(&suffix_command).expect("CAS command encodes");
    append_committed_suffix(&mut opened.node, &suffix_payload);
    drop(opened);

    let stale_app = load_persisted_app(&root);
    assert_eq!(stale_app.applied, LogIndex::ZERO);
    assert!(stale_app.kv.is_empty());

    let reopened = open_application_node(&root, FOLLOWER, peers)
        .expect("production reopen restores the promoted application snapshot");
    assert_eq!(reopened.app.applied, SNAPSHOT_INDEX);
    assert_eq!(reopened.app.kv, expected_kv(1));
    assert_eq!(reopened.node.snapshot(), Some(&expected_snapshot));
    assert_eq!(
        read_snapshot_payload(&reopened.node, &expected_snapshot)
            .expect("reopened snapshot serves the exact durable payload"),
        snapshot_payload
    );
    assert_eq!(
        reopened.recovery_outputs,
        vec![Output::Apply {
            index: SUFFIX_INDEX,
            term: CURRENT_TERM,
            payload: suffix_payload.into(),
            local_proposal_id: None,
        }],
        "recovery must emit the suffix exactly once with no snapshot-covered Apply"
    );
    drop(reopened);

    let process = initialize_cluster_process(&root, "n2", &["n1", "n2", "n3"]);
    let initialized = process.initialized.as_ref().expect("follower initializes");
    assert_eq!(initialized.app.applied, SUFFIX_INDEX);
    assert_eq!(
        initialized.app.kv,
        expected_kv(2),
        "production dispatch must apply the CAS after restoring snapshot state"
    );
    assert_eq!(initialized.node.committed_membership(), membership);

    let persisted = load_persisted_app(&root);
    assert_eq!(persisted.applied, SUFFIX_INDEX);
    assert_eq!(persisted.kv, expected_kv(2));
    remove_test_root(root);
}

fn install_inbound_snapshot(
    node: &mut crate::raft_node::FileNode,
    membership: MembershipConfig,
    payload: &[u8],
) -> RaftSnapshot {
    let metadata =
        application_snapshot_metadata(LEADER, SNAPSHOT_INDEX, SNAPSHOT_TERM, CURRENT_TERM)
            .expect("Maelstrom snapshot metadata is valid")
            .with_committed_membership(membership);
    let expected = RaftSnapshot::from_payload(metadata.clone(), payload);
    let outputs = node
        .step(Input::Message {
            from: LEADER,
            message: Message::InstallSnapshot(InstallSnapshot {
                term: CURRENT_TERM,
                leader_id: LEADER,
                metadata,
                application_payload: payload.to_vec(),
            }),
        })
        .expect("runtime durably promotes inbound snapshot before outputs escape");
    assert!(matches!(
        outputs.as_slice(),
        [
            Output::StageSnapshotChunk { chunk },
            Output::ApplySnapshot { snapshot },
            Output::Send { to: LEADER, .. },
        ] if chunk.done
            && chunk.offset == 0
            && chunk.bytes == payload
            && snapshot == &expected
    ));
    assert_eq!(node.snapshot(), Some(&expected));
    assert_eq!(
        read_snapshot_payload(node, &expected)
            .expect("promoted durable snapshot serves its exact payload"),
        payload
    );
    expected
}

fn append_committed_suffix(node: &mut crate::raft_node::FileNode, payload: &[u8]) {
    let outputs = node
        .step(Input::Message {
            from: LEADER,
            message: Message::AppendEntries(AppendEntries {
                term: CURRENT_TERM,
                leader_id: LEADER,
                prev_log_index: SNAPSHOT_INDEX,
                prev_log_term: SNAPSHOT_TERM,
                sequence: 1,
                entries: vec![LogEntry::application(CURRENT_TERM, payload.to_vec())].into(),
                leader_commit: SUFFIX_INDEX,
            }),
        })
        .expect("committed suffix persists while application dispatch is withheld");
    assert_eq!(
        apply_outputs(&outputs),
        vec![(SUFFIX_INDEX, payload.to_vec())]
    );
}

#[test]
fn production_open_rejects_foreign_application_snapshot_identity() {
    let root = test_root("foreign-application-snapshot");
    let peers = vec![NodeId(1), NodeId(3)];
    let mut opened = open_application_node(&root, FOLLOWER, peers.clone())
        .expect("fresh file-backed follower opens");
    let snapshot_payload =
        encode_snapshot_payload(&expected_kv(1)).expect("application snapshot encodes");
    let mut metadata =
        application_snapshot_metadata(LEADER, SNAPSHOT_INDEX, SNAPSHOT_TERM, CURRENT_TERM)
            .expect("base metadata is valid");
    metadata.application.kind =
        ApplicationSnapshotKind::new("foreign-kv").expect("foreign kind is valid");

    let outputs = opened
        .node
        .step(Input::Message {
            from: LEADER,
            message: Message::InstallSnapshot(InstallSnapshot {
                term: CURRENT_TERM,
                leader_id: LEADER,
                metadata,
                application_payload: snapshot_payload,
            }),
        })
        .expect("runtime durably promotes the opaque application snapshot");
    assert!(outputs
        .iter()
        .any(|output| matches!(output, Output::ApplySnapshot { .. })));
    drop(opened);

    let error = open_application_node(&root, FOLLOWER, peers)
        .err()
        .expect("Maelstrom reopen must fail closed on a foreign snapshot kind");
    assert!(error
        .to_string()
        .contains("application snapshot kind foreign-kv is not lin-kv-v1"));
    let app = load_persisted_app(&root);
    assert_eq!(app.applied, LogIndex::ZERO);
    assert!(app.kv.is_empty());
    remove_test_root(root);
}

fn apply_outputs(outputs: &[Output]) -> Vec<(LogIndex, Vec<u8>)> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::Apply { index, payload, .. } => Some((*index, payload.to_vec())),
            _ => None,
        })
        .collect()
}

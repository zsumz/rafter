use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotVersion, Input, InstallSnapshot, LogIndex,
    MembershipConfig, MembershipSet, Message, NodeId, Output, SnapshotGroupId, Term,
};

use super::support::{
    expected_kv, initialize_cluster_process, load_persisted_app, remove_test_root, test_root,
};
use crate::{
    app::encode_snapshot_payload, raft::snapshots::application_snapshot_metadata,
    runtime::dispatch_recovery_outputs,
};

const LEADER: NodeId = NodeId(1);
const SNAPSHOT_INDEX: LogIndex = LogIndex(3);
const SNAPSHOT_TERM: Term = Term(4);
const CURRENT_TERM: Term = Term(5);

#[derive(Clone, Copy, Debug)]
enum SnapshotIdentityCase {
    Valid,
    WrongGroup,
    WrongKind,
    WrongVersion,
}

#[test]
fn live_snapshot_dispatch_validates_identity_before_application_persist() {
    for case in [
        SnapshotIdentityCase::Valid,
        SnapshotIdentityCase::WrongGroup,
        SnapshotIdentityCase::WrongKind,
        SnapshotIdentityCase::WrongVersion,
    ] {
        assert_live_snapshot_dispatch(case);
    }
}

fn assert_live_snapshot_dispatch(case: SnapshotIdentityCase) {
    let root = test_root(&format!("live-snapshot-{case:?}"));
    let mut process = initialize_cluster_process(&root, "n2", &["n1", "n2", "n3"]);
    let initialized = process.initialized.as_mut().expect("follower initializes");
    let before = initialized.app.clone();
    let before_snapshot_index = initialized.last_snapshot_index;

    let payload = encode_snapshot_payload(&expected_kv(1)).expect("snapshot payload encodes");
    let mut metadata =
        application_snapshot_metadata(LEADER, SNAPSHOT_INDEX, SNAPSHOT_TERM, CURRENT_TERM)
            .expect("base Maelstrom snapshot metadata is valid")
            .with_committed_membership(three_voter_membership());
    case.mutate(&mut metadata);

    let outputs = initialized
        .node
        .step(Input::Message {
            from: LEADER,
            message: Message::InstallSnapshot(InstallSnapshot {
                term: CURRENT_TERM,
                leader_id: LEADER,
                metadata,
                application_payload: payload,
            }),
        })
        .expect("runtime durably promotes opaque snapshot before dispatch");
    let apply_outputs = outputs
        .into_iter()
        .filter(|output| matches!(output, Output::ApplySnapshot { .. }))
        .collect::<Vec<_>>();
    assert_eq!(apply_outputs.len(), 1, "case {case:?}");
    dispatch_recovery_outputs(initialized, apply_outputs);

    let persisted = load_persisted_app(&root);
    if matches!(case, SnapshotIdentityCase::Valid) {
        assert_eq!(initialized.app.applied, SNAPSHOT_INDEX);
        assert_eq!(initialized.app.kv, expected_kv(1));
        assert_eq!(initialized.last_snapshot_index, SNAPSHOT_INDEX);
        assert_eq!(persisted.applied, SNAPSHOT_INDEX);
        assert_eq!(persisted.kv, expected_kv(1));
        assert!(root.join("app.json").exists());
    } else {
        assert_eq!(initialized.app.applied, before.applied, "case {case:?}");
        assert_eq!(initialized.app.kv, before.kv, "case {case:?}");
        assert_eq!(
            initialized.last_snapshot_index, before_snapshot_index,
            "case {case:?}"
        );
        assert_eq!(persisted.applied, before.applied, "case {case:?}");
        assert_eq!(persisted.kv, before.kv, "case {case:?}");
        assert!(!root.join("app.json").exists(), "case {case:?}");
    }
    remove_test_root(root);
}

impl SnapshotIdentityCase {
    fn mutate(self, metadata: &mut rafter::RaftSnapshotMetadata) {
        match self {
            Self::Valid => {}
            Self::WrongGroup => {
                metadata.group_id =
                    SnapshotGroupId::new("foreign-group").expect("foreign group is valid");
            }
            Self::WrongKind => {
                metadata.application.kind =
                    ApplicationSnapshotKind::new("foreign-kind").expect("foreign kind is valid");
            }
            Self::WrongVersion => {
                metadata.application.version =
                    ApplicationSnapshotVersion::new(2).expect("version two is valid");
            }
        }
    }
}

fn three_voter_membership() -> MembershipConfig {
    MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("three-voter membership is valid"),
    )
}

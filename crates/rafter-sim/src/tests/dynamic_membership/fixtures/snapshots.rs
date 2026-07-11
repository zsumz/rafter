use super::super::super::*;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    BootstrapState, MembershipConfig, RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId,
};

pub(crate) fn snapshot_metadata(membership: MembershipConfig) -> RaftSnapshotMetadata {
    snapshot_metadata_at(NodeId(1), LogIndex(1), Term(1), Term(1), membership)
}

fn snapshot_metadata_at(
    writer_id: NodeId,
    last_included_index: LogIndex,
    last_included_term: Term,
    hard_state_term: Term,
    membership: MembershipConfig,
) -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("sim-membership").expect("valid snapshot group id"),
        writer_id,
        last_included_index,
        last_included_term,
        hard_state_term,
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("membership").expect("valid snapshot kind"),
            ApplicationSnapshotVersion::new(1).expect("valid snapshot version"),
        ),
    )
    .expect("valid snapshot metadata")
    .with_committed_membership(membership)
}

pub(crate) fn restart_all_nodes_from_compacted_snapshots(cluster: &mut Cluster) {
    for node_id in [NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
        let live = cluster.bootstrap_state(node_id);
        let snapshot_index = live.commit_index;
        let snapshot_term = term_at(&live, snapshot_index);
        let snapshot_membership = cluster.effective_membership(node_id);
        let payload =
            format!("compacted dynamic membership for {node_id} through {snapshot_index}")
                .into_bytes();
        let snapshot = RaftSnapshot::from_payload(
            snapshot_metadata_at(
                node_id,
                snapshot_index,
                snapshot_term,
                live.current_term,
                snapshot_membership,
            ),
            &payload,
        );
        cluster.seed_snapshot_payload(node_id, &snapshot, payload);
        cluster
            .restart_node_from_bootstrap(
                node_id,
                BootstrapState {
                    current_term: live.current_term,
                    voted_for: live.voted_for,
                    commit_index: live.commit_index,
                    committed_configuration: live.committed_configuration,
                    snapshot: Some(snapshot),
                    log: live
                        .log
                        .into_iter()
                        .filter(|entry| entry.index > snapshot_index)
                        .collect(),
                },
            )
            .expect("compacted dynamic-membership bootstrap is valid");
    }
}

fn term_at(state: &BootstrapState, index: LogIndex) -> Term {
    if let Some(snapshot) = state.snapshot.as_ref() {
        if snapshot.metadata.last_included_index == index {
            return snapshot.metadata.last_included_term;
        }
    }
    state
        .log
        .iter()
        .find_map(|entry| (entry.index == index).then_some(entry.term))
        .expect("snapshot boundary must be present in the retained bootstrap state")
}

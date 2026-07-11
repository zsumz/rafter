use super::super::helpers::{
    config, deliver_append_entries, deliver_append_entries_response, elect_node_one,
    three_node_cluster,
};
use super::super::*;
use crate::disk_fault::{DirtyRecovery, FaultInjectingDisk};
use rafter::ConfigurationId;

pub(super) const LEADER: NodeId = NodeId(1);
pub(super) const FAULTED_FOLLOWER: NodeId = NodeId(2);
const INTACT_FOLLOWER: NodeId = NodeId(3);

pub(super) fn cluster_with_unacknowledged_follower_tail() -> (Cluster, FaultInjectingDisk, LogIndex)
{
    let mut cluster = three_node_cluster();
    elect_node_one(&mut cluster);
    commit_everywhere(&mut cluster, b"stable");
    let durable_floor = cluster.delivered_ack_floor(FAULTED_FOLLOWER);
    assert!(durable_floor >= LogIndex(2));

    cluster.propose(LEADER, b"dirty-tail".to_vec());
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(LEADER, FAULTED_FOLLOWER)),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(LEADER, INTACT_FOLLOWER)),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries_response(INTACT_FOLLOWER, LEADER)),
        1
    );
    let _ = cluster.drop_matching(|_| true);
    assert!(
        cluster.last_log_index(FAULTED_FOLLOWER) > durable_floor,
        "faulted follower should hold an unacknowledged tail"
    );
    assert_eq!(
        cluster.delivered_ack_floor(FAULTED_FOLLOWER),
        durable_floor,
        "the dirty tail was never acknowledged to the leader"
    );

    let disk = FaultInjectingDisk::new(cluster.bootstrap_state(FAULTED_FOLLOWER));
    (cluster, disk, durable_floor)
}

fn commit_everywhere(cluster: &mut Cluster, payload: &[u8]) {
    cluster.propose(LEADER, payload.to_vec());
    cluster.deliver_all();
    cluster.tick(LEADER);
    cluster.deliver_all();
    for node_id in [LEADER, FAULTED_FOLLOWER, INTACT_FOLLOWER] {
        assert!(
            cluster
                .applied()
                .iter()
                .any(|applied| applied.node_id == node_id && applied.payload.as_ref() == payload),
            "{node_id} should apply the stable prefix"
        );
    }
}

pub(super) fn reopen_faulted_follower(cluster: &mut Cluster, recovery: DirtyRecovery) {
    cluster
        .restart_node_from_bootstrap_losing_application_state(FAULTED_FOLLOWER, recovery.bootstrap)
        .unwrap_or_else(|error| panic!("dirty recovery {:?} reopens: {error:?}", recovery.fault));
}

pub(super) fn repair_faulted_follower(cluster: &mut Cluster) {
    for _ in 0..8 {
        cluster.tick(LEADER);
        let _ = cluster.deliver_matching(|envelope| {
            (envelope.from == LEADER && envelope.to == FAULTED_FOLLOWER)
                || (envelope.from == FAULTED_FOLLOWER && envelope.to == LEADER)
        });
    }
}

pub(super) fn commit_on_intact_quorum(cluster: &mut Cluster, payload: &[u8]) {
    cluster.propose(LEADER, payload.to_vec());
    for _ in 0..4 {
        let _ = cluster.deliver_matching(|envelope| {
            (envelope.from == LEADER && envelope.to == INTACT_FOLLOWER)
                || (envelope.from == INTACT_FOLLOWER && envelope.to == LEADER)
        });
        if cluster
            .applied()
            .iter()
            .any(|applied| applied.node_id == LEADER && applied.payload.as_ref() == payload)
        {
            return;
        }
        cluster.tick(LEADER);
    }
    panic!("intact quorum did not commit payload {payload:?}");
}

pub(super) fn assert_committed_on_intact_quorum(cluster: &Cluster, payload: &[u8]) {
    assert!(
        cluster
            .applied()
            .iter()
            .any(|applied| applied.node_id == LEADER && applied.payload.as_ref() == payload),
        "leader should apply payload committed through intact quorum"
    );
    assert_eq!(
        cluster.log_entries_from(INTACT_FOLLOWER, LogIndex(1)),
        cluster.log_entries_from(LEADER, LogIndex(1)),
        "intact follower log should match leader after recovery drive"
    );
}

pub(super) fn committed_configuration_bootstrap() -> BootstrapState {
    let config_id = ConfigurationId(7);
    BootstrapState {
        current_term: Term(4),
        voted_for: Some(NodeId(2)),
        commit_index: LogIndex(2),
        committed_configuration: Some(CommittedConfiguration {
            index: LogIndex(1),
            config_id,
        }),
        snapshot: None,
        log: vec![
            BootstrapLogEntry::configuration(
                LogIndex(1),
                Term(3),
                ConfigurationEntry::stable(
                    config_id,
                    MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
                        .expect("three-voter membership is valid"),
                ),
            ),
            BootstrapLogEntry::application(LogIndex(2), Term(3), b"committed".to_vec()),
            BootstrapLogEntry::application(LogIndex(3), Term(4), b"uncommitted-tail".to_vec()),
        ],
    }
}

pub(super) fn test_node_config() -> NodeConfig {
    config(1, &[2, 3], 3)
}

pub(super) fn retained_last_log_index(bootstrap: &BootstrapState) -> LogIndex {
    bootstrap.log.last().map_or_else(
        || {
            bootstrap
                .snapshot
                .as_ref()
                .map_or(LogIndex::ZERO, |snapshot| {
                    snapshot.metadata.last_included_index
                })
        },
        |entry| entry.index,
    )
}

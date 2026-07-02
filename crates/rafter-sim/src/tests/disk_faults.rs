use super::helpers::{
    config, deliver_append_entries, deliver_append_entries_response, elect_node_one,
    three_node_cluster,
};
use super::*;
use crate::disk_fault::{DirtyRecovery, FaultInjectingDisk};
use rafter::ConfigurationId;

const LEADER: NodeId = NodeId(1);
const FAULTED_FOLLOWER: NodeId = NodeId(2);
const INTACT_FOLLOWER: NodeId = NodeId(3);

fn cluster_with_unacknowledged_follower_tail() -> (Cluster, FaultInjectingDisk, LogIndex) {
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

#[test]
fn running_sim_reopens_after_each_modeled_disk_io_crash() {
    let (cluster, disk, _) = cluster_with_unacknowledged_follower_tail();
    let recoveries = disk.crash_after_each_io();
    assert!(
        recoveries.len() >= 4,
        "disk model should expose hard-state, log, and commit crash points"
    );

    for (index, recovery) in recoveries.into_iter().enumerate() {
        let mut scenario = cluster.clone();
        scenario
            .restart_node_from_bootstrap(FAULTED_FOLLOWER, recovery.bootstrap)
            .unwrap_or_else(|error| panic!("dirty recovery {index} reopens: {error:?}"));

        let payload = format!("after-crash-point-{index}").into_bytes();
        commit_on_intact_quorum(&mut scenario, &payload);
        assert_committed_on_intact_quorum(&scenario, &payload);
    }
}

#[test]
fn running_sim_repairs_dirty_disk_tail_shapes() {
    let (cluster, disk, durable_floor) = cluster_with_unacknowledged_follower_tail();
    let mut recoveries = Vec::new();
    recoveries.push(disk.torn_tail().expect("dirty follower has a tail"));
    recoveries.push(disk.lost_unfsynced_suffix(durable_floor));
    let hard_state_reorder = disk.hard_state_log_reorder(durable_floor);
    assert!(
        retained_last_log_index(&hard_state_reorder.bootstrap)
            >= hard_state_reorder.bootstrap.commit_index,
        "legal hard-state/log reorder repair case must retain the committed prefix"
    );
    recoveries.push(hard_state_reorder);

    for recovery in recoveries {
        let mut scenario = cluster.clone();
        reopen_faulted_follower(&mut scenario, recovery);
        repair_faulted_follower(&mut scenario);
        assert_eq!(
            scenario.log_entries_from(FAULTED_FOLLOWER, LogIndex(1)),
            scenario.log_entries_from(LEADER, LogIndex(1)),
            "faulted follower should converge after dirty recovery repair"
        );
    }
}

#[test]
fn lost_unfsynced_suffix_is_term_vote_only_survival() {
    let clean = committed_configuration_bootstrap();
    let recovery = FaultInjectingDisk::new(clean.clone()).lost_unfsynced_suffix(LogIndex(1));

    assert_eq!(recovery.bootstrap.current_term, clean.current_term);
    assert_eq!(recovery.bootstrap.voted_for, clean.voted_for);
    assert_eq!(recovery.bootstrap.commit_index, LogIndex::ZERO);
    assert_eq!(recovery.bootstrap.committed_configuration, None);
    assert_eq!(retained_last_log_index(&recovery.bootstrap), LogIndex(1));

    let node = Node::from_bootstrap(test_node_config(), recovery.bootstrap)
        .expect("term/vote-only truncation remains a legal bootstrap image");
    assert_eq!(node.commit_index(), LogIndex::ZERO);
}

#[test]
fn hard_state_log_reorder_preserves_commit_and_config_beyond_retained_log() {
    let clean = committed_configuration_bootstrap();
    let recovery = FaultInjectingDisk::new(clean.clone()).hard_state_log_reorder(LogIndex(1));

    assert_eq!(recovery.bootstrap.current_term, clean.current_term);
    assert_eq!(recovery.bootstrap.voted_for, clean.voted_for);
    assert_eq!(recovery.bootstrap.commit_index, clean.commit_index);
    assert_eq!(
        recovery.bootstrap.committed_configuration,
        clean.committed_configuration
    );
    assert_eq!(retained_last_log_index(&recovery.bootstrap), LogIndex(1));

    let error = Node::from_bootstrap(test_node_config(), recovery.bootstrap)
        .expect_err("commit index ahead of retained log should be rejected");
    assert!(matches!(
        error,
        BootstrapValidationError::CommitIndexBeyondLog {
            commit_index: LogIndex(2),
            last_log_index: LogIndex(1),
        }
    ));
}

#[test]
fn hard_state_log_reorder_reopens_when_retained_log_covers_commit() {
    let clean = committed_configuration_bootstrap();
    let recovery = FaultInjectingDisk::new(clean.clone()).hard_state_log_reorder(LogIndex(2));

    assert_eq!(retained_last_log_index(&recovery.bootstrap), LogIndex(2));
    let node = Node::from_bootstrap(test_node_config(), recovery.bootstrap)
        .expect("retained log covers committed hard state");

    assert_eq!(node.commit_index(), clean.commit_index);
    assert_eq!(
        node.committed_configuration_state(),
        clean.committed_configuration
    );
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

fn reopen_faulted_follower(cluster: &mut Cluster, recovery: DirtyRecovery) {
    cluster
        .restart_node_from_bootstrap(FAULTED_FOLLOWER, recovery.bootstrap)
        .unwrap_or_else(|error| panic!("dirty recovery {:?} reopens: {error:?}", recovery.fault));
}

fn repair_faulted_follower(cluster: &mut Cluster) {
    for _ in 0..8 {
        cluster.tick(LEADER);
        let _ = cluster.deliver_matching(|envelope| {
            (envelope.from == LEADER && envelope.to == FAULTED_FOLLOWER)
                || (envelope.from == FAULTED_FOLLOWER && envelope.to == LEADER)
        });
    }
}

fn commit_on_intact_quorum(cluster: &mut Cluster, payload: &[u8]) {
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

fn assert_committed_on_intact_quorum(cluster: &Cluster, payload: &[u8]) {
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

fn committed_configuration_bootstrap() -> BootstrapState {
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

fn test_node_config() -> NodeConfig {
    config(1, &[2, 3], 3)
}

fn retained_last_log_index(bootstrap: &BootstrapState) -> LogIndex {
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

use super::super::*;
use super::fixtures::{
    assert_committed_on_intact_quorum, cluster_with_unacknowledged_follower_tail,
    commit_on_intact_quorum, reopen_faulted_follower, repair_faulted_follower,
    retained_last_log_index, FAULTED_FOLLOWER, LEADER,
};

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
            .restart_node_from_bootstrap_losing_application_state(
                FAULTED_FOLLOWER,
                recovery.bootstrap,
            )
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
fn failed_application_loss_restart_leaves_running_cluster_unchanged() {
    let (cluster, disk, _) = cluster_with_unacknowledged_follower_tail();
    let invalid = disk.hard_state_log_reorder(LogIndex(1));
    let mut scenario = cluster.clone();
    let before_bootstrap = scenario.bootstrap_state(FAULTED_FOLLOWER);
    let before_applied = scenario.applied().to_vec();
    let before_installs = scenario.snapshot_installs().to_vec();

    let result = scenario
        .restart_node_from_bootstrap_losing_application_state(FAULTED_FOLLOWER, invalid.bootstrap);

    assert!(result.is_err());
    assert_eq!(scenario.bootstrap_state(FAULTED_FOLLOWER), before_bootstrap);
    assert_eq!(scenario.applied(), before_applied.as_slice());
    assert_eq!(scenario.snapshot_installs(), before_installs.as_slice());

    scenario
        .restart_node_from_bootstrap(FAULTED_FOLLOWER, before_bootstrap)
        .expect("clean restart after failed dirty recovery should preserve application floor");
    assert_eq!(
        scenario.applied(),
        before_applied.as_slice(),
        "failed application-loss restart must not reset the durable application floor"
    );
}

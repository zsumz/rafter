//! Durable, production-shaped process acceptance for the sharded counter.
//!
//! These tests are ignored in ordinary package lanes and selected exactly by
//! `scripts/reference-process-check`. Every wait is predicate-based and
//! bounded; every port is bound ephemerally by the process under test.

use std::{fs, thread};

use rafter_reference_harness::process::LineConnection;

#[path = "support/process_counter.rs"]
mod process_counter;

use process_counter::{
    fill_peer_connection_bound, number_field, open_session, send_stale_vote, ProcessCluster,
    ProcessHistory, CONNECTION_TIMEOUTS, GROUP_COUNT, NODE_IDS,
};

include!("process_cluster/review_regressions.rs");

fn occupy_all_workers(cluster: &mut ProcessCluster, host: u64, excluded_group: u32) -> Vec<u32> {
    occupy_all_workers_for(cluster, host, excluded_group, 5_000)
}

fn occupy_all_workers_for(
    cluster: &mut ProcessCluster,
    host: u64,
    excluded_group: u32,
    milliseconds: u64,
) -> Vec<u32> {
    let blockers = (1..=GROUP_COUNT)
        .filter(|candidate| *candidate != excluded_group)
        .take(4)
        .collect::<Vec<_>>();
    for blocker in &blockers {
        assert_eq!(
            cluster.request_on(host, &format!("SLOW {blocker} {milliseconds}")),
            format!("OK SLOW group={blocker} milliseconds={milliseconds}")
        );
        assert_eq!(
            cluster.request_on(host, &format!("PRESSURE {blocker} 1 bulk 1")),
            "OK PRESSURE accepted=1 refused=0"
        );
    }
    cluster.wait_status_at_least(host, "workers", 4);
    blockers
}

fn fill_group_queue(cluster: &mut ProcessCluster, host: u64, group: u32) {
    assert_eq!(
        cluster.request_on(host, &format!("PRESSURE {group} 1 bulk 64")),
        "OK PRESSURE accepted=64 refused=0"
    );
}

fn fill_global_queue(cluster: &mut ProcessCluster, host: u64) {
    loop {
        let status = cluster.request_on(host, "STATUS");
        if number_field(&status, "queued") >= 1024 {
            return;
        }
        let mut progressed = false;
        for group in 1..=GROUP_COUNT {
            let response = cluster.request_on(host, &format!("PRESSURE {group} 1 bulk 64"));
            assert!(response.starts_with("OK PRESSURE "), "{response}");
            progressed |= number_field(&response, "accepted") != 0;
            if number_field(&cluster.request_on(host, "STATUS"), "queued") >= 1024 {
                return;
            }
        }
        assert!(
            progressed,
            "global queue stopped below its configured bound"
        );
    }
}

#[test]
#[ignore = "real three-host process topology"]
fn many_groups_preserve_fairness_isolation_and_control_progress() {
    let mut cluster = ProcessCluster::start("many-groups");
    let mut history = ProcessHistory::default();

    for group in 1..=GROUP_COUNT {
        open_session(&mut cluster, group, 1, group);
        history.add(&mut cluster, group, 1, group, 1, i64::from(group));
    }

    for sequence in 2..=17 {
        history.add(&mut cluster, 1, 1, 1, sequence, 1);
    }
    let scheduling_host = cluster.leader();
    assert_eq!(
        cluster.request_on(scheduling_host, "SLOW 1 200"),
        "OK SLOW group=1 milliseconds=200"
    );
    let pressure = cluster.request_on(scheduling_host, "PRESSURE 1 1 bulk 64");
    assert!(pressure.starts_with("OK PRESSURE "), "{pressure}");
    let snapshot_pressure = cluster.request_on(scheduling_host, "PRESSURE 2 1 snapshot 32");
    assert!(
        snapshot_pressure.starts_with("OK PRESSURE "),
        "{snapshot_pressure}"
    );
    history.add(&mut cluster, 3, 1, 3, 2, 7);
    assert_eq!(
        cluster.request_on(scheduling_host, "SLOW 1 0"),
        "OK SLOW group=1 milliseconds=0"
    );
    history.assert_complete(&mut cluster);

    let poison_host = cluster.leader();
    let poisoned = cluster.request_on(poison_host, "FAULT 4 1");
    assert!(
        poisoned.starts_with("OK ")
            || poisoned.starts_with("ERR UNKNOWN ")
            || poisoned.starts_with("ERR NOT_COMMITTED "),
        "{poisoned}"
    );
    let healthy = cluster.request_leader("ADD 5 1 5 1 2 11");
    assert_eq!(healthy, "OK ADDED value=16");
    cluster.wait_value(poison_host, 5, 1, 16);
    let connection_full = number_field(
        &cluster.request_on(poison_host, "STATUS"),
        "link_inbound_connection_full",
    );
    let held = fill_peer_connection_bound(cluster.scratch_path(), poison_host);
    cluster.wait_status_above(poison_host, "link_inbound_connection_full", connection_full);
    drop(held);
    cluster.assert_audits();
}

#[test]
#[ignore = "real SIGKILL, recovery, and compaction"]
fn sigkill_exact_retry_snapshot_and_clean_restart_preserve_history() {
    let mut cluster = ProcessCluster::start("restart");
    let mut history = ProcessHistory::default();
    open_session(&mut cluster, 1, 1, 41);
    history.add(&mut cluster, 1, 1, 41, 1, 5);

    let leader = cluster.leader();
    let snapshot = cluster.request_on(leader, "SNAPSHOT 1 1");
    assert!(snapshot.starts_with("OK SNAPSHOT applied="), "{snapshot}");

    let addr = cluster.node_addr(leader);
    let in_flight = thread::spawn(move || {
        let mut connection =
            LineConnection::connect(addr, CONNECTION_TIMEOUTS).expect("leader connection opens");
        connection.request("ADD 1 1 41 1 2 7")
    });
    cluster.kill(leader);
    let first_observation = in_flight.join().expect("request thread does not panic");
    if let Ok(response) = &first_observation {
        assert!(
            response.starts_with("OK ADDED ")
                || response.starts_with("ERR UNKNOWN ")
                || response.starts_with("ERR NOT_COMMITTED "),
            "{response}"
        );
    }

    let retry = history.add(&mut cluster, 1, 1, 41, 2, 7);
    assert!(
        retry == "OK ADDED value=12" || retry == "OK REPLAY ADDED value=12",
        "{retry}"
    );
    history.add(&mut cluster, 1, 1, 41, 3, 3);

    cluster.restart(leader);
    cluster.wait_ready();
    cluster.wait_value(leader, 1, 1, 15);

    let clean = NODE_IDS
        .into_iter()
        .find(|candidate| *candidate != leader)
        .expect("a second node exists");
    cluster.clean_stop(clean);
    cluster.restart(clean);
    cluster.wait_ready();
    cluster.wait_value(clean, 1, 1, 15);

    assert_eq!(history.expected(1, 1), 15);
    cluster.wait_all_values(&std::collections::BTreeMap::from([((1, 1), 15)]));
    cluster.assert_audits();
}

#[test]
#[ignore = "real lifecycle fencing and late network traffic"]
fn removal_reopen_and_tombstone_fence_old_clients_and_peers() {
    let mut cluster = ProcessCluster::start("lifecycle");
    let mut history = ProcessHistory::default();
    open_session(&mut cluster, 7, 1, 57);
    history.add(&mut cluster, 7, 1, 57, 1, 9);
    history.assert_complete(&mut cluster);

    for response in cluster.request_each("DRAIN 7 1") {
        assert_eq!(response, "OK DRAIN group=7");
    }
    assert!(cluster
        .request_on(1, "VALUE 7 1")
        .starts_with("ERR LIFECYCLE Draining"));
    for node_id in NODE_IDS {
        cluster.wait_response(node_id, "REMOVE 7 1", "OK REMOVE group=7");
    }
    for response in cluster.request_each("REOPEN 7 1 4") {
        assert_eq!(response, "OK REOPEN group=7 incarnation=2");
    }
    cluster.wait_ready();
    assert_eq!(
        cluster.request_on(1, "VALUE 7 1"),
        "ERR STALE_INCARNATION current=2"
    );

    let baseline = cluster.refused_peer(1);
    send_stale_vote(cluster.scratch_path(), 1, 7, 1);
    cluster.wait_refused_peer_above(1, baseline);

    open_session(&mut cluster, 7, 2, 58);
    history.reset_group(7, 2);
    history.add(&mut cluster, 7, 2, 58, 1, 4);
    cluster.wait_all_values(&std::collections::BTreeMap::from([((7, 2), 4)]));

    for response in cluster.request_each("DRAIN 7 2") {
        assert_eq!(response, "OK DRAIN group=7");
    }
    for node_id in NODE_IDS {
        cluster.wait_response(node_id, "REMOVE 7 2", "OK REMOVE group=7");
    }
    for response in cluster.request_each("TOMBSTONE 7 2") {
        assert_eq!(response, "OK TOMBSTONE group=7");
    }
    for response in cluster.request_each("REOPEN 7 2 4") {
        assert_eq!(response, "ERR TOMBSTONED");
    }
    for node_id in NODE_IDS {
        cluster.clean_stop(node_id);
    }
    for node_id in NODE_IDS {
        cluster.restart(node_id);
    }
    cluster.wait_ready();
    for response in cluster.request_each("REOPEN 7 2 4") {
        assert_eq!(response, "ERR TOMBSTONED");
    }
    for response in cluster.request_each("VALUE 7 1") {
        assert_eq!(response, "ERR STALE_INCARNATION current=2");
    }
    for node_id in cluster.live_node_ids() {
        let status = cluster.request_on(node_id, "STATUS");
        assert_eq!(number_field(&status, "poisoned"), 0, "{status}");
    }
    cluster.assert_audits();
}

#[test]
#[ignore = "durable host identity corruption refusals"]
fn missing_application_records_never_rebootstrap_known_slots() {
    for state in ["active", "removed", "tombstoned"] {
        let mut cluster = ProcessCluster::start(&format!("missing-app-{state}"));
        match state {
            "active" => {}
            "removed" | "tombstoned" => {
                assert_eq!(cluster.request_on(1, "DRAIN 7 1"), "OK DRAIN group=7");
                cluster.wait_response(1, "REMOVE 7 1", "OK REMOVE group=7");
                if state == "tombstoned" {
                    assert_eq!(
                        cluster.request_on(1, "TOMBSTONE 7 1"),
                        "OK TOMBSTONE group=7"
                    );
                }
            }
            _ => unreachable!(),
        }
        cluster.kill(1);
        let record = cluster
            .scratch_path()
            .join("host-1/groups/7/app/state.rcap");
        fs::remove_file(&record)
            .unwrap_or_else(|error| panic!("could not remove {}: {error}", record.display()));
        cluster.restart_expect_fatal(1, "host registry proves this slot already exists");
    }
}

#[test]
#[ignore = "directed crash points in the removal filesystem transaction"]
fn retirement_intent_reconciles_every_directed_crash_point() {
    const FAILPOINTS: [&str; 9] = [
        "before_intent_publish",
        "after_intent_publish",
        "after_driver_detach",
        "before_raft_rename",
        "after_raft_rename",
        "after_parent_sync",
        "before_removed_publish",
        "after_removed_publish",
        "before_intent_cleanup",
    ];
    let mut cluster = ProcessCluster::start("retirement-failpoints");
    for (offset, failpoint) in FAILPOINTS.into_iter().enumerate() {
        let group = u32::try_from(offset + 1).expect("nine failpoints fit u32");
        cluster.clean_stop(1);
        cluster.restart_with_failpoint(1, Some(failpoint));
        cluster.wait_ready();
        assert_eq!(
            cluster.request_on(1, &format!("DRAIN {group} 1")),
            format!("OK DRAIN group={group}")
        );
        cluster.trigger_failpoint(1, &format!("REMOVE {group} 1"), failpoint);
        cluster.restart(1);
        cluster.wait_ready();

        let value = cluster.request_on(1, &format!("VALUE {group} 1"));
        if failpoint == "before_intent_publish" {
            assert_eq!(value, "ERR LIFECYCLE Draining");
            cluster.wait_response(
                1,
                &format!("REMOVE {group} 1"),
                &format!("OK REMOVE group={group}"),
            );
        } else {
            assert_eq!(value, "ERR LIFECYCLE Removed");
        }
        let group_dir = cluster
            .scratch_path()
            .join(format!("host-1/groups/{group}"));
        assert!(!group_dir.join("retirement.intent").exists());
        assert!(!group_dir.join("raft").exists());
        assert!(group_dir.join("raft.retired-1").is_dir());
    }
}

#[test]
#[ignore = "directed crash points in first bootstrap"]
fn bootstrap_intent_reconciles_every_directed_crash_point() {
    const FAILPOINTS: [&str; 11] = [
        "before_bootstrap_intent_publication",
        "after_bootstrap_intent_publication",
        "before_staged_raft_creation",
        "after_staged_raft_creation",
        "after_staged_raft_sync",
        "after_bootstrap_application_publication",
        "after_bootstrap_registry_publication",
        "before_activation_raft_rename",
        "after_activation_raft_rename",
        "after_activation_parent_sync",
        "before_bootstrap_intent_cleanup",
    ];
    for failpoint in FAILPOINTS {
        let mut cluster = ProcessCluster::start_after_bootstrap_failpoint(
            &format!("bootstrap-{failpoint}"),
            failpoint,
        );
        assert_eq!(
            cluster.request_on(1, "VALUE 1 1"),
            "OK VALUE group=1 incarnation=1 value=0 applied=0"
        );
        let groups_dir = cluster.scratch_path().join("host-1/groups");
        assert!(!groups_dir.join("bootstrap.intent").exists());
        for group in 1..=GROUP_COUNT {
            let slot_dir = groups_dir.join(group.to_string());
            assert!(slot_dir.join("raft").is_dir());
            assert!(!slot_dir.join("raft.activating-1").exists());
        }
    }
}

#[test]
#[ignore = "directed crash points in removed-slot activation"]
fn activation_intent_reconciles_every_directed_crash_point() {
    const FAILPOINTS: [&str; 11] = [
        "before_activation_intent_publication",
        "after_activation_intent_publication",
        "before_staged_raft_creation",
        "after_staged_raft_creation",
        "after_staged_raft_sync",
        "after_activation_application_publication",
        "after_activation_registry_publication",
        "before_activation_raft_rename",
        "after_activation_raft_rename",
        "after_activation_parent_sync",
        "before_activation_intent_cleanup",
    ];
    let mut cluster = ProcessCluster::start("activation-failpoints");
    for (offset, failpoint) in FAILPOINTS.into_iter().enumerate() {
        let group = u32::try_from(offset + 1).expect("eleven failpoints fit u32");
        for response in cluster.request_each(&format!("DRAIN {group} 1")) {
            assert_eq!(response, format!("OK DRAIN group={group}"));
        }
        for node_id in NODE_IDS {
            cluster.wait_response(
                node_id,
                &format!("REMOVE {group} 1"),
                &format!("OK REMOVE group={group}"),
            );
        }
        cluster.clean_stop(1);
        cluster.restart_with_failpoint(1, Some(failpoint));
        cluster.wait_ready();
        cluster.trigger_failpoint(1, &format!("REOPEN {group} 1 4"), failpoint);
        for node_id in [2, 3] {
            assert_eq!(
                cluster.request_on(node_id, &format!("REOPEN {group} 1 4")),
                format!("OK REOPEN group={group} incarnation=2")
            );
        }
        cluster.restart(1);

        if failpoint == "before_activation_intent_publication" {
            cluster.wait_response(
                1,
                &format!("REOPEN {group} 1 4"),
                &format!("OK REOPEN group={group} incarnation=2"),
            );
        }
        cluster.wait_ready();
        assert_eq!(
            cluster.request_on(1, &format!("VALUE {group} 2")),
            format!("OK VALUE group={group} incarnation=2 value=0 applied=0")
        );
        let group_dir = cluster
            .scratch_path()
            .join(format!("host-1/groups/{group}"));
        assert!(!group_dir.join("activation.intent").exists());
        assert!(!group_dir.join("raft.activating-2").exists());
        assert!(group_dir.join("raft").is_dir());
    }
}

#[test]
#[ignore = "accepted request recovery across directed draining crashes"]
fn draining_publication_crashes_preserve_durable_outstanding_work() {
    for failpoint in [
        "after_draining_application_publication",
        "after_draining_registry_publication",
    ] {
        let mut cluster = ProcessCluster::start(&format!("draining-{failpoint}"));
        let host = cluster.leader();
        let group = cluster.leader_group_on(host);
        cluster.arm_failpoint(host, failpoint);
        assert!(
            cluster
                .request_on(host, &format!("OPEN {group} 1 61 1"))
                .starts_with("OK SESSION "),
            "the selected host must lead the selected group"
        );
        assert_eq!(
            cluster.request_on(host, &format!("SLOW {group} 5000")),
            format!("OK SLOW group={group} milliseconds=5000")
        );
        let baseline = number_field(&cluster.request_on(host, "STATUS"), "client_admitted");
        let address = cluster.node_addr(host);
        let request = format!("ADD {group} 1 61 1 1 5");
        let in_flight_request = request.clone();
        let client = thread::spawn(move || {
            let mut connection = LineConnection::connect(address, CONNECTION_TIMEOUTS)
                .expect("leader connection opens");
            connection.request(&in_flight_request)
        });
        cluster.wait_status_above(host, "client_admitted", baseline);
        cluster.trigger_failpoint(host, &format!("DRAIN {group} 1"), failpoint);
        assert!(
            client
                .join()
                .expect("request thread does not panic")
                .is_err(),
            "the directed crash must sever the accepted request"
        );

        cluster.restart(host);
        cluster.wait_ready();
        assert_eq!(
            cluster.request_on(host, &request),
            "ERR NOT_COMMITTED PROCESS_RESTARTED"
        );
        cluster.wait_response(
            host,
            &format!("REMOVE {group} 1"),
            &format!("OK REMOVE group={group}"),
        );
    }
}

#[test]
#[ignore = "directed crashes around durable backpressure cancellation"]
fn backpressure_is_reported_only_after_durable_cancellation() {
    const REQUEST: &str = "ADD {group} 1 51 1 1 5";
    for failpoint in [
        "after_managed_refusal_before_durable_cancellation",
        "after_durable_cancellation_before_backpressure_response",
    ] {
        let mut cluster = ProcessCluster::start(&format!("backpressure-{failpoint}"));
        let host = cluster.leader();
        let group = cluster.leader_group_on(host);
        assert!(
            cluster
                .request_on(host, &format!("OPEN {group} 1 51 1"))
                .starts_with("OK SESSION "),
            "the selected host must lead the selected group"
        );
        let _blockers = occupy_all_workers(&mut cluster, host, group);
        fill_group_queue(&mut cluster, host, group);
        cluster.arm_failpoint(host, failpoint);
        let request = REQUEST.replace("{group}", &group.to_string());
        let address = cluster.node_addr(host);
        let client = thread::spawn(move || {
            let mut connection = LineConnection::connect(address, CONNECTION_TIMEOUTS)
                .expect("leader connection opens");
            connection.request(&request)
        });
        cluster.wait_for_failpoint_exit(host, failpoint);
        assert!(
            client
                .join()
                .expect("request thread does not panic")
                .is_err(),
            "neither crash boundary may expose a backpressure response"
        );

        cluster.restart(host);
        cluster.wait_ready();
        if failpoint == "after_managed_refusal_before_durable_cancellation" {
            let retry = cluster.request_leader(&REQUEST.replace("{group}", &group.to_string()));
            assert!(
                retry == "OK ADDED value=5" || retry == "OK REPLAY ADDED value=5",
                "{retry}"
            );
            cluster.wait_value(host, group, 1, 5);
        } else {
            cluster.wait_value(host, group, 1, 0);
            assert_eq!(
                cluster.request_on(host, &format!("DRAIN {group} 1")),
                format!("OK DRAIN group={group}")
            );
            cluster.wait_response(
                host,
                &format!("REMOVE {group} 1"),
                &format!("OK REMOVE group={group}"),
            );
        }
    }

    let mut cluster = ProcessCluster::start("backpressure-observed");
    let host = cluster.leader();
    let group = cluster.leader_group_on(host);
    assert!(
        cluster
            .request_on(host, &format!("OPEN {group} 1 51 1"))
            .starts_with("OK SESSION "),
        "the selected host must lead the selected group"
    );
    let _blockers = occupy_all_workers(&mut cluster, host, group);
    fill_group_queue(&mut cluster, host, group);
    let request = REQUEST.replace("{group}", &group.to_string());
    let response = cluster.request_on(host, &request);
    assert!(
        response.starts_with("ERR BACKPRESSURE GroupQueueFull"),
        "{response}"
    );
    cluster.kill(host);
    cluster.restart(host);
    cluster.wait_ready();
    cluster.wait_value(host, group, 1, 0);
}

#[test]
#[ignore = "reservation publication certainty across directed crash seams"]
fn reservation_publication_never_exposes_a_false_rejection() {
    for failpoint in [
        "before_state_rcap_rename",
        "after_state_rcap_rename",
        "before_state_rcap_parent_sync",
        "state_rcap_parent_sync_failure",
    ] {
        let mut cluster = ProcessCluster::start(&format!("reservation-{failpoint}"));
        let group = 1;
        let client = 52;
        assert!(cluster
            .request_leader(&format!("OPEN {group} 1 {client} 1"))
            .starts_with("OK SESSION "));
        let host = cluster.leader_for_group(group);
        let request = format!("ADD {group} 1 {client} 1 1 5");
        cluster.arm_failpoint(host, failpoint);
        cluster.trigger_failpoint(host, &request, failpoint);

        cluster.restart(host);
        cluster.wait_ready();
        let response = cluster.request_leader(&request);
        assert!(
            matches!(
                response.as_str(),
                "OK ADDED value=5" | "OK REPLAY ADDED value=5"
            ),
            "retry after {failpoint} must resolve exactly once: {response}"
        );
        cluster.wait_all_values(&std::collections::BTreeMap::from([((group, 1), 5)]));
    }
}

#[test]
#[ignore = "durable outstanding retries cannot receive NOT_COMMITTED"]
fn durable_outstanding_barrier_failure_remains_unknown_under_queue_pressure() {
    let mut cluster = ProcessCluster::start("durable-outstanding-barrier");
    let group = 1;
    let client = 53;
    assert!(cluster
        .request_leader(&format!("OPEN {group} 1 {client} 1"))
        .starts_with("OK SESSION "));
    let old_leader = cluster.leader_for_group(group);
    let request = format!("ADD {group} 1 {client} 1 1 5");
    cluster.arm_failpoint(
        old_leader,
        "after_reservation_publication_before_managed_admission",
    );
    cluster.trigger_failpoint(
        old_leader,
        &request,
        "after_reservation_publication_before_managed_admission",
    );
    let leader = cluster.leader_for_group_excluding(group, Some(old_leader));
    cluster.restart(old_leader);
    cluster.wait_status_at_least(old_leader, "recovery_deferred", 1);

    let _blockers = occupy_all_workers(&mut cluster, old_leader, group);
    fill_group_queue(&mut cluster, old_leader, group);
    let refused = number_field(
        &cluster.request_on(old_leader, "STATUS"),
        "recovery_refused",
    );
    cluster.wait_status_above(old_leader, "recovery_refused", refused);
    let status = cluster.request_on(old_leader, "STATUS");
    assert_eq!(number_field(&status, "durable_outstanding"), 1, "{status}");
    assert_eq!(number_field(&status, "pending_proposals"), 0, "{status}");
    assert_eq!(
        cluster.request_on(old_leader, &request),
        "ERR UNKNOWN accepted operation remains durable"
    );

    cluster.wait_response(leader, &request, "OK ADDED value=5");
    cluster.wait_value(leader, group, 1, 5);
}

#[test]
#[ignore = "snapshot replay must reconcile exact durable outstanding work"]
fn snapshot_replay_clears_exact_outstanding_before_the_next_admission() {
    let mut cluster = ProcessCluster::start("snapshot-replay-ledger-cleanup");
    let old_leader = 1;
    let group = cluster.leader_group_on(old_leader);
    let client = 58;
    assert_eq!(
        cluster.request_on(old_leader, &format!("OPEN {group} 1 {client} 1")),
        "OK SESSION opened"
    );
    let request = format!("ADD {group} 1 {client} 1 1 5");
    cluster.arm_failpoint(
        old_leader,
        "after_reservation_publication_before_managed_admission",
    );
    cluster.trigger_failpoint(
        old_leader,
        &request,
        "after_reservation_publication_before_managed_admission",
    );

    let leader = cluster.leader_for_group_excluding(group, Some(old_leader));
    cluster.wait_response(leader, &request, "OK ADDED value=5");
    let snapshot = cluster.request_on(leader, &format!("SNAPSHOT {group} 1"));
    assert!(snapshot.starts_with("OK SNAPSHOT applied="), "{snapshot}");

    cluster.restart(old_leader);
    cluster.wait_ready();
    cluster.wait_value(old_leader, group, 1, 5);
    let snapshot_dir = cluster
        .scratch_path()
        .join(format!("host-{old_leader}/groups/{group}/raft/snapshots"));
    assert!(
        fs::read_dir(&snapshot_dir)
            .unwrap_or_else(|error| panic!("could not inspect {}: {error}", snapshot_dir.display()))
            .next()
            .is_some(),
        "the lagging replica must catch up through the compacted snapshot"
    );
    let before = cluster.request_on(old_leader, "STATUS");
    assert_eq!(
        number_field(&before, "durable_outstanding"),
        1,
        "snapshot installation deliberately leaves the consumer ledger for authoritative reconciliation: {before}"
    );

    cluster.wait_status_at_least(old_leader, "recovery_deferred", 1);
    let deferred = cluster.request_on(old_leader, "STATUS");
    assert_eq!(
        number_field(&deferred, "pending_proposals"),
        0,
        "leadership transfer starts from a deferred, not locally queued, recovery: {deferred}"
    );
    assert_eq!(
        cluster.request_on(old_leader, "PAUSE_RECOVERY"),
        "OK RECOVERY paused"
    );
    assert_eq!(
        cluster.request_on(leader, &format!("TRANSFER {group} 1 {old_leader}"),),
        format!("OK TRANSFER target={old_leader}")
    );
    cluster.wait_for_group_leader(old_leader, group);
    cluster.wait_response(
        old_leader,
        &format!("ADD {group} 1 59 1 1 1"),
        "ERR SESSION_NOT_OPEN",
    );
    let _blockers = occupy_all_workers_for(&mut cluster, old_leader, group, 15_000);
    fill_group_queue(&mut cluster, old_leader, group);
    let refused = number_field(
        &cluster.request_on(old_leader, "STATUS"),
        "recovery_refused",
    );
    assert_eq!(
        cluster.request_on(old_leader, "RESUME_RECOVERY"),
        "OK RECOVERY resumed"
    );
    cluster.wait_status_above(old_leader, "recovery_refused", refused);
    assert_eq!(
        cluster.request_on(old_leader, "PAUSE_RECOVERY"),
        "OK RECOVERY paused"
    );
    let before_replay = cluster.request_on(old_leader, "STATUS");
    assert_eq!(
        number_field(&before_replay, "pending_proposals"),
        0,
        "queue-blocked recovery must leave no local proposal: {before_replay}"
    );
    let replay = cluster.request_on(old_leader, &request);
    assert_eq!(
        replay, "OK REPLAY ADDED value=5",
        "exact retry did not reach authoritative replay; status={before_replay}"
    );
    let reconciled = cluster.request_on(old_leader, "STATUS");
    assert_eq!(
        number_field(&reconciled, "durable_outstanding"),
        0,
        "the replay response must follow durable ledger cleanup: {reconciled}"
    );
    let next = cluster.request_on(old_leader, &format!("ADD {group} 1 {client} 1 2 1"));
    assert!(
        next.starts_with("ERR BACKPRESSURE ") || next == "OK ADDED value=6",
        "the next sequence must reach the real queue decision, not stale conflict: {next}"
    );
}

#[test]
#[ignore = "authoritative replay cleanup must publish before its response"]
fn authoritative_replay_cleanup_crash_severs_the_response() {
    let mut cluster = ProcessCluster::start("replay-cleanup-publication");
    let old_leader = 1;
    let group = cluster.leader_group_on(old_leader);
    let client = 61;
    assert!(cluster
        .request_on(old_leader, &format!("OPEN {group} 1 {client} 1"))
        .starts_with("OK SESSION "));
    let request = format!("ADD {group} 1 {client} 1 1 5");
    cluster.arm_failpoint(
        old_leader,
        "after_reservation_publication_before_managed_admission",
    );
    cluster.trigger_failpoint(
        old_leader,
        &request,
        "after_reservation_publication_before_managed_admission",
    );

    let leader = cluster.leader_for_group_excluding(group, Some(old_leader));
    cluster.wait_response(leader, &request, "OK ADDED value=5");
    let snapshot = cluster.request_on(leader, &format!("SNAPSHOT {group} 1"));
    assert!(snapshot.starts_with("OK SNAPSHOT applied="), "{snapshot}");
    cluster.restart(old_leader);
    cluster.wait_ready();
    cluster.wait_value(old_leader, group, 1, 5);
    cluster.wait_status_at_least(old_leader, "recovery_deferred", 1);
    assert_eq!(
        cluster.request_on(old_leader, "PAUSE_RECOVERY"),
        "OK RECOVERY paused"
    );
    assert_eq!(
        cluster.request_on(leader, &format!("TRANSFER {group} 1 {old_leader}"),),
        format!("OK TRANSFER target={old_leader}")
    );
    cluster.wait_for_group_leader(old_leader, group);
    cluster.wait_response(
        old_leader,
        &format!("ADD {group} 1 59 1 1 1"),
        "ERR SESSION_NOT_OPEN",
    );

    cluster.arm_failpoint(old_leader, "after_state_rcap_rename");
    cluster.trigger_failpoint(old_leader, &request, "after_state_rcap_rename");
    cluster.restart(old_leader);
    cluster.wait_ready();
    let status = cluster.request_on(old_leader, "STATUS");
    assert_eq!(number_field(&status, "durable_outstanding"), 0, "{status}");
    assert_eq!(number_field(&status, "poisoned"), 0, "{status}");
    let leader = cluster.leader_for_group(group);
    cluster.wait_response(leader, &request, "OK REPLAY ADDED value=5");
}

#[test]
#[ignore = "real client-range admission and restart boundary"]
fn client_range_is_refused_before_admission_and_survives_restart() {
    let mut cluster = ProcessCluster::start("client-range");
    let group = 1;
    for client in 0..64 {
        let response = cluster.request_leader(&format!("OPEN {group} 1 {client} 1"));
        assert!(
            response.starts_with("OK SESSION "),
            "in-range client {client} must open: {response}"
        );
    }
    let leader = cluster.leader_for_group(group);
    let before = cluster.request_on(leader, "STATUS");
    let value_before = cluster.request_on(leader, &format!("VALUE {group} 1"));
    assert_eq!(
        cluster.request_on(leader, &format!("OPEN {group} 1 64 1")),
        "ERR CLIENT_OUT_OF_RANGE"
    );
    let after = cluster.request_on(leader, "STATUS");
    let value_after = cluster.request_on(leader, &format!("VALUE {group} 1"));
    assert_eq!(
        number_field(&after, "client_admitted"),
        number_field(&before, "client_admitted"),
        "the 65th client cannot enter managed admission"
    );
    assert_eq!(
        number_field(&value_after, "applied"),
        number_field(&value_before, "applied"),
        "the 65th client cannot enter the Raft log"
    );

    cluster.kill(leader);
    cluster.restart(leader);
    cluster.wait_ready();
    let status = cluster.request_on(leader, "STATUS");
    assert_eq!(number_field(&status, "poisoned"), 0, "{status}");
}

#[test]
#[ignore = "linearized policy gate under saturated managed queues"]
fn linearized_policy_refusals_outrank_saturated_queues() {
    let mut cluster = ProcessCluster::start("policy-before-queues");
    let group = 1;
    assert!(cluster
        .request_leader(&format!("OPEN {group} 1 1 2"))
        .starts_with("OK SESSION "));
    assert_eq!(
        cluster.request_leader(&format!("ADD {group} 1 1 2 1 1")),
        "OK ADDED value=1"
    );
    assert_eq!(
        cluster.request_leader(&format!("ADD {group} 1 1 2 2 1")),
        "OK ADDED value=2"
    );
    let host = cluster.leader_for_group(group);
    let _blockers = occupy_all_workers(&mut cluster, host, group);
    fill_global_queue(&mut cluster, host);
    let before = cluster.request_on(host, "STATUS");
    assert_eq!(number_field(&before, "queued"), 1024, "{before}");

    let cases = [
        (format!("OPEN {group} 1 64 1"), "ERR CLIENT_OUT_OF_RANGE"),
        (format!("ADD {group} 1 64 1 1 1"), "ERR CLIENT_OUT_OF_RANGE"),
        (format!("READ {group} 1 64 1 1"), "ERR CLIENT_OUT_OF_RANGE"),
        (format!("OPEN {group} 1 1 1"), "ERR STALE_SESSION current=2"),
        (format!("ADD {group} 1 2 1 1 1"), "ERR SESSION_NOT_OPEN"),
        (
            format!("ADD {group} 1 1 1 3 1"),
            "ERR STALE_SESSION current=2",
        ),
        (
            format!("ADD {group} 1 1 3 3 1"),
            "ERR FUTURE_SESSION current=2",
        ),
        (
            format!("ADD {group} 1 1 2 1 1"),
            "ERR STALE_SEQUENCE highest=2",
        ),
        (
            format!("ADD {group} 1 1 2 4 1"),
            "ERR SEQUENCE_GAP expected=3",
        ),
        (format!("ADD {group} 1 1 2 2 2"), "ERR CONFLICTING_RETRY"),
        (format!("ADD {group} 1 1 2 2 1"), "OK REPLAY ADDED value=2"),
    ];
    for (request, expected) in cases {
        assert_eq!(cluster.request_on(host, &request), expected, "{request}");
    }
    let after = cluster.request_on(host, "STATUS");
    assert_eq!(
        number_field(&after, "admitted"),
        number_field(&before, "admitted"),
        "policy decisions and exact replay consume no scheduler identity"
    );
    assert_eq!(
        number_field(&after, "client_admitted"),
        number_field(&before, "client_admitted"),
        "policy decisions and exact replay admit no client proposal"
    );
}

fn assert_admission_pending_precedence(cluster: &mut ProcessCluster, group: u32, client: u32) {
    let host = cluster.leader_for_group(group);
    assert_eq!(cluster.request_on(host, "PAUSE_PEERS"), "OK PEERS paused");
    let address = cluster.node_addr(host);
    let primary = thread::spawn(move || {
        let mut connection =
            LineConnection::connect(address, CONNECTION_TIMEOUTS).expect("leader connection opens");
        connection.request(&format!("ADD {group} 1 {client} 1 4 1"))
    });
    cluster.wait_status_at_least(host, "admission_reads", 1);
    let alternatives = [
        (
            format!("ADD {group} 1 {client} 1 3 1"),
            "OK REPLAY ADDED value=3",
        ),
        (
            format!("ADD {group} 1 {client} 1 3 2"),
            "ERR CONFLICTING_RETRY",
        ),
        (
            format!("ADD {group} 1 {client} 1 2 1"),
            "ERR STALE_SEQUENCE highest=3",
        ),
        (
            format!("OPEN {group} 1 {client} 1"),
            "OK SESSION already_open",
        ),
    ]
    .into_iter()
    .map(|(request, expected)| {
        let address = cluster.node_addr(host);
        (
            thread::spawn(move || {
                let mut connection = LineConnection::connect(address, CONNECTION_TIMEOUTS)
                    .expect("leader connection opens");
                connection.request(&request)
            }),
            expected,
        )
    })
    .collect::<Vec<_>>();
    cluster.wait_status_at_least(host, "admission_candidates", 5);
    assert_eq!(
        number_field(&cluster.request_on(host, "STATUS"), "admission_reads"),
        1,
        "one client owns one shared authoritative barrier"
    );
    assert_eq!(cluster.request_on(host, "RESUME_PEERS"), "OK PEERS resumed");
    for (request, expected) in alternatives {
        assert_eq!(
            request
                .join()
                .expect("alternative request thread does not panic")
                .expect("alternative request receives a response"),
            expected
        );
    }
    assert_eq!(
        primary
            .join()
            .expect("primary admission thread does not panic")
            .expect("primary admission receives a response"),
        "OK ADDED value=4"
    );
}

#[test]
#[ignore = "completed history outranks managed and admission-pending work"]
fn completed_history_outranks_different_local_pending_operations() {
    let mut cluster = ProcessCluster::start("completed-before-pending");
    let group = 1;
    let client = 54;
    assert!(cluster
        .request_leader(&format!("OPEN {group} 1 {client} 1"))
        .starts_with("OK SESSION "));
    for (sequence, value) in [(1, 1), (2, 2)] {
        assert_eq!(
            cluster.request_leader(&format!("ADD {group} 1 {client} 1 {sequence} 1")),
            format!("OK ADDED value={value}")
        );
    }

    let host = cluster.leader_for_group(group);
    assert_eq!(
        cluster.request_on(host, &format!("SLOW {group} 2000")),
        format!("OK SLOW group={group} milliseconds=2000")
    );
    let admitted = number_field(&cluster.request_on(host, "STATUS"), "client_admitted");
    let address = cluster.node_addr(host);
    let pending = thread::spawn(move || {
        let mut connection =
            LineConnection::connect(address, CONNECTION_TIMEOUTS).expect("leader connection opens");
        connection.request(&format!("ADD {group} 1 {client} 1 3 1"))
    });
    cluster.wait_status_above(host, "client_admitted", admitted);
    cluster.wait_status_at_least(host, "workers", 1);
    assert_eq!(
        cluster.request_on(host, &format!("ADD {group} 1 {client} 1 2 1")),
        "OK REPLAY ADDED value=2"
    );
    assert_eq!(
        cluster.request_on(host, &format!("ADD {group} 1 {client} 1 2 2")),
        "ERR CONFLICTING_RETRY"
    );
    assert_eq!(
        cluster.request_on(host, &format!("ADD {group} 1 {client} 1 1 1")),
        "ERR STALE_SEQUENCE highest=2"
    );
    assert_eq!(
        cluster.request_on(host, &format!("OPEN {group} 1 {client} 1")),
        "OK SESSION already_open"
    );
    let pending_response = pending
        .join()
        .expect("pending proposal thread does not panic")
        .expect("pending proposal receives a response");
    assert!(
        pending_response == "OK ADDED value=3"
            || pending_response.starts_with("ERR UNKNOWN ")
            || pending_response.starts_with("ERR NOT_COMMITTED "),
        "the delayed proposal returned an unexpected outcome: {pending_response}"
    );
    assert_eq!(
        cluster.request_on(host, &format!("SLOW {group} 0")),
        format!("OK SLOW group={group} milliseconds=0")
    );
    if pending_response != "OK ADDED value=3" {
        let retry = cluster.request_leader(&format!("ADD {group} 1 {client} 1 3 1"));
        assert!(
            matches!(
                retry.as_str(),
                "OK ADDED value=3" | "OK REPLAY ADDED value=3"
            ),
            "the exact unknown retry must resolve: {retry}"
        );
    }
    let leader = cluster.leader_for_group(group);
    cluster.wait_value(leader, group, 1, 3);
    assert_admission_pending_precedence(&mut cluster, group, client);
}

#[test]
#[ignore = "draining responses follow durable lifecycle publication"]
fn draining_publication_precedes_pending_admission_responses() {
    for (failpoint, expected) in [
        ("before_draining_application_publication", "OK VALUE"),
        (
            "after_draining_application_publication",
            "ERR LIFECYCLE Draining",
        ),
    ] {
        let mut cluster = ProcessCluster::start(&format!("pending-drain-{failpoint}"));
        let group = 1;
        let host = cluster.leader_for_group(group);
        assert_eq!(cluster.request_on(host, "PAUSE_PEERS"), "OK PEERS paused");
        let address = cluster.node_addr(host);
        let pending = thread::spawn(move || {
            let mut connection = LineConnection::connect(address, CONNECTION_TIMEOUTS)
                .expect("leader connection opens");
            connection.request(&format!("OPEN {group} 1 55 1"))
        });
        cluster.wait_status_at_least(host, "admission_reads", 1);
        cluster.arm_failpoint(host, failpoint);
        cluster.trigger_failpoint(host, &format!("DRAIN {group} 1"), failpoint);
        assert!(
            pending
                .join()
                .expect("pending admission thread does not panic")
                .is_err(),
            "a crash at {failpoint} cannot expose a lifecycle response"
        );

        cluster.restart(host);
        if failpoint == "before_draining_application_publication" {
            cluster.wait_ready();
        }
        let response = cluster.request_on(host, &format!("VALUE {group} 1"));
        assert!(
            response.starts_with(expected),
            "{failpoint} recovered unexpected lifecycle: {response}"
        );
    }

    let mut cluster = ProcessCluster::start("pending-drain-success");
    let group = 1;
    let host = cluster.leader_for_group(group);
    assert_eq!(cluster.request_on(host, "PAUSE_PEERS"), "OK PEERS paused");
    let address = cluster.node_addr(host);
    let pending = thread::spawn(move || {
        let mut connection =
            LineConnection::connect(address, CONNECTION_TIMEOUTS).expect("leader connection opens");
        connection.request(&format!("OPEN {group} 1 55 1"))
    });
    cluster.wait_status_at_least(host, "admission_reads", 1);
    assert_eq!(
        cluster.request_on(host, &format!("DRAIN {group} 1")),
        format!("OK DRAIN group={group}")
    );
    assert_eq!(
        pending
            .join()
            .expect("pending admission thread does not panic")
            .expect("published draining returns a response"),
        "ERR LIFECYCLE Draining"
    );
    cluster.kill(host);
    cluster.restart(host);
    assert_eq!(
        cluster.request_on(host, &format!("VALUE {group} 1")),
        "ERR LIFECYCLE Draining"
    );
}

#[test]
#[ignore = "stale replica replay requires quorum-confirmed authority"]
fn stale_replica_never_replays_from_its_local_cache() {
    let mut cluster = ProcessCluster::start("stale-replay");
    let group = 1;
    assert!(cluster
        .request_leader(&format!("OPEN {group} 1 7 1"))
        .starts_with("OK SESSION "));
    assert_eq!(
        cluster.request_leader(&format!("ADD {group} 1 7 1 1 1")),
        "OK ADDED value=1"
    );
    for node in NODE_IDS {
        cluster.wait_value(node, group, 1, 1);
    }

    let stale = cluster.leader_for_group(group);
    assert_eq!(cluster.request_on(stale, "PAUSE_PEERS"), "OK PEERS paused");
    let leader = cluster.leader_for_group_excluding(group, Some(stale));
    cluster.wait_response(
        leader,
        &format!("ADD {group} 1 7 1 2 1"),
        "OK ADDED value=2",
    );
    let stale_retry = cluster.request_on(stale, &format!("ADD {group} 1 7 1 1 1"));
    assert!(
        !stale_retry.starts_with("OK "),
        "a stale local cache cannot authorize replay: {stale_retry}"
    );

    assert_eq!(
        cluster.request_on(stale, "RESUME_PEERS"),
        "OK PEERS resumed"
    );
    cluster.wait_value(stale, group, 1, 2);
    let leader = cluster.leader_for_group(group);
    cluster.wait_response(
        leader,
        &format!("ADD {group} 1 7 1 1 1"),
        "ERR STALE_SEQUENCE highest=2",
    );
}

#[test]
#[ignore = "non-injected fatal apply errors publish durable quarantine"]
fn non_synthetic_apply_failure_is_durably_quarantined() {
    let mut cluster = ProcessCluster::start("capacity-fault-quarantine");
    let host = cluster.leader();
    let group = cluster.leader_group_on(host);
    cluster.arm_failpoint(host, "after_poison_publication_before_driver_error");
    let address = cluster.node_addr(host);
    let fault = thread::spawn(move || {
        let mut connection =
            LineConnection::connect(address, CONNECTION_TIMEOUTS).expect("leader connection opens");
        connection.request(&format!("FAULT_CAPACITY {group} 1"))
    });
    cluster.wait_for_failpoint_exit(host, "after_poison_publication_before_driver_error");
    assert!(
        fault.join().expect("fault thread does not panic").is_err(),
        "the durable generic-poison boundary severs the fault request"
    );

    cluster.restart(host);
    cluster.wait_ready();
    let status = cluster.request_on(host, "STATUS");
    assert_eq!(number_field(&status, "poisoned"), 1, "{status}");
    assert_eq!(
        cluster.request_on(host, &format!("VALUE {group} 1")),
        "ERR GROUP_POISONED"
    );
    let healthy = (1..=GROUP_COUNT)
        .find(|candidate| *candidate != group)
        .expect("another group exists");
    cluster.wait_value(host, healthy, 1, 0);
    assert_eq!(
        cluster.request_on(host, &format!("DRAIN {group} 1")),
        format!("OK DRAIN group={group}")
    );
    cluster.wait_response(
        host,
        &format!("REMOVE {group} 1"),
        &format!("OK REMOVE group={group}"),
    );
}

#[test]
#[ignore = "accepted session-open recovery across a draining crash"]
fn session_open_drain_restart_has_a_durable_terminal_outcome() {
    let mut cluster = ProcessCluster::start("session-open-drain");
    let host = cluster.leader();
    let group = cluster.leader_group_on(host);
    let _blockers = occupy_all_workers(&mut cluster, host, group);
    let baseline = number_field(&cluster.request_on(host, "STATUS"), "client_admitted");
    let request = format!("OPEN {group} 1 62 1");
    let address = cluster.node_addr(host);
    let open_request = request.clone();
    let client = thread::spawn(move || {
        let mut connection =
            LineConnection::connect(address, CONNECTION_TIMEOUTS).expect("leader connection opens");
        connection.request(&open_request)
    });
    cluster.wait_status_above(host, "client_admitted", baseline);
    cluster.arm_failpoint(host, "after_draining_application_publication");
    cluster.trigger_failpoint(
        host,
        &format!("DRAIN {group} 1"),
        "after_draining_application_publication",
    );
    assert!(
        client
            .join()
            .expect("request thread does not panic")
            .is_err(),
        "the draining crash must sever the accepted session request"
    );

    cluster.restart(host);
    cluster.wait_ready();
    assert_eq!(
        cluster.request_on(host, &request),
        "ERR NOT_COMMITTED PROCESS_RESTARTED"
    );
    cluster.wait_response(
        host,
        &format!("REMOVE {group} 1"),
        &format!("OK REMOVE group={group}"),
    );
}

#[test]
#[ignore = "durable poison quarantine before an explicit drain"]
fn pre_drain_poison_restart_quarantines_only_the_failed_group() {
    let mut cluster = ProcessCluster::start("pre-drain-poison");
    let host = cluster.leader();
    let group = cluster.leader_group_on(host);
    for client in [62, 63] {
        assert!(
            cluster
                .request_on(host, &format!("OPEN {group} 1 {client} 1"))
                .starts_with("OK SESSION "),
            "the selected host must lead the selected group"
        );
    }
    assert_eq!(
        cluster.request_on(host, &format!("SLOW {group} 250")),
        format!("OK SLOW group={group} milliseconds=250")
    );
    cluster.arm_failpoint(host, "after_poison_publication_before_driver_error");
    let baseline = number_field(&cluster.request_on(host, "STATUS"), "admitted");
    let address = cluster.node_addr(host);
    let fault = thread::spawn(move || {
        let mut connection =
            LineConnection::connect(address, CONNECTION_TIMEOUTS).expect("leader connection opens");
        connection.request(&format!("FAULT {group} 1"))
    });
    cluster.wait_status_at_least(host, "workers", 1);
    let requests = [62, 63]
        .into_iter()
        .map(|client| format!("ADD {group} 1 {client} 1 1 1"))
        .collect::<Vec<_>>();
    let clients = requests
        .iter()
        .cloned()
        .map(|request| {
            let address = cluster.node_addr(host);
            thread::spawn(move || {
                let mut connection = LineConnection::connect(address, CONNECTION_TIMEOUTS)
                    .expect("leader connection opens");
                connection.request(&request)
            })
        })
        .collect::<Vec<_>>();
    cluster.wait_status_at_least(host, "admitted", baseline + 3);
    cluster.wait_status_at_least(host, "queued", 2);
    assert_eq!(
        cluster.request_on(host, &format!("SLOW {group} 0")),
        format!("OK SLOW group={group} milliseconds=0")
    );
    cluster.wait_for_failpoint_exit(host, "after_poison_publication_before_driver_error");
    assert!(
        fault.join().expect("fault thread does not panic").is_err(),
        "the durable poison boundary severs the fault request"
    );
    for client in clients {
        assert!(
            client
                .join()
                .expect("counter request thread does not panic")
                .is_err(),
            "the durable poison boundary severs accepted work"
        );
    }

    cluster.restart(host);
    let status = cluster.request_on(host, "STATUS");
    assert_eq!(number_field(&status, "poisoned"), 1, "{status}");
    assert_eq!(
        cluster.request_on(host, &format!("VALUE {group} 1")),
        "ERR GROUP_POISONED",
        "durable poison must not silently become a drain transition"
    );
    let healthy = (1..=GROUP_COUNT)
        .find(|candidate| *candidate != group)
        .expect("another group exists");
    cluster.wait_value(host, healthy, 1, 0);
    for request in &requests {
        let outcome = cluster.wait_response_one_of(
            host,
            request,
            &[
                "ERR NOT_COMMITTED GROUP_POISONED",
                "ERR UNKNOWN GROUP_POISONED",
            ],
        );
        assert!(
            outcome.contains("GROUP_POISONED"),
            "accepted work receives a durable typed poison outcome"
        );
    }
    assert_eq!(
        cluster.request_on(host, &format!("DRAIN {group} 1")),
        format!("OK DRAIN group={group}")
    );
    cluster.wait_response(
        host,
        &format!("REMOVE {group} 1"),
        &format!("OK REMOVE group={group}"),
    );
}

fn wait_for_poison_outcomes(
    cluster: &mut ProcessCluster,
    host: u64,
    requests: &[String],
) -> Vec<String> {
    let expected = [
        "ERR NOT_COMMITTED GROUP_POISONED",
        "ERR UNKNOWN GROUP_POISONED",
    ];
    let outcomes = requests
        .iter()
        .map(|request| cluster.wait_response_one_of(host, request, &expected))
        .collect::<Vec<_>>();
    assert!(
        outcomes
            .iter()
            .any(|outcome| outcome == "ERR NOT_COMMITTED GROUP_POISONED"),
        "the fixture must leave scheduler-queued work behind the poisoned driver"
    );
    outcomes
}

#[test]
#[ignore = "poisoned queue retirement crash boundaries"]
fn poisoned_queue_crashes_preserve_typed_terminal_failures() {
    const FAILPOINTS: [&str; 4] = [
        "before_queued_retirement",
        "after_queued_retirement_before_durable_failure_publication",
        "midway_through_queued_retirement",
        "after_durable_failure_publication",
    ];
    for failpoint in FAILPOINTS {
        eprintln!("checking poison retirement failpoint {failpoint}");
        let mut cluster = ProcessCluster::start(&format!("poison-retirement-{failpoint}"));
        let host = cluster.leader();
        let group = cluster.leader_group_on(host);
        for client in 40..=51 {
            assert!(
                cluster
                    .request_on(host, &format!("OPEN {group} 1 {client} 1"))
                    .starts_with("OK SESSION "),
                "the selected host must lead the selected group"
            );
        }
        let blockers = (1..=GROUP_COUNT)
            .filter(|candidate| *candidate != group)
            .take(4)
            .collect::<Vec<_>>();
        for blocker in &blockers {
            assert_eq!(
                cluster.request_on(host, &format!("SLOW {blocker} 250")),
                format!("OK SLOW group={blocker} milliseconds=250")
            );
            assert!(cluster
                .request_on(host, &format!("PRESSURE {blocker} 1 bulk 1"))
                .starts_with("OK PRESSURE "));
        }
        cluster.wait_status_at_least(host, "workers", 4);
        for blocker in blockers {
            assert_eq!(
                cluster.request_on(host, &format!("SLOW {blocker} 0")),
                format!("OK SLOW group={blocker} milliseconds=0")
            );
        }
        cluster.arm_failpoint(host, failpoint);
        let baseline = number_field(&cluster.request_on(host, "STATUS"), "admitted");
        let address = cluster.node_addr(host);
        let fault = thread::spawn(move || {
            let mut connection = LineConnection::connect(address, CONNECTION_TIMEOUTS)
                .expect("leader connection opens");
            connection.request(&format!("FAULT {group} 1"))
        });
        cluster.wait_status_at_least(host, "admitted", baseline + 1);

        let requests = (40..=51)
            .map(|client| format!("ADD {group} 1 {client} 1 1 1"))
            .collect::<Vec<_>>();
        let clients = requests
            .iter()
            .cloned()
            .map(|request| {
                let address = cluster.node_addr(host);
                thread::spawn(move || {
                    let mut connection = LineConnection::connect(address, CONNECTION_TIMEOUTS)
                        .expect("leader connection opens");
                    connection.request(&request)
                })
            })
            .collect::<Vec<_>>();
        cluster.wait_status_at_least(host, "admitted", baseline + 13);
        let fault_result = fault.join().expect("fault request thread does not panic");
        let fault_response =
            fault_result.expect("the fault request must receive the poisoned response");
        assert!(
            matches!(
                fault_response.as_str(),
                "ERR NOT_COMMITTED GROUP_POISONED" | "ERR UNKNOWN client deadline elapsed"
            ),
            "the accepted fault returned an unexpected response: {fault_response}",
        );
        cluster.wait_status_above(host, "poisoned", 0);
        cluster.trigger_failpoint(host, &format!("DRAIN {group} 1"), failpoint);
        for client in clients {
            let _ = client
                .join()
                .expect("counter request thread does not panic");
        }

        cluster.restart(host);
        cluster.wait_response(
            host,
            &format!("DRAIN {group} 1"),
            &format!("OK DRAIN group={group}"),
        );
        let outcomes = wait_for_poison_outcomes(&mut cluster, host, &requests);
        cluster.wait_response(
            host,
            &format!("REMOVE {group} 1"),
            &format!("OK REMOVE group={group}"),
        );
        cluster.kill(host);
        cluster.restart(host);
        for (request, outcome) in requests.iter().zip(outcomes) {
            cluster.wait_response(host, request, &outcome);
        }
    }
}

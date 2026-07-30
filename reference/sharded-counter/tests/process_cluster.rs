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
        poisoned.starts_with("OK ") || poisoned.starts_with("ERR UNKNOWN "),
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
    open_session(&mut cluster, 7, 1, 77);
    history.add(&mut cluster, 7, 1, 77, 1, 9);
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

    open_session(&mut cluster, 7, 2, 78);
    history.reset_group(7, 2);
    history.add(&mut cluster, 7, 2, 78, 1, 4);
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

//! Published-to-candidate rolling upgrades over the existing authenticated link.
//!
//! `scripts/rolling-upgrade-check` supplies two independently built archive
//! consumers. No dependency fallback or same-version substitute is accepted.

#[allow(
    dead_code,
    reason = "this suite uses a subset of the shared process harness"
)]
#[path = "support/process.rs"]
mod process;
#[allow(
    dead_code,
    reason = "this suite uses a subset of the shared production harness"
)]
#[path = "support/production_process.rs"]
mod production_process;
#[path = "support/rolling_client.rs"]
mod rolling_client;
#[path = "support/rolling_crash.rs"]
mod rolling_crash;
#[path = "support/scratch.rs"]
mod scratch;
#[allow(
    dead_code,
    reason = "this suite uses a subset of the shared command builders"
)]
mod support;

use std::path::PathBuf;

use rafter::{LogIndex, NodeId};
use rafter_reference_fenced_lock::{
    check_linearizable_with_budget,
    production::{allocate_replica, retire_replica, AllocationCrashPoint, ReplicaIdentity},
    ApplyDisposition, Command, FencingToken, OperationResult, SearchBudget,
    MAX_SEARCH_CONFIGURATIONS,
};

use process::SubmitOutcome;
use production_process::{wait_until, ProductionCluster, ProductionNode, NODE_IDS};
use rolling_client::{UpgradeClient, HISTORY_LIMIT};
use support::{acquire, config, open_session, renew, resource, submit};

#[test]
#[ignore = "requires published and candidate archive binaries; use scripts/rolling-upgrade-check"]
fn published_nodes_roll_to_candidate_with_tls_snapshots_and_membership() {
    let baseline = binary("RAFTER_BASELINE_NODE");
    let candidate = binary("RAFTER_CANDIDATE_NODE");
    assert_ne!(
        baseline, candidate,
        "the two archive roles must use distinct binaries"
    );
    assert_eq!(
        candidate,
        PathBuf::from(env!("CARGO_BIN_EXE_lock-production-node"))
            .canonicalize()
            .unwrap()
    );
    let replacements = rolling_crash::Replacements::from_env();
    let bounds = config(4, 4);
    let mut cluster = ProductionCluster::start_with_binary("rolling-upgrade", bounds, &baseline);
    assert_eq!(cluster.wait_for_leader(), NodeId(1));
    assert!(matches!(
        cluster.submit_to_leader(open_session(0, 1)),
        SubmitOutcome::Applied {
            disposition: ApplyDisposition::SessionOpened,
            ..
        }
    ));
    let token = cluster
        .submit_to_leader(submit(0, 1, 1, acquire("vault", 1000)))
        .acquired_token();
    let mut sequence = 1;
    let mut client = UpgradeClient::default();
    observe(&mut cluster, "all-published", token);

    // The old leader publishes and compacts while replica 3 is absent. A new
    // binary must fetch that snapshot, not recover by replaying the removed log.
    replacements.stop(&mut cluster, NodeId(3), "published-to-candidate");
    let old_floor = advance(&mut cluster, &mut client, token, &mut sequence);
    let old_boundary = cold_checkpoint(&mut cluster, &baseline);
    assert!(old_boundary > old_floor);
    cluster.restart_with_binary(NodeId(3), &NODE_IDS, &candidate);
    require_snapshot(&mut cluster, NodeId(3), old_boundary);
    observe(&mut cluster, "candidate-received-published-snapshot", token);

    // Stop the old leader and keep serving with an old/new quorum before its
    // replacement reopens exactly the durable directory it left behind.
    replacements.stop(&mut cluster, NodeId(1), "published-to-candidate");
    let last = write(&mut cluster, &mut client, token, &mut sequence);
    cluster.restart_with_binary(NodeId(1), &NODE_IDS, &candidate);
    require_replay(&mut cluster, &mut client, last);
    observe(&mut cluster, "leader-upgraded", token);

    // Now require the reverse direction: the remaining published replica is
    // behind a snapshot emitted by a candidate leader.
    cluster.stop_checked(NodeId(2));
    assert_ne!(cluster.wait_for_leader(), NodeId(2));
    let candidate_floor = advance(&mut cluster, &mut client, token, &mut sequence);
    let candidate_boundary = cold_checkpoint(&mut cluster, &candidate);
    assert!(candidate_boundary > candidate_floor);
    cluster.restart_with_binary(NodeId(2), &NODE_IDS, &baseline);
    require_snapshot(&mut cluster, NodeId(2), candidate_boundary);
    observe(&mut cluster, "published-received-candidate-snapshot", token);

    change_membership_while_mixed(&mut cluster, &candidate);
    let last = write(&mut cluster, &mut client, token, &mut sequence);
    replacements.stop(&mut cluster, NodeId(2), "published-to-candidate");
    cluster.restart_with_binary(NodeId(2), &NODE_IDS, &candidate);
    require_replay(&mut cluster, &mut client, last);
    observe(&mut cluster, "all-candidate", token);

    // A compacted membership history must keep its retirement policy after
    // each candidate restarts. No repair, reseed, or state copying is allowed.
    checkpoint(&mut cluster);
    // Exercise uncertain-outcome resolution on every run without inventing an
    // application result or assuming whether the first attempt committed.
    client.lose_next_reply();
    for node in NODE_IDS {
        replacements.stop(&mut cluster, node, "candidate-restart");
        cluster.restart_with_binary(node, &NODE_IDS, &candidate);
        write(&mut cluster, &mut client, token, &mut sequence);
        wait_until("retired identity high-water to survive restart", || {
            (cluster
                .observe(node)
                .optional_number("committed_id_high_water")
                == Some(4))
            .then_some(())
        });
    }
    observe(&mut cluster, "candidate-restarts-complete", token);
    assert!(
        client.unknown_outcomes() >= 1,
        "the lost reply must be retried"
    );
    let budget = SearchBudget::new(MAX_SEARCH_CONFIGURATIONS)
        .unwrap()
        .with_operation_limit(HISTORY_LIMIT)
        .unwrap();
    let report = check_linearizable_with_budget(bounds, cluster.history(), budget)
        .expect("mixed-version lock history, including unknown attempts, is linearizable");
    assert_eq!(report.searched_operations(), 24 + client.unknown_outcomes());
    println!(
        "UPGRADE_HISTORY searched={} discharged={} events={:?}",
        report.searched_operations(),
        report.discharged_operations(),
        cluster.history()
    );
    for node in cluster.live_nodes() {
        cluster.stop_checked(node);
    }
    if let Some(destination) = std::env::var_os("RAFTER_UPGRADE_STATE") {
        copy_state(cluster.root(), &PathBuf::from(destination));
    }
    println!("UPGRADE_COMPLETE published-to-candidate tls snapshots-both-directions membership durable-restarts");
}

fn binary(variable: &str) -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os(variable).unwrap_or_else(|| panic!("{variable} is required")),
    );
    assert!(
        path.is_absolute() && path.is_file(),
        "{variable} names a real archive binary"
    );
    path.canonicalize().unwrap()
}

fn write(
    cluster: &mut ProductionCluster,
    client: &mut UpgradeClient,
    token: FencingToken,
    sequence: &mut u64,
) -> Command {
    *sequence += 1;
    let command = submit(
        0,
        1,
        *sequence,
        renew("vault", token.get(), 1000 + *sequence),
    );
    let outcome = client.submit(cluster, command);
    assert!(
        matches!(outcome, SubmitOutcome::Applied { .. }),
        "renewal sequence {sequence} did not settle: {outcome:?}"
    );
    assert!(matches!(
        outcome.operation(),
        OperationResult::Renewed { .. }
    ));
    assert_eq!(
        cluster.query(resource("vault")).view().held_token,
        Some(token.get())
    );
    command
}

fn advance(
    cluster: &mut ProductionCluster,
    client: &mut UpgradeClient,
    token: FencingToken,
    sequence: &mut u64,
) -> LogIndex {
    let leader = cluster.wait_for_leader();
    let before = LogIndex(cluster.observe(leader).number("applied_index"));
    write(cluster, client, token, sequence);
    before
}

fn checkpoint(cluster: &mut ProductionCluster) -> LogIndex {
    let response = cluster.ask_leader("CHECKPOINT");
    let index = LogIndex(
        response
            .strip_prefix("OK CHECKPOINT ")
            .unwrap_or_else(|| panic!("checkpoint was refused: {response}"))
            .parse()
            .unwrap(),
    );
    let leader = cluster.wait_for_leader();
    assert_eq!(cluster.observe(leader).number("snapshot_index"), index.0);
    index
}

// Buffered pre-compaction AppendEntries may otherwise let a returning peer
// catch up without ever requesting a snapshot. Compact every possible sender
// and cold-restart that cohort to remove this alternate path. This is an
// explicit recovery phase, not a claim of uninterrupted quorum availability.
fn cold_checkpoint(cluster: &mut ProductionCluster, binary: &std::path::Path) -> LogIndex {
    let leader = cluster.wait_for_leader();
    let floor = cluster.observe(leader).number("applied_index");
    let nodes = cluster.live_nodes();
    for node in &nodes {
        wait_until("each sender to apply the snapshot boundary", || {
            (cluster.observe(*node).number("applied_index") >= floor).then_some(())
        });
        let reply = cluster.node_mut(*node).ask("CHECKPOINT").unwrap();
        let boundary: u64 = reply
            .strip_prefix("OK CHECKPOINT ")
            .unwrap_or_else(|| panic!("checkpoint refused: {reply}"))
            .parse()
            .unwrap();
        assert!(boundary >= floor);
        assert!(cluster.observe(*node).number("snapshot_index") >= floor);
    }
    for node in &nodes {
        cluster.stop_checked(*node);
    }
    for node in &nodes {
        let process = ProductionNode::spawn_binary(
            cluster.root(),
            *node,
            &NODE_IDS,
            cluster.config(),
            "open",
            binary,
        );
        cluster.adopt(*node, process);
    }
    for node in nodes {
        cluster.wait_ready(node);
    }
    println!("UPGRADE_COLD_SENDERS boundary={floor}");
    LogIndex(floor)
}

fn require_snapshot(cluster: &mut ProductionCluster, node: NodeId, boundary: LogIndex) {
    let wait = rafter_reference_harness::process::Wait::new(
        std::time::Duration::from_secs(30),
        std::time::Duration::from_millis(10),
    );
    let result = wait.until("lagging replica to install the compacted snapshot", || {
        let observation = cluster.observe(node);
        (observation.number("snapshot_index") >= boundary.0
            && observation.number("applied_index") >= boundary.0)
            .then_some(())
    });
    if let Err(error) = result {
        for live in cluster.live_nodes() {
            println!(
                "UPGRADE_SNAPSHOT_TIMEOUT target={} boundary={} node={} {}",
                node.0,
                boundary.0,
                live.0,
                cluster.observe(live)
            );
        }
        panic!("{error}");
    }
}

fn require_replay(cluster: &mut ProductionCluster, client: &mut UpgradeClient, command: Command) {
    assert!(matches!(
        client.submit(cluster, command),
        SubmitOutcome::Applied {
            disposition: ApplyDisposition::Replayed,
            ..
        }
    ));
}

fn observe(cluster: &mut ProductionCluster, stage: &str, token: FencingToken) {
    let view = cluster.query(resource("vault"));
    assert_eq!(view.view().owner, Some(0));
    assert_eq!(view.view().held_token, Some(token.get()));
    for node in cluster.live_nodes() {
        wait_until(
            "every replica to expose the acknowledged application state",
            || {
                let response = cluster.node_mut(node).ask("LOCAL LOCK vault").ok()?;
                matches!(process::parse_query_response(&response), process::QueryOutcome::Ready(ref local)
                if local == view.view()).then_some(())
            },
        );
        let observation = cluster.observe(node);
        assert!(observation.boolean("ready"));
        println!(
            "UPGRADE_OBSERVATION stage={stage} node={} {observation}",
            node.0
        );
    }
}

fn change_membership_while_mixed(cluster: &mut ProductionCluster, candidate: &std::path::Path) {
    let identity = allocate_replica(cluster.root(), 1, AllocationCrashPoint::None).unwrap();
    assert_eq!(identity.node_id, NodeId(4));
    let process = ProductionNode::spawn_binary(
        cluster.root(),
        NodeId(4),
        &NODE_IDS,
        cluster.config(),
        "open",
        candidate,
    );
    cluster.adopt(NodeId(4), process);
    assert_eq!(
        cluster.ask_leader("ADD_LEARNER 4"),
        "OK MEMBERSHIP_ACCEPTED"
    );
    let leader = cluster.wait_for_leader();
    cluster.wait_for_membership(leader, NodeId(4), true);
    let boundary = wait_until("new learner to catch up before promotion", || {
        let observation = cluster.observe(leader);
        let matched = observation.replication_match(NodeId(4))?;
        (matched.0 >= observation.number("commit_index")).then_some(matched)
    });
    cluster.wait_ready(NodeId(4));
    assert_eq!(
        cluster.ask_leader(&format!("PROMOTE_LEARNER 4 {}", boundary.0)),
        "OK MEMBERSHIP_ACCEPTED"
    );
    leave_joint(cluster);
    for node in cluster.live_nodes() {
        wait_until("each version to commit the promotion", || {
            cluster
                .observe(node)
                .contains_member("voters", NodeId(4))
                .then_some(())
        });
    }
    cluster.stop_checked(NodeId(4));
    assert_eq!(
        cluster.ask_leader("REMOVE_NODE 4"),
        "OK MEMBERSHIP_ACCEPTED"
    );
    leave_joint(cluster);
    for node in cluster.live_nodes() {
        cluster.wait_for_membership(node, NodeId(4), false);
    }
    retire_replica(&ReplicaIdentity::path(cluster.root(), NodeId(4)), 1).unwrap();
    println!("UPGRADE_MEMBERSHIP mixed-versions learner-promoted-and-retired");
}

fn leave_joint(cluster: &mut ProductionCluster) {
    membership_phase(cluster, "joint");
    assert_eq!(cluster.ask_leader("LEAVE_JOINT"), "OK MEMBERSHIP_ACCEPTED");
    membership_phase(cluster, "stable");
}

fn membership_phase(cluster: &mut ProductionCluster, phase: &str) {
    for node in cluster.live_nodes() {
        let observation = wait_until("each version to commit the membership phase", || {
            let observed = cluster.observe(node);
            (observed.string("committed_membership_phase") == phase).then_some(observed)
        });
        println!(
            "UPGRADE_MEMBERSHIP_PHASE phase={phase} node={} {observation}",
            node.0
        );
    }
}

fn copy_state(source: &std::path::Path, destination: &std::path::Path) {
    std::fs::create_dir(destination).expect("evidence destination must be fresh");
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        let target = destination.join(entry.file_name());
        if kind.is_dir() {
            copy_state(&entry.path(), &target);
        } else {
            assert!(
                kind.is_file(),
                "durable evidence must contain only regular files"
            );
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

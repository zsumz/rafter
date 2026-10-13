//! Explicit reseeding restores a real local snapshot before its committed suffix.
//!
//! The seal fault is deliberate, after a checked stop. It models the operator
//! decision following a refused repair; it does not claim another crash boundary.

use std::{fs, path::Path};

use rafter::NodeId;
use rafter_reference_fenced_lock::{
    check_evidence,
    store::{LockStore, LockStoreError, SlotDamage},
    ApplyDisposition, Command, FencingToken, GuardedRejection, LockResponse,
    RecordingGuardedResource,
};

use crate::{
    applied, guarded_write,
    process::{parse_query_response, QueryOutcome},
    process_config,
    production_process::{wait_until, ProductionCluster, ProductionNode, NODE_IDS},
    production_recovery::remove_seal,
    support::{acquire, open_session, release, renew, resource, submit},
};

pub fn explicit_reseed_restores_snapshot_and_suffix() {
    let mut cluster = ProductionCluster::start("production-explicit-reseed", process_config());
    let victim = NodeId(3);
    assert_ne!(cluster.wait_for_leader(), victim);
    let vault = resource("vault");
    let mut guarded = RecordingGuardedResource::new(vault);
    cluster.submit_to_leader(open_session(0, 1));
    let first = cluster
        .submit_to_leader(submit(0, 1, 1, acquire("vault", 10)))
        .acquired_token();
    cluster.submit_to_leader(submit(0, 1, 2, release("vault", first.get())));
    let snapshotted = cluster
        .submit_to_leader(submit(0, 1, 3, acquire("vault", 10)))
        .acquired_token();
    assert!(snapshotted > first);
    assert_eq!(guarded_write(&mut guarded, snapshotted, 22), Ok(22));
    let snapshot_floor = checkpoint_victim(&mut cluster, victim);

    // This acquisition is newer than the compacted prefix. Recovering just the
    // snapshot, or replaying the suffix onto an empty store, cannot preserve it.
    cluster.submit_to_leader(submit(0, 1, 4, release("vault", snapshotted.get())));
    let suffix = cluster
        .submit_to_leader(submit(0, 1, 5, acquire("vault", 10)))
        .acquired_token();
    assert!(suffix > snapshotted);
    assert_eq!(guarded_write(&mut guarded, suffix, 33), Ok(33));
    let local_floor = applied_on_victim(&mut cluster, victim);
    assert!(local_floor > snapshot_floor);
    cluster.stop_checked(victim);
    let app_dir = cluster.root().join("node-3/app");
    let discarded_bytes = damage_and_refuse_repair(&app_dir, suffix, snapshotted);

    // Keep the quorum live and advance past the stopped replica's own log.
    // Reaching this last renewal requires authenticated replication as well as
    // local snapshot recovery; no Raft or transport metadata is deleted.
    let latest = submit(0, 1, 6, renew("vault", suffix.get(), 30));
    let expected = applied(&cluster.submit_to_leader(latest)).1;
    let leader = cluster.wait_for_leader();
    let catchup_floor = cluster.observe(leader).number("applied_index");
    assert!(catchup_floor > local_floor);
    let view = cluster.query(vault).view().clone();
    let mut reseeded = ProductionNode::spawn_with_mode(
        cluster.root(),
        victim,
        &NODE_IDS,
        cluster.config(),
        "reseed",
    );
    let report = reseeded.wait_lifecycle("RESEEDED");
    assert!(
        report.contains(&format!("discarded_bytes={discarded_bytes}")),
        "{report}"
    );
    assert!(
        report.contains(&format!("discarded_applied_index={local_floor}")),
        "{report}"
    );
    assert!(reseeded.wait_ready().0 >= local_floor);
    cluster.adopt(victim, reseeded);
    wait_until(
        "reseeded replica to catch up over authenticated TLS",
        || {
            let observed = cluster.observe(victim);
            (observed.number("applied_index") >= catchup_floor
                && observed.number("authenticated_connections") > 0)
                .then_some(())
        },
    );
    assert_eq!(
        cluster.observe(victim).number("snapshot_index"),
        snapshot_floor
    );
    let reply = cluster.node_mut(victim).ask("LOCAL LOCK vault").unwrap();
    assert!(matches!(parse_query_response(&reply), QueryOutcome::Ready(local) if local == view));
    cluster.stop_checked(victim);
    require_cold_state(&cluster, victim, catchup_floor, latest, expected, suffix);
    assert!(cluster.restart(victim, &NODE_IDS).0 >= catchup_floor);

    // A subsequent tenure must exceed every token held before reseeding. The
    // independent resource continues rejecting the older acknowledged tenure.
    cluster.submit_to_leader(submit(0, 1, 7, release("vault", suffix.get())));
    let next = cluster
        .submit_to_leader(submit(0, 1, 8, acquire("vault", 10)))
        .acquired_token();
    assert!(next > suffix);
    assert_eq!(guarded_write(&mut guarded, next, 44), Ok(44));
    assert_eq!(
        guarded_write(&mut guarded, suffix, 99),
        Err(GuardedRejection::StaleFencingToken {
            highest_accepted: next
        })
    );
    applied_on_victim(&mut cluster, victim);
    check_evidence(process_config(), cluster.history(), guarded.history())
        .expect("the recovered TLS cluster and guarded-resource histories remain valid");
    for node in cluster.live_nodes() {
        cluster.stop_checked(node);
    }
    let recovered = LockStore::open(&app_dir, process_config()).unwrap();
    assert_eq!(recovered.acknowledged_mark(vault), Some(next));
    println!("TLS_EXPLICIT_RESEED snapshot={snapshot_floor} suffix={local_floor} catchup={catchup_floor} discarded_bytes={discarded_bytes} snapshot-before-suffix tls-catchup cold-cache-and-fencing higher-token stale-token-rejected");
}

fn applied_on_victim(cluster: &mut ProductionCluster, victim: NodeId) -> u64 {
    let leader = cluster.wait_for_leader();
    let boundary = cluster.observe(leader).number("applied_index");
    wait_until("the victim to apply the acknowledged command", || {
        (cluster.observe(victim).number("applied_index") >= boundary).then_some(())
    });
    boundary
}

fn require_cold_state(
    cluster: &ProductionCluster,
    victim: NodeId,
    floor: u64,
    latest: Command,
    expected: LockResponse,
    token: FencingToken,
) {
    let store = LockStore::open(
        &cluster.root().join(format!("node-{}/app", victim.0)),
        process_config(),
    )
    .unwrap();
    assert!(store.applied_index().0 >= floor);
    assert_eq!(store.acknowledged_mark(resource("vault")), Some(token));
    let replay = store.service().clone().apply(latest);
    assert_eq!(replay.disposition, ApplyDisposition::Replayed);
    assert_eq!(replay.response, expected);
}

fn checkpoint_victim(cluster: &mut ProductionCluster, victim: NodeId) -> u64 {
    let floor = applied_on_victim(cluster, victim);
    let reply = cluster.node_mut(victim).ask("CHECKPOINT").unwrap();
    assert_eq!(reply, format!("OK CHECKPOINT {floor}"));
    assert_eq!(cluster.observe(victim).number("snapshot_index"), floor);
    floor
}

fn damage_and_refuse_repair(
    app_dir: &Path,
    acknowledged: FencingToken,
    offered: FencingToken,
) -> u64 {
    remove_seal(app_dir);
    let error = LockStore::open_and_repair(app_dir, process_config()).unwrap_err();
    assert!(
        matches!(&error, LockStoreError::DiscardWouldRegressMark {
        resource: named, acknowledged: mark, offered: Some(previous),
        damage: SlotDamage::UnsealedCompleteImage { .. }, ..
    } if *named == resource("vault") && *mark == acknowledged && *previous == offered),
        "{error}"
    );
    ["lock-state.0", "lock-state.1"]
        .iter()
        .map(|name| fs::metadata(app_dir.join(name)).unwrap().len())
        .sum()
}

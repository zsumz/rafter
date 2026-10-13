//! Explicit repair of the ambiguous image observed during rolling SIGKILL.
//!
//! This deliberately clears a stopped replica's seal byte; it is a disk-image
//! fault, not another claim about where a real kill landed. A complete unsealed
//! renewal is repairable only by explicit choice and the existing mark checks.

use std::{fs, io::Write, path::Path};

use rafter::NodeId;
use rafter_reference_fenced_lock::{
    check_evidence, check_linearizable,
    store::{LockStore, LockStoreError, SlotDamage},
    ApplyDisposition, GuardedRejection, RecordingGuardedResource,
};

use crate::{
    applied, guarded_write,
    process::{parse_query_response, QueryOutcome},
    process_config,
    production_process::{wait_until, ProductionCluster, ProductionNode, NODE_IDS},
    support::{acquire, open_session, release, renew, resource, submit},
};

pub fn explicit_repair_recovers_committed_state() {
    let mut cluster = ProductionCluster::start("production-explicit-repair", process_config());
    let leader = cluster.wait_for_leader();
    let victim = NodeId(3);
    assert_ne!(leader, victim);
    cluster.submit_to_leader(open_session(0, 1));
    let token = cluster
        .submit_to_leader(submit(0, 1, 1, acquire("vault", 10)))
        .acquired_token();
    cluster.submit_to_leader(submit(0, 1, 2, renew("vault", token.get(), 20)));
    let before = cluster.observe(leader).number("applied_index");
    wait_until("renewal to be durably applied by the victim", || {
        (cluster.observe(victim).number("applied_index") >= before).then_some(())
    });
    cluster.stop_checked(victim);
    let node_dir = cluster.root().join("node-3");
    let app_dir = node_dir.join("app");
    let generation = remove_seal(&app_dir);
    require_unchanged_refusals(&cluster, victim, &node_dir);

    // The surviving quorum advances while the refused replica is absent. Its
    // replacement must recover locally and catch up over the authenticated link.
    let latest = submit(0, 1, 3, renew("vault", token.get(), 30));
    let expected = applied(&cluster.submit_to_leader(latest)).1;
    let leader = cluster.wait_for_leader();
    let boundary = cluster.observe(leader).number("applied_index");
    assert!(boundary > before);
    let view = cluster.query(resource("vault")).view().clone();

    let mut repaired = ProductionNode::spawn_with_mode(
        cluster.root(),
        victim,
        &NODE_IDS,
        cluster.config(),
        "repair",
    );
    let report = repaired.wait_lifecycle("REPAIRED");
    assert!(report.contains("damage=UnsealedCompleteImage"), "{report}");
    assert!(report.contains("marks_cross_checked=true"), "{report}");
    assert!(report.contains(&format!("discarded_generation={generation}")));
    assert!(report.contains(&format!("adopted_generation={}", generation - 1)));
    repaired.wait_ready();
    cluster.adopt(victim, repaired);
    wait_until(
        "explicitly repaired replica to catch up through TLS",
        || {
            let observed = cluster.observe(victim);
            (observed.number("applied_index") >= boundary
                && observed.number("authenticated_connections") > 0)
                .then_some(())
        },
    );
    let response = cluster.node_mut(victim).ask("LOCAL LOCK vault").unwrap();
    assert!(matches!(parse_query_response(&response), QueryOutcome::Ready(local) if local == view));
    check_linearizable(process_config(), cluster.history()).expect("the live TLS history is valid");
    for node in cluster.live_nodes() {
        cluster.stop_checked(node);
    }

    // Inspect the cold durable image, including the session cache, rather than
    // inferring recovery solely from a read served by the unaffected leader.
    let recovered = LockStore::open(&app_dir, process_config()).unwrap();
    let authoritative = LockStore::open(
        &cluster
            .root()
            .join(format!("node-{}", leader.0))
            .join("app"),
        process_config(),
    )
    .unwrap();
    assert!(recovered.applied_index().0 >= boundary);
    assert_eq!(recovered.service(), authoritative.service());
    assert_eq!(recovered.acknowledged_mark(resource("vault")), Some(token));
    let replay = recovered.service().clone().apply(latest);
    assert_eq!(replay.disposition, ApplyDisposition::Replayed);
    assert_eq!(replay.response, expected);
    println!("TLS_EXPLICIT_REPAIR complete-unsealed-renewal strict-refusal-preserves-bytes explicit-repair tls-catchup cold-cache-and-fencing");
}

/// Acknowledged acquisitions cannot be repaired by adopting a lower mark, even
/// when the operator explicitly selects repair. The unaffected quorum keeps
/// serving; the refused replica must preserve the evidence for a later decision.
pub fn explicit_repair_refuses_acknowledged_mark_loss() {
    let mut cluster = ProductionCluster::start("production-repair-refusal", process_config());
    let leader = cluster.wait_for_leader();
    let victim = NodeId(3);
    assert_ne!(leader, victim);
    let vault = resource("vault");
    let mut guarded = RecordingGuardedResource::new(vault);
    cluster.submit_to_leader(open_session(0, 1));
    let first = cluster
        .submit_to_leader(submit(0, 1, 1, acquire("vault", 10)))
        .acquired_token();
    assert_eq!(guarded_write(&mut guarded, first, 11), Ok(11));
    cluster.submit_to_leader(submit(0, 1, 2, release("vault", first.get())));
    let acknowledged = cluster
        .submit_to_leader(submit(0, 1, 3, acquire("vault", 10)))
        .acquired_token();
    assert!(acknowledged > first);
    assert_eq!(guarded_write(&mut guarded, acknowledged, 22), Ok(22));
    let boundary = cluster.observe(leader).number("applied_index");
    wait_until("acquisition to be durably applied by the victim", || {
        (cluster.observe(victim).number("applied_index") >= boundary).then_some(())
    });
    cluster.stop_checked(victim);
    let node_dir = cluster.root().join("node-3");
    let app_dir = node_dir.join("app");
    remove_seal(&app_dir);
    let paths = [
        "app/lock-state.0",
        "app/lock-state.1",
        "transport-replay",
        "control-plane",
        "raft/log",
        "raft/hard-state",
    ];
    let before = paths.map(|name| fs::read(node_dir.join(name)).unwrap());
    let refusal = LockStore::open_and_repair(&app_dir, process_config()).unwrap_err();
    assert!(
        matches!(
            &refusal,
            LockStoreError::DiscardWouldRegressMark {
                resource: named,
                acknowledged: mark,
                offered: Some(offered),
                damage: SlotDamage::UnsealedCompleteImage { .. },
                ..
            } if *named == vault && *mark == acknowledged && *offered == first
        ),
        "{refusal}"
    );

    for attempt in 1..=2 {
        let mut process = ProductionNode::spawn_with_mode(
            cluster.root(),
            victim,
            &NODE_IDS,
            cluster.config(),
            "repair",
        );
        let (status, output) = process.wait_refused();
        assert!(!status.success(), "{output}");
        assert!(output.contains("\"kind\":\"NEEDS_DECISION\""), "{output}");
        assert!(output.contains(&refusal.to_string()), "{output}");
        assert!(output.contains("--recover reseed"), "{output}");
        require_no_successful_start(&output);
        assert_eq!(
            paths.map(|name| fs::read(node_dir.join(name)).unwrap()),
            before
        );
        println!("TLS_REPAIR_REFUSAL attempt={attempt} acknowledged={} offered={} unchanged-six-durable-files no-ready-no-peer-listener", acknowledged.get(), first.get());
    }

    cluster.submit_to_leader(submit(0, 1, 4, release("vault", acknowledged.get())));
    let next = cluster
        .submit_to_leader(submit(0, 1, 5, acquire("vault", 10)))
        .acquired_token();
    assert!(next > acknowledged);
    assert_eq!(guarded_write(&mut guarded, next, 33), Ok(33));
    assert_eq!(
        guarded_write(&mut guarded, acknowledged, 99),
        Err(GuardedRejection::StaleFencingToken {
            highest_accepted: next
        })
    );
    assert_eq!(cluster.query(vault).view().held_token, Some(next.get()));
    check_evidence(process_config(), cluster.history(), guarded.history())
        .expect("the live quorum and guarded-resource histories remain valid");
    for node in cluster.live_nodes() {
        cluster.stop_checked(node);
    }
    assert_eq!(
        paths.map(|name| fs::read(node_dir.join(name)).unwrap()),
        before
    );
    println!(
        "TLS_REPAIR_REFUSAL quorum-progress higher-token stale-token-rejected no-implicit-reseed"
    );
}

fn require_no_successful_start(output: &str) {
    for kind in [
        "READY",
        "PEER_LISTENING",
        "REPAIRED",
        "RESEEDED",
        "CREATED",
        "RECOVERED",
    ] {
        assert!(
            !output.contains(&format!("\"kind\":\"{kind}\"")),
            "{output}"
        );
    }
}

pub(super) fn remove_seal(app_dir: &Path) -> u64 {
    let store = LockStore::open(app_dir, process_config()).unwrap();
    let live = store
        .live_slot()
        .expect("the acknowledged command has a live slot");
    let generation = store.generation();
    assert!(generation >= 3);
    drop(store);
    let path = app_dir.join(live.file_name());
    assert_eq!(fs::read(&path).unwrap()[0], b'R');
    let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.write_all(&[0]).unwrap();
    file.sync_all().unwrap();
    assert!(matches!(
        LockStore::open(app_dir, process_config()),
        Err(LockStoreError::UnreadableSlot {
            damage: SlotDamage::UnsealedCompleteImage { .. },
            ..
        })
    ));
    generation
}

fn require_unchanged_refusals(cluster: &ProductionCluster, victim: NodeId, node_dir: &Path) {
    let paths = ["app/lock-state.0", "app/lock-state.1", "transport-replay"];
    let before = paths.map(|name| fs::read(node_dir.join(name)).unwrap());
    for attempt in 1..=2 {
        let mut process =
            ProductionNode::spawn(cluster.root(), victim, &NODE_IDS, cluster.config());
        let (status, output) = process.wait_refused();
        assert!(!status.success(), "{output}");
        assert!(output.contains("\"kind\":\"NEEDS_DECISION\""), "{output}");
        assert!(
            output.contains("publication mark reads unsealed"),
            "{output}"
        );
        assert!(output.contains("--recover repair"), "{output}");
        require_no_successful_start(&output);
        assert_eq!(
            paths.map(|name| fs::read(node_dir.join(name)).unwrap()),
            before
        );
        println!("TLS_EXPLICIT_REPAIR refusal={attempt} unchanged-app-and-transport no-ready-no-peer-listener");
    }
}

use std::{
    fs,
    path::{Path, PathBuf},
};

const DIRECT_CLUSTER_TRANSITION_NEEDLES: &[&str] = &[
    "cluster.tick(",
    "cluster.deliver(",
    "cluster.deliver_all(",
    "cluster.deliver_matching(",
    "cluster.deliver_message(",
    "cluster.deliver_one_matching(",
    "cluster.deliver_random_ready(",
    "cluster.read_index(",
    "cluster.propose(",
    "cluster.transfer_leadership(",
    "cluster.add_learner(",
    "cluster.promote_learner(",
    "cluster.remove_voter(",
    "cluster.enter_joint(",
    "cluster.leave_joint(",
    "cluster.change_membership(",
    "cluster.restart_node_from_bootstrap(",
    "cluster.restart_node_from_bootstrap_losing_application_state(",
    "cluster.restart_node_lossy(",
    "cluster.seed_snapshot_payload(",
    "cluster.partition_between(",
    "cluster.heal_partitions(",
    "cluster.delay_matching(",
    "cluster.drop_matching(",
    "cluster.duplicate_matching(",
    "cluster.queue_message(",
];

const DIRECT_CLUSTER_TRANSITION_ALLOWLIST: &[(&str, &str)] = &[
    (
        "src/model_check/application/cluster.rs",
        "the central transition adapter is the only production boundary that may call Cluster directly",
    ),
    (
        "src/model_check/helpers.rs",
        "shared cluster fixture setup happens before ExplorationState exists",
    ),
    (
        "src/model_check/state/seeds.rs",
        "static seed construction happens before ExplorationState exists",
    ),
    (
        "src/model_check/state.rs",
        "test-only detector fixture injection is owned by ExplorationState",
    ),
    (
        "src/model_check/checks/witnesses.rs",
        "semantic witness messages are queued before ExplorationState exists",
    ),
    (
        "src/model_check/invariants/tests/application_epoch.rs",
        "application-epoch detector fixtures seed snapshots before ExplorationState exists",
    ),
    (
        "src/model_check/invariants/tests/commit_history_snapshot.rs",
        "commit-history detector fixtures seed snapshots before ExplorationState exists",
    ),
    (
        "src/model_check/invariants/tests/log_history.rs",
        "logical-log detector fixtures seed snapshots before ExplorationState exists",
    ),
    (
        "src/model_check/tests/replay.rs",
        "expected-cluster replay fixtures intentionally build an independent comparison state",
    ),
    (
        "src/model_check/tests/soak/core.rs",
        "soak fixture setup happens before ExplorationState exists",
    ),
    (
        "src/model_check/tests/soak/membership.rs",
        "membership soak fixture setup happens before ExplorationState exists",
    ),
];

#[test]
fn model_check_drivers_use_instrumented_transition_boundary() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let model_check_dir = manifest_dir.join("src/model_check");
    let mut violations = Vec::new();

    for path in rust_files_under(&model_check_dir) {
        let relative = path.strip_prefix(&manifest_dir).unwrap_or(&path);
        let relative = relative.to_string_lossy();
        if relative == "src/model_check/tests/transition_boundary.rs" {
            continue;
        }
        let source = fs::read_to_string(&path).expect("model-check source is readable");
        for (line_index, line) in source.lines().enumerate() {
            if !DIRECT_CLUSTER_TRANSITION_NEEDLES
                .iter()
                .any(|needle| line.contains(needle))
            {
                continue;
            }
            if direct_transition_allowed(&relative) {
                continue;
            }
            violations.push(format!("{}:{}: {}", relative, line_index + 1, line.trim()));
        }
    }

    assert!(
        violations.is_empty(),
        "model-check transitions must flow through the state-owned transition engine; add a narrow allowlist reason for fixture-only setup:\n{}",
        violations.join("\n")
    );
}

#[test]
fn transition_capability_is_owned_by_state_and_has_no_mutable_deref() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let state = fs::read_to_string(manifest_dir.join("src/model_check/state.rs"))
        .expect("model-check state source is readable");
    let application = fs::read_to_string(manifest_dir.join("src/model_check/application.rs"))
        .expect("transition engine source is readable");
    let root = fs::read_to_string(manifest_dir.join("src/model_check.rs"))
        .expect("model-check root source is readable");

    assert!(state.contains("mod application;"));
    assert!(state.contains("cluster: InstrumentedCluster,"));
    assert!(!state.contains("pub(super) cluster: InstrumentedCluster,"));
    for field in [
        "proposals_issued",
        "restarts_issued",
        "client_history",
        "election_history",
        "logical_log_history",
        "commit_history",
        "observations",
    ] {
        assert!(
            !state.contains(&format!("pub(super) {field}:")),
            "verifier field {field} must remain private to the state-owned engine"
        );
    }
    assert!(application.contains("fn apply_transition("));
    assert!(application.contains("Transition::SchedulerIndex"));
    assert!(application.contains("Transition::RandomReadyPosition"));
    assert!(application.contains("impl Deref for InstrumentedCluster"));
    assert!(!application.contains("DerefMut"));
    assert!(!root.contains("mod application;"));
}

#[test]
fn direct_cluster_transition_allowlist_entries_are_used() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let unused = DIRECT_CLUSTER_TRANSITION_ALLOWLIST
        .iter()
        .filter(|(relative, _reason)| {
            let source = fs::read_to_string(manifest_dir.join(relative)).unwrap_or_default();
            !DIRECT_CLUSTER_TRANSITION_NEEDLES
                .iter()
                .any(|needle| source.contains(needle))
        })
        .map(|(relative, reason)| format!("{relative}: {reason}"))
        .collect::<Vec<_>>();

    assert!(
        unused.is_empty(),
        "direct cluster transition allowlist entries no longer match source:\n{}",
        unused.join("\n")
    );
}

fn direct_transition_allowed(relative: &str) -> bool {
    DIRECT_CLUSTER_TRANSITION_ALLOWLIST
        .iter()
        .any(|(allowed, reason)| relative == *allowed && !reason.trim().is_empty())
}

fn rust_files_under(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_files(root, &mut files);
    files
}

fn collect_rust_files(path: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(path) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

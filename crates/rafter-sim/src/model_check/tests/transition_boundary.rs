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
    "cluster.propose(",
    "cluster.transfer_leadership(",
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
        "src/model_check/tests/replay.rs",
        "expected-cluster replay fixtures intentionally build an independent comparison state",
    ),
    (
        "src/model_check/tests/seeded.rs",
        "seeded regressions intentionally prepare fixture states before exercising apply_to_state",
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
        "model-check protocol transitions must flow through apply_to_state; add a narrow allowlist reason for fixture-only setup:\n{}",
        violations.join("\n")
    );
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

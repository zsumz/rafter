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

const DIRECT_CLUSTER_TRANSITION_ALLOWLIST: &[DirectTransitionAllowance] = &[
    allow(
        "src/model_check/application/cluster.rs",
        "apply_to_cluster",
        "central transition adapter",
    ),
    allow(
        "src/model_check/helpers.rs",
        "elect_node_one",
        "pre-state fixture setup",
    ),
    allow(
        "src/model_check/state/seeds.rs",
        "seeded_low_empty_probe",
        "pre-state fixture setup",
    ),
    allow(
        "src/model_check/state/seeds.rs",
        "seeded_divergent_suffix_probe",
        "pre-state fixture setup",
    ),
    allow(
        "src/model_check/state.rs",
        "inject_bootstrap_state",
        "test-only detector injection",
    ),
    allow(
        "src/model_check/state.rs",
        "inject_message",
        "test-only detector injection",
    ),
    allow(
        "src/model_check/state.rs",
        "drop_all_messages",
        "test-only detector injection",
    ),
    allow(
        "src/model_check/checks/witnesses.rs",
        "nonvoter_vote_summary",
        "pre-state semantic witness setup",
    ),
    allow(
        "src/model_check/invariants/tests/application_epoch.rs",
        "full_prefix_application_replay_matches_snapshot_anchored_replay",
        "pre-state detector fixture setup",
    ),
    allow(
        "src/model_check/invariants/tests/application_epoch.rs",
        "read_reconstruction_ignores_values_from_previous_application_epoch",
        "pre-state detector fixture setup",
    ),
    allow(
        "src/model_check/invariants/tests/commit_history_snapshot.rs",
        "leader_completeness_rejects_unwitnessed_snapshot_with_matching_boundary",
        "pre-state detector fixture setup",
    ),
    allow(
        "src/model_check/invariants/tests/commit_history_snapshot.rs",
        "leader_completeness_snapshot_only_committed_state_is_not_vacuous_success",
        "pre-state detector fixture setup",
    ),
    allow(
        "src/model_check/invariants/tests/log_history.rs",
        "log_matching_rejects_snapshot_witness_shorter_than_boundary",
        "pre-state detector fixture setup",
    ),
    allow(
        "src/model_check/invariants/tests/log_history.rs",
        "log_matching_rejects_snapshot_witness_with_wrong_boundary_term",
        "pre-state detector fixture setup",
    ),
    allow(
        "src/model_check/invariants/tests/log_history.rs",
        "seeded_snapshot_without_prefix_is_coverage_not_reached",
        "pre-state detector fixture setup",
    ),
    allow(
        "src/model_check/tests/replay.rs",
        "replay_raft_trace_reaches_expected_final_state",
        "independent expected-state fixture",
    ),
    allow(
        "src/model_check/tests/replay.rs",
        "commit_safety_allows_old_leader_commit_before_newer_candidate_wins",
        "independent expected-state fixture",
    ),
    allow(
        "src/model_check/tests/soak/core.rs",
        "ordinary_restart_preserves_durable_state_digest",
        "pre-state soak fixture setup",
    ),
    allow(
        "src/model_check/tests/soak/membership.rs",
        "enabled_membership_soak_actions_cover_joint_transition_phases",
        "pre-state soak fixture setup",
    ),
];

#[derive(Clone, Copy)]
struct DirectTransitionAllowance {
    relative: &'static str,
    function: &'static str,
    reason: &'static str,
}

const fn allow(
    relative: &'static str,
    function: &'static str,
    reason: &'static str,
) -> DirectTransitionAllowance {
    DirectTransitionAllowance {
        relative,
        function,
        reason,
    }
}

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
        violations.extend(direct_transition_violations(&relative, &source));
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
        .filter(|allowance| {
            let source =
                fs::read_to_string(manifest_dir.join(allowance.relative)).unwrap_or_default();
            !source.lines().enumerate().any(|(line_index, line)| {
                DIRECT_CLUSTER_TRANSITION_NEEDLES
                    .iter()
                    .any(|needle| line.contains(needle))
                    && enclosing_function_name(&source, line_index).as_deref()
                        == Some(allowance.function)
            })
        })
        .map(|allowance| {
            format!(
                "{}::{}: {}",
                allowance.relative, allowance.function, allowance.reason
            )
        })
        .collect::<Vec<_>>();

    assert!(
        unused.is_empty(),
        "direct cluster transition allowlist entries no longer match source:\n{}",
        unused.join("\n")
    );
}

#[test]
fn function_allowance_does_not_exempt_another_function_in_the_same_file() {
    let source = r"
fn elect_node_one() {
    cluster.tick(node_id);
}

fn accidental_driver() {
    cluster.tick(node_id);
}
";

    let violations = direct_transition_violations("src/model_check/helpers.rs", source);
    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("accidental_driver"));
    assert!(violations[0].contains("cluster.tick(node_id)"));
}

fn direct_transition_violations(relative: &str, source: &str) -> Vec<String> {
    source
        .lines()
        .enumerate()
        .filter_map(|(line_index, line)| {
            let is_transition = DIRECT_CLUSTER_TRANSITION_NEEDLES
                .iter()
                .any(|needle| line.contains(needle));
            let function = enclosing_function_name(source, line_index);
            (is_transition && !direct_transition_allowed(relative, function.as_deref())).then(
                || {
                    format!(
                        "{}:{} in {}: {}",
                        relative,
                        line_index + 1,
                        function.as_deref().unwrap_or("<module>"),
                        line.trim()
                    )
                },
            )
        })
        .collect()
}

fn direct_transition_allowed(relative: &str, function: Option<&str>) -> bool {
    DIRECT_CLUSTER_TRANSITION_ALLOWLIST.iter().any(|allowance| {
        relative == allowance.relative
            && function == Some(allowance.function)
            && !allowance.reason.trim().is_empty()
    })
}

fn enclosing_function_name(source: &str, line_index: usize) -> Option<String> {
    source
        .lines()
        .take(line_index + 1)
        .filter_map(declared_function_name)
        .last()
        .map(str::to_owned)
}

fn declared_function_name(line: &str) -> Option<&str> {
    let function = line.find("fn ")?;
    if function > 0
        && line[..function]
            .chars()
            .next_back()
            .is_some_and(|character| character == '_' || character.is_alphanumeric())
    {
        return None;
    }
    let name = line[function + 3..]
        .trim_start()
        .split(|character: char| !(character == '_' || character.is_alphanumeric()))
        .next()?;
    (!name.is_empty()).then_some(name)
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

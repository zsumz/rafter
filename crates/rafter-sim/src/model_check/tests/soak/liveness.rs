use std::collections::BTreeSet;

use super::super::super::helpers::{config as node_config, three_node_configs};
use super::super::super::liveness::{self, run_soak_liveness_check_with_budget_overrides};
use super::super::super::{
    run_raft_random_soak,
    soak::SoakAction,
    state::{ClientWriteStatus, ExplorationState},
    SoakActionKind, SoakConfig,
};
use crate::{Cluster, SimSeed};
use rafter_invariant_test::{oracle_assert, oracle_assert_eq, oracle_expect_err};

#[derive(Clone, Copy, Debug)]
struct HistoryCounts {
    elections: usize,
    log_prefixes: usize,
    committed_prefix_entries: usize,
    commit_certificates: usize,
}

impl HistoryCounts {
    fn from_state(state: &ExplorationState) -> Self {
        Self {
            elections: state.election_history().elected_by_term.len(),
            log_prefixes: state.logical_log_history().prefixes_by_index_term.len(),
            committed_prefix_entries: state
                .commit_history()
                .committed_prefix
                .as_ref()
                .map_or(0, |prefix| prefix.entries.len()),
            commit_certificates: state.commit_history().certificates.len(),
        }
    }
}

#[test]
fn randomized_soak_liveness_phase_elects_leader_and_commits_probe() {
    let summary = run_raft_random_soak(three_node_configs(), SoakConfig::new(SimSeed(0x11_5e), 0))
        .expect("post-soak liveness phase should elect and commit without random steps");

    assert_eq!(summary.steps_executed(), 0);
    for kind in [
        SoakActionKind::Tick,
        SoakActionKind::Deliver,
        SoakActionKind::Propose,
    ] {
        assert!(
            summary.observed_actions().contains(&kind),
            "liveness phase should observe {kind:?}"
        );
    }

    let convergence = summary
        .liveness_reports()
        .iter()
        .find(|report| report.feature_id() == "leader-convergence")
        .expect("post-heal convergence should emit clause-a evidence");
    let usability = summary
        .liveness_reports()
        .iter()
        .find(|report| report.feature_id() == "leader-usability")
        .expect("post-heal usability should emit clause-b evidence");
    assert_eq!(
        convergence.to_json()["clause_ids"],
        serde_json::json!(["LV-01.a"])
    );
    assert_eq!(
        usability.to_json()["clause_ids"],
        serde_json::json!(["LV-01.b"])
    );
}

fn reviewed_pr_soak_config(seed: SimSeed) -> SoakConfig {
    SoakConfig::new(seed, 320)
        .with_max_proposals(24)
        .with_max_restarts(12)
        .with_max_read_indexes(4)
        .with_max_membership_changes(8)
        .with_max_transfers(2)
        .with_max_partitions(2)
        .with_max_lossy_restarts(2)
        .with_snapshot_catchup_probe()
        .with_tick_skew(rafter::NodeId(1), 3)
}

fn pr_three_node_configs() -> Vec<rafter::NodeConfig> {
    vec![
        node_config(1, &[2, 3], 2),
        node_config(2, &[1, 3], 2),
        node_config(3, &[1, 2], 2),
    ]
}

#[test]
fn reviewed_soak_seeds_find_a_stable_leader_usability_window() {
    for seed in [0x9103, 0x9104, 0x9105, 0x9106] {
        let summary = run_raft_random_soak(
            pr_three_node_configs(),
            reviewed_pr_soak_config(SimSeed(seed)),
        )
        .unwrap_or_else(|failure| {
            panic!(
                "reviewed PR soak seed 0x{seed:x} must find a stable usability window: {failure}"
            )
        });

        let convergence = summary
            .liveness_reports()
            .iter()
            .find(|report| report.feature_id() == "leader-convergence")
            .expect("reviewed seed emits convergence evidence")
            .to_json();
        let usability = summary
            .liveness_reports()
            .iter()
            .find(|report| report.feature_id() == "leader-usability")
            .expect("reviewed seed emits usability evidence")
            .to_json();
        oracle_assert_eq!(
            convergence["stable_leader"]["node_id"],
            usability["stable_leader"]["node_id"]
        );
        oracle_assert_eq!(usability["proposal"]["terminal_outcome"], "committed");
    }
}

#[test]
fn liveness_retry_terminates_each_abandoned_accepted_probe() {
    let config = reviewed_pr_soak_config(SimSeed(0x9103));
    let mut state =
        ExplorationState::new(Cluster::new_with_seed(pr_three_node_configs(), config.seed));
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();

    let reports = run_soak_liveness_check_with_budget_overrides(
        &mut state,
        config,
        &mut trace,
        &mut observed_actions,
        None,
        None,
    )
    .expect("reviewed retry seed must eventually find a usable leader");
    let final_proposal_id = reports
        .iter()
        .find(|report| report.feature_id() == "leader-usability")
        .expect("usability evidence exists")
        .to_json()["proposal"]["proposal_id"]
        .as_u64()
        .expect("proposal ID is numeric");

    oracle_assert!(state.client_history().writes.len() > 1);
    for (proposal_id, write) in &state.client_history().writes {
        if proposal_id.0 == final_proposal_id {
            continue;
        }
        oracle_assert!(matches!(
            write.status,
            ClientWriteStatus::Completed { .. }
                | ClientWriteStatus::Rejected
                | ClientWriteStatus::Unknown { .. }
        ));
    }
}

#[test]
fn liveness_retry_fails_red_when_candidate_churn_exhausts_the_bound() {
    let config = reviewed_pr_soak_config(SimSeed(0x9103));
    let mut state =
        ExplorationState::new(Cluster::new_with_seed(pr_three_node_configs(), config.seed));
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();

    let failure = oracle_expect_err!(
        run_soak_liveness_check_with_budget_overrides(
            &mut state,
            config,
            &mut trace,
            &mut observed_actions,
            None,
            Some(2),
        ),
        "candidate churn must remain red after the fixed usability bound"
    );

    oracle_assert!(failure
        .failure
        .message()
        .contains("within 2 bounded-fair usability rounds"));
    oracle_assert!(trace
        .iter()
        .any(|action| matches!(action, SoakAction::Propose { .. })));
}

#[rafter_invariant_test::detector_test]
fn post_heal_leader_convergence_monitor_reports_exhausted_bound() {
    let config = SoakConfig::new(SimSeed(0x11_5e), 0);
    let mut state =
        ExplorationState::new(Cluster::new_with_seed(three_node_configs(), config.seed));
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();

    let failure = oracle_expect_err!(
        run_soak_liveness_check_with_budget_overrides(
            &mut state,
            config,
            &mut trace,
            &mut observed_actions,
            Some(0),
            None,
        ),
        "zero post-heal rounds must fail the convergence detector"
    );

    oracle_assert!(failure
        .failure
        .message()
        .contains("within 0 post-heal convergence rounds"));
    oracle_assert!(trace
        .iter()
        .any(|action| matches!(action, SoakAction::Heal)));
    oracle_assert!(!trace
        .iter()
        .any(|action| matches!(action, SoakAction::Propose { .. })));
}

#[rafter_invariant_test::detector_test]
fn post_heal_leader_usability_monitor_reports_exhausted_bound() {
    let config = SoakConfig::new(SimSeed(0x11_5e), 0);
    let mut state =
        ExplorationState::new(Cluster::new_with_seed(three_node_configs(), config.seed));
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();

    let failure = oracle_expect_err!(
        run_soak_liveness_check_with_budget_overrides(
            &mut state,
            config,
            &mut trace,
            &mut observed_actions,
            None,
            Some(0),
        ),
        "a converged leader cannot complete a fresh proposal in zero rounds"
    );

    oracle_assert!(failure
        .failure
        .message()
        .contains("within 0 bounded-fair usability rounds"));
    oracle_assert!(trace
        .iter()
        .any(|action| matches!(action, SoakAction::Propose { .. })));
}

#[test]
fn randomized_soak_liveness_phase_completes_read_barrier_probe() {
    let summary = run_raft_random_soak(
        three_node_configs(),
        SoakConfig::new(SimSeed(0x11_5e), 0).with_max_read_indexes(1),
    )
    .expect("post-soak liveness phase should complete a read barrier");

    assert!(
        summary
            .observed_actions()
            .contains(&SoakActionKind::ReadIndex),
        "read-barrier liveness phase should observe a read-index action"
    );
}

#[test]
fn randomized_soak_liveness_phase_completes_membership_transition_probe() {
    let summary = run_raft_random_soak(
        three_node_configs(),
        SoakConfig::new(SimSeed(0x11_5e), 0).with_max_membership_changes(1),
    )
    .expect("post-soak liveness phase should complete a membership transition");

    for kind in [SoakActionKind::RemoveVoter, SoakActionKind::LeaveJoint] {
        assert!(
            summary.observed_actions().contains(&kind),
            "membership-transition liveness phase should observe {kind:?}"
        );
    }
}

#[test]
fn randomized_soak_liveness_phase_completes_leadership_transfer_probe() {
    let summary = run_raft_random_soak(
        three_node_configs(),
        SoakConfig::new(SimSeed(0x11_5e), 0).with_max_transfers(1),
    )
    .expect("post-soak liveness phase should complete a leadership transfer");

    assert!(
        summary
            .observed_actions()
            .contains(&SoakActionKind::Transfer),
        "leadership-transfer liveness phase should observe a transfer action"
    );
}

#[test]
fn liveness_phase_updates_p0_histories_through_instrumented_transitions() {
    let config = SoakConfig::new(SimSeed(0x11_5e), 0).with_max_transfers(1);
    let mut state =
        ExplorationState::new(Cluster::new_with_seed(three_node_configs(), config.seed));
    let before = HistoryCounts::from_state(&state);
    let mut trace = Vec::<SoakAction>::new();
    let mut observed_actions = BTreeSet::new();

    liveness::run_soak_liveness_check(&mut state, config, &mut trace, &mut observed_actions)
        .expect("instrumented liveness phase should converge");

    let after = HistoryCounts::from_state(&state);
    assert!(
        after.elections > before.elections,
        "liveness convergence should record election certificates"
    );
    assert!(
        after.log_prefixes > before.log_prefixes,
        "liveness convergence should refresh logical-log prefix witnesses"
    );
    assert!(
        after.committed_prefix_entries > before.committed_prefix_entries,
        "liveness convergence should refresh committed-prefix history"
    );
    assert!(
        after.commit_certificates > before.commit_certificates,
        "liveness convergence should record commit certificates"
    );
    assert!(
        observed_actions.contains(&SoakActionKind::Transfer),
        "liveness phase should exercise transfer instrumentation"
    );

    let target = state
        .cluster()
        .leaders()
        .into_iter()
        .min_by_key(|node_id| node_id.0)
        .expect("transfer liveness should leave a leader");
    assert!(
        state
            .election_history()
            .elected_by_term
            .values()
            .flatten()
            .any(|certificate| certificate.leader_id == target),
        "the leader elected during transfer liveness should receive an election certificate"
    );
}

#[test]
fn snapshot_catchup_liveness_monitor_installs_expected_snapshot() {
    liveness::run_snapshot_catchup_liveness_check(
        SoakConfig::new(SimSeed(0x5a_a9), 0).with_snapshot_catchup_probe(),
        256,
    )
    .expect("snapshot catch-up liveness monitor should install the snapshot");
}

#[test]
fn randomized_soak_liveness_phase_runs_snapshot_catchup_probe() {
    run_raft_random_soak(
        three_node_configs(),
        SoakConfig::new(SimSeed(0x11_5e), 0).with_snapshot_catchup_probe(),
    )
    .expect("post-soak liveness phase should run the snapshot catch-up probe");
}

use std::collections::BTreeSet;

use rafter::NodeId;

use crate::{
    model_check::{
        catalog, run_raft_random_soak,
        scheduling::SoakOperation,
        soak::{SoakAction, SoakActionKind, SoakConfig},
        state::{try_apply_soak_action, ExplorationState},
        FailureKind,
    },
    Cluster, SimSeed,
};

use super::{
    membership::run_membership_transition_liveness_detector, production_configs,
    read::run_read_barrier_liveness_detector, run_feature_liveness_checks,
    snapshot::run_snapshot_catchup_liveness_detector,
    transfer::run_leadership_transfer_liveness_detector, FaultStateRequirement,
    LivenessPreconditionProbe, LivenessPreconditions, TerminalRecorderMode,
};
use crate::model_check::liveness::driver::soak_liveness_round_budget;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq, oracle_expect_err};

#[test]
fn independent_reports_do_not_relabel_quorum_only_as_post_heal() {
    let config = SoakConfig::new(SimSeed(0x11_7e), 0);
    let mut observed_actions = BTreeSet::<SoakActionKind>::new();

    let reports = run_feature_liveness_checks(config, &mut observed_actions)
        .expect("independent liveness monitors should complete");

    assert_eq!(reports.len(), 4);
    assert!(reports
        .iter()
        .all(|report| report.feature_id() != "leader-convergence"));
    assert!(reports.iter().any(|report| {
        report.feature_id() == "quorum-only-leader-convergence"
            && report.scenario_id() == "minority-unavailable-stable-quorum-v1"
    }));
    assert!(reports.iter().any(|report| {
        report.feature_id() == "quorum-only-leader-usability"
            && report.scenario_id() == "minority-unavailable-stable-quorum-v1"
    }));
}

#[test]
fn healed_soak_returns_distinct_measured_convergence_evidence() {
    let config = SoakConfig::new(SimSeed(0x11_7e), 0).with_max_partitions(1);
    let summary = run_raft_random_soak(
        production_configs().expect("production liveness configuration should be valid"),
        config,
    )
    .expect("healed soak should complete its measured liveness contract");

    let post_heal = summary
        .liveness_reports()
        .iter()
        .find(|report| report.feature_id() == "leader-convergence")
        .expect("actual healed execution must emit leader-convergence evidence");
    let quorum_only = summary
        .liveness_reports()
        .iter()
        .find(|report| report.feature_id() == "quorum-only-leader-convergence")
        .expect("independent quorum-only execution must remain distinct");
    let post_heal_usability = summary
        .liveness_reports()
        .iter()
        .find(|report| report.feature_id() == "leader-usability")
        .expect("actual healed execution must emit leader-usability evidence");
    let quorum_only_usability = summary
        .liveness_reports()
        .iter()
        .find(|report| report.feature_id() == "quorum-only-leader-usability")
        .expect("independent quorum-only usability evidence must remain distinct");

    assert_eq!(post_heal.scenario_id(), "post-heal-stable-quorum-v1");
    assert_eq!(post_heal.observation_id(), "post_heal_quiescent_leaders");
    assert_eq!(
        post_heal.to_json()["clause_ids"],
        serde_json::json!(["LV-01.a"])
    );
    assert_eq!(
        post_heal_usability.to_json()["clause_ids"],
        serde_json::json!(["LV-01.b"])
    );
    assert_eq!(
        quorum_only_usability.to_json()["clause_ids"],
        serde_json::json!(["LV-01.b"])
    );
    assert_ne!(post_heal.scenario_id(), quorum_only.scenario_id());
    assert!(summary
        .observed_actions()
        .contains(&SoakActionKind::Partition));
    assert!(summary.observed_actions().contains(&SoakActionKind::Heal));
    assert!(summary.action_count(SoakActionKind::Partition) >= 1);
    assert!(summary.action_count(SoakActionKind::Heal) >= 1);
    let fault_cycle = &post_heal.to_json()["fault_cycle"];
    assert_eq!(fault_cycle["partition_observed"], true);
    assert_eq!(fault_cycle["partitioned_rounds"], 1);
    assert_eq!(fault_cycle["ticks_executed"], 3);
    assert_eq!(fault_cycle["partition_active_after_exercise"], true);
    assert_eq!(fault_cycle["heal_observed"], true);
    post_heal
        .validate_structure()
        .expect("measured post-heal report should be internally valid");
}

#[test]
fn post_heal_report_rejects_missing_or_unhealed_fault_cycle() {
    let config = SoakConfig::new(SimSeed(0xfa17), 0);
    let summary = run_raft_random_soak(
        production_configs().expect("production liveness configuration should be valid"),
        config,
    )
    .expect("post-heal scenario should inject and heal its own fault");
    let report = summary
        .liveness_reports()
        .iter()
        .find(|report| report.feature_id() == "leader-convergence")
        .expect("post-heal report should exist");

    let mut missing = report.clone();
    missing.fault_cycle = None;
    assert!(missing
        .validate_structure()
        .expect_err("missing fault-cycle evidence must fail")
        .contains("fault-cycle"));

    let mut unhealed = report.clone();
    unhealed
        .fault_cycle
        .as_mut()
        .expect("real fault-cycle evidence should exist")
        .heal_observed = super::EvidenceStatus::Unsatisfied;
    assert!(unhealed
        .validate_structure()
        .expect_err("an unhealed fault must fail")
        .contains("heal observation"));

    let mut no_protocol_work = report.clone();
    no_protocol_work
        .fault_cycle
        .as_mut()
        .expect("real fault-cycle evidence should exist")
        .ticks_executed = 0;
    assert!(no_protocol_work
        .validate_structure()
        .expect_err("a no-op partition must fail")
        .contains("partitioned tick execution"));

    let mut unchanged_protocol = report.clone();
    unchanged_protocol
        .fault_cycle
        .as_mut()
        .expect("real fault-cycle evidence should exist")
        .protocol_state_changed = false;
    assert!(unchanged_protocol
        .validate_structure()
        .expect_err("a protocol-no-op partition must fail")
        .contains("protocol state change"));

    let mut vanished_during_exercise = report.clone();
    vanished_during_exercise
        .fault_cycle
        .as_mut()
        .expect("real fault-cycle evidence should exist")
        .partition_active_after_exercise = super::EvidenceStatus::Unsatisfied;
    assert!(vanished_during_exercise
        .validate_structure()
        .expect_err("the fault must remain active through protocol exercise")
        .contains("partition persistence"));
}

#[test]
fn captured_preconditions_reject_stopped_fault_or_quorum_claims_that_are_false() {
    let config = SoakConfig::new(SimSeed(0xfa17), 0);
    let mut state = ExplorationState::new(Cluster::new_with_seed(
        production_configs().expect("production liveness configuration should be valid"),
        config.seed(),
    ));
    for (a, b) in [(1, 2), (1, 3), (2, 3)] {
        try_apply_soak_action(
            &mut state,
            SoakOperation::Partition {
                a: NodeId(a),
                b: NodeId(b),
            },
        )
        .expect("fixture partition must remain valid");
    }

    let preconditions = LivenessPreconditions::capture(
        &state,
        LivenessPreconditionProbe {
            leader: Some(NodeId(1)),
            fault_requirement: FaultStateRequirement::Stopped,
            stable_leader_observed: None,
            accepted_proposal_observed: None,
            authority_loss_observed: None,
        },
    );
    let error = preconditions
        .validate()
        .expect_err("captured false preconditions must not become success evidence");

    assert!(matches!(error, "fault_state" | "mutually_reachable_quorum"));
}

#[test]
fn fault_preconditions_are_measured_for_healed_and_partitioned_scenarios() {
    let config = SoakConfig::new(SimSeed(0x51_7e), 0);
    let summary = run_raft_random_soak(
        production_configs().expect("production liveness configuration should be valid"),
        config,
    )
    .expect("liveness scenarios should complete");

    let post_heal = summary
        .liveness_reports_json()
        .into_iter()
        .find(|report| report["feature_id"] == "leader-convergence")
        .expect("post-heal report should exist");
    assert_eq!(post_heal["preconditions"]["fault_requirement"], "stopped");
    assert_eq!(post_heal["preconditions"]["faults_stopped"], true);
    assert_eq!(post_heal["preconditions"]["partition_active"], false);

    let quorum_only = summary
        .liveness_reports_json()
        .into_iter()
        .find(|report| report["feature_id"] == "quorum-only-leader-convergence")
        .expect("quorum-only report should exist");
    assert_eq!(
        quorum_only["preconditions"]["fault_requirement"],
        "active-partition"
    );
    assert_eq!(quorum_only["preconditions"]["faults_stopped"], false);
    assert_eq!(quorum_only["preconditions"]["partition_active"], true);
}

#[test]
fn optional_features_use_fresh_fixtures_and_emit_honest_evidence() {
    let config = SoakConfig::new(SimSeed(0x51_7e), 0)
        .with_max_read_indexes(1)
        .with_max_membership_changes(1)
        .with_max_transfers(1)
        .with_snapshot_catchup_probe();
    let summary = run_raft_random_soak(
        production_configs().expect("production liveness configuration should be valid"),
        config,
    )
    .expect("fresh optional feature fixtures should all complete");

    assert_eq!(summary.liveness_reports().len(), 10);
    for report in summary.liveness_reports() {
        report
            .validate_structure()
            .unwrap_or_else(|error| panic!("{} report is invalid: {error}", report.feature_id()));
    }
    for feature_id in ["membership-transition", "leadership-transfer"] {
        let report = summary
            .liveness_reports()
            .iter()
            .find(|report| report.feature_id() == feature_id)
            .unwrap_or_else(|| panic!("missing {feature_id} report"));
        assert!(report.to_json()["stable_leader"].is_null());
    }
}

#[rafter_invariant_test::detector_test]
fn lv_03_read_barrier_detector_rejects_exhausted_bound() {
    let config = SoakConfig::new(SimSeed(0x51_7e), 0).with_max_read_indexes(1);
    let (mut state, convergence_budget) = optional_monitor_fixture(config);
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();

    let failure = oracle_expect_err!(
        run_read_barrier_liveness_detector(
            &mut state,
            config,
            &mut trace,
            &mut observed_actions,
            convergence_budget,
            1,
            TerminalRecorderMode::DropTerminalRecord,
        ),
        "a fresh read barrier cannot finish in one delayed operation round",
    );

    assert_bounded_operation_failure(&failure, SoakActionKind::ReadIndex);
}

#[rafter_invariant_test::detector_test]
fn lv_03_membership_transition_detector_rejects_exhausted_bound() {
    let config = SoakConfig::new(SimSeed(0x51_7e), 0).with_max_membership_changes(1);
    let (mut state, convergence_budget) = optional_monitor_fixture(config);
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();

    let failure = oracle_expect_err!(
        run_membership_transition_liveness_detector(
            &mut state,
            config,
            &mut trace,
            &mut observed_actions,
            convergence_budget,
            1,
            TerminalRecorderMode::DropTerminalRecord,
        ),
        "an issued membership transition cannot finish in one operation round",
    );

    assert_bounded_operation_failure(&failure, SoakActionKind::RemoveVoter);
}

#[rafter_invariant_test::detector_test]
fn lv_03_leadership_transfer_detector_rejects_exhausted_bound() {
    let config = SoakConfig::new(SimSeed(0x51_7e), 0).with_max_transfers(1);
    let (mut state, convergence_budget) = optional_monitor_fixture(config);
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();

    let failure = oracle_expect_err!(
        run_leadership_transfer_liveness_detector(
            &mut state,
            config,
            &mut trace,
            &mut observed_actions,
            convergence_budget,
            1,
            TerminalRecorderMode::DropTerminalRecord,
        ),
        "an issued leadership transfer cannot finish in one operation round",
    );

    assert_bounded_operation_failure(&failure, SoakActionKind::Transfer);
}

#[rafter_invariant_test::detector_test]
fn lv_03_snapshot_catch_up_detector_rejects_exhausted_bound() {
    let config = SoakConfig::new(SimSeed(0x51_7e), 0).with_snapshot_catchup_probe();
    let failure = oracle_expect_err!(
        run_snapshot_catchup_liveness_detector(config, 1, TerminalRecorderMode::DropTerminalRecord),
        "a pending snapshot transfer cannot finish in one bounded round",
    );

    oracle_assert_eq!(
        failure.failure.invariant(),
        catalog::LV_03_FEATURE_OPERATION_PROGRESS
    );
    oracle_assert_eq!(failure.failure.kind(), FailureKind::InvariantViolation);
    oracle_assert!(failure
        .failure
        .message()
        .contains("within 1 bounded rounds"));
}

#[test]
fn read_barrier_detector_classifies_unreached_leader_antecedent_as_coverage() {
    let config = SoakConfig::new(SimSeed(0x51_7e), 0).with_max_read_indexes(1);
    let (mut state, _) = optional_monitor_fixture(config);
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();

    let failure = run_read_barrier_liveness_detector(
        &mut state,
        config,
        &mut trace,
        &mut observed_actions,
        0,
        0,
        TerminalRecorderMode::Production,
    )
    .expect_err("zero convergence rounds cannot establish the read antecedent");

    assert_eq!(failure.failure.kind(), FailureKind::CoverageNotReached);
    assert!(!trace
        .iter()
        .any(|action| matches!(action, SoakAction::ReadIndex { .. })));
}

fn optional_monitor_fixture(config: SoakConfig) -> (ExplorationState, usize) {
    let state = ExplorationState::new(Cluster::new_with_seed(
        production_configs().expect("production liveness configuration should be valid"),
        config.seed(),
    ));
    let budget = soak_liveness_round_budget(&state, config);
    (state, budget)
}

fn assert_bounded_operation_failure(
    failure: &crate::model_check::soak::SoakFailure,
    expected_action: SoakActionKind,
) {
    oracle_assert_eq!(
        failure.failure.invariant(),
        catalog::LV_03_FEATURE_OPERATION_PROGRESS
    );
    oracle_assert_eq!(failure.failure.kind(), FailureKind::InvariantViolation);
    oracle_assert!(failure.failure.message().contains("within 1"));
    oracle_assert!(failure
        .trace
        .iter()
        .any(|action| action.kind() == expected_action));
    oracle_assert!(failure
        .trace
        .iter()
        .any(|action| matches!(action, SoakAction::Tick(_))));
}

use rafter_sim::model_check::SoakConfig;

use super::{
    ExpectedLivenessFeature, FaultRequirement, ProposalOutcomeExpectation, StableRoundsExpectation,
};

pub(super) fn expected_liveness_features(config: SoakConfig) -> Vec<ExpectedLivenessFeature> {
    let mut expected = required_liveness_features();
    if config.checks_read_progress() {
        expected.push(ExpectedLivenessFeature {
            feature_id: "read-barrier",
            invariant_id: "LV-03",
            clause_ids: &["LV-03.a"],
            scenario_id: "stable-leader-read-barrier-v1",
            observation_id: "terminated_liveness_read_barriers",
            remained_leader_through_probe: Some(true),
            stable_rounds: StableRoundsExpectation::Exact(2),
            proposal_outcome: ProposalOutcomeExpectation::None,
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 2,
            fixed_rounds: 0,
        });
    }
    append_optional_operation_features(&mut expected, config);
    expected
}

fn required_liveness_features() -> Vec<ExpectedLivenessFeature> {
    vec![
        ExpectedLivenessFeature {
            feature_id: "leader-convergence",
            invariant_id: "LV-01",
            clause_ids: &["LV-01.a"],
            scenario_id: "post-heal-stable-quorum-v1",
            observation_id: "post_heal_quiescent_leaders",
            remained_leader_through_probe: Some(true),
            stable_rounds: StableRoundsExpectation::Exact(2),
            proposal_outcome: ProposalOutcomeExpectation::None,
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: true,
            phase_count: 1,
            fixed_rounds: 1,
        },
        ExpectedLivenessFeature {
            feature_id: "leader-usability",
            invariant_id: "LV-01",
            clause_ids: &["LV-01.b"],
            scenario_id: "post-heal-stable-quorum-v1",
            observation_id: "post_heal_stable_leader_usability_windows",
            remained_leader_through_probe: Some(true),
            stable_rounds: StableRoundsExpectation::Exact(2),
            proposal_outcome: ProposalOutcomeExpectation::Exact("committed"),
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 1,
            fixed_rounds: 0,
        },
        ExpectedLivenessFeature {
            feature_id: "quorum-only-leader-convergence",
            invariant_id: "LV-01",
            clause_ids: &["LV-01.a"],
            scenario_id: "minority-unavailable-stable-quorum-v1",
            observation_id: "quorum_only_post_fault_leaders",
            remained_leader_through_probe: Some(true),
            stable_rounds: StableRoundsExpectation::Exact(2),
            proposal_outcome: ProposalOutcomeExpectation::None,
            authority_loss: false,
            fault_requirement: FaultRequirement::ActivePartition,
            fault_cycle: false,
            phase_count: 1,
            fixed_rounds: 0,
        },
        ExpectedLivenessFeature {
            feature_id: "quorum-only-leader-usability",
            invariant_id: "LV-01",
            clause_ids: &["LV-01.b"],
            scenario_id: "minority-unavailable-stable-quorum-v1",
            observation_id: "quorum_only_stable_leader_usability_windows",
            remained_leader_through_probe: Some(true),
            stable_rounds: StableRoundsExpectation::Exact(2),
            proposal_outcome: ProposalOutcomeExpectation::Exact("committed"),
            authority_loss: false,
            fault_requirement: FaultRequirement::ActivePartition,
            fault_cycle: false,
            phase_count: 1,
            fixed_rounds: 0,
        },
        ExpectedLivenessFeature {
            feature_id: "proposal-progress",
            invariant_id: "LV-02",
            clause_ids: &["LV-02.a"],
            scenario_id: "stable-leader-reachable-quorum-v1",
            observation_id: "accepted_completed_liveness_proposals",
            remained_leader_through_probe: Some(true),
            stable_rounds: StableRoundsExpectation::ProbeRounds,
            proposal_outcome: ProposalOutcomeExpectation::Exact("committed"),
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 1,
            fixed_rounds: 0,
        },
        ExpectedLivenessFeature {
            feature_id: "proposal-termination",
            invariant_id: "LV-02",
            clause_ids: &["LV-02.b"],
            scenario_id: "accepted-proposal-authority-loss-v1",
            observation_id: "terminated_liveness_proposals",
            remained_leader_through_probe: Some(false),
            stable_rounds: StableRoundsExpectation::Exact(1),
            proposal_outcome: ProposalOutcomeExpectation::ExplicitTerminal,
            authority_loss: true,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 2,
            fixed_rounds: 0,
        },
    ]
}

fn append_optional_operation_features(
    expected: &mut Vec<ExpectedLivenessFeature>,
    config: SoakConfig,
) {
    if config.checks_membership_progress() {
        expected.push(ExpectedLivenessFeature {
            feature_id: "membership-transition",
            invariant_id: "LV-03",
            clause_ids: &["LV-03.c"],
            scenario_id: "stable-remove-voter-joint-consensus-v1",
            observation_id: "terminated_stable_membership_operations",
            remained_leader_through_probe: None,
            stable_rounds: StableRoundsExpectation::None,
            proposal_outcome: ProposalOutcomeExpectation::None,
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 2,
            fixed_rounds: 0,
        });
    }
    if config.checks_transfer_progress() {
        expected.push(ExpectedLivenessFeature {
            feature_id: "leadership-transfer",
            invariant_id: "LV-03",
            clause_ids: &["LV-03.d"],
            scenario_id: "caught-up-voter-transfer-v1",
            observation_id: "terminated_target_leadership_transfers",
            remained_leader_through_probe: None,
            stable_rounds: StableRoundsExpectation::None,
            proposal_outcome: ProposalOutcomeExpectation::None,
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 2,
            fixed_rounds: 0,
        });
    }
    if config.checks_snapshot_progress() {
        expected.push(ExpectedLivenessFeature {
            feature_id: "snapshot-catch-up",
            invariant_id: "LV-03",
            clause_ids: &["LV-03.b"],
            scenario_id: "restart-snapshot-transfer-v1",
            observation_id: "completed_expected_snapshot_catchups",
            remained_leader_through_probe: None,
            stable_rounds: StableRoundsExpectation::None,
            proposal_outcome: ProposalOutcomeExpectation::None,
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 1,
            fixed_rounds: 0,
        });
    }
}

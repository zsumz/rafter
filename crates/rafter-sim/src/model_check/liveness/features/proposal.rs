use std::collections::BTreeSet;

use rafter::NodeId;

use super::{
    super::driver::{
        check_soak_safety, drive_liveness_rounds_until_observed, issue_liveness_proposal,
        liveness_proposal_completed, liveness_proposal_terminal_outcome, single_leader,
        soak_liveness_failure, LivenessRoundBudget, ProposalTerminalOutcome, StableLeaderGuard,
    },
    production_monitor_state, FaultStateRequirement, LivenessFeatureReport,
    LivenessPreconditionProbe, LivenessPreconditions, ProposalEvidence, StableLeaderEvidence,
    LV_02_PROGRESS_CLAUSE_IDS, LV_02_TERMINATION_CLAUSE_IDS,
};
use crate::model_check::{
    catalog,
    helpers::{deliver_all_in_state, elect_node_one_in_state},
    scheduling::SoakOperation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_soak_action,
};

pub(super) fn run_proposal_progress_liveness_check(
    config: SoakConfig,
    budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let mut state = production_monitor_state(config, catalog::LV_02_PROPOSAL_PROGRESS)?;
    let round_budget = LivenessRoundBudget::capture(&state, config, 1);
    elect_node_one_in_state(&mut state);
    deliver_all_in_state(&mut state);

    let leader = NodeId(1);
    if single_leader(&state) != Some(leader) {
        return Err(soak_liveness_failure(
            &state,
            config,
            &[],
            catalog::LV_02_PROPOSAL_PROGRESS,
            "stable-proposal monitor did not establish its expected leader".to_owned(),
        ));
    }

    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();
    let Some(proposal_id) =
        issue_liveness_proposal(&mut state, leader, &mut trace, &mut observed_actions)
    else {
        return Err(soak_liveness_failure(
            &state,
            config,
            &trace,
            catalog::LV_02_PROPOSAL_PROGRESS,
            "stable-proposal monitor could not establish an accepted proposal".to_owned(),
        ));
    };
    check_soak_safety(&state, config, &trace)?;

    let mut guard = StableLeaderGuard::new(leader, budget);
    let completion = drive_liveness_rounds_until_observed(
        &mut state,
        config,
        &mut trace,
        &mut observed_actions,
        budget,
        |state| liveness_proposal_completed(state, proposal_id),
        |state| guard.observe(single_leader(state)).is_ok(),
    )?;
    let outcome = liveness_proposal_terminal_outcome(&state, proposal_id);
    if !completion.completed
        || !completion.observer_held
        || single_leader(&state) != Some(leader)
        || outcome != Some(ProposalTerminalOutcome::Committed)
    {
        return Err(soak_liveness_failure(
            &state,
            config,
            &trace,
            catalog::LV_02_PROPOSAL_PROGRESS,
            format!(
                "accepted proposal {} did not commit under the same stable leader within {budget} bounded-fair rounds",
                proposal_id.0
            ),
        ));
    }

    Ok(LivenessFeatureReport {
        invariant_id: "LV-02",
        clause_ids: LV_02_PROGRESS_CLAUSE_IDS,
        feature_id: "proposal-progress",
        scenario_id: "stable-leader-reachable-quorum-v1",
        observation_id: "accepted_completed_liveness_proposals",
        preconditions: LivenessPreconditions::capture(
            &state,
            LivenessPreconditionProbe {
                leader: Some(leader),
                fault_requirement: FaultStateRequirement::Stopped,
                stable_leader_observed: Some(single_leader(&state) == Some(leader)),
                accepted_proposal_observed: Some(true),
                authority_loss_observed: None,
            },
        ),
        round_budget,
        round_limit: budget,
        rounds_used: completion.rounds_used,
        fault_cycle: None,
        stable_leader: Some(StableLeaderEvidence {
            leader,
            stable_rounds: completion.rounds_used.max(1),
            remained_leader_through_probe: true,
        }),
        proposal: Some(ProposalEvidence {
            proposal_id,
            outcome: ProposalTerminalOutcome::Committed,
        }),
    })
}

pub(super) fn run_proposal_termination_liveness_check(
    config: SoakConfig,
    budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let mut state = production_monitor_state(config, catalog::LV_02_PROPOSAL_PROGRESS)?;
    let round_budget = LivenessRoundBudget::capture(&state, config, 1);
    elect_node_one_in_state(&mut state);
    deliver_all_in_state(&mut state);

    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();
    let Some(proposal_id) =
        issue_liveness_proposal(&mut state, NodeId(1), &mut trace, &mut observed_actions)
    else {
        return Err(soak_liveness_failure(
            &state,
            config,
            &trace,
            catalog::LV_02_PROPOSAL_PROGRESS,
            "proposal-termination monitor could not establish an accepted proposal".to_owned(),
        ));
    };
    let stable_leader_at_acceptance = single_leader(&state) == Some(NodeId(1));

    for peer in [NodeId(2), NodeId(3)] {
        apply_soak_action(
            &mut state,
            SoakOperation::Partition {
                a: NodeId(1),
                b: peer,
            },
        );
        trace.push(SoakAction::Partition {
            a: NodeId(1),
            b: peer,
        });
        observed_actions.insert(SoakActionKind::Partition);
    }
    check_soak_safety(&state, config, &trace)?;

    let termination = drive_liveness_rounds_until_observed(
        &mut state,
        config,
        &mut trace,
        &mut observed_actions,
        budget,
        |state| liveness_proposal_terminal_outcome(state, proposal_id).is_some(),
        |_| true,
    )?;
    if termination.completed {
        let Some(outcome) = liveness_proposal_terminal_outcome(&state, proposal_id) else {
            return Err(soak_liveness_failure(
                &state,
                config,
                &trace,
                catalog::LV_02_PROPOSAL_PROGRESS,
                "proposal-termination monitor reported completion without an outcome".to_owned(),
            ));
        };
        return Ok(LivenessFeatureReport {
            invariant_id: "LV-02",
            clause_ids: LV_02_TERMINATION_CLAUSE_IDS,
            feature_id: "proposal-termination",
            scenario_id: "accepted-proposal-authority-loss-v1",
            observation_id: "terminated_liveness_proposals",
            preconditions: LivenessPreconditions::capture(
                &state,
                LivenessPreconditionProbe {
                    leader: single_leader(&state),
                    fault_requirement: FaultStateRequirement::ActivePartition,
                    stable_leader_observed: Some(stable_leader_at_acceptance),
                    accepted_proposal_observed: Some(true),
                    authority_loss_observed: Some(single_leader(&state) != Some(NodeId(1))),
                },
            ),
            round_budget,
            round_limit: budget,
            rounds_used: termination.rounds_used,
            fault_cycle: None,
            stable_leader: Some(StableLeaderEvidence {
                leader: NodeId(1),
                stable_rounds: 1,
                remained_leader_through_probe: false,
            }),
            proposal: Some(ProposalEvidence {
                proposal_id,
                outcome,
            }),
        });
    }

    Err(soak_liveness_failure(
        &state,
        config,
        &trace,
        catalog::LV_02_PROPOSAL_PROGRESS,
        format!(
            "accepted proposal {} did not reach an explicit terminal state within {budget} authority-loss rounds",
            proposal_id.0
        ),
    ))
}

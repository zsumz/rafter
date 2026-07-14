use std::collections::BTreeSet;

use super::{
    driver::{
        check_soak_safety, drive_liveness_rounds_until_observed_from_round,
        drive_until_stable_leader_from_round, issue_liveness_proposal, liveness_proposal_accepted,
        liveness_proposal_completed, liveness_proposal_terminal_outcome, single_leader,
        soak_liveness_invariant_failure, BoundedRun, LeaderConvergence, LivenessScheduleWindow,
        ProposalTerminalOutcome, StableLeaderGuard,
    },
    SoakAction, SoakActionKind, SoakConfig, SoakFailure,
};
use crate::model_check::{catalog, state::ExplorationState, ProposalId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct StableLeaderUsability {
    pub(super) convergence: LeaderConvergence,
    pub(super) completion: BoundedRun,
    pub(super) proposal_id: ProposalId,
    pub(super) accepted_proposal: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PostHealBudgets {
    pub(super) schedule_round_offset: usize,
    pub(super) convergence_rounds: usize,
    pub(super) usability_rounds: usize,
}

pub(super) fn drive_until_stable_leader_commits(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    initial_convergence: LeaderConvergence,
    budgets: PostHealBudgets,
) -> Result<StableLeaderUsability, SoakFailure> {
    UsabilitySearch {
        state,
        config,
        trace,
        observed_actions,
        budgets,
        rounds_used: 0,
        post_heal_rounds_used: initial_convergence.rounds_used,
        convergence: initial_convergence,
    }
    .run()
}

struct UsabilitySearch<'a> {
    state: &'a mut ExplorationState,
    config: SoakConfig,
    trace: &'a mut Vec<SoakAction>,
    observed_actions: &'a mut BTreeSet<SoakActionKind>,
    budgets: PostHealBudgets,
    rounds_used: usize,
    post_heal_rounds_used: usize,
    convergence: LeaderConvergence,
}

impl UsabilitySearch<'_> {
    fn run(mut self) -> Result<StableLeaderUsability, SoakFailure> {
        loop {
            let leader = self.convergence.leader;
            let (proposal_id, accepted_proposal) = self.issue_proposal(leader)?;
            let remaining = self.remaining_rounds();
            if remaining == 0 {
                break;
            }

            let attempt = self.drive_attempt(leader, proposal_id, remaining)?;
            if self.committed_under_same_leader(leader, proposal_id, attempt) {
                return Ok(self.success(proposal_id, accepted_proposal));
            }

            self.require_terminal_outcome(proposal_id)?;
            if !self.reconverge()? {
                break;
            }
        }

        Err(soak_liveness_invariant_failure(
            self.state,
            self.config,
            self.trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!(
                "no stable post-heal leader committed an accepted usability proposal within {} bounded-fair usability rounds",
                self.budgets.usability_rounds
            ),
        ))
    }

    fn issue_proposal(
        &mut self,
        leader: rafter::NodeId,
    ) -> Result<(ProposalId, bool), SoakFailure> {
        let Some(proposal_id) =
            issue_liveness_proposal(self.state, leader, self.trace, self.observed_actions)
        else {
            return Err(soak_liveness_invariant_failure(
                self.state,
                self.config,
                self.trace,
                catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
                "post-heal stable leader rejected the usability proposal".to_owned(),
            ));
        };
        let accepted = liveness_proposal_accepted(self.state, proposal_id);
        check_soak_safety(self.state, self.config, self.trace)?;
        Ok((proposal_id, accepted))
    }

    fn drive_attempt(
        &mut self,
        leader: rafter::NodeId,
        proposal_id: ProposalId,
        remaining: usize,
    ) -> Result<BoundedRun, SoakFailure> {
        let mut guard = StableLeaderGuard::new(leader, remaining);
        let attempt = drive_liveness_rounds_until_observed_from_round(
            self.state,
            self.config,
            self.trace,
            self.observed_actions,
            self.schedule(remaining),
            |state| liveness_proposal_completed(state, proposal_id),
            |state| guard.observe(single_leader(state)).is_ok(),
        )?;
        self.record_rounds(attempt.rounds_used);
        Ok(attempt)
    }

    fn require_terminal_outcome(&mut self, proposal_id: ProposalId) -> Result<(), SoakFailure> {
        if liveness_proposal_terminal_outcome(self.state, proposal_id).is_some() {
            return Ok(());
        }
        let terminal = drive_liveness_rounds_until_observed_from_round(
            self.state,
            self.config,
            self.trace,
            self.observed_actions,
            self.schedule(self.remaining_rounds()),
            |state| liveness_proposal_terminal_outcome(state, proposal_id).is_some(),
            |_| true,
        )?;
        self.record_rounds(terminal.rounds_used);
        if terminal.completed
            && liveness_proposal_terminal_outcome(self.state, proposal_id).is_some()
        {
            return Ok(());
        }
        Err(soak_liveness_invariant_failure(
            self.state,
            self.config,
            self.trace,
            catalog::LV_02_PROPOSAL_PROGRESS,
            format!(
                "accepted usability proposal {} did not reach an explicit terminal outcome before retry",
                proposal_id.0
            ),
        ))
    }

    fn reconverge(&mut self) -> Result<bool, SoakFailure> {
        let remaining = self.remaining_rounds();
        let Some(next) = drive_until_stable_leader_from_round(
            self.state,
            self.config,
            self.trace,
            self.observed_actions,
            self.schedule(remaining).round_offset,
            remaining,
        )?
        else {
            return Ok(false);
        };
        self.record_rounds(next.rounds_used);
        self.convergence = next;
        Ok(true)
    }

    fn committed_under_same_leader(
        &self,
        leader: rafter::NodeId,
        proposal_id: ProposalId,
        attempt: BoundedRun,
    ) -> bool {
        attempt.completed
            && attempt.observer_held
            && liveness_proposal_terminal_outcome(self.state, proposal_id)
                == Some(ProposalTerminalOutcome::Committed)
            && single_leader(self.state) == Some(leader)
    }

    fn success(&self, proposal_id: ProposalId, accepted_proposal: bool) -> StableLeaderUsability {
        StableLeaderUsability {
            convergence: LeaderConvergence {
                rounds_used: self.post_heal_rounds_used,
                ..self.convergence
            },
            completion: BoundedRun {
                completed: true,
                rounds_used: self.rounds_used,
                observer_held: true,
            },
            proposal_id,
            accepted_proposal,
        }
    }

    fn remaining_rounds(&self) -> usize {
        self.budgets
            .usability_rounds
            .saturating_sub(self.rounds_used)
            .min(
                self.budgets
                    .convergence_rounds
                    .saturating_sub(self.post_heal_rounds_used),
            )
    }

    fn schedule(&self, budget: usize) -> LivenessScheduleWindow {
        LivenessScheduleWindow::new(
            self.budgets
                .schedule_round_offset
                .saturating_add(self.rounds_used),
            budget,
        )
    }

    fn record_rounds(&mut self, rounds: usize) {
        self.rounds_used = self.rounds_used.saturating_add(rounds);
        self.post_heal_rounds_used = self.post_heal_rounds_used.saturating_add(rounds);
    }
}

//! Derivation and validation of the bounded liveness round budget.
//!
//! Every monitor's round limit is a pure function of the observed cluster and
//! the soak configuration, so a report can be rechecked later without replaying
//! the run that produced it.

use super::super::MIN_SOAK_LIVENESS_ROUNDS;
use crate::model_check::{soak::SoakConfig, state::ExplorationState};

use super::LivenessRoundBudget;

impl LivenessRoundBudget {
    pub(in crate::model_check::liveness) fn capture(
        state: &ExplorationState,
        config: SoakConfig,
        phase_count: usize,
    ) -> Self {
        let node_count = state.cluster().nodes.len();
        let queued_messages = state.cluster().network.len();
        let base_rounds = calculate_liveness_round_budget(
            node_count,
            queued_messages,
            config.max_proposals,
            config.max_membership_changes,
            config.max_partitions,
            config.snapshot_catchup_probe,
        );
        Self {
            minimum_rounds: MIN_SOAK_LIVENESS_ROUNDS,
            node_count,
            queued_messages,
            max_proposals: config.max_proposals,
            max_membership_changes: config.max_membership_changes,
            max_partitions: config.max_partitions,
            snapshot_catchup_probe: config.snapshot_catchup_probe,
            base_rounds,
            phase_count,
            fixed_rounds: 0,
        }
    }

    pub(in crate::model_check::liveness) const fn with_fixed_rounds(
        mut self,
        fixed_rounds: usize,
    ) -> Self {
        self.fixed_rounds = fixed_rounds;
        self
    }

    pub(in crate::model_check::liveness) const fn round_limit(self) -> usize {
        self.base_rounds
            .saturating_mul(self.phase_count)
            .saturating_add(self.fixed_rounds)
    }

    pub(in crate::model_check::liveness) fn validate(self) -> Result<(), &'static str> {
        if self.phase_count == 0 {
            return Err("phase_count");
        }
        let expected = calculate_liveness_round_budget(
            self.node_count,
            self.queued_messages,
            self.max_proposals,
            self.max_membership_changes,
            self.max_partitions,
            self.snapshot_catchup_probe,
        );
        if self.minimum_rounds != MIN_SOAK_LIVENESS_ROUNDS || self.base_rounds != expected {
            return Err("base_rounds");
        }
        Ok(())
    }
}

fn calculate_liveness_round_budget(
    node_count: usize,
    queued_messages: usize,
    max_proposals: usize,
    max_membership_changes: usize,
    max_partitions: usize,
    snapshot_catchup_probe: bool,
) -> usize {
    MIN_SOAK_LIVENESS_ROUNDS
        .saturating_add(node_count.saturating_mul(16))
        .saturating_add(queued_messages.saturating_mul(4))
        .saturating_add(max_proposals.saturating_mul(8))
        .saturating_add(max_membership_changes.saturating_mul(16))
        .saturating_add(max_partitions.saturating_mul(16))
        .saturating_add(usize::from(snapshot_catchup_probe).saturating_mul(64))
}

pub(in crate::model_check::liveness) fn soak_liveness_round_budget(
    state: &ExplorationState,
    config: SoakConfig,
) -> usize {
    LivenessRoundBudget::capture(state, config, 1).base_rounds
}

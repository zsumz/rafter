use std::collections::{BTreeMap, BTreeSet};

use rafter::NodeId;

use crate::{model_check::liveness::LivenessFeatureReport, SimSeed};

use super::{SoakAction, SoakActionKind, SoakConfig};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LivenessConfigProvenance {
    max_proposals: usize,
    max_membership_changes: usize,
    max_partitions: usize,
    snapshot_catchup_probe: bool,
}

/// Summary returned after a successful randomized soak run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SoakSummary {
    seed: SimSeed,
    steps_executed: usize,
    observed_actions: BTreeSet<SoakActionKind>,
    action_counts: BTreeMap<SoakActionKind, usize>,
    restarted_nodes: BTreeSet<NodeId>,
    liveness_config: LivenessConfigProvenance,
    liveness_reports: Vec<LivenessFeatureReport>,
}

impl SoakSummary {
    pub(in crate::model_check) fn from_trace(
        config: SoakConfig,
        trace: &[SoakAction],
        observed_actions: &BTreeSet<SoakActionKind>,
        liveness_reports: Vec<LivenessFeatureReport>,
    ) -> Self {
        let mut action_counts = BTreeMap::<SoakActionKind, usize>::new();
        let mut restarted_nodes = BTreeSet::<NodeId>::new();
        for action in trace {
            let kind = action.kind();
            *action_counts.entry(kind).or_default() += 1;
            if let SoakAction::Restart(node_id) = action {
                restarted_nodes.insert(*node_id);
            }
        }
        Self {
            seed: config.seed,
            steps_executed: config.steps,
            observed_actions: observed_actions.clone(),
            action_counts,
            restarted_nodes,
            liveness_config: LivenessConfigProvenance {
                max_proposals: config.max_proposals,
                max_membership_changes: config.max_membership_changes,
                max_partitions: config.max_partitions,
                snapshot_catchup_probe: config.snapshot_catchup_probe,
            },
            liveness_reports,
        }
    }

    /// Returns the deterministic simulator seed.
    #[must_use]
    pub const fn seed(&self) -> SimSeed {
        self.seed
    }

    /// Returns the number of steps executed.
    #[must_use]
    pub const fn steps_executed(&self) -> usize {
        self.steps_executed
    }

    /// Returns the action families observed during the run.
    #[must_use]
    pub const fn observed_actions(&self) -> &BTreeSet<SoakActionKind> {
        &self.observed_actions
    }

    /// Returns how many times an action family was observed.
    #[must_use]
    pub fn action_count(&self, kind: SoakActionKind) -> usize {
        self.action_counts.get(&kind).copied().unwrap_or_default()
    }

    /// Returns nodes that were restarted during the run.
    #[must_use]
    pub const fn restarted_nodes(&self) -> &BTreeSet<NodeId> {
        &self.restarted_nodes
    }

    /// Returns the exact simulator configuration fields that bind liveness evidence.
    #[must_use]
    pub fn liveness_config_provenance_json(&self) -> serde_json::Value {
        serde_json::json!({
            "max_proposals": self.liveness_config.max_proposals,
            "max_membership_changes": self.liveness_config.max_membership_changes,
            "max_partitions": self.liveness_config.max_partitions,
            "snapshot_catchup_probe": self.liveness_config.snapshot_catchup_probe,
        })
    }

    /// Returns the measured per-feature bounded-liveness reports.
    #[must_use]
    pub fn liveness_reports(&self) -> &[LivenessFeatureReport] {
        &self.liveness_reports
    }

    /// Serializes measured liveness reports at the machine-event boundary.
    #[must_use]
    pub fn liveness_reports_json(&self) -> Vec<serde_json::Value> {
        self.liveness_reports
            .iter()
            .map(LivenessFeatureReport::to_json)
            .collect()
    }

    /// Validates the internal structure of every measured liveness report.
    ///
    /// # Errors
    ///
    /// Returns the first structural contract violation.
    pub fn validate_liveness_report_structure(&self) -> Result<(), String> {
        self.liveness_reports
            .iter()
            .try_for_each(LivenessFeatureReport::validate_structure)
    }
}

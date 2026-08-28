use rafter::{NodeConfig, NodeConfigError, NodeId};

use crate::model_check::{
    soak::{SoakConfig, SoakFailure},
    state::ExplorationState,
    ProposalId,
};
use crate::Cluster;

use super::driver::{soak_liveness_coverage_failure, LivenessRoundBudget, ProposalTerminalOutcome};

mod checks;
mod leader;
#[cfg(test)]
mod leader_tests;
mod membership;
mod preconditions;
mod proposal;
#[cfg(test)]
mod proposal_tests;
mod read;
mod report;
#[cfg(test)]
mod report_tests;
mod snapshot;
mod terminal;
mod transfer;
mod validation;

pub(super) use checks::run_feature_liveness_checks;
pub(in crate::model_check) use snapshot::run_snapshot_catchup_liveness_check;

#[derive(Clone, Copy)]
enum TerminalRecorderMode {
    Production,
    #[cfg(test)]
    DropTerminalRecord,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OperationTerminalOutcome {
    Completed,
    Rejected,
    Canceled,
    Committed,
    Installed,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct OperationEvidence {
    operation_id: String,
    outcome: OperationTerminalOutcome,
}

struct TerminalEvidenceRecorder {
    operation_id: String,
    mode: TerminalRecorderMode,
    evidence: Option<OperationEvidence>,
}

pub(super) const LV_01_CONVERGENCE_CLAUSE_IDS: &[&str] = &["LV-01.a"];
pub(super) const LV_01_USABILITY_CLAUSE_IDS: &[&str] = &["LV-01.b"];
pub(super) const LV_02_PROGRESS_CLAUSE_IDS: &[&str] = &["LV-02.a"];
pub(super) const LV_02_TERMINATION_CLAUSE_IDS: &[&str] = &["LV-02.b"];
pub(super) const LV_03_READ_CLAUSE_IDS: &[&str] = &["LV-03.a"];
pub(super) const LV_03_SNAPSHOT_CLAUSE_IDS: &[&str] = &["LV-03.b"];
pub(super) const LV_03_MEMBERSHIP_CLAUSE_IDS: &[&str] = &["LV-03.c"];
pub(super) const LV_03_TRANSFER_CLAUSE_IDS: &[&str] = &["LV-03.d"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EvidenceStatus {
    Satisfied,
    Unsatisfied,
    NotRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FaultStateRequirement {
    Stopped,
    ActivePartition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct LivenessPreconditionProbe {
    pub(super) leader: Option<NodeId>,
    pub(super) fault_requirement: FaultStateRequirement,
    pub(super) stable_leader_observed: Option<bool>,
    pub(super) accepted_proposal_observed: Option<bool>,
    pub(super) authority_loss_observed: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LivenessPreconditions {
    fault_requirement: FaultStateRequirement,
    fault_state: EvidenceStatus,
    faults_stopped: bool,
    partition_active: bool,
    mutually_reachable_quorum: EvidenceStatus,
    stable_membership: EvidenceStatus,
    stable_leader: EvidenceStatus,
    accepted_proposal: EvidenceStatus,
    authority_loss: EvidenceStatus,
    voter_ids: Vec<NodeId>,
    reachable_voters: usize,
    quorum_size: usize,
    unavailable_voters: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct StableLeaderEvidence {
    pub(super) leader: NodeId,
    pub(super) stable_rounds: usize,
    pub(super) remained_leader_through_probe: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProposalEvidence {
    pub(super) proposal_id: ProposalId,
    pub(super) outcome: ProposalTerminalOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct FaultCycleEvidence {
    pub(super) partition_a: NodeId,
    pub(super) partition_b: NodeId,
    pub(super) partition_observed: EvidenceStatus,
    pub(super) partitioned_rounds: usize,
    pub(super) nodes_exercised: usize,
    pub(super) ticks_executed: usize,
    pub(super) deliveries_executed: usize,
    pub(super) drops_executed: usize,
    pub(super) protocol_state_changed: bool,
    pub(super) partition_active_after_exercise: EvidenceStatus,
    pub(super) heal_observed: EvidenceStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Measured evidence emitted by one bounded liveness monitor.
pub struct LivenessFeatureReport {
    pub(super) invariant_id: &'static str,
    pub(super) clause_ids: &'static [&'static str],
    pub(super) feature_id: &'static str,
    pub(super) scenario_id: &'static str,
    pub(super) observation_id: &'static str,
    pub(super) preconditions: LivenessPreconditions,
    pub(super) round_budget: LivenessRoundBudget,
    pub(super) round_limit: usize,
    pub(super) rounds_used: usize,
    pub(super) fault_cycle: Option<FaultCycleEvidence>,
    pub(super) stable_leader: Option<StableLeaderEvidence>,
    pub(super) proposal: Option<ProposalEvidence>,
    pub(super) operation: Option<OperationEvidence>,
}

fn production_monitor_state(
    config: SoakConfig,
    invariant: &'static str,
) -> Result<ExplorationState, SoakFailure> {
    match production_configs() {
        Ok(configs) => Ok(ExplorationState::new(Cluster::new_with_seed(
            configs,
            config.seed,
        ))),
        Err(error) => {
            let empty = ExplorationState::new(Cluster::new_with_seed(Vec::new(), config.seed));
            Err(soak_liveness_coverage_failure(
                &empty,
                config,
                &[],
                invariant,
                format!("invalid production liveness configuration: {error}"),
            ))
        }
    }
}

fn production_configs() -> Result<Vec<NodeConfig>, NodeConfigError> {
    [1_u64, 2, 3]
        .into_iter()
        .map(|id| {
            NodeConfig::new(
                NodeId(id),
                [1_u64, 2, 3]
                    .into_iter()
                    .filter(|peer| *peer != id)
                    .map(NodeId)
                    .collect(),
                3,
            )
        })
        .collect()
}

use std::collections::BTreeSet;

use rafter::{MembershipConfig, NodeConfig, NodeConfigError, NodeId};
use serde_json::json;

use crate::model_check::{
    catalog,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::ExplorationState,
    ProposalId,
};
use crate::Cluster;

use super::driver::{
    single_leader, soak_liveness_coverage_failure, soak_liveness_round_budget, LivenessRoundBudget,
};

mod leader;
#[cfg(test)]
mod leader_tests;
mod membership;
mod proposal;
#[cfg(test)]
mod proposal_tests;
mod read;
#[cfg(test)]
mod report_tests;
mod snapshot;
mod transfer;

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

impl OperationTerminalOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Rejected => "rejected",
            Self::Canceled => "canceled",
            Self::Committed => "committed",
            Self::Installed => "installed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct OperationEvidence {
    operation_id: String,
    outcome: OperationTerminalOutcome,
}

impl OperationEvidence {
    fn new(operation_id: String, outcome: OperationTerminalOutcome) -> Self {
        Self {
            operation_id,
            outcome,
        }
    }
}

struct TerminalEvidenceRecorder {
    operation_id: String,
    mode: TerminalRecorderMode,
    evidence: Option<OperationEvidence>,
}

impl TerminalEvidenceRecorder {
    fn new(operation_id: String, mode: TerminalRecorderMode) -> Self {
        Self {
            operation_id,
            mode,
            evidence: None,
        }
    }

    fn observe(&mut self, outcome: Option<OperationTerminalOutcome>) -> bool {
        if self.evidence.is_some() {
            return true;
        }
        let Some(outcome) = outcome else {
            return false;
        };
        match self.mode {
            TerminalRecorderMode::Production => {
                self.evidence = Some(OperationEvidence::new(self.operation_id.clone(), outcome));
            }
            #[cfg(test)]
            TerminalRecorderMode::DropTerminalRecord => {}
        }
        self.evidence.is_some()
    }

    fn evidence(&self) -> Option<OperationEvidence> {
        self.evidence.clone()
    }
}

use leader::run_quorum_only_leader_liveness_check;
use membership::run_membership_transition_liveness_check;
use proposal::{run_proposal_progress_liveness_check, run_proposal_termination_liveness_check};
use read::run_read_barrier_liveness_check;
pub(in crate::model_check) use snapshot::run_snapshot_catchup_liveness_check;
use snapshot::snapshot_liveness_round_budget;
use transfer::run_leadership_transfer_liveness_check;

use super::driver::{
    ProposalTerminalOutcome, FAIR_DELIVERY_BOUND_ROUNDS, FAIR_MAX_DELIVERY_WAVES_PER_TICK,
    FAIR_SCHEDULER_POLICY_ID, FAIR_TICK_BOUND_ROUNDS, STABLE_LEADER_WINDOW_ROUNDS,
};

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

impl EvidenceStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Satisfied => "satisfied",
            Self::Unsatisfied => "unsatisfied",
            Self::NotRequired => "not-required",
        }
    }

    const fn is_satisfied(self) -> bool {
        matches!(self, Self::Satisfied)
    }

    const fn is_required(self) -> bool {
        !matches!(self, Self::NotRequired)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FaultStateRequirement {
    Stopped,
    ActivePartition,
}

impl FaultStateRequirement {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::ActivePartition => "active-partition",
        }
    }
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

impl LivenessPreconditions {
    pub(super) fn capture(state: &ExplorationState, probe: LivenessPreconditionProbe) -> Self {
        let membership_node = probe.leader.or_else(|| single_leader(state));
        let (stable_membership, voters) = membership_node.map_or_else(
            || (EvidenceStatus::Unsatisfied, Vec::new()),
            |node_id| match state.cluster().effective_membership(node_id) {
                MembershipConfig::Stable(membership)
                    if state.cluster().committed_membership(node_id)
                        == MembershipConfig::Stable(membership.clone()) =>
                {
                    (EvidenceStatus::Satisfied, membership.voters().to_vec())
                }
                MembershipConfig::Stable(membership) => {
                    (EvidenceStatus::Unsatisfied, membership.voters().to_vec())
                }
                MembershipConfig::Joint(joint) => (
                    EvidenceStatus::Unsatisfied,
                    joint
                        .old()
                        .voters()
                        .iter()
                        .chain(joint.new_membership().voters())
                        .copied()
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                ),
            },
        );
        let quorum_size = voters.len() / 2 + 1;
        let reachable_voters = largest_mutually_reachable_group(state.cluster(), &voters);
        let mutually_reachable_quorum = evidence(reachable_voters >= quorum_size);
        let stable_leader = probe
            .stable_leader_observed
            .map_or(EvidenceStatus::NotRequired, evidence);
        let accepted_proposal = probe
            .accepted_proposal_observed
            .map_or(EvidenceStatus::NotRequired, evidence);
        let authority_loss = probe
            .authority_loss_observed
            .map_or(EvidenceStatus::NotRequired, evidence);
        let partition_active = super::driver::has_partition(state.cluster());
        let faults_stopped = !partition_active;
        let fault_state = evidence(match probe.fault_requirement {
            FaultStateRequirement::Stopped => faults_stopped,
            FaultStateRequirement::ActivePartition => partition_active,
        });

        Self {
            fault_requirement: probe.fault_requirement,
            fault_state,
            faults_stopped,
            partition_active,
            mutually_reachable_quorum,
            stable_membership,
            stable_leader,
            accepted_proposal,
            authority_loss,
            voter_ids: voters.clone(),
            reachable_voters,
            quorum_size,
            unavailable_voters: voters.len().saturating_sub(reachable_voters),
        }
    }

    fn validate(&self) -> Result<(), &'static str> {
        for (name, status) in [
            ("fault_state", self.fault_state),
            ("mutually_reachable_quorum", self.mutually_reachable_quorum),
            ("stable_membership", self.stable_membership),
            ("stable_leader", self.stable_leader),
            ("accepted_proposal", self.accepted_proposal),
            ("authority_loss", self.authority_loss),
        ] {
            if status == EvidenceStatus::Unsatisfied {
                return Err(name);
            }
        }
        if self.quorum_size == 0 || self.reachable_voters < self.quorum_size {
            return Err("reachable_voters");
        }
        if self.voter_ids.is_empty()
            || self.quorum_size != self.voter_ids.len() / 2 + 1
            || self.unavailable_voters != self.voter_ids.len().saturating_sub(self.reachable_voters)
        {
            return Err("voter_ids");
        }
        Ok(())
    }
}

const fn evidence(value: bool) -> EvidenceStatus {
    if value {
        EvidenceStatus::Satisfied
    } else {
        EvidenceStatus::Unsatisfied
    }
}

fn largest_mutually_reachable_group(cluster: &Cluster, voters: &[NodeId]) -> usize {
    fn search(
        cluster: &Cluster,
        voters: &[NodeId],
        chosen: &mut Vec<NodeId>,
        index: usize,
    ) -> usize {
        if index == voters.len() {
            return chosen.len();
        }
        let without = search(cluster, voters, chosen, index + 1);
        let candidate = voters[index];
        if chosen
            .iter()
            .all(|member| !cluster.partitioned(*member, candidate))
        {
            chosen.push(candidate);
            let with = search(cluster, voters, chosen, index + 1);
            chosen.pop();
            without.max(with)
        } else {
            without
        }
    }

    search(cluster, voters, &mut Vec::new(), 0)
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

impl FaultCycleEvidence {
    fn validate(self) -> Result<(), &'static str> {
        if self.partition_a == self.partition_b {
            return Err("partition endpoints");
        }
        if self.partition_observed != EvidenceStatus::Satisfied {
            return Err("partition observation");
        }
        if self.partitioned_rounds == 0 || self.nodes_exercised < 2 {
            return Err("partitioned execution rounds");
        }
        if self.ticks_executed != self.partitioned_rounds.saturating_mul(self.nodes_exercised) {
            return Err("partitioned tick execution");
        }
        if self
            .ticks_executed
            .saturating_add(self.deliveries_executed)
            .saturating_add(self.drops_executed)
            == 0
        {
            return Err("partitioned protocol transitions");
        }
        if !self.protocol_state_changed {
            return Err("partitioned protocol state change");
        }
        if self.partition_active_after_exercise != EvidenceStatus::Satisfied {
            return Err("partition persistence through exercise");
        }
        if self.heal_observed != EvidenceStatus::Satisfied {
            return Err("heal observation");
        }
        Ok(())
    }
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

impl LivenessFeatureReport {
    /// Returns the strict machine-readable representation consumed by the invariant runner.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        json!({
            "invariant_id": self.invariant_id,
            "clause_ids": self.clause_ids,
            "feature_id": self.feature_id,
            "scenario_id": self.scenario_id,
            "observation_id": self.observation_id,
            "preconditions": {
                "fault_requirement": self.preconditions.fault_requirement.as_str(),
                "fault_state_satisfied": self.preconditions.fault_state.is_satisfied(),
                "fault_state_status": self.preconditions.fault_state.as_str(),
                "faults_stopped": self.preconditions.faults_stopped,
                "partition_active": self.preconditions.partition_active,
                "mutually_reachable_quorum": self.preconditions.mutually_reachable_quorum.is_satisfied(),
                "mutually_reachable_quorum_status": self.preconditions.mutually_reachable_quorum.as_str(),
                "stable_membership": self.preconditions.stable_membership.is_satisfied(),
                "stable_membership_status": self.preconditions.stable_membership.as_str(),
                "stable_leader_required": self.preconditions.stable_leader.is_required(),
                "stable_leader_satisfied": self.preconditions.stable_leader.is_satisfied(),
                "stable_leader_status": self.preconditions.stable_leader.as_str(),
                "accepted_proposal_required": self.preconditions.accepted_proposal.is_required(),
                "accepted_proposal_satisfied": self.preconditions.accepted_proposal.is_satisfied(),
                "accepted_proposal_status": self.preconditions.accepted_proposal.as_str(),
                "authority_loss_required": self.preconditions.authority_loss.is_required(),
                "authority_loss_satisfied": self.preconditions.authority_loss.is_satisfied(),
                "authority_loss_status": self.preconditions.authority_loss.as_str(),
                "voter_ids": self.preconditions.voter_ids.iter().map(|node_id| node_id.0).collect::<Vec<_>>(),
                "reachable_voters": self.preconditions.reachable_voters,
                "quorum_size": self.preconditions.quorum_size,
                "unavailable_voters": self.preconditions.unavailable_voters,
            },
            "fairness": {
                "policy_id": FAIR_SCHEDULER_POLICY_ID,
                "tick_bound_rounds": FAIR_TICK_BOUND_ROUNDS,
                "delivery_bound_rounds": FAIR_DELIVERY_BOUND_ROUNDS,
                "max_delivery_waves_per_tick": FAIR_MAX_DELIVERY_WAVES_PER_TICK,
            },
            "round_budget": {
                "minimum_rounds": self.round_budget.minimum_rounds,
                "node_count": self.round_budget.node_count,
                "queued_messages": self.round_budget.queued_messages,
                "max_proposals": self.round_budget.max_proposals,
                "max_membership_changes": self.round_budget.max_membership_changes,
                "max_partitions": self.round_budget.max_partitions,
                "snapshot_catchup_probe": self.round_budget.snapshot_catchup_probe,
                "base_rounds": self.round_budget.base_rounds,
                "phase_count": self.round_budget.phase_count,
                "fixed_rounds": self.round_budget.fixed_rounds,
            },
            "round_limit": self.round_limit,
            "rounds_used": self.rounds_used,
            "fault_cycle": self.fault_cycle.map(|evidence| json!({
                "partition_a": evidence.partition_a.0,
                "partition_b": evidence.partition_b.0,
                "partition_observed": evidence.partition_observed.is_satisfied(),
                "partitioned_rounds": evidence.partitioned_rounds,
                "nodes_exercised": evidence.nodes_exercised,
                "ticks_executed": evidence.ticks_executed,
                "deliveries_executed": evidence.deliveries_executed,
                "drops_executed": evidence.drops_executed,
                "protocol_state_changed": evidence.protocol_state_changed,
                "partition_active_after_exercise": evidence.partition_active_after_exercise.is_satisfied(),
                "heal_observed": evidence.heal_observed.is_satisfied(),
            })),
            "stable_leader": self.stable_leader.map(|evidence| json!({
                "node_id": evidence.leader.0,
                "stable_rounds": evidence.stable_rounds,
                "remained_leader_through_probe": evidence.remained_leader_through_probe,
            })),
            "proposal": self.proposal.map(|evidence| json!({
                "proposal_id": evidence.proposal_id.0,
                "terminal_outcome": evidence.outcome.as_str(),
            })),
            "operation": self.operation.as_ref().map(|evidence| json!({
                "operation_id": evidence.operation_id,
                "terminal_outcome": evidence.outcome.as_str(),
            })),
        })
    }

    /// Returns the stable liveness feature identifier.
    #[must_use]
    pub fn feature_id(&self) -> &'static str {
        self.feature_id
    }

    /// Returns the stable scenario identifier used to bind this report.
    #[must_use]
    pub fn scenario_id(&self) -> &'static str {
        self.scenario_id
    }

    /// Returns the observation counter qualified by this report.
    #[must_use]
    pub fn observation_id(&self) -> &'static str {
        self.observation_id
    }

    /// Returns the execution-bound budget and round provenance for this feature.
    #[must_use]
    pub fn execution_provenance_json(&self) -> serde_json::Value {
        json!({
            "feature_id": self.feature_id,
            "round_budget": {
                "minimum_rounds": self.round_budget.minimum_rounds,
                "node_count": self.round_budget.node_count,
                "queued_messages": self.round_budget.queued_messages,
                "max_proposals": self.round_budget.max_proposals,
                "max_membership_changes": self.round_budget.max_membership_changes,
                "max_partitions": self.round_budget.max_partitions,
                "snapshot_catchup_probe": self.round_budget.snapshot_catchup_probe,
                "base_rounds": self.round_budget.base_rounds,
                "phase_count": self.round_budget.phase_count,
                "fixed_rounds": self.round_budget.fixed_rounds,
            },
            "round_limit": self.round_limit,
            "rounds_used": self.rounds_used,
            "operation": self.operation.as_ref().map(|evidence| json!({
                "operation_id": evidence.operation_id,
                "terminal_outcome": evidence.outcome.as_str(),
            })),
        })
    }

    /// Validates the report's preconditions, derived bounds, and feature-specific evidence.
    ///
    /// # Errors
    ///
    /// Returns a description of the first malformed or unsatisfied contract field.
    pub fn validate_structure(&self) -> Result<(), String> {
        self.preconditions
            .validate()
            .map_err(|name| format!("unsatisfied liveness precondition: {name}"))?;
        self.round_budget
            .validate()
            .map_err(|name| format!("invalid liveness round budget: {name}"))?;
        if self.round_limit != self.round_budget.round_limit() {
            return Err(format!(
                "liveness round limit {} does not match derived limit {}",
                self.round_limit,
                self.round_budget.round_limit()
            ));
        }
        if self.rounds_used > self.round_limit {
            return Err(format!(
                "liveness rounds used {} exceed limit {}",
                self.rounds_used, self.round_limit
            ));
        }
        if self.preconditions.stable_leader.is_required() != self.stable_leader.is_some() {
            return Err("stable-leader evidence does not match its precondition".to_owned());
        }
        if self.preconditions.accepted_proposal.is_required() != self.proposal.is_some() {
            return Err("proposal evidence does not match its precondition".to_owned());
        }
        if let Some(leader) = self.stable_leader {
            if !self.preconditions.voter_ids.contains(&leader.leader) {
                return Err("stable leader is not a measured voter".to_owned());
            }
            validate_stable_window(self.feature_id, leader, self.rounds_used)?;
        }
        if let Some(proposal) = self.proposal {
            if proposal.proposal_id.0 == 0 {
                return Err("proposal ID must be positive".to_owned());
            }
            validate_proposal_outcome(self.feature_id, proposal.outcome)?;
        }
        let operation_required = matches!(
            self.feature_id,
            "read-barrier" | "snapshot-catch-up" | "membership-transition" | "leadership-transfer"
        );
        if operation_required != self.operation.is_some() {
            return Err("operation evidence does not match the liveness feature".to_owned());
        }
        if let Some(operation) = &self.operation {
            if operation.operation_id.is_empty() {
                return Err("operation ID must not be empty".to_owned());
            }
            validate_operation_outcome(self.feature_id, operation.outcome)?;
        }
        if expected_clause_ids(self.feature_id) != Some(self.clause_ids) {
            return Err("clause IDs do not match the liveness feature".to_owned());
        }
        let fault_cycle_required = self.feature_id == "leader-convergence"
            && self.scenario_id == "post-heal-stable-quorum-v1";
        if fault_cycle_required != self.fault_cycle.is_some() {
            return Err("fault-cycle evidence does not match the scenario".to_owned());
        }
        if let Some(fault_cycle) = self.fault_cycle {
            fault_cycle
                .validate()
                .map_err(|name| format!("invalid fault-cycle evidence: {name}"))?;
        }
        Ok(())
    }
}

fn expected_clause_ids(feature_id: &str) -> Option<&'static [&'static str]> {
    match feature_id {
        "leader-convergence" | "quorum-only-leader-convergence" => {
            Some(LV_01_CONVERGENCE_CLAUSE_IDS)
        }
        "leader-usability" | "quorum-only-leader-usability" => Some(LV_01_USABILITY_CLAUSE_IDS),
        "proposal-progress" => Some(LV_02_PROGRESS_CLAUSE_IDS),
        "proposal-termination" => Some(LV_02_TERMINATION_CLAUSE_IDS),
        "read-barrier" => Some(LV_03_READ_CLAUSE_IDS),
        "snapshot-catch-up" => Some(LV_03_SNAPSHOT_CLAUSE_IDS),
        "membership-transition" => Some(LV_03_MEMBERSHIP_CLAUSE_IDS),
        "leadership-transfer" => Some(LV_03_TRANSFER_CLAUSE_IDS),
        _ => None,
    }
}

fn validate_stable_window(
    feature_id: &str,
    evidence: StableLeaderEvidence,
    rounds_used: usize,
) -> Result<(), String> {
    let valid = match feature_id {
        "leader-convergence"
        | "leader-usability"
        | "quorum-only-leader-convergence"
        | "quorum-only-leader-usability"
        | "read-barrier" => {
            evidence.stable_rounds == STABLE_LEADER_WINDOW_ROUNDS
                && evidence.remained_leader_through_probe
        }
        "proposal-progress" => {
            evidence.stable_rounds == rounds_used.max(1) && evidence.remained_leader_through_probe
        }
        "proposal-termination" => {
            evidence.stable_rounds == 1 && !evidence.remained_leader_through_probe
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err("stable-leader window does not match monitor semantics".to_owned())
    }
}

fn validate_proposal_outcome(
    feature_id: &str,
    outcome: ProposalTerminalOutcome,
) -> Result<(), String> {
    let valid = match feature_id {
        "leader-usability" | "quorum-only-leader-usability" | "proposal-progress" => {
            outcome == ProposalTerminalOutcome::Committed
        }
        "proposal-termination" => matches!(
            outcome,
            ProposalTerminalOutcome::Committed
                | ProposalTerminalOutcome::Rejected
                | ProposalTerminalOutcome::Unknown
        ),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err("proposal terminal outcome does not match monitor semantics".to_owned())
    }
}

fn validate_operation_outcome(
    feature_id: &str,
    outcome: OperationTerminalOutcome,
) -> Result<(), String> {
    let valid = match feature_id {
        "read-barrier" => matches!(
            outcome,
            OperationTerminalOutcome::Completed
                | OperationTerminalOutcome::Rejected
                | OperationTerminalOutcome::Canceled
        ),
        "snapshot-catch-up" => outcome == OperationTerminalOutcome::Installed,
        "membership-transition" => matches!(
            outcome,
            OperationTerminalOutcome::Committed | OperationTerminalOutcome::Rejected
        ),
        "leadership-transfer" => matches!(
            outcome,
            OperationTerminalOutcome::Completed | OperationTerminalOutcome::Rejected
        ),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err("operation terminal outcome does not match monitor semantics".to_owned())
    }
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

pub(super) fn run_feature_liveness_checks(
    config: SoakConfig,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Result<Vec<LivenessFeatureReport>, SoakFailure> {
    let budget_state =
        production_monitor_state(config, catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE)?;
    let budget = soak_liveness_round_budget(&budget_state, config);
    let mut reports = run_quorum_only_leader_liveness_check(config, budget)?;
    reports.push(run_proposal_progress_liveness_check(config, budget)?);
    reports.push(run_proposal_termination_liveness_check(
        config, budget, budget,
    )?);
    if config.max_read_indexes > 0 {
        let mut state =
            production_monitor_state(config, catalog::LV_03_FEATURE_OPERATION_PROGRESS)?;
        let mut trace = Vec::<SoakAction>::new();
        let mut feature_actions = BTreeSet::new();
        let budget = soak_liveness_round_budget(&state, config);
        reports.push(run_read_barrier_liveness_check(
            &mut state,
            config,
            &mut trace,
            &mut feature_actions,
            budget,
            budget,
        )?);
        observed_actions.extend(feature_actions);
    }
    if config.max_membership_changes > 0 {
        let mut state =
            production_monitor_state(config, catalog::LV_03_FEATURE_OPERATION_PROGRESS)?;
        let mut trace = Vec::<SoakAction>::new();
        let mut feature_actions = BTreeSet::new();
        let budget = soak_liveness_round_budget(&state, config);
        reports.push(run_membership_transition_liveness_check(
            &mut state,
            config,
            &mut trace,
            &mut feature_actions,
            budget,
            budget,
        )?);
        observed_actions.extend(feature_actions);
    }
    if config.max_transfers > 0 {
        let mut state =
            production_monitor_state(config, catalog::LV_03_FEATURE_OPERATION_PROGRESS)?;
        let mut trace = Vec::<SoakAction>::new();
        let mut feature_actions = BTreeSet::new();
        let budget = soak_liveness_round_budget(&state, config);
        reports.push(run_leadership_transfer_liveness_check(
            &mut state,
            config,
            &mut trace,
            &mut feature_actions,
            budget,
            budget,
        )?);
        observed_actions.extend(feature_actions);
    }
    if config.snapshot_catchup_probe {
        let budget = snapshot_liveness_round_budget(config);
        reports.push(run_snapshot_catchup_liveness_check(config, budget)?);
    }
    Ok(reports)
}

//! The machine-readable liveness feature report.
//!
//! `to_json` is the exact artifact the invariant runner consumes, and
//! `validate_structure` rechecks every derived bound and evidence field from
//! the report alone, without replaying the run that produced it.

use serde_json::json;

use super::super::driver::{
    FAIR_DELIVERY_BOUND_ROUNDS, FAIR_MAX_DELIVERY_WAVES_PER_TICK, FAIR_SCHEDULER_POLICY_ID,
    FAIR_TICK_BOUND_ROUNDS,
};
use super::validation::{
    expected_clause_ids, validate_operation_outcome, validate_proposal_outcome,
    validate_stable_window,
};
use super::{EvidenceStatus, FaultCycleEvidence, LivenessFeatureReport};

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

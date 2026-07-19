//! Reviewed contracts for bounded simulator-liveness evidence.

use serde::{Deserialize, Serialize};

/// Registry-owned semantics for one structured bounded-liveness report family.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SimulatorLivenessContract {
    pub invariant_id: String,
    pub clause_ids: Vec<String>,
    pub feature_id: String,
    pub scenario_id: String,
    pub observation_id: String,
    pub fault_requirement: String,
    pub stable_leader_retained: Option<bool>,
    pub stable_leader_rounds_minimum: Option<u64>,
    pub stable_leader_rounds_exact: Option<u64>,
    pub stable_leader_rounds_relation: String,
    pub proposal_outcome: String,
    pub authority_loss_required: bool,
    pub fault_cycle_required: bool,
    pub fairness_policy_id: String,
    pub fairness_tick_bound_rounds: u64,
    pub fairness_delivery_bound_rounds: u64,
    pub fairness_max_delivery_waves_per_tick: u64,
    pub round_budget_provenance: String,
    pub minimum_rounds: u64,
    pub rounds_per_node: u64,
    pub rounds_per_queued_message: u64,
    pub rounds_per_proposal: u64,
    pub rounds_per_membership_change: u64,
    pub rounds_per_partition: u64,
    pub snapshot_catchup_rounds: u64,
    pub phase_count: u64,
    pub fixed_rounds: u64,
}

/// Exact profile and check configuration independently expected for a soak run.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SimulatorExecutionContract {
    pub contract_id: String,
    pub profile_id: String,
    pub check_id: String,
    pub check_kind: String,
    pub node_config_id: String,
    pub node_count: u64,
    pub steps: u64,
    pub max_proposals: u64,
    pub max_restarts: u64,
    pub max_read_indexes: u64,
    pub max_membership_changes: u64,
    pub max_transfers: u64,
    pub max_partitions: u64,
    pub max_lossy_restarts: u64,
    pub snapshot_catchup_probe: bool,
    pub tick_skew_node_id: Option<u64>,
    pub tick_skew_weight: Option<u64>,
}

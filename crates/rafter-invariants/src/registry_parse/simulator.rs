use std::collections::BTreeMap;

use crate::catalog::{CatalogError, SimulatorIdentity, TestIdentity};
use crate::types::SimulatorLivenessContract;

use super::evidence::validate_test_identity;
use super::syntax::{
    parse_bool, parse_optional_bool, parse_optional_u64, parse_u64, parse_usize, split_list,
};

pub(super) fn parse_simulator_identity(
    index: usize,
    record: &BTreeMap<String, String>,
    required: &impl Fn(&str) -> Result<String, CatalogError>,
) -> Result<SimulatorIdentity, CatalogError> {
    let checks = required("simulator_check")?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let negative_test = if let Some(fixture) = record.get("negative_fixture") {
        let identity = TestIdentity {
            package: required("negative_fixture_package")?,
            target_kind: required("negative_fixture_target_kind")?,
            target: required("negative_fixture_target")?,
            test_name: required("negative_fixture_test_name")?,
        };
        validate_test_identity(index, &identity, fixture, "simulator negative fixture")?;
        Some(identity)
    } else {
        None
    };
    let liveness_report = record
        .get("required_liveness_feature")
        .cloned()
        .map(|feature_id| parse_liveness_contract(feature_id, required))
        .transpose()?;
    let identity = SimulatorIdentity {
        checks,
        required_observation: required("required_observation")?,
        minimum_observation: parse_usize(&required("minimum_observation")?)?,
        minimum_protocol_states: optional_usize(record, "minimum_protocol_states")?,
        minimum_verifier_states: optional_usize(record, "minimum_verifier_states")?,
        minimum_runs_per_check: optional_usize(record, "minimum_runs_per_check")?,
        minimum_steps: optional_usize(record, "minimum_steps")?,
        liveness_report,
        negative_test,
    };
    let safety = identity.liveness_report.is_none()
        && identity.minimum_protocol_states.is_some()
        && identity.minimum_verifier_states.is_some();
    let liveness = identity.liveness_report.as_ref().is_some_and(|contract| {
        contract.invariant_id == record.get("id").cloned().unwrap_or_default()
            && !contract.clause_ids.is_empty()
            && split_list(
                record
                    .get("clauses")
                    .map(String::as_str)
                    .unwrap_or_default(),
            ) == contract.clause_ids
            && contract.observation_id == identity.required_observation
            && !contract.feature_id.is_empty()
            && !contract.scenario_id.is_empty()
            && !contract.fairness_policy_id.is_empty()
            && contract.round_budget_provenance == "liveness-round-budget-v1"
            && matches!(
                contract.stable_leader_rounds_relation.as_str(),
                "none" | "exact" | "probe-rounds"
            )
    }) && identity.minimum_runs_per_check.is_some()
        && identity.minimum_steps.is_some();
    if identity.checks.is_empty() || identity.minimum_observation == 0 || !(safety || liveness) {
        return Err(CatalogError(
            "simulator evidence has an incomplete execution contract".to_owned(),
        ));
    }
    Ok(identity)
}

fn parse_liveness_contract(
    feature_id: String,
    required: &impl Fn(&str) -> Result<String, CatalogError>,
) -> Result<SimulatorLivenessContract, CatalogError> {
    Ok(SimulatorLivenessContract {
        invariant_id: required("required_liveness_invariant")?,
        clause_ids: split_list(&required("required_liveness_clauses")?),
        feature_id,
        scenario_id: required("required_liveness_scenario")?,
        observation_id: required("required_observation")?,
        fault_requirement: required("liveness_fault_requirement")?,
        stable_leader_retained: parse_optional_bool(&required("liveness_stable_leader_retained")?)?,
        stable_leader_rounds_minimum: parse_optional_u64(&required(
            "liveness_stable_leader_rounds_minimum",
        )?)?,
        stable_leader_rounds_exact: parse_optional_u64(&required(
            "liveness_stable_leader_rounds_exact",
        )?)?,
        stable_leader_rounds_relation: required("liveness_stable_leader_rounds_relation")?,
        proposal_outcome: required("liveness_proposal_outcome")?,
        authority_loss_required: parse_bool(&required("liveness_authority_loss_required")?)?,
        fault_cycle_required: parse_bool(&required("liveness_fault_cycle_required")?)?,
        fairness_policy_id: required("liveness_fairness_policy")?,
        fairness_tick_bound_rounds: parse_u64(&required("liveness_tick_bound_rounds")?)?,
        fairness_delivery_bound_rounds: parse_u64(&required("liveness_delivery_bound_rounds")?)?,
        fairness_max_delivery_waves_per_tick: parse_u64(&required(
            "liveness_max_delivery_waves_per_tick",
        )?)?,
        round_budget_provenance: required("liveness_round_budget_provenance")?,
        minimum_rounds: parse_u64(&required("liveness_minimum_rounds")?)?,
        rounds_per_node: parse_u64(&required("liveness_rounds_per_node")?)?,
        rounds_per_queued_message: parse_u64(&required("liveness_rounds_per_queued_message")?)?,
        rounds_per_proposal: parse_u64(&required("liveness_rounds_per_proposal")?)?,
        rounds_per_membership_change: parse_u64(&required(
            "liveness_rounds_per_membership_change",
        )?)?,
        rounds_per_partition: parse_u64(&required("liveness_rounds_per_partition")?)?,
        snapshot_catchup_rounds: parse_u64(&required("liveness_snapshot_catchup_rounds")?)?,
        phase_count: parse_u64(&required("liveness_phase_count")?)?,
        fixed_rounds: parse_u64(&required("liveness_fixed_rounds")?)?,
    })
}

fn optional_usize(
    record: &BTreeMap<String, String>,
    field: &str,
) -> Result<Option<usize>, CatalogError> {
    record
        .get(field)
        .map(|value| parse_usize(value))
        .transpose()
}

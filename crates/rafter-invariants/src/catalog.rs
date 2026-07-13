use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::Path,
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::types::{
    SimulatorExecutionContract, SimulatorLivenessBinding, SimulatorLivenessContract,
    SimulatorLivenessReportBinding,
};

use crate::registry_parse::{
    parse_clauses, parse_evidence, parse_invariants, parse_registry_schema_version,
};

const REGISTRY_SCHEMA_VERSION: u32 = 2;
const PROFILE_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Debug)]
/// Reviewed invariant IDs and their declared executable evidence.
pub struct Catalog {
    pub ids: Vec<String>,
    pub invariants: Vec<InvariantDescriptor>,
    pub canonical_ids: BTreeSet<String>,
    pub clauses: Vec<ClauseDescriptor>,
    pub evidence: Vec<EvidenceDescriptor>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// One reviewed parent invariant and its documented verification boundary.
pub struct InvariantDescriptor {
    pub id: String,
    pub statement: String,
    pub scope: String,
    pub assumptions: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// One stable, atomic normative obligation owned by a parent invariant.
pub struct ClauseDescriptor {
    pub invariant_id: String,
    pub clause_id: String,
    pub statement: String,
    pub scope: String,
    pub assumptions: String,
    pub required: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// One direct or end-to-end evidence declaration from the registry.
pub struct EvidenceDescriptor {
    pub invariant_id: String,
    pub clause_id: String,
    pub layer: String,
    pub strength: String,
    pub path: String,
    pub symbol: String,
    pub negative_fixture: Option<String>,
    pub test: Option<TestIdentity>,
    pub simulator: Option<SimulatorIdentity>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// Exact Cargo target and libtest identity for tests-layer evidence.
pub struct TestIdentity {
    pub package: String,
    pub target_kind: String,
    pub target: String,
    pub test_name: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// Exact simulator legs, coverage floors, and detector qualification.
pub struct SimulatorIdentity {
    pub checks: Vec<String>,
    pub required_observation: String,
    pub minimum_observation: usize,
    pub minimum_protocol_states: Option<usize>,
    pub minimum_verifier_states: Option<usize>,
    pub minimum_runs_per_check: Option<usize>,
    pub minimum_steps: Option<usize>,
    pub liveness_report: Option<SimulatorLivenessContract>,
    pub negative_test: Option<TestIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LivenessReportErrorKind {
    Missing,
    Malformed,
}

#[derive(Debug)]
pub(crate) struct LivenessReportError {
    pub kind: LivenessReportErrorKind,
    pub message: String,
}

impl TestIdentity {
    /// Returns the stable check identity required in a tests-layer receipt.
    #[must_use]
    pub fn check_id(&self) -> String {
        format!(
            "tests/{}/{}/{}#{}",
            self.package, self.target_kind, self.target, self.test_name
        )
    }
}

pub(crate) fn derive_liveness_binding(
    profile: &str,
    identity: &SimulatorIdentity,
    available_contracts: &[SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<SimulatorLivenessBinding, LivenessReportError> {
    let contract = identity.liveness_report.as_ref().ok_or_else(|| {
        malformed("simulator identity does not declare a liveness report contract")
    })?;
    let mut reports = Vec::new();
    for check_id in &identity.checks {
        let expected_execution = expected_execution_contract(profile, check_id)
            .map_err(|message| malformed(format!("liveness run `{check_id}`: {message}")))?;
        let expected_reports = expected_report_contracts(available_contracts, &expected_execution)?;
        let runs = events.get(check_id).map(Vec::as_slice).unwrap_or_default();
        if runs.is_empty() {
            return Err(missing(format!(
                "required simulator check `{check_id}` has no liveness run"
            )));
        }
        for event in runs {
            reports.push(derive_liveness_run_binding(
                profile,
                contract,
                check_id,
                &expected_execution,
                &expected_reports,
                event,
            )?);
        }
    }
    reports.sort();
    let contract_sha256 = serialized_digest(contract);
    let reports_sha256 = serialized_digest(&reports);
    Ok(SimulatorLivenessBinding {
        schema_version: 1,
        contract: contract.clone(),
        contract_sha256,
        reports_sha256,
        reports,
    })
}

fn derive_liveness_run_binding(
    profile: &str,
    contract: &SimulatorLivenessContract,
    check_id: &str,
    expected_execution: &SimulatorExecutionContract,
    expected_reports: &BTreeMap<String, &SimulatorLivenessContract>,
    event: &Value,
) -> Result<SimulatorLivenessReportBinding, LivenessReportError> {
    validate_run_execution(profile, check_id, expected_execution, event)?;
    let by_feature = index_run_reports(check_id, expected_reports, event)?;
    let mut selected = None;
    for (feature_id, expected) in expected_reports {
        let report = by_feature[feature_id.as_str()];
        let measured = validate_liveness_report(expected, expected_execution, report)
            .map_err(|message| malformed(format!("liveness run `{check_id}`: {message}")))?;
        if feature_id == &contract.feature_id {
            selected = Some((report, measured));
        }
    }
    let (report, (round_limit, rounds_used)) = selected.ok_or_else(|| {
        malformed(format!(
            "registry feature `{}` is not enabled for liveness run `{check_id}`",
            contract.feature_id
        ))
    })?;
    let seed = event
        .get("seed")
        .and_then(Value::as_u64)
        .ok_or_else(|| malformed(format!("liveness run `{check_id}` has no integer seed")))?;
    Ok(SimulatorLivenessReportBinding {
        check_id: check_id.to_owned(),
        seed,
        execution_contract_sha256: serialized_digest(expected_execution),
        execution_contract: expected_execution.clone(),
        report_sha256: canonical_value_digest(report),
        round_limit,
        rounds_used,
    })
}

fn validate_run_execution(
    profile: &str,
    check_id: &str,
    expected: &SimulatorExecutionContract,
    event: &Value,
) -> Result<(), LivenessReportError> {
    if event.get("status").and_then(Value::as_str) != Some("pass") {
        return Err(malformed(format!(
            "liveness run `{check_id}` is not a passing soak-check"
        )));
    }
    if event.get("check_id").and_then(Value::as_str) != Some(expected.check_id.as_str())
        || event.get("steps").and_then(Value::as_u64) != Some(expected.steps)
    {
        return Err(malformed(format!(
            "liveness run `{check_id}` does not match its expected check or step identity"
        )));
    }
    let value = event.get("execution_contract").ok_or_else(|| {
        malformed(format!(
            "liveness run `{check_id}` has no execution contract"
        ))
    })?;
    let observed =
        serde_json::from_value::<SimulatorExecutionContract>(value.clone()).map_err(|error| {
            malformed(format!(
                "liveness run `{check_id}` has malformed execution contract: {error}"
            ))
        })?;
    if observed != *expected {
        return Err(malformed(format!(
            "liveness run `{check_id}` execution contract does not match profile `{profile}`"
        )));
    }
    Ok(())
}

fn index_run_reports<'a>(
    check_id: &str,
    expected: &BTreeMap<String, &SimulatorLivenessContract>,
    event: &'a Value,
) -> Result<BTreeMap<&'a str, &'a Value>, LivenessReportError> {
    let values = match event.get("liveness_reports") {
        None | Some(Value::Null) => {
            return Err(missing(format!(
                "liveness run `{check_id}` has no structured reports"
            )))
        }
        Some(Value::Array(values)) => values,
        Some(_) => {
            return Err(malformed(format!(
                "liveness run `{check_id}` reports are not an array"
            )))
        }
    };
    let mut by_feature = BTreeMap::new();
    for report in values {
        let feature_id = report
            .get("feature_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                malformed(format!(
                    "liveness run `{check_id}` contains a report without feature identity"
                ))
            })?;
        if by_feature.insert(feature_id, report).is_some() {
            return Err(malformed(format!(
                "liveness run `{check_id}` contains duplicate feature `{feature_id}`"
            )));
        }
    }
    validate_feature_inventory(check_id, expected, &by_feature)?;
    Ok(by_feature)
}

fn validate_feature_inventory(
    check_id: &str,
    expected: &BTreeMap<String, &SimulatorLivenessContract>,
    observed: &BTreeMap<&str, &Value>,
) -> Result<(), LivenessReportError> {
    let observed_features = observed.keys().copied().collect::<BTreeSet<_>>();
    let expected_features = expected.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if observed_features == expected_features {
        return Ok(());
    }
    let missing_features = expected_features
        .difference(&observed_features)
        .copied()
        .collect::<Vec<_>>();
    let unknown_features = observed_features
        .difference(&expected_features)
        .copied()
        .collect::<Vec<_>>();
    if !missing_features.is_empty() && unknown_features.is_empty() {
        return Err(missing(format!(
            "liveness run `{check_id}` is missing features {missing_features:?}"
        )));
    }
    Err(malformed(format!(
        "liveness run `{check_id}` has missing {missing_features:?} and unknown {unknown_features:?} features"
    )))
}

fn expected_report_contracts<'a>(
    available: &'a [SimulatorLivenessContract],
    execution: &SimulatorExecutionContract,
) -> Result<BTreeMap<String, &'a SimulatorLivenessContract>, LivenessReportError> {
    let mut expected = BTreeMap::new();
    for contract in available {
        let required = match contract.feature_id.as_str() {
            "leader-convergence"
            | "quorum-only-leader-convergence"
            | "proposal-progress"
            | "proposal-termination" => true,
            "read-barrier" => execution.max_read_indexes > 0,
            "membership-transition" => execution.max_membership_changes > 0,
            "leadership-transfer" => execution.max_transfers > 0,
            "snapshot-catch-up" => execution.snapshot_catchup_probe,
            feature => {
                return Err(malformed(format!(
                    "registry contains unknown liveness feature `{feature}`"
                )))
            }
        };
        if required {
            if let Some(previous) = expected.insert(contract.feature_id.clone(), contract) {
                if previous != contract {
                    return Err(malformed(format!(
                        "registry contains conflicting liveness contracts for `{}`",
                        contract.feature_id
                    )));
                }
            }
        }
    }
    let mut required = BTreeSet::from([
        "leader-convergence",
        "quorum-only-leader-convergence",
        "proposal-progress",
        "proposal-termination",
    ]);
    if execution.max_read_indexes > 0 {
        required.insert("read-barrier");
    }
    if execution.max_membership_changes > 0 {
        required.insert("membership-transition");
    }
    if execution.max_transfers > 0 {
        required.insert("leadership-transfer");
    }
    if execution.snapshot_catchup_probe {
        required.insert("snapshot-catch-up");
    }
    if expected.keys().map(String::as_str).collect::<BTreeSet<_>>() != required {
        return Err(malformed(
            "registry does not define the complete expected liveness feature set",
        ));
    }
    Ok(expected)
}

pub(crate) fn expected_execution_contract(
    profile: &str,
    canonical_check: &str,
) -> Result<SimulatorExecutionContract, String> {
    let (profile_id, steps, maxima, tick_skew_weight) = match profile {
        "pr" => ("raft-soak", 320, [24, 12, 4, 8, 2, 2, 2], 3),
        "nightly" => ("raft-nightly-soak", 1024, [64, 32, 4, 16, 2, 2, 2], 3),
        "weekly" => ("raft-weekly-soak", 4096, [192, 96, 16, 48, 8, 8, 8], 5),
        value => return Err(format!("unsupported simulator profile `{value}`")),
    };
    let (suffix, check_kind, node_config_id, node_count) = match canonical_check {
        "raft-soak" => ("", "standard", "three-node-standard-v1", 3),
        "raft-soak-lease" => ("-lease", "lease", "three-node-lease-v1", 3),
        "raft-soak-membership" => (
            "-membership",
            "membership",
            "four-node-future-learner-v1",
            4,
        ),
        value => return Err(format!("unsupported canonical soak check `{value}`")),
    };
    Ok(SimulatorExecutionContract {
        contract_id: "rafter-soak-execution-v1".to_owned(),
        profile_id: profile_id.to_owned(),
        check_id: format!("{profile_id}{suffix}"),
        check_kind: check_kind.to_owned(),
        node_config_id: node_config_id.to_owned(),
        node_count,
        steps,
        max_proposals: maxima[0],
        max_restarts: maxima[1],
        max_read_indexes: maxima[2],
        max_membership_changes: maxima[3],
        max_transfers: maxima[4],
        max_partitions: maxima[5],
        max_lossy_restarts: maxima[6],
        snapshot_catchup_probe: true,
        tick_skew_node_id: Some(1),
        tick_skew_weight: Some(tick_skew_weight),
    })
}

pub(crate) fn liveness_contract_digest(contract: &SimulatorLivenessContract) -> String {
    serialized_digest(contract)
}

pub(crate) fn execution_contract_digest(contract: &SimulatorExecutionContract) -> String {
    serialized_digest(contract)
}

pub(crate) fn liveness_reports_digest(reports: &[SimulatorLivenessReportBinding]) -> String {
    serialized_digest(&reports)
}

fn validate_liveness_report(
    contract: &SimulatorLivenessContract,
    execution: &SimulatorExecutionContract,
    report: &Value,
) -> Result<(u64, u64), String> {
    require_exact_fields(
        report,
        &[
            "invariant_id",
            "clause_ids",
            "feature_id",
            "scenario_id",
            "observation_id",
            "preconditions",
            "fairness",
            "round_budget",
            "round_limit",
            "rounds_used",
            "fault_cycle",
            "stable_leader",
            "proposal",
        ],
        "liveness report",
    )?;
    exact_string(report, "invariant_id", &contract.invariant_id)?;
    exact_string_array(report, "clause_ids", &contract.clause_ids)?;
    exact_string(report, "feature_id", &contract.feature_id)?;
    exact_string(report, "scenario_id", &contract.scenario_id)?;
    exact_string(report, "observation_id", &contract.observation_id)?;
    let voter_ids = validate_preconditions(
        contract,
        execution,
        required_object(report, "preconditions")?,
    )?;
    validate_fairness(contract, required_object(report, "fairness")?)?;
    let derived_limit = validate_round_budget(
        contract,
        execution,
        required_object(report, "round_budget")?,
    )?;
    let round_limit = required_u64(report, "round_limit")?;
    let rounds_used = required_u64(report, "rounds_used")?;
    if round_limit != derived_limit || rounds_used > round_limit {
        return Err("round limit is not the registry-derived bound".to_owned());
    }
    validate_optional_evidence(contract, report, &voter_ids, rounds_used)?;
    validate_fault_cycle(contract, report.get("fault_cycle"))?;
    Ok((round_limit, rounds_used))
}

const LIVENESS_PRECONDITION_FIELDS: &[&str] = &[
    "fault_requirement",
    "fault_state_satisfied",
    "fault_state_status",
    "faults_stopped",
    "partition_active",
    "mutually_reachable_quorum",
    "mutually_reachable_quorum_status",
    "stable_membership",
    "stable_membership_status",
    "stable_leader_required",
    "stable_leader_satisfied",
    "stable_leader_status",
    "accepted_proposal_required",
    "accepted_proposal_satisfied",
    "accepted_proposal_status",
    "authority_loss_required",
    "authority_loss_satisfied",
    "authority_loss_status",
    "voter_ids",
    "reachable_voters",
    "quorum_size",
    "unavailable_voters",
];

fn validate_preconditions(
    contract: &SimulatorLivenessContract,
    execution: &SimulatorExecutionContract,
    preconditions: &Map<String, Value>,
) -> Result<Vec<u64>, String> {
    require_exact_object_fields(
        preconditions,
        LIVENESS_PRECONDITION_FIELDS,
        "liveness preconditions",
    )?;
    let fault_requirement = required_map_str(preconditions, "fault_requirement")?;
    let fault_state_satisfied = required_map_bool(preconditions, "fault_state_satisfied")?;
    let fault_state_status = required_map_str(preconditions, "fault_state_status")?;
    let faults_stopped = required_map_bool(preconditions, "faults_stopped")?;
    let partition_active = required_map_bool(preconditions, "partition_active")?;
    let measured_fault = match contract.fault_requirement.as_str() {
        "stopped" => faults_stopped && !partition_active,
        "active-partition" => !faults_stopped && partition_active,
        value => return Err(format!("unknown registry fault requirement `{value}`")),
    };
    if fault_requirement != contract.fault_requirement
        || !fault_state_satisfied
        || fault_state_status != "satisfied"
        || !measured_fault
    {
        return Err("fault-state precondition is inconsistent".to_owned());
    }
    for (value, status) in [
        (
            "mutually_reachable_quorum",
            "mutually_reachable_quorum_status",
        ),
        ("stable_membership", "stable_membership_status"),
    ] {
        if !required_map_bool(preconditions, value)?
            || required_map_str(preconditions, status)? != "satisfied"
        {
            return Err(format!("precondition `{value}` is not satisfied"));
        }
    }
    let voter_ids = required_map_u64_array(preconditions, "voter_ids")?;
    let expected_voter_ids = match execution.node_config_id.as_str() {
        "three-node-standard-v1" | "three-node-lease-v1" | "four-node-future-learner-v1" => {
            [1, 2, 3].as_slice()
        }
        value => return Err(format!("unknown node configuration `{value}`")),
    };
    let unique_voters = voter_ids.iter().copied().collect::<BTreeSet<_>>();
    let reachable = required_map_u64(preconditions, "reachable_voters")?;
    let quorum = required_map_u64(preconditions, "quorum_size")?;
    let unavailable = required_map_u64(preconditions, "unavailable_voters")?;
    if voter_ids != expected_voter_ids
        || unique_voters.len() != voter_ids.len()
        || voter_ids.contains(&0)
        || quorum != voter_ids.len() as u64 / 2 + 1
        || reachable < quorum
        || unavailable != voter_ids.len() as u64 - reachable
    {
        return Err("reachable voters do not prove a quorum".to_owned());
    }
    validate_requirement(
        preconditions,
        "stable_leader",
        contract.stable_leader_retained.is_some(),
    )?;
    validate_requirement(
        preconditions,
        "accepted_proposal",
        contract.proposal_outcome != "none",
    )?;
    let authority_required = required_map_bool(preconditions, "authority_loss_required")?;
    let authority_satisfied = required_map_bool(preconditions, "authority_loss_satisfied")?;
    let authority_status = required_map_str(preconditions, "authority_loss_status")?;
    let expected_status = if contract.authority_loss_required {
        "satisfied"
    } else {
        "not-required"
    };
    if authority_required != contract.authority_loss_required
        || authority_satisfied != contract.authority_loss_required
        || authority_status != expected_status
    {
        return Err("authority-loss precondition is inconsistent".to_owned());
    }
    Ok(voter_ids)
}

fn validate_requirement(
    preconditions: &Map<String, Value>,
    stem: &str,
    expected: bool,
) -> Result<(), String> {
    let required = required_map_bool(preconditions, &format!("{stem}_required"))?;
    let satisfied = required_map_bool(preconditions, &format!("{stem}_satisfied"))?;
    let status = required_map_str(preconditions, &format!("{stem}_status"))?;
    let expected_status = if expected {
        "satisfied"
    } else {
        "not-required"
    };
    if required == expected && satisfied == expected && status == expected_status {
        Ok(())
    } else {
        Err(format!("precondition `{stem}` is inconsistent"))
    }
}

fn validate_fairness(
    contract: &SimulatorLivenessContract,
    fairness: &Map<String, Value>,
) -> Result<(), String> {
    require_exact_object_fields(
        fairness,
        &[
            "policy_id",
            "tick_bound_rounds",
            "delivery_bound_rounds",
            "max_delivery_waves_per_tick",
        ],
        "liveness fairness",
    )?;
    if required_map_str(fairness, "policy_id")? != contract.fairness_policy_id
        || required_map_u64(fairness, "tick_bound_rounds")? != contract.fairness_tick_bound_rounds
        || required_map_u64(fairness, "delivery_bound_rounds")?
            != contract.fairness_delivery_bound_rounds
        || required_map_u64(fairness, "max_delivery_waves_per_tick")?
            != contract.fairness_max_delivery_waves_per_tick
    {
        return Err("fairness policy or numeric bound is inconsistent".to_owned());
    }
    Ok(())
}

fn validate_round_budget(
    contract: &SimulatorLivenessContract,
    execution: &SimulatorExecutionContract,
    budget: &Map<String, Value>,
) -> Result<u64, String> {
    if contract.round_budget_provenance != "liveness-round-budget-v1" {
        return Err("unknown registry round-budget provenance".to_owned());
    }
    require_exact_object_fields(
        budget,
        &[
            "minimum_rounds",
            "node_count",
            "queued_messages",
            "max_proposals",
            "max_membership_changes",
            "max_partitions",
            "snapshot_catchup_probe",
            "base_rounds",
            "phase_count",
            "fixed_rounds",
        ],
        "liveness round budget",
    )?;
    let minimum_rounds = required_map_u64(budget, "minimum_rounds")?;
    let node_count = required_map_u64(budget, "node_count")?;
    let queued_messages = required_map_u64(budget, "queued_messages")?;
    let max_proposals = required_map_u64(budget, "max_proposals")?;
    let max_membership_changes = required_map_u64(budget, "max_membership_changes")?;
    let max_partitions = required_map_u64(budget, "max_partitions")?;
    let snapshot_probe = required_map_bool(budget, "snapshot_catchup_probe")?;
    let base_rounds = required_map_u64(budget, "base_rounds")?;
    let phase_count = required_map_u64(budget, "phase_count")?;
    let fixed_rounds = required_map_u64(budget, "fixed_rounds")?;
    let expected_base = contract
        .minimum_rounds
        .checked_add(weight(node_count, contract.rounds_per_node)?)
        .and_then(|value| {
            value.checked_add(weight(queued_messages, contract.rounds_per_queued_message).ok()?)
        })
        .and_then(|value| {
            value.checked_add(weight(max_proposals, contract.rounds_per_proposal).ok()?)
        })
        .and_then(|value| {
            value.checked_add(
                weight(
                    max_membership_changes,
                    contract.rounds_per_membership_change,
                )
                .ok()?,
            )
        })
        .and_then(|value| {
            value.checked_add(weight(max_partitions, contract.rounds_per_partition).ok()?)
        })
        .and_then(|value| {
            value.checked_add(if snapshot_probe {
                contract.snapshot_catchup_rounds
            } else {
                0
            })
        })
        .ok_or_else(|| "round-budget derivation overflowed".to_owned())?;
    let expected_node_count = if contract.feature_id == "leader-convergence" {
        execution.node_count
    } else {
        3
    };
    if minimum_rounds != contract.minimum_rounds
        || node_count != expected_node_count
        || max_proposals != execution.max_proposals
        || max_membership_changes != execution.max_membership_changes
        || max_partitions != execution.max_partitions
        || snapshot_probe != execution.snapshot_catchup_probe
        || base_rounds != expected_base
        || phase_count != contract.phase_count
        || fixed_rounds != contract.fixed_rounds
    {
        return Err("round-budget provenance or derivation is inconsistent".to_owned());
    }
    base_rounds
        .checked_mul(phase_count)
        .and_then(|value| value.checked_add(fixed_rounds))
        .ok_or_else(|| "round limit overflowed".to_owned())
}

fn weight(value: u64, multiplier: u64) -> Result<u64, String> {
    value
        .checked_mul(multiplier)
        .ok_or_else(|| "round-budget component overflowed".to_owned())
}

fn validate_optional_evidence(
    contract: &SimulatorLivenessContract,
    report: &Value,
    voter_ids: &[u64],
    rounds_used: u64,
) -> Result<(), String> {
    match contract.stable_leader_retained {
        Some(expected) => {
            let leader = required_object(report, "stable_leader")?;
            require_exact_object_fields(
                leader,
                &["node_id", "stable_rounds", "remained_leader_through_probe"],
                "stable-leader evidence",
            )?;
            let node_id = required_map_u64(leader, "node_id")?;
            let stable_rounds = required_map_u64(leader, "stable_rounds")?;
            let valid_rounds = match contract.stable_leader_rounds_relation.as_str() {
                "exact" => contract.stable_leader_rounds_exact == Some(stable_rounds),
                "probe-rounds" => stable_rounds == rounds_used.max(1),
                value => return Err(format!("unknown stable-leader rounds relation `{value}`")),
            };
            if node_id == 0
                || !voter_ids.contains(&node_id)
                || contract
                    .stable_leader_rounds_minimum
                    .is_none_or(|minimum| stable_rounds < minimum)
                || !valid_rounds
                || required_map_bool(leader, "remained_leader_through_probe")? != expected
            {
                return Err(
                    "leader identity, stable window, or retention is inconsistent".to_owned(),
                );
            }
        }
        None if report
            .get("stable_leader")
            .is_some_and(|value| !value.is_null()) =>
        {
            return Err("unexpected stable-leader evidence".to_owned());
        }
        None => {
            if contract.stable_leader_rounds_relation != "none"
                || contract.stable_leader_rounds_minimum.is_some()
                || contract.stable_leader_rounds_exact.is_some()
            {
                return Err("registry stable-leader contract is inconsistent".to_owned());
            }
        }
    }
    match contract.proposal_outcome.as_str() {
        "none" => {
            if report.get("proposal").is_some_and(|value| !value.is_null()) {
                return Err("unexpected proposal evidence".to_owned());
            }
        }
        expected @ ("committed" | "explicit-terminal") => {
            let proposal = required_object(report, "proposal")?;
            require_exact_object_fields(
                proposal,
                &["proposal_id", "terminal_outcome"],
                "proposal evidence",
            )?;
            let proposal_id = required_map_u64(proposal, "proposal_id")?;
            let outcome = required_map_str(proposal, "terminal_outcome")?;
            let valid = if expected == "committed" {
                outcome == "committed"
            } else {
                matches!(outcome, "committed" | "rejected" | "canceled" | "unknown")
            };
            if proposal_id == 0 || !valid {
                return Err("proposal terminal outcome is inconsistent".to_owned());
            }
        }
        value => return Err(format!("unknown registry proposal outcome `{value}`")),
    }
    Ok(())
}

fn validate_fault_cycle(
    contract: &SimulatorLivenessContract,
    value: Option<&Value>,
) -> Result<(), String> {
    if !contract.fault_cycle_required {
        return if value.is_none_or(Value::is_null) {
            Ok(())
        } else {
            Err("unexpected fault-cycle evidence".to_owned())
        };
    }
    let cycle = value
        .and_then(Value::as_object)
        .ok_or_else(|| "required fault-cycle evidence is missing".to_owned())?;
    require_exact_object_fields(
        cycle,
        &[
            "partition_a",
            "partition_b",
            "partition_observed",
            "partitioned_rounds",
            "nodes_exercised",
            "ticks_executed",
            "deliveries_executed",
            "drops_executed",
            "protocol_state_changed",
            "partition_active_after_exercise",
            "heal_observed",
        ],
        "fault-cycle evidence",
    )?;
    let partition_a = required_map_u64(cycle, "partition_a")?;
    let partition_b = required_map_u64(cycle, "partition_b")?;
    let partitioned_rounds = required_map_u64(cycle, "partitioned_rounds")?;
    let nodes_exercised = required_map_u64(cycle, "nodes_exercised")?;
    let ticks_executed = required_map_u64(cycle, "ticks_executed")?;
    let _deliveries = required_map_u64(cycle, "deliveries_executed")?;
    let _drops = required_map_u64(cycle, "drops_executed")?;
    let state_changed = required_map_bool(cycle, "protocol_state_changed")?;
    if partition_a == partition_b
        || !required_map_bool(cycle, "partition_observed")?
        || partitioned_rounds != contract.fixed_rounds
        || nodes_exercised < 2
        || ticks_executed != partitioned_rounds.saturating_mul(nodes_exercised)
        || !state_changed
        || !required_map_bool(cycle, "partition_active_after_exercise")?
        || !required_map_bool(cycle, "heal_observed")?
    {
        return Err("fault-cycle evidence is inconsistent".to_owned());
    }
    Ok(())
}

fn exact_string(value: &Value, field: &str, expected: &str) -> Result<(), String> {
    let actual = value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("field `{field}` is missing or not a string"))?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "field `{field}` expected `{expected}`, found `{actual}`"
        ))
    }
}

fn exact_string_array(value: &Value, field: &str, expected: &[String]) -> Result<(), String> {
    let observed = value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("field `{field}` is missing or not an array"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("field `{field}` contains a non-string"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if observed == expected {
        Ok(())
    } else {
        Err(format!("field `{field}` does not match the registry"))
    }
}

fn require_exact_fields(value: &Value, expected: &[&str], context: &str) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{context} is not an object"))?;
    require_exact_object_fields(object, expected, context)
}

fn require_exact_object_fields(
    object: &Map<String, Value>,
    expected: &[&str],
    context: &str,
) -> Result<(), String> {
    let observed = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    if observed == expected {
        Ok(())
    } else {
        let missing = expected.difference(&observed).copied().collect::<Vec<_>>();
        let unknown = observed.difference(&expected).copied().collect::<Vec<_>>();
        Err(format!(
            "{context} has missing fields {missing:?} or unknown fields {unknown:?}"
        ))
    }
}

fn required_object<'a>(value: &'a Value, field: &str) -> Result<&'a Map<String, Value>, String> {
    value
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| format!("field `{field}` is missing or not an object"))
}

fn required_u64(value: &Value, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("field `{field}` is missing or not an integer"))
}

fn required_map_str<'a>(value: &'a Map<String, Value>, field: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("field `{field}` is missing or not a string"))
}

fn required_map_u64(value: &Map<String, Value>, field: &str) -> Result<u64, String> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("field `{field}` is missing or not an integer"))
}

fn required_map_bool(value: &Map<String, Value>, field: &str) -> Result<bool, String> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("field `{field}` is missing or not a boolean"))
}

fn required_map_u64_array(value: &Map<String, Value>, field: &str) -> Result<Vec<u64>, String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("field `{field}` is missing or not an array"))?
        .iter()
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| format!("field `{field}` contains a non-integer"))
        })
        .collect()
}

fn serialized_digest(value: &impl Serialize) -> String {
    let bytes = serde_json::to_vec(value).expect("serializable liveness receipt contract");
    format!("{:x}", Sha256::digest(bytes))
}

fn canonical_value_digest(value: &Value) -> String {
    let canonical = canonical_value(value);
    let bytes = serde_json::to_vec(&canonical).expect("serializable simulator report");
    format!("{:x}", Sha256::digest(bytes))
}

fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_value).collect()),
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            let mut canonical = Map::new();
            for key in keys {
                canonical.insert(key.clone(), canonical_value(&values[key]));
            }
            Value::Object(canonical)
        }
        value => value.clone(),
    }
}

fn missing(message: impl Into<String>) -> LivenessReportError {
    LivenessReportError {
        kind: LivenessReportErrorKind::Missing,
        message: message.into(),
    }
}

fn malformed(message: impl Into<String>) -> LivenessReportError {
    LivenessReportError {
        kind: LivenessReportErrorKind::Malformed,
        message: message.into(),
    }
}

impl EvidenceDescriptor {
    /// Returns the stable aggregate key for this evidence declaration.
    #[must_use]
    pub fn evidence_id(&self) -> String {
        let base = format!(
            "{}/{}/{}/{}/{}#{}",
            self.invariant_id, self.clause_id, self.layer, self.strength, self.path, self.symbol
        );
        self.negative_fixture
            .as_ref()
            .map_or(base.clone(), |fixture| format!("{base}@{fixture}"))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Explicit invariant IDs and evidence policies for every scheduled profile.
pub struct ProfileManifest {
    pub schema_version: u32,
    pub reviewed_ids: Vec<String>,
    pub profiles: BTreeMap<String, ProfileContract>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Evidence-selection and independent-layer policy for one profile.
pub struct ProfileContract {
    pub description: String,
    pub evidence_policy: String,
    pub clause_policy: String,
    pub required_clause_strength: String,
    pub required_layers: Vec<String>,
    pub required_strengths: Vec<String>,
    pub canonical_minimum_independent_layers: usize,
    pub runners: BTreeMap<String, RunnerContract>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Required deterministic producer identity and bounds for one layer.
pub struct RunnerContract {
    pub producer: String,
    /// Human-facing command that reproduces this runner; actual argv is
    /// recorded separately in each execution receipt.
    pub command: Vec<String>,
    pub configuration: BTreeMap<String, String>,
    pub minimum_observed_checks: usize,
    pub require_peak_rss: bool,
}

#[derive(Debug)]
/// Error reading or validating the invariant catalog and profile manifest.
pub struct CatalogError(pub(super) String);

impl fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CatalogError {}

impl Catalog {
    /// Loads the invariant IDs and executable evidence declarations.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry cannot be read or any evidence
    /// declaration is missing a field required by the aggregate contract.
    pub fn load(path: &Path) -> Result<Self, CatalogError> {
        let source = fs::read_to_string(path)
            .map_err(|error| CatalogError(format!("read {}: {error}", path.display())))?;
        let schema_version = parse_registry_schema_version(&source)?;
        if schema_version != REGISTRY_SCHEMA_VERSION {
            return Err(CatalogError(format!(
                "unsupported registry schema {schema_version}"
            )));
        }
        let (invariants, canonical_ids) = parse_invariants(&source)?;
        let ids = invariants
            .iter()
            .map(|invariant| invariant.id.clone())
            .collect::<Vec<_>>();
        let unique_ids = ids.iter().collect::<BTreeSet<_>>();
        if unique_ids.len() != ids.len() {
            return Err(CatalogError(
                "registry invariant IDs must be unique".to_owned(),
            ));
        }
        let clauses = parse_clauses(&source)?;
        let clause_ids = clauses
            .iter()
            .map(|clause| clause.clause_id.as_str())
            .collect::<BTreeSet<_>>();
        if clause_ids.len() != clauses.len() {
            return Err(CatalogError(
                "registry clause IDs must be globally unique".to_owned(),
            ));
        }
        for clause in &clauses {
            if !unique_ids.contains(&clause.invariant_id) {
                return Err(CatalogError(format!(
                    "clause {} refers to unknown invariant {}",
                    clause.clause_id, clause.invariant_id
                )));
            }
        }
        for invariant_id in &ids {
            if !clauses
                .iter()
                .any(|clause| clause.invariant_id == *invariant_id && clause.required)
            {
                return Err(CatalogError(format!(
                    "invariant {invariant_id} has no required normative clauses"
                )));
            }
        }
        let evidence = parse_evidence(&source)?;
        for descriptor in &evidence {
            let Some(clause) = clauses
                .iter()
                .find(|clause| clause.clause_id == descriptor.clause_id)
            else {
                return Err(CatalogError(format!(
                    "evidence for {} refers to unknown clause {}",
                    descriptor.invariant_id, descriptor.clause_id
                )));
            };
            if clause.invariant_id != descriptor.invariant_id {
                return Err(CatalogError(format!(
                    "evidence parent {} does not own clause {}",
                    descriptor.invariant_id, descriptor.clause_id
                )));
            }
        }
        let evidence_ids = evidence
            .iter()
            .map(EvidenceDescriptor::evidence_id)
            .collect::<BTreeSet<_>>();
        if evidence_ids.len() != evidence.len() {
            return Err(CatalogError(
                "registry evidence declarations must have unique identities".to_owned(),
            ));
        }
        Ok(Self {
            ids,
            invariants,
            canonical_ids,
            clauses,
            evidence,
        })
    }

    #[must_use]
    /// Returns the ordered normative clauses owned by one parent invariant.
    pub fn clauses_for(&self, invariant_id: &str) -> Vec<ClauseDescriptor> {
        self.clauses
            .iter()
            .filter(|clause| clause.invariant_id == invariant_id)
            .cloned()
            .collect()
    }

    #[must_use]
    /// Selects and deduplicates registry evidence required by a profile.
    pub fn required_evidence(
        &self,
        contract: &ProfileContract,
    ) -> BTreeMap<String, Vec<EvidenceDescriptor>> {
        let layers = contract.required_layers.iter().collect::<BTreeSet<_>>();
        let strengths = contract.required_strengths.iter().collect::<BTreeSet<_>>();
        let mut required = self
            .ids
            .iter()
            .cloned()
            .map(|id| (id, Vec::new()))
            .collect::<BTreeMap<_, _>>();
        let mut deduplicated = BTreeSet::new();
        for evidence in &self.evidence {
            if !layers.contains(&evidence.layer) || !strengths.contains(&evidence.strength) {
                continue;
            }
            if deduplicated.insert(evidence.clone()) {
                required
                    .entry(evidence.invariant_id.clone())
                    .or_default()
                    .push(evidence.clone());
            }
        }
        required
    }
}

impl ProfileManifest {
    /// Loads explicit PR, nightly, and weekly evidence policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or is not valid strict
    /// profile-manifest JSON.
    pub fn load(path: &Path) -> Result<Self, CatalogError> {
        let source = fs::read_to_string(path)
            .map_err(|error| CatalogError(format!("read {}: {error}", path.display())))?;
        serde_json::from_str(&source)
            .map_err(|error| CatalogError(format!("parse {}: {error}", path.display())))
    }

    /// Checks the profile manifest against the reviewed registry.
    ///
    /// # Errors
    ///
    /// Returns an error unless the manifest and registry contain exactly the
    /// same 44 IDs and all required profiles have supported nonempty policy.
    pub fn validate(&self, catalog: &Catalog) -> Result<(), CatalogError> {
        if self.schema_version != PROFILE_SCHEMA_VERSION {
            return Err(CatalogError(format!(
                "unsupported profile manifest schema {}",
                self.schema_version
            )));
        }
        if catalog.ids.len() != 44 {
            return Err(CatalogError(format!(
                "registry must contain exactly 44 invariants, found {}",
                catalog.ids.len()
            )));
        }
        let catalog_ids = catalog.ids.iter().collect::<BTreeSet<_>>();
        let reviewed_ids = self.reviewed_ids.iter().collect::<BTreeSet<_>>();
        if reviewed_ids.len() != 44 || reviewed_ids != catalog_ids {
            return Err(CatalogError(
                "reviewed_ids must contain exactly the registry's 44 unique IDs".to_owned(),
            ));
        }
        for profile in ["pr", "nightly", "weekly"] {
            let Some(contract) = self.profiles.get(profile) else {
                return Err(CatalogError(format!("missing required profile {profile}")));
            };
            if contract.evidence_policy != "all_matching_registry_evidence" {
                return Err(CatalogError(format!(
                    "profile {profile} has unsupported evidence policy {}",
                    contract.evidence_policy
                )));
            }
            if contract.clause_policy != "all_required_clauses"
                || contract.required_clause_strength != "direct"
            {
                return Err(CatalogError(format!(
                    "profile {profile} must require direct evidence for all normative clauses"
                )));
            }
            if contract.description.trim().is_empty()
                || contract.required_layers.is_empty()
                || contract.required_strengths.is_empty()
                || contract
                    .required_layers
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != contract.required_layers.len()
                || contract
                    .required_strengths
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != contract.required_strengths.len()
                || contract.canonical_minimum_independent_layers < 2
                || contract.runners.keys().collect::<BTreeSet<_>>()
                    != contract.required_layers.iter().collect::<BTreeSet<_>>()
            {
                return Err(CatalogError(format!(
                    "profile {profile} must document nonempty evidence requirements"
                )));
            }
            for (layer, runner) in &contract.runners {
                if runner.producer.trim().is_empty()
                    || runner.command.is_empty()
                    || runner.configuration.is_empty()
                    || runner.minimum_observed_checks == 0
                {
                    return Err(CatalogError(format!(
                        "profile {profile} runner {layer} has an incomplete execution contract"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod liveness_report_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn missing_and_duplicate_reports_fail_closed() {
        let (identity, contracts, mut missing_events) = fixture();
        report_array_mut(&mut missing_events).remove(0);
        let missing = derive(&identity, &contracts, &missing_events)
            .expect_err("missing feature report must fail");
        assert_eq!(missing.kind, LivenessReportErrorKind::Missing);

        let (_, _, mut duplicate_events) = fixture();
        let duplicate = report_array_mut(&mut duplicate_events)[0].clone();
        report_array_mut(&mut duplicate_events).push(duplicate);
        let duplicate = derive(&identity, &contracts, &duplicate_events)
            .expect_err("duplicate report must fail");
        assert_eq!(duplicate.kind, LivenessReportErrorKind::Malformed);
        assert!(duplicate.message.contains("duplicate feature"));
    }

    #[test]
    fn swapped_report_identity_is_malformed() {
        for (field, value) in [
            ("invariant_id", json!("LV-01")),
            ("feature_id", json!("invented-feature")),
            ("scenario_id", json!("accepted-proposal-authority-loss-v1")),
            ("observation_id", json!("terminated_liveness_proposals")),
            ("clause_ids", json!(["LV-02.b"])),
        ] {
            let (identity, contracts, mut events) = fixture();
            report_mut(&mut events, "proposal-progress")[field] = value;
            let error =
                derive(&identity, &contracts, &events).expect_err("swapped identity must fail");
            assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
        }
    }

    #[test]
    fn false_precondition_is_malformed() {
        let (identity, contracts, mut events) = fixture();
        report_mut(&mut events, "proposal-progress")["preconditions"]
            ["mutually_reachable_quorum"] = json!(false);
        let error =
            derive(&identity, &contracts, &events).expect_err("false precondition must fail");
        assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
        assert!(error.message.contains("precondition"));
    }

    #[test]
    fn fairness_tamper_is_malformed() {
        let (identity, contracts, mut events) = fixture();
        report_mut(&mut events, "proposal-progress")["fairness"]["max_delivery_waves_per_tick"] =
            json!(65);
        let error = derive(&identity, &contracts, &events).expect_err("fairness tamper must fail");
        assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
        assert!(error.message.contains("fairness"));
    }

    #[test]
    fn bound_or_provenance_tamper_is_malformed() {
        for field in ["base_rounds", "max_proposals"] {
            let (identity, contracts, mut events) = fixture();
            report_mut(&mut events, "proposal-progress")["round_budget"][field] = json!(999);
            let error = derive(&identity, &contracts, &events).expect_err("bound tamper must fail");
            assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
            assert!(error.message.contains("round"));
        }
    }

    #[test]
    fn no_op_fault_cycle_is_malformed() {
        let (identity, contracts, mut events) = fixture();
        report_mut(&mut events, "leader-convergence")["fault_cycle"]["protocol_state_changed"] =
            json!(false);
        let error =
            derive(&identity, &contracts, &events).expect_err("a no-op partition cycle must fail");
        assert!(error.message.contains("fault-cycle"));
    }

    #[test]
    fn wrong_leader_retention_or_proposal_outcome_is_malformed() {
        let (identity, contracts, mut leader_events) = fixture();
        report_mut(&mut leader_events, "proposal-progress")["stable_leader"]
            ["remained_leader_through_probe"] = json!(false);
        let leader_error = derive(&identity, &contracts, &leader_events)
            .expect_err("leader retention tamper must fail");
        assert!(leader_error.message.contains("retention"));

        let (_, _, mut invented_voters) = fixture();
        let report = report_mut(&mut invented_voters, "proposal-progress");
        report["preconditions"]["voter_ids"] = json!([4, 5, 6]);
        report["stable_leader"]["node_id"] = json!(4);
        let leader_error = derive(&identity, &contracts, &invented_voters)
            .expect_err("invented voter and leader identities must fail");
        assert!(leader_error.message.contains("quorum"));

        let (_, _, mut proposal_events) = fixture();
        report_mut(&mut proposal_events, "proposal-progress")["proposal"]["terminal_outcome"] =
            json!("pending");
        let proposal_error = derive(&identity, &contracts, &proposal_events)
            .expect_err("proposal outcome tamper must fail");
        assert!(proposal_error.message.contains("proposal terminal outcome"));
    }

    #[test]
    fn coordinated_execution_contract_and_round_budget_tamper_is_rejected() {
        let (identity, contracts, mut events) = fixture();
        let event = &mut events.get_mut("raft-soak").expect("soak events")[0];
        event["execution_contract"]["max_proposals"] = json!(25);
        for report in report_array_mut(&mut events) {
            report["round_budget"]["max_proposals"] = json!(25);
            let phase_count = report["round_budget"]["phase_count"]
                .as_u64()
                .expect("phase count");
            let fixed_rounds = report["round_budget"]["fixed_rounds"]
                .as_u64()
                .expect("fixed rounds");
            report["round_budget"]["base_rounds"] = json!(600);
            report["round_limit"] = json!(600 * phase_count + fixed_rounds);
        }
        let error = derive(&identity, &contracts, &events)
            .expect_err("coordinated execution tamper must fail");
        assert!(error.message.contains("execution contract"));
    }

    #[test]
    fn unknown_fields_and_complete_set_substitution_are_rejected() {
        let (identity, contracts, mut unknown_field_events) = fixture();
        report_mut(&mut unknown_field_events, "proposal-progress")["invented"] = json!(true);
        let error = derive(&identity, &contracts, &unknown_field_events)
            .expect_err("unknown report field must fail");
        assert!(error.message.contains("unknown fields"));

        let (_, _, mut substituted_events) = fixture();
        report_mut(&mut substituted_events, "snapshot-catch-up")["feature_id"] =
            json!("invented-feature");
        let error = derive(&identity, &contracts, &substituted_events)
            .expect_err("feature-set substitution must fail");
        assert_eq!(error.kind, LivenessReportErrorKind::Malformed);
    }

    pub(crate) fn fixture() -> (
        SimulatorIdentity,
        Vec<SimulatorLivenessContract>,
        BTreeMap<String, Vec<Value>>,
    ) {
        let (catalog, _) = crate::tests::loaded();
        let contracts = catalog
            .evidence
            .iter()
            .filter_map(|descriptor| descriptor.simulator.as_ref()?.liveness_report.clone())
            .map(|contract| (contract.feature_id.clone(), contract))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();
        let mut identity = catalog
            .evidence
            .iter()
            .find_map(|descriptor| {
                let identity = descriptor.simulator.as_ref()?;
                (identity.liveness_report.as_ref()?.feature_id == "proposal-progress")
                    .then(|| identity.clone())
            })
            .expect("proposal progress identity");
        identity.checks = vec!["raft-soak".to_owned()];
        identity.minimum_runs_per_check = Some(1);
        let execution =
            expected_execution_contract("pr", "raft-soak").expect("PR execution contract");
        let reports = contracts
            .iter()
            .map(|contract| valid_report(contract, &execution))
            .collect::<Vec<_>>();
        let events = BTreeMap::from([(
            "raft-soak".to_owned(),
            vec![json!({
                "event": "soak-check",
                "check_id": "raft-soak",
                "status": "pass",
                "classification": null,
                "message": null,
                "seed": 1,
                "steps": 320,
                "execution_contract": execution,
                "observations": {"accepted_completed_liveness_proposals": 99},
                "liveness_reports": reports,
            })],
        )]);
        (identity, contracts, events)
    }

    fn derive(
        identity: &SimulatorIdentity,
        contracts: &[SimulatorLivenessContract],
        events: &BTreeMap<String, Vec<Value>>,
    ) -> Result<SimulatorLivenessBinding, LivenessReportError> {
        derive_liveness_binding("pr", identity, contracts, events)
    }

    fn report_array_mut(events: &mut BTreeMap<String, Vec<Value>>) -> &mut Vec<Value> {
        events.get_mut("raft-soak").expect("soak events")[0]["liveness_reports"]
            .as_array_mut()
            .expect("liveness report array")
    }

    fn report_mut<'a>(
        events: &'a mut BTreeMap<String, Vec<Value>>,
        feature_id: &str,
    ) -> &'a mut Value {
        report_array_mut(events)
            .iter_mut()
            .find(|report| report["feature_id"] == feature_id)
            .unwrap_or_else(|| panic!("missing report {feature_id}"))
    }

    #[allow(clippy::too_many_lines)]
    fn valid_report(
        contract: &SimulatorLivenessContract,
        execution: &SimulatorExecutionContract,
    ) -> Value {
        let active_partition = contract.fault_requirement == "active-partition";
        let stable_required = contract.stable_leader_retained.is_some();
        let proposal_required = contract.proposal_outcome != "none";
        let reachable_voters = if active_partition { 2 } else { 3 };
        let rounds_used = contract.stable_leader_rounds_exact.unwrap_or(1);
        let node_count = if contract.feature_id == "leader-convergence" {
            execution.node_count
        } else {
            3
        };
        let base_rounds = contract.minimum_rounds
            + node_count * contract.rounds_per_node
            + execution.max_proposals * contract.rounds_per_proposal
            + execution.max_membership_changes * contract.rounds_per_membership_change
            + execution.max_partitions * contract.rounds_per_partition
            + contract.snapshot_catchup_rounds;
        let round_limit = base_rounds * contract.phase_count + contract.fixed_rounds;
        let fault_cycle = contract.fault_cycle_required.then(|| {
            json!({
                "partition_a": 1,
                "partition_b": 2,
                "partition_observed": true,
                "partitioned_rounds": contract.fixed_rounds,
                "nodes_exercised": 3,
                "ticks_executed": contract.fixed_rounds * 3,
                "deliveries_executed": 1,
                "drops_executed": 0,
                "protocol_state_changed": true,
                "partition_active_after_exercise": true,
                "heal_observed": true
            })
        });
        let stable_leader = stable_required.then(|| {
            json!({
                "node_id": 1,
                "stable_rounds": rounds_used.max(1),
                "remained_leader_through_probe": contract.stable_leader_retained
            })
        });
        let proposal = proposal_required.then(|| {
            json!({
                "proposal_id": 1,
                "terminal_outcome": if contract.proposal_outcome == "committed" {
                    "committed"
                } else {
                    "unknown"
                }
            })
        });
        json!({
            "invariant_id": contract.invariant_id,
            "clause_ids": contract.clause_ids,
            "feature_id": contract.feature_id,
            "scenario_id": contract.scenario_id,
            "observation_id": contract.observation_id,
            "preconditions": {
                "fault_requirement": contract.fault_requirement,
                "fault_state_satisfied": true,
                "fault_state_status": "satisfied",
                "faults_stopped": !active_partition,
                "partition_active": active_partition,
                "mutually_reachable_quorum": true,
                "mutually_reachable_quorum_status": "satisfied",
                "stable_membership": true,
                "stable_membership_status": "satisfied",
                "stable_leader_required": stable_required,
                "stable_leader_satisfied": stable_required,
                "stable_leader_status": if stable_required { "satisfied" } else { "not-required" },
                "accepted_proposal_required": proposal_required,
                "accepted_proposal_satisfied": proposal_required,
                "accepted_proposal_status": if proposal_required { "satisfied" } else { "not-required" },
                "authority_loss_required": contract.authority_loss_required,
                "authority_loss_satisfied": contract.authority_loss_required,
                "authority_loss_status": if contract.authority_loss_required { "satisfied" } else { "not-required" },
                "voter_ids": [1, 2, 3],
                "reachable_voters": reachable_voters,
                "quorum_size": 2,
                "unavailable_voters": 3 - reachable_voters
            },
            "fairness": {
                "policy_id": contract.fairness_policy_id,
                "tick_bound_rounds": contract.fairness_tick_bound_rounds,
                "delivery_bound_rounds": contract.fairness_delivery_bound_rounds,
                "max_delivery_waves_per_tick": contract.fairness_max_delivery_waves_per_tick
            },
            "round_budget": {
                "minimum_rounds": contract.minimum_rounds,
                "node_count": node_count,
                "queued_messages": 0,
                "max_proposals": execution.max_proposals,
                "max_membership_changes": execution.max_membership_changes,
                "max_partitions": execution.max_partitions,
                "snapshot_catchup_probe": execution.snapshot_catchup_probe,
                "base_rounds": base_rounds,
                "phase_count": contract.phase_count,
                "fixed_rounds": contract.fixed_rounds
            },
            "round_limit": round_limit,
            "rounds_used": rounds_used,
            "fault_cycle": fault_cycle,
            "stable_leader": stable_leader,
            "proposal": proposal
        })
    }
}

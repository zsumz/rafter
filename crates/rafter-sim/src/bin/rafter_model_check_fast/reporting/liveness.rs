#[path = "liveness/expectations.rs"]
mod expectations;
#[path = "liveness/preconditions.rs"]
mod preconditions;
#[path = "liveness/shape.rs"]
mod shape;

use std::collections::BTreeMap;

use rafter_sim::model_check::{SoakConfig, SoakSummary};

use expectations::expected_liveness_features;
use preconditions::{
    validate_fault_cycle, validate_liveness_fairness, validate_liveness_preconditions,
};
use shape::{
    require_exact, require_exact_fields, require_exact_object_fields, require_exact_strings,
    required_object_u64, required_str, required_u64, required_u64_array,
};

#[derive(Clone, Copy)]
struct ExpectedLivenessFeature {
    feature_id: &'static str,
    invariant_id: &'static str,
    clause_ids: &'static [&'static str],
    scenario_id: &'static str,
    observation_id: &'static str,
    remained_leader_through_probe: Option<bool>,
    stable_rounds: StableRoundsExpectation,
    proposal_outcome: ProposalOutcomeExpectation,
    authority_loss: bool,
    fault_requirement: FaultRequirement,
    fault_cycle: bool,
    phase_count: u64,
    fixed_rounds: u64,
}

#[derive(Clone, Copy)]
enum StableRoundsExpectation {
    None,
    Exact(u64),
    ProbeRounds,
}

#[derive(Clone, Copy)]
enum FaultRequirement {
    Stopped,
    ActivePartition,
}

impl FaultRequirement {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::ActivePartition => "active-partition",
        }
    }
}

#[derive(Clone, Copy)]
enum ProposalOutcomeExpectation {
    None,
    Exact(&'static str),
    ExplicitTerminal,
}

impl ProposalOutcomeExpectation {
    const fn required(self) -> bool {
        !matches!(self, Self::None)
    }

    fn accepts(self, actual: Option<&str>) -> bool {
        match self {
            Self::None => actual.is_none(),
            Self::Exact(expected) => actual == Some(expected),
            Self::ExplicitTerminal => matches!(
                actual,
                Some("committed" | "rejected" | "canceled" | "unknown")
            ),
        }
    }
}

pub(super) fn validate_liveness_reports(
    summary: &SoakSummary,
    config: SoakConfig,
    reports: &[serde_json::Value],
) -> Result<(), String> {
    let expected = expected_liveness_features(config);
    if reports.len() != expected.len() {
        return Err(format!(
            "expected {} liveness reports, found {}",
            expected.len(),
            reports.len()
        ));
    }

    let mut by_feature = BTreeMap::new();
    for report in reports {
        let feature_id = required_str(report, "feature_id")?;
        if by_feature.insert(feature_id, report).is_some() {
            return Err(format!("duplicate liveness feature report `{feature_id}`"));
        }
    }
    let mut execution_provenance = BTreeMap::new();
    for typed_report in summary.liveness_reports() {
        let feature_id = typed_report.feature_id();
        if execution_provenance
            .insert(feature_id, typed_report.execution_provenance_json())
            .is_some()
        {
            return Err(format!(
                "duplicate typed liveness provenance for `{feature_id}`"
            ));
        }
    }
    let config_provenance = summary.liveness_config_provenance_json();
    for feature in expected {
        let report = by_feature
            .remove(feature.feature_id)
            .ok_or_else(|| format!("missing liveness feature report `{}`", feature.feature_id))?;
        let provenance = execution_provenance
            .remove(feature.feature_id)
            .ok_or_else(|| {
                format!(
                    "missing typed liveness provenance for `{}`",
                    feature.feature_id
                )
            })?;
        validate_liveness_report(report, &provenance, &config_provenance, feature)?;
    }
    if let Some(feature_id) = by_feature.keys().next() {
        return Err(format!("unexpected liveness feature report `{feature_id}`"));
    }
    if let Some(feature_id) = execution_provenance.keys().next() {
        return Err(format!(
            "unexpected typed liveness provenance for `{feature_id}`"
        ));
    }
    Ok(())
}

fn validate_liveness_report(
    report: &serde_json::Value,
    execution_provenance: &serde_json::Value,
    config_provenance: &serde_json::Value,
    expected: ExpectedLivenessFeature,
) -> Result<(), String> {
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
            "operation",
        ],
        "liveness report",
    )?;
    require_exact(report, "invariant_id", expected.invariant_id)?;
    require_exact_strings(report, "clause_ids", expected.clause_ids)?;
    require_exact(report, "feature_id", expected.feature_id)?;
    require_exact(report, "scenario_id", expected.scenario_id)?;
    require_exact(report, "observation_id", expected.observation_id)?;

    validate_liveness_preconditions(report, expected)?;
    validate_execution_provenance(
        report,
        execution_provenance,
        config_provenance,
        expected.feature_id,
    )?;

    let round_limit = required_u64(report, "round_limit")?;
    let rounds_used = required_u64(report, "rounds_used")?;
    let derived_round_limit = validate_liveness_round_budget(report, expected)?;
    if round_limit != derived_round_limit || rounds_used > round_limit {
        return Err(format!(
            "{} has an invalid liveness round bound",
            expected.feature_id
        ));
    }
    validate_liveness_fairness(report, expected.feature_id)?;
    validate_fault_cycle(report, expected)?;
    if let Some(remained_leader) = expected.remained_leader_through_probe {
        if report["stable_leader"]["remained_leader_through_probe"].as_bool()
            != Some(remained_leader)
        {
            return Err(format!(
                "{} has the wrong leader-retention evidence",
                expected.feature_id
            ));
        }
        validate_stable_leader_semantics(report, expected)?;
    }
    if !expected
        .proposal_outcome
        .accepts(report["proposal"]["terminal_outcome"].as_str())
    {
        return Err(format!(
            "{} has an invalid proposal terminal outcome",
            expected.feature_id
        ));
    }
    validate_operation_evidence(report, expected.feature_id)?;
    Ok(())
}

fn validate_operation_evidence(report: &serde_json::Value, feature_id: &str) -> Result<(), String> {
    let expected_outcomes: Option<&[&str]> = match feature_id {
        "read-barrier" => Some(&["completed", "rejected", "canceled"]),
        "snapshot-catch-up" => Some(&["installed"]),
        "membership-transition" => Some(&["committed", "rejected"]),
        "leadership-transfer" => Some(&["completed", "rejected"]),
        _ => None,
    };
    let Some(expected_outcomes) = expected_outcomes else {
        return if report
            .get("operation")
            .is_some_and(serde_json::Value::is_null)
        {
            Ok(())
        } else {
            Err(format!(
                "{feature_id} unexpectedly carries operation evidence"
            ))
        };
    };
    let operation = report
        .get("operation")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{feature_id} has no operation evidence"))?;
    require_exact_object_fields(
        operation,
        &["operation_id", "terminal_outcome"],
        "operation evidence",
    )?;
    let operation_id = operation
        .get("operation_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{feature_id} has an invalid operation identity"))?;
    let outcome = operation
        .get("terminal_outcome")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("{feature_id} has no operation terminal outcome"))?;
    if !expected_outcomes.contains(&outcome) {
        return Err(format!(
            "{feature_id} has invalid outcome `{outcome}` for operation `{operation_id}`"
        ));
    }
    Ok(())
}

fn validate_stable_leader_semantics(
    report: &serde_json::Value,
    expected: ExpectedLivenessFeature,
) -> Result<(), String> {
    let stable_leader = report["stable_leader"]
        .as_object()
        .ok_or_else(|| format!("{} has no stable-leader evidence", expected.feature_id))?;
    require_exact_object_fields(
        stable_leader,
        &["node_id", "stable_rounds", "remained_leader_through_probe"],
        "stable-leader evidence",
    )?;
    let leader = required_object_u64(stable_leader, "node_id")?;
    let stable_rounds = required_object_u64(stable_leader, "stable_rounds")?;
    let rounds_used = required_u64(report, "rounds_used")?;
    let valid_rounds = match expected.stable_rounds {
        StableRoundsExpectation::None => false,
        StableRoundsExpectation::Exact(rounds) => stable_rounds == rounds,
        StableRoundsExpectation::ProbeRounds => stable_rounds == rounds_used.max(1),
    };
    let voters = required_u64_array(
        report["preconditions"]
            .as_object()
            .ok_or_else(|| "liveness report has no preconditions".to_owned())?,
        "voter_ids",
    )?;
    if leader == 0 || !voters.contains(&leader) || !valid_rounds {
        return Err(format!(
            "{} has invalid leader identity or stable window",
            expected.feature_id
        ));
    }
    Ok(())
}

fn validate_execution_provenance(
    report: &serde_json::Value,
    execution_provenance: &serde_json::Value,
    config_provenance: &serde_json::Value,
    feature_id: &str,
) -> Result<(), String> {
    require_exact(execution_provenance, "feature_id", feature_id)?;
    let report_budget = report
        .get("round_budget")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{feature_id} has no round-budget evidence"))?;
    for field in ["max_proposals", "max_membership_changes", "max_partitions"] {
        if required_object_u64(report_budget, field)? != required_u64(config_provenance, field)? {
            return Err(format!(
                "{feature_id} `{field}` does not match typed SoakConfig provenance"
            ));
        }
    }
    let report_snapshot = report_budget
        .get("snapshot_catchup_probe")
        .and_then(serde_json::Value::as_bool);
    let config_snapshot = config_provenance
        .get("snapshot_catchup_probe")
        .and_then(serde_json::Value::as_bool);
    if report_snapshot.is_none() || report_snapshot != config_snapshot {
        return Err(format!(
            "{feature_id} `snapshot_catchup_probe` does not match typed SoakConfig provenance"
        ));
    }
    let typed_budget = execution_provenance
        .get("round_budget")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{feature_id} has no typed round-budget provenance"))?;
    for field in [
        "minimum_rounds",
        "node_count",
        "queued_messages",
        "max_proposals",
        "max_membership_changes",
        "max_partitions",
        "base_rounds",
        "phase_count",
        "fixed_rounds",
    ] {
        if required_object_u64(report_budget, field)? != required_object_u64(typed_budget, field)? {
            return Err(format!(
                "{feature_id} `{field}` does not match typed execution provenance"
            ));
        }
    }
    let typed_snapshot = typed_budget
        .get("snapshot_catchup_probe")
        .and_then(serde_json::Value::as_bool);
    if report_snapshot.is_none() || report_snapshot != typed_snapshot {
        return Err(format!(
            "{feature_id} `snapshot_catchup_probe` does not match typed execution provenance"
        ));
    }
    for field in ["round_limit", "rounds_used"] {
        if required_u64(report, field)? != required_u64(execution_provenance, field)? {
            return Err(format!(
                "{feature_id} `{field}` does not match typed execution provenance"
            ));
        }
    }
    if report.get("operation") != execution_provenance.get("operation") {
        return Err(format!(
            "{feature_id} operation evidence does not match typed execution provenance"
        ));
    }
    Ok(())
}

fn validate_liveness_round_budget(
    report: &serde_json::Value,
    expected: ExpectedLivenessFeature,
) -> Result<u64, String> {
    let budget = report
        .get("round_budget")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{} has no round-budget evidence", expected.feature_id))?;
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
    let minimum_rounds = required_object_u64(budget, "minimum_rounds")?;
    let node_count = required_object_u64(budget, "node_count")?;
    let queued_messages = required_object_u64(budget, "queued_messages")?;
    let max_proposals = required_object_u64(budget, "max_proposals")?;
    let max_membership_changes = required_object_u64(budget, "max_membership_changes")?;
    let max_partitions = required_object_u64(budget, "max_partitions")?;
    let snapshot_catchup_probe = budget
        .get("snapshot_catchup_probe")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| "liveness round budget `snapshot_catchup_probe` is missing".to_owned())?;
    let base_rounds = required_object_u64(budget, "base_rounds")?;
    let phase_count = required_object_u64(budget, "phase_count")?;
    let fixed_rounds = required_object_u64(budget, "fixed_rounds")?;
    let expected_base = 128_u64
        .saturating_add(node_count.saturating_mul(16))
        .saturating_add(queued_messages.saturating_mul(4))
        .saturating_add(max_proposals.saturating_mul(8))
        .saturating_add(max_membership_changes.saturating_mul(16))
        .saturating_add(max_partitions.saturating_mul(16))
        .saturating_add(u64::from(snapshot_catchup_probe).saturating_mul(64));
    if minimum_rounds != 128
        || base_rounds != expected_base
        || phase_count != expected.phase_count
        || fixed_rounds != expected.fixed_rounds
    {
        return Err(format!(
            "{} has invalid round-budget derivation",
            expected.feature_id
        ));
    }
    Ok(base_rounds
        .saturating_mul(phase_count)
        .saturating_add(fixed_rounds))
}

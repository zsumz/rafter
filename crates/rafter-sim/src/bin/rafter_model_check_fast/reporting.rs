use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use rafter_sim::model_check::{
    ExplorationCompletion, Failure, FailureKind, SoakConfig, SoakFailure, SoakSummary, Summary,
};
use serde_json::json;

#[cfg(test)]
use crate::profile::SoakCheckKind;
use crate::profile::SoakExecutionContract;

const EVENT_PREFIX: &str = "RAFTER_EVENT ";

pub(crate) fn print_raft_summary(name: &str, summary: Summary, duration: Duration) {
    println!("{}", raft_summary_line(name, summary, duration));
    println!("{EVENT_PREFIX}{}", raft_event(name, summary, duration));
}

fn raft_event(name: &str, summary: Summary, duration: Duration) -> serde_json::Value {
    let frontier_exhausted = summary.completion() == ExplorationCompletion::FrontierExhausted;
    let observations = summary
        .observation_labels()
        .map(|label| (label.to_owned(), json!(1)))
        .collect::<serde_json::Map<_, _>>();
    json!({
        "event": "exhaustive-check",
        "check_id": name,
        "status": if frontier_exhausted { "pass" } else { "incomplete" },
        "classification": if frontier_exhausted { serde_json::Value::Null } else { json!("coverage-not-reached") },
        "completion": summary.completion().to_string(),
        "configured_depth": summary.max_depth(),
        "reached_depth": summary.reached_depth(),
        "unique_states": summary.unique_states(),
        "unique_protocol_states": summary.unique_protocol_states(),
        "unique_verifier_states": summary.unique_verifier_states(),
        "explored_states": summary.explored_states(),
        "explored_actions": summary.explored_actions(),
        "observations": observations,
        "duration_ms": duration.as_millis(),
    })
}

pub(crate) fn raft_summary_line(name: &str, summary: Summary, duration: Duration) -> String {
    let pruned_states = summary
        .explored_states()
        .saturating_sub(summary.unique_verifier_states());
    format_raft_summary_line(
        name,
        RaftSummaryMetrics {
            unique_states: summary.unique_states(),
            unique_protocol_states: summary.unique_protocol_states(),
            unique_verifier_states: summary.unique_verifier_states(),
            explored_states: summary.explored_states(),
            explored_actions: summary.explored_actions(),
            pruned_states,
            configured_depth: summary.max_depth(),
            reached_depth: summary.reached_depth(),
            completion: summary.completion(),
        },
        duration,
    )
}

#[cfg(test)]
pub(crate) fn raft_summary_line_for_counts(
    name: &str,
    unique_protocol_states: usize,
    unique_verifier_states: usize,
    explored_states: usize,
    explored_actions: usize,
    max_depth: usize,
    duration: Duration,
) -> String {
    let pruned_states = explored_states.saturating_sub(unique_verifier_states);
    format_raft_summary_line(
        name,
        RaftSummaryMetrics {
            unique_states: unique_verifier_states,
            unique_protocol_states,
            unique_verifier_states,
            explored_states,
            explored_actions,
            pruned_states,
            configured_depth: max_depth,
            reached_depth: max_depth,
            completion: ExplorationCompletion::FrontierExhausted,
        },
        duration,
    )
}

#[derive(Clone, Copy)]
struct RaftSummaryMetrics {
    unique_states: usize,
    unique_protocol_states: usize,
    unique_verifier_states: usize,
    explored_states: usize,
    explored_actions: usize,
    pruned_states: usize,
    configured_depth: usize,
    reached_depth: usize,
    completion: ExplorationCompletion,
}

fn format_raft_summary_line(name: &str, metrics: RaftSummaryMetrics, duration: Duration) -> String {
    let pruning_parts_per_million = metrics
        .pruned_states
        .saturating_mul(1_000_000)
        .checked_div(metrics.explored_states)
        .unwrap_or_default();
    let pruning_whole = pruning_parts_per_million / 1_000_000;
    let pruning_fraction = pruning_parts_per_million % 1_000_000;
    format!(
        "model-check {name}: unique_states={} unique_protocol_states={} unique_verifier_states={} explored_states={} explored_actions={} pruned_states={} pruning_rate={pruning_whole}.{pruning_fraction:06} configured_depth={} reached_depth={} completion={} duration_ms={}",
        metrics.unique_states,
        metrics.unique_protocol_states,
        metrics.unique_verifier_states,
        metrics.explored_states,
        metrics.explored_actions,
        metrics.pruned_states,
        metrics.configured_depth,
        metrics.reached_depth,
        metrics.completion,
        duration.as_millis()
    )
}

pub(crate) fn print_soak_summary(
    contract: &SoakExecutionContract,
    summary: &SoakSummary,
    config: SoakConfig,
    duration: Duration,
) {
    let name = &contract.check_id;
    println!(
        "model-check {name}: seed={:#x} steps={} observed_actions={:?} duration_ms={}",
        summary.seed().0,
        summary.steps_executed(),
        summary.observed_actions(),
        duration.as_millis()
    );
    let observed_actions = summary
        .observed_actions()
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>();
    println!(
        "{EVENT_PREFIX}{}",
        soak_event_with_contract(contract, summary, config, &observed_actions, duration)
    );
}

#[cfg(test)]
fn soak_event(
    name: &str,
    summary: &SoakSummary,
    config: SoakConfig,
    observed_actions: &[&str],
    duration: Duration,
) -> serde_json::Value {
    let contract = test_execution_contract(name, config);
    soak_event_with_contract(&contract, summary, config, observed_actions, duration)
}

fn soak_event_with_contract(
    contract: &SoakExecutionContract,
    summary: &SoakSummary,
    config: SoakConfig,
    observed_actions: &[&str],
    duration: Duration,
) -> serde_json::Value {
    let reports = summary.liveness_reports_json();
    soak_event_from_reports_with_contract(
        contract,
        summary,
        config,
        observed_actions,
        duration,
        &reports,
    )
}

#[cfg(test)]
fn soak_event_from_reports(
    name: &str,
    summary: &SoakSummary,
    config: SoakConfig,
    observed_actions: &[&str],
    duration: Duration,
    reports: &[serde_json::Value],
) -> serde_json::Value {
    let contract = test_execution_contract(name, config);
    soak_event_from_reports_with_contract(
        &contract,
        summary,
        config,
        observed_actions,
        duration,
        reports,
    )
}

fn soak_event_from_reports_with_contract(
    contract: &SoakExecutionContract,
    summary: &SoakSummary,
    config: SoakConfig,
    observed_actions: &[&str],
    duration: Duration,
    reports: &[serde_json::Value],
) -> serde_json::Value {
    let validation = summary
        .validate_liveness_report_structure()
        .and_then(|()| contract.validate_config(config))
        .and_then(|()| validate_liveness_reports(summary, config, reports));
    let passed = validation.is_ok();
    let liveness_features = if passed {
        reports
            .iter()
            .filter_map(|report| report["feature_id"].as_str())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let observations = if passed {
        reports
            .iter()
            .filter_map(|report| report["observation_id"].as_str())
            .map(|observation| (observation.to_owned(), json!(1)))
            .collect::<serde_json::Map<_, _>>()
    } else {
        serde_json::Map::new()
    };
    json!({
        "event": "soak-check",
        "check_id": contract.check_id,
        "execution_contract": contract.to_json(),
        "status": if passed { "pass" } else { "error" },
        "classification": if passed { serde_json::Value::Null } else { json!("harness-error") },
        "message": validation.err(),
        "seed": summary.seed().0,
        "steps": summary.steps_executed(),
        "observed_actions": observed_actions,
        "liveness_features": liveness_features,
        "observations": observations,
        "liveness_reports": reports,
        "duration_ms": duration.as_millis(),
    })
}

#[cfg(test)]
fn test_execution_contract(name: &str, config: SoakConfig) -> SoakExecutionContract {
    let kind = if name.ends_with("-membership") {
        SoakCheckKind::Membership
    } else if name.ends_with("-lease") {
        SoakCheckKind::Lease
    } else {
        SoakCheckKind::Standard
    };
    SoakExecutionContract::from_config("test-soak", name, kind, config)
}

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

fn validate_liveness_reports(
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

fn expected_liveness_features(config: SoakConfig) -> Vec<ExpectedLivenessFeature> {
    let mut expected = required_liveness_features();
    if config.checks_read_progress() {
        expected.push(ExpectedLivenessFeature {
            feature_id: "read-barrier",
            invariant_id: "LV-03",
            clause_ids: &["LV-03.a"],
            scenario_id: "stable-leader-read-barrier-v1",
            observation_id: "completed_liveness_read_barriers",
            remained_leader_through_probe: Some(true),
            stable_rounds: StableRoundsExpectation::Exact(2),
            proposal_outcome: ProposalOutcomeExpectation::None,
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 2,
            fixed_rounds: 0,
        });
    }
    append_optional_operation_features(&mut expected, config);
    expected
}

fn required_liveness_features() -> Vec<ExpectedLivenessFeature> {
    vec![
        ExpectedLivenessFeature {
            feature_id: "leader-convergence",
            invariant_id: "LV-01",
            clause_ids: &["LV-01.a", "LV-01.b"],
            scenario_id: "post-heal-stable-quorum-v1",
            observation_id: "post_heal_quiescent_leaders",
            remained_leader_through_probe: Some(true),
            stable_rounds: StableRoundsExpectation::Exact(2),
            proposal_outcome: ProposalOutcomeExpectation::Exact("committed"),
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: true,
            phase_count: 2,
            fixed_rounds: 1,
        },
        ExpectedLivenessFeature {
            feature_id: "quorum-only-leader-convergence",
            invariant_id: "LV-01",
            clause_ids: &["LV-01.a", "LV-01.b"],
            scenario_id: "minority-unavailable-stable-quorum-v1",
            observation_id: "quorum_only_post_fault_leaders",
            remained_leader_through_probe: Some(true),
            stable_rounds: StableRoundsExpectation::Exact(2),
            proposal_outcome: ProposalOutcomeExpectation::Exact("committed"),
            authority_loss: false,
            fault_requirement: FaultRequirement::ActivePartition,
            fault_cycle: false,
            phase_count: 2,
            fixed_rounds: 0,
        },
        ExpectedLivenessFeature {
            feature_id: "proposal-progress",
            invariant_id: "LV-02",
            clause_ids: &["LV-02.a"],
            scenario_id: "stable-leader-reachable-quorum-v1",
            observation_id: "accepted_completed_liveness_proposals",
            remained_leader_through_probe: Some(true),
            stable_rounds: StableRoundsExpectation::ProbeRounds,
            proposal_outcome: ProposalOutcomeExpectation::Exact("committed"),
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 1,
            fixed_rounds: 0,
        },
        ExpectedLivenessFeature {
            feature_id: "proposal-termination",
            invariant_id: "LV-02",
            clause_ids: &["LV-02.b"],
            scenario_id: "accepted-proposal-authority-loss-v1",
            observation_id: "terminated_liveness_proposals",
            remained_leader_through_probe: Some(false),
            stable_rounds: StableRoundsExpectation::Exact(1),
            proposal_outcome: ProposalOutcomeExpectation::ExplicitTerminal,
            authority_loss: true,
            fault_requirement: FaultRequirement::ActivePartition,
            fault_cycle: false,
            phase_count: 1,
            fixed_rounds: 0,
        },
    ]
}

fn append_optional_operation_features(
    expected: &mut Vec<ExpectedLivenessFeature>,
    config: SoakConfig,
) {
    if config.checks_membership_progress() {
        expected.push(ExpectedLivenessFeature {
            feature_id: "membership-transition",
            invariant_id: "LV-03",
            clause_ids: &["LV-03.c"],
            scenario_id: "stable-remove-voter-joint-consensus-v1",
            observation_id: "completed_stable_membership_transitions",
            remained_leader_through_probe: None,
            stable_rounds: StableRoundsExpectation::None,
            proposal_outcome: ProposalOutcomeExpectation::None,
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 2,
            fixed_rounds: 0,
        });
    }
    if config.checks_transfer_progress() {
        expected.push(ExpectedLivenessFeature {
            feature_id: "leadership-transfer",
            invariant_id: "LV-03",
            clause_ids: &["LV-03.d"],
            scenario_id: "caught-up-voter-transfer-v1",
            observation_id: "completed_target_leadership_transfers",
            remained_leader_through_probe: None,
            stable_rounds: StableRoundsExpectation::None,
            proposal_outcome: ProposalOutcomeExpectation::None,
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 2,
            fixed_rounds: 0,
        });
    }
    if config.checks_snapshot_progress() {
        expected.push(ExpectedLivenessFeature {
            feature_id: "snapshot-catch-up",
            invariant_id: "LV-03",
            clause_ids: &["LV-03.b"],
            scenario_id: "restart-snapshot-transfer-v1",
            observation_id: "completed_expected_snapshot_catchups",
            remained_leader_through_probe: None,
            stable_rounds: StableRoundsExpectation::None,
            proposal_outcome: ProposalOutcomeExpectation::None,
            authority_loss: false,
            fault_requirement: FaultRequirement::Stopped,
            fault_cycle: false,
            phase_count: 1,
            fixed_rounds: 0,
        });
    }
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

fn validate_fault_cycle(
    report: &serde_json::Value,
    expected: ExpectedLivenessFeature,
) -> Result<(), String> {
    let fault_cycle = report.get("fault_cycle").filter(|value| !value.is_null());
    if !expected.fault_cycle {
        return if fault_cycle.is_none() {
            Ok(())
        } else {
            Err(format!(
                "{} has unexpected fault-cycle evidence",
                expected.feature_id
            ))
        };
    }
    let fault_cycle = fault_cycle
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{} has no fault-cycle evidence", expected.feature_id))?;
    require_exact_object_fields(
        fault_cycle,
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
    let partition_a = required_object_u64(fault_cycle, "partition_a")?;
    let partition_b = required_object_u64(fault_cycle, "partition_b")?;
    let partition_observed = fault_cycle
        .get("partition_observed")
        .and_then(serde_json::Value::as_bool);
    let heal_observed = fault_cycle
        .get("heal_observed")
        .and_then(serde_json::Value::as_bool);
    let partitioned_rounds = required_object_u64(fault_cycle, "partitioned_rounds")?;
    let nodes_exercised = required_object_u64(fault_cycle, "nodes_exercised")?;
    let ticks_executed = required_object_u64(fault_cycle, "ticks_executed")?;
    let _deliveries_executed = required_object_u64(fault_cycle, "deliveries_executed")?;
    let _drops_executed = required_object_u64(fault_cycle, "drops_executed")?;
    let protocol_state_changed = fault_cycle
        .get("protocol_state_changed")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| {
            format!(
                "{} has no protocol-state-change evidence",
                expected.feature_id
            )
        })?;
    let partition_active_after_exercise = fault_cycle
        .get("partition_active_after_exercise")
        .and_then(serde_json::Value::as_bool);
    if partition_a == partition_b
        || partition_observed != Some(true)
        || partitioned_rounds != expected.fixed_rounds
        || nodes_exercised < 2
        || ticks_executed != partitioned_rounds.saturating_mul(nodes_exercised)
        || !protocol_state_changed
        || partition_active_after_exercise != Some(true)
        || heal_observed != Some(true)
    {
        return Err(format!(
            "{} has invalid fault-cycle evidence",
            expected.feature_id
        ));
    }
    Ok(())
}

fn validate_liveness_preconditions(
    report: &serde_json::Value,
    expected: ExpectedLivenessFeature,
) -> Result<(), String> {
    let preconditions = report
        .get("preconditions")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{} has no precondition object", expected.feature_id))?;
    require_exact_object_fields(
        preconditions,
        &[
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
        ],
        "liveness preconditions",
    )?;
    validate_fault_precondition(preconditions, expected)?;
    for field in ["mutually_reachable_quorum", "stable_membership"] {
        if preconditions
            .get(field)
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        {
            return Err(format!(
                "{} precondition `{field}` is not satisfied",
                expected.feature_id
            ));
        }
    }
    for field in [
        "mutually_reachable_quorum_status",
        "stable_membership_status",
    ] {
        if preconditions.get(field).and_then(serde_json::Value::as_str) != Some("satisfied") {
            return Err(format!(
                "{} precondition status `{field}` is invalid",
                expected.feature_id
            ));
        }
    }
    validate_quorum_counts(preconditions, expected.feature_id)?;
    validate_required_evidence(
        report,
        preconditions,
        "stable_leader",
        "stable_leader_required",
        "stable_leader_satisfied",
        expected.remained_leader_through_probe.is_some(),
    )?;
    validate_required_evidence(
        report,
        preconditions,
        "proposal",
        "accepted_proposal_required",
        "accepted_proposal_satisfied",
        expected.proposal_outcome.required(),
    )?;
    validate_authority_loss(preconditions, expected)
}

fn validate_fault_precondition(
    preconditions: &serde_json::Map<String, serde_json::Value>,
    expected: ExpectedLivenessFeature,
) -> Result<(), String> {
    let requirement = preconditions
        .get("fault_requirement")
        .and_then(serde_json::Value::as_str);
    let satisfied = preconditions
        .get("fault_state_satisfied")
        .and_then(serde_json::Value::as_bool);
    let status = preconditions
        .get("fault_state_status")
        .and_then(serde_json::Value::as_str);
    let faults_stopped = preconditions
        .get("faults_stopped")
        .and_then(serde_json::Value::as_bool);
    let partition_active = preconditions
        .get("partition_active")
        .and_then(serde_json::Value::as_bool);
    let measured_state_matches = match expected.fault_requirement {
        FaultRequirement::Stopped => {
            faults_stopped == Some(true) && partition_active == Some(false)
        }
        FaultRequirement::ActivePartition => {
            faults_stopped == Some(false) && partition_active == Some(true)
        }
    };
    if requirement == Some(expected.fault_requirement.as_str())
        && satisfied == Some(true)
        && status == Some("satisfied")
        && measured_state_matches
    {
        Ok(())
    } else {
        Err(format!(
            "{} fault-state evidence is inconsistent",
            expected.feature_id
        ))
    }
}

fn validate_quorum_counts(
    preconditions: &serde_json::Map<String, serde_json::Value>,
    feature_id: &str,
) -> Result<(), String> {
    let reachable_voters = required_object_u64(preconditions, "reachable_voters")?;
    let quorum_size = required_object_u64(preconditions, "quorum_size")?;
    let unavailable_voters = required_object_u64(preconditions, "unavailable_voters")?;
    let voters = required_u64_array(preconditions, "voter_ids")?;
    let unique = voters.iter().copied().collect::<BTreeSet<_>>();
    if voters.is_empty()
        || unique.len() != voters.len()
        || voters.contains(&0)
        || quorum_size != voters.len() as u64 / 2 + 1
        || reachable_voters < quorum_size
        || unavailable_voters != voters.len() as u64 - reachable_voters
    {
        Err(format!("{feature_id} has invalid reachable-quorum counts"))
    } else {
        Ok(())
    }
}

fn validate_authority_loss(
    preconditions: &serde_json::Map<String, serde_json::Value>,
    expected: ExpectedLivenessFeature,
) -> Result<(), String> {
    let required = preconditions
        .get("authority_loss_required")
        .and_then(serde_json::Value::as_bool);
    let satisfied = preconditions
        .get("authority_loss_satisfied")
        .and_then(serde_json::Value::as_bool);
    let expected_status = if expected.authority_loss {
        "satisfied"
    } else {
        "not-required"
    };
    let status = preconditions
        .get("authority_loss_status")
        .and_then(serde_json::Value::as_str);
    if required == Some(expected.authority_loss)
        && satisfied == Some(expected.authority_loss)
        && status == Some(expected_status)
    {
        Ok(())
    } else {
        Err(format!(
            "{} authority-loss evidence is inconsistent",
            expected.feature_id
        ))
    }
}

fn validate_liveness_fairness(report: &serde_json::Value, feature_id: &str) -> Result<(), String> {
    let fairness = report
        .get("fairness")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("{feature_id} has no fairness evidence"))?;
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
    let valid = fairness
        .get("policy_id")
        .and_then(serde_json::Value::as_str)
        == Some("all-node-ticks-fifo-ready-waves-v1")
        && fairness
            .get("tick_bound_rounds")
            .and_then(serde_json::Value::as_u64)
            == Some(1)
        && fairness
            .get("delivery_bound_rounds")
            .and_then(serde_json::Value::as_u64)
            == Some(1)
        && fairness
            .get("max_delivery_waves_per_tick")
            .and_then(serde_json::Value::as_u64)
            == Some(64);
    if valid {
        Ok(())
    } else {
        Err(format!("{feature_id} has invalid fairness evidence"))
    }
}

fn validate_required_evidence(
    report: &serde_json::Value,
    preconditions: &serde_json::Map<String, serde_json::Value>,
    evidence_field: &str,
    required_field: &str,
    satisfied_field: &str,
    expected: bool,
) -> Result<(), String> {
    let required = preconditions
        .get(required_field)
        .and_then(serde_json::Value::as_bool);
    let satisfied = preconditions
        .get(satisfied_field)
        .and_then(serde_json::Value::as_bool);
    let present = report
        .get(evidence_field)
        .is_some_and(|value| !value.is_null());
    if required != Some(expected) || satisfied != Some(expected) || present != expected {
        return Err(format!("`{evidence_field}` evidence is inconsistent"));
    }
    let status_field = required_field.replace("_required", "_status");
    let expected_status = if expected {
        "satisfied"
    } else {
        "not-required"
    };
    if preconditions
        .get(&status_field)
        .and_then(serde_json::Value::as_str)
        != Some(expected_status)
    {
        return Err(format!("`{evidence_field}` status is inconsistent"));
    }
    if expected {
        let evidence = report[evidence_field]
            .as_object()
            .ok_or_else(|| format!("`{evidence_field}` evidence is not an object"))?;
        match evidence_field {
            "stable_leader" => {
                require_exact_object_fields(
                    evidence,
                    &["node_id", "stable_rounds", "remained_leader_through_probe"],
                    "stable-leader evidence",
                )?;
                if evidence
                    .get("node_id")
                    .and_then(serde_json::Value::as_u64)
                    .is_none()
                    || evidence
                        .get("stable_rounds")
                        .and_then(serde_json::Value::as_u64)
                        .is_none_or(|rounds| rounds == 0)
                    || evidence
                        .get("remained_leader_through_probe")
                        .and_then(serde_json::Value::as_bool)
                        .is_none()
                {
                    return Err("`stable_leader` evidence is malformed".to_owned());
                }
            }
            "proposal" => {
                require_exact_object_fields(
                    evidence,
                    &["proposal_id", "terminal_outcome"],
                    "proposal evidence",
                )?;
                if evidence
                    .get("proposal_id")
                    .and_then(serde_json::Value::as_u64)
                    .is_none_or(|proposal_id| proposal_id == 0)
                    || evidence
                        .get("terminal_outcome")
                        .and_then(serde_json::Value::as_str)
                        .is_none()
                {
                    return Err("`proposal` evidence is malformed".to_owned());
                }
            }
            _ => {
                return Err(format!(
                    "unknown required evidence field `{evidence_field}`"
                ))
            }
        }
    }
    Ok(())
}

fn require_exact_fields(
    value: &serde_json::Value,
    expected: &[&str],
    context: &str,
) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("{context} is not an object"))?;
    require_exact_object_fields(object, expected, context)
}

fn require_exact_object_fields(
    object: &serde_json::Map<String, serde_json::Value>,
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

fn require_exact_strings(
    value: &serde_json::Value,
    field: &str,
    expected: &[&str],
) -> Result<(), String> {
    let observed = value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("liveness report field `{field}` is missing or not an array"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| format!("liveness report field `{field}` contains a non-string"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if observed == expected {
        Ok(())
    } else {
        Err(format!("liveness report field `{field}` is inconsistent"))
    }
}

fn required_u64_array(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Vec<u64>, String> {
    object
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("liveness precondition `{field}` is missing or not an array"))?
        .iter()
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| format!("liveness precondition `{field}` contains a non-integer"))
        })
        .collect()
}

fn required_object_u64(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<u64, String> {
    object
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("liveness precondition `{field}` is missing or not an integer"))
}

fn required_str<'a>(report: &'a serde_json::Value, field: &str) -> Result<&'a str, String> {
    report
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("liveness report field `{field}` is missing or not a string"))
}

fn required_u64(report: &serde_json::Value, field: &str) -> Result<u64, String> {
    report
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("liveness report field `{field}` is missing or not an integer"))
}

fn require_exact(report: &serde_json::Value, field: &str, expected: &str) -> Result<(), String> {
    let actual = required_str(report, field)?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "liveness report field `{field}` expected `{expected}`, found `{actual}`"
        ))
    }
}

pub(crate) fn print_profile_total(
    profile: &str,
    protocol_states: usize,
    verifier_states: usize,
    target_protocol_states: usize,
    target_verifier_states: usize,
) {
    let passed =
        protocol_states >= target_protocol_states && verifier_states >= target_verifier_states;
    println!(
        "{EVENT_PREFIX}{}",
        json!({
            "event": "profile-total",
            "check_id": format!("raft-profile-total-{}", profile.trim_start_matches("raft-")),
            "profile": profile,
            "status": if passed { "pass" } else { "incomplete" },
            "classification": if passed { serde_json::Value::Null } else { json!("coverage-not-reached") },
            "unique_protocol_states": protocol_states,
            "unique_verifier_states": verifier_states,
            "target_protocol_states": target_protocol_states,
            "target_verifier_states": target_verifier_states,
        })
    );
}

pub(crate) fn print_raft_failure(name: &str, failure: &Failure) {
    print_failure_event(name, failure.kind(), failure.invariant(), failure.message());
    eprintln!("model-check {name} failed: {failure}");
    for line in failure_timeline_lines(
        name,
        failure.kind(),
        failure.invariant(),
        failure.message(),
        failure
            .trace()
            .iter()
            .enumerate()
            .map(|(index, action)| (index, action.to_string())),
    ) {
        eprintln!("  {line}");
    }
    eprintln!("state={:?}", failure.state());
}

pub(crate) fn print_soak_failure(name: &str, failure: &SoakFailure) {
    print_failure_event(
        name,
        failure.failure().kind(),
        failure.failure().invariant(),
        failure.failure().message(),
    );
    eprintln!("model-check {name} failed: {failure}");
    eprintln!("seed={:#x}", failure.seed().0);
    eprintln!("step={}", failure.step());
    for line in failure_timeline_lines(
        name,
        failure.failure().kind(),
        failure.failure().invariant(),
        failure.failure().message(),
        failure
            .trace()
            .iter()
            .enumerate()
            .map(|(index, action)| (index, action.to_string())),
    ) {
        eprintln!("  {line}");
    }
    eprintln!("state={:?}", failure.failure().state());
}

fn print_failure_event(name: &str, kind: FailureKind, invariant: &str, message: &str) {
    println!(
        "{EVENT_PREFIX}{}",
        failure_event(name, kind, invariant, message)
    );
}

fn failure_event(
    name: &str,
    kind: FailureKind,
    invariant: &str,
    message: &str,
) -> serde_json::Value {
    let status = match kind {
        FailureKind::InvariantViolation => "fail",
        FailureKind::CoverageNotReached => "incomplete",
        FailureKind::HarnessError => "error",
    };
    json!({
        "event": "check-failure",
        "check_id": name,
        "status": status,
        "classification": kind.as_str(),
        "invariant": invariant,
        "message": message,
    })
}

pub(crate) fn failure_timeline_lines(
    name: &str,
    failure_kind: FailureKind,
    invariant: &str,
    message: &str,
    trace: impl IntoIterator<Item = (usize, String)>,
) -> Vec<String> {
    let mut lines = vec![format!(
        "ERROR test model failure name={} failure_kind={} invariant={} error_message={}",
        field_value(name),
        failure_kind,
        field_value(invariant),
        field_value(message)
    )];
    lines.extend(trace.into_iter().map(|(index, action)| {
        format!(
            "DEBUG test trace step step={index} action={}",
            field_value(&action)
        )
    }));
    lines
}

fn field_value(value: &str) -> String {
    if value.contains(char::is_whitespace) {
        format!("{value:?}")
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rafter_sim::{
        model_check::{run_raft_random_soak, FailureKind, SoakConfig, SoakSummary},
        SimSeed,
    };
    use serde_json::json;

    use crate::raft_config::three_node_configs;

    use super::{
        failure_event, soak_event, soak_event_from_reports, soak_event_from_reports_with_contract,
        test_execution_contract,
    };

    #[test]
    fn machine_failure_event_preserves_classification_and_message() {
        let event = failure_event(
            "raft-commit",
            FailureKind::CoverageNotReached,
            "CM-02",
            "required witness absent",
        );
        assert_eq!(event["event"], "check-failure");
        assert_eq!(event["status"], "incomplete");
        assert_eq!(event["classification"], "coverage-not-reached");
        assert_eq!(event["invariant"], "CM-02");
        assert_eq!(event["message"], "required witness absent");
    }

    #[test]
    fn soak_event_derives_liveness_evidence_from_monitor_reports() {
        let config = SoakConfig::new(SimSeed(0x51_7e), 0);
        let summary = run_raft_random_soak(three_node_configs(2), config)
            .expect("zero-step soak should complete measured liveness monitors");
        let event = soak_event("raft-soak", &summary, config, &[], Duration::from_millis(7));

        assert_eq!(event["liveness_reports"].as_array().map(Vec::len), Some(4));
        assert_eq!(event["observations"]["post_heal_quiescent_leaders"], 1);
        assert_eq!(event["observations"]["terminated_liveness_proposals"], 1);
        assert!(event["observations"]
            .get("completed_liveness_read_barriers")
            .is_none());
        assert!(event["liveness_reports"].as_array().is_some_and(|reports| {
            reports.iter().all(|report| {
                report["round_limit"].is_number() && report["rounds_used"].is_number()
            })
        }));
    }

    #[test]
    fn soak_event_fails_closed_on_missing_liveness_report() {
        let (summary, config, mut reports) = base_soak_reports();
        reports.pop();
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "expected 4 liveness reports");
        assert!(event["observations"]
            .as_object()
            .is_some_and(serde_json::Map::is_empty));
    }

    #[test]
    fn soak_event_fails_closed_on_duplicate_liveness_report() {
        let (summary, config, mut reports) = base_soak_reports();
        reports[3] = reports[0].clone();
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "duplicate liveness feature report");
    }

    #[test]
    fn soak_event_fails_closed_on_unknown_feature_identity() {
        let (summary, config, mut reports) = base_soak_reports();
        reports[0]["feature_id"] = json!("invented-feature");
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "missing liveness feature report");
    }

    #[test]
    fn soak_event_fails_closed_on_malformed_report_structure() {
        let (summary, config, mut reports) = base_soak_reports();
        reports[0]
            .as_object_mut()
            .expect("report is an object")
            .remove("round_limit");
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "round_limit");
    }

    #[test]
    fn soak_event_fails_closed_on_missing_required_leader_evidence() {
        let (summary, config, mut reports) = base_soak_reports();
        reports[0]["stable_leader"] = serde_json::Value::Null;
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "stable_leader");
    }

    #[test]
    fn soak_event_fails_closed_on_wrong_scenario_identity() {
        let (summary, config, mut reports) = base_soak_reports();
        reports[0]["scenario_id"] = json!("quorum-only-output-relabelled");
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "scenario_id");
    }

    #[test]
    fn soak_event_fails_closed_on_false_precondition() {
        let (summary, config, mut reports) = base_soak_reports();
        reports[0]["preconditions"]["mutually_reachable_quorum"] = json!(false);
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "mutually_reachable_quorum");
    }

    #[test]
    fn soak_event_fails_closed_on_tampered_delivery_fairness() {
        let (summary, config, mut reports) = base_soak_reports();
        reports[0]["fairness"]["max_delivery_waves_per_tick"] = json!(65);
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "fairness evidence");
    }

    #[test]
    fn soak_event_accepts_every_explicit_proposal_termination_outcome() {
        for outcome in ["committed", "rejected", "canceled", "unknown"] {
            let (summary, config, mut reports) = base_soak_reports();
            let report = report_mut(&mut reports, "proposal-termination");
            report["proposal"]["terminal_outcome"] = json!(outcome);
            let event = soak_event_from_reports(
                "raft-soak",
                &summary,
                config,
                &[],
                Duration::ZERO,
                &reports,
            );

            assert_eq!(event["status"], "pass", "outcome {outcome}: {event}");
        }
    }

    #[test]
    fn soak_event_rejects_missing_or_nonterminal_proposal_outcome() {
        for (outcome, expected_message) in [
            (serde_json::Value::Null, "`proposal` evidence is malformed"),
            (json!("pending"), "proposal terminal outcome"),
        ] {
            let (summary, config, mut reports) = base_soak_reports();
            report_mut(&mut reports, "proposal-termination")["proposal"]["terminal_outcome"] =
                outcome;
            let event = soak_event_from_reports(
                "raft-soak",
                &summary,
                config,
                &[],
                Duration::ZERO,
                &reports,
            );

            assert_harness_error(&event, expected_message);
        }
    }

    #[test]
    fn soak_event_binds_leader_retention_to_each_scenario() {
        for (feature_id, tampered) in [
            ("leader-convergence", false),
            ("proposal-termination", true),
        ] {
            let (summary, config, mut reports) = base_soak_reports();
            report_mut(&mut reports, feature_id)["stable_leader"]
                ["remained_leader_through_probe"] = json!(tampered);
            let event = soak_event_from_reports(
                "raft-soak",
                &summary,
                config,
                &[],
                Duration::ZERO,
                &reports,
            );

            assert_harness_error(&event, "leader-retention evidence");
        }
    }

    #[test]
    fn soak_event_rejects_tampered_exact_round_limit() {
        let (summary, config, mut reports) = base_soak_reports();
        let report = report_mut(&mut reports, "leader-convergence");
        report["round_limit"] = json!(report["round_limit"].as_u64().unwrap() + 1);
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "typed execution provenance");
    }

    #[test]
    fn soak_event_rejects_tampered_round_budget_derivation() {
        for field in ["base_rounds", "phase_count"] {
            let (summary, config, mut reports) = base_soak_reports();
            let report = report_mut(&mut reports, "leader-convergence");
            report["round_budget"][field] =
                json!(report["round_budget"][field].as_u64().unwrap() + 1);
            let event = soak_event_from_reports(
                "raft-soak",
                &summary,
                config,
                &[],
                Duration::ZERO,
                &reports,
            );

            assert_harness_error(&event, "typed execution provenance");
        }
    }

    #[test]
    fn soak_event_rejects_coordinated_round_budget_tampering() {
        let (summary, config, mut reports) = base_soak_reports();
        let report = report_mut(&mut reports, "leader-convergence");
        let phase_count = report["round_budget"]["phase_count"].as_u64().unwrap();
        report["round_budget"]["max_proposals"] =
            json!(report["round_budget"]["max_proposals"].as_u64().unwrap() + 1);
        report["round_budget"]["base_rounds"] =
            json!(report["round_budget"]["base_rounds"].as_u64().unwrap() + 8);
        report["round_limit"] = json!(report["round_limit"].as_u64().unwrap() + 8 * phase_count);
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "typed SoakConfig provenance");
    }

    #[test]
    fn soak_event_rejects_execution_contract_tampering() {
        let (summary, config, reports) = base_soak_reports();
        let mut contract = test_execution_contract("raft-soak", config);
        contract.max_proposals += 1;
        let event = soak_event_from_reports_with_contract(
            &contract,
            &summary,
            config,
            &[],
            Duration::ZERO,
            &reports,
        );

        assert_harness_error(&event, "does not match the actual SoakConfig");
    }

    #[test]
    fn soak_event_rejects_rounds_used_not_backed_by_execution() {
        let (summary, config, mut reports) = base_soak_reports();
        let report = report_mut(&mut reports, "leader-convergence");
        report["rounds_used"] = json!(report["rounds_used"].as_u64().unwrap() + 1);
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "rounds_used");
    }

    #[test]
    fn soak_event_rejects_fault_state_relabeling() {
        let (summary, config, mut reports) = base_soak_reports();
        let preconditions =
            &mut report_mut(&mut reports, "quorum-only-leader-convergence")["preconditions"];
        preconditions["fault_requirement"] = json!("stopped");
        preconditions["faults_stopped"] = json!(true);
        preconditions["partition_active"] = json!(false);
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);

        assert_harness_error(&event, "fault-state evidence");
    }

    #[test]
    fn soak_event_requires_a_real_healed_fault_cycle() {
        for field in [
            "partition_observed",
            "partition_active_after_exercise",
            "heal_observed",
            "protocol_state_changed",
        ] {
            let (summary, config, mut reports) = base_soak_reports();
            report_mut(&mut reports, "leader-convergence")["fault_cycle"][field] = json!(false);
            let event = soak_event_from_reports(
                "raft-soak",
                &summary,
                config,
                &[],
                Duration::ZERO,
                &reports,
            );

            assert_harness_error(&event, "fault-cycle evidence");
        }

        let (summary, config, mut reports) = base_soak_reports();
        report_mut(&mut reports, "leader-convergence")["fault_cycle"]["ticks_executed"] = json!(0);
        let event =
            soak_event_from_reports("raft-soak", &summary, config, &[], Duration::ZERO, &reports);
        assert_harness_error(&event, "fault-cycle evidence");
    }

    #[test]
    fn soak_event_requires_the_exact_optional_feature_set() {
        let config = SoakConfig::new(SimSeed(0x51_7e), 0)
            .with_max_read_indexes(1)
            .with_max_membership_changes(1)
            .with_max_transfers(1)
            .with_snapshot_catchup_probe();
        let summary = run_raft_random_soak(three_node_configs(2), config)
            .expect("all optional liveness fixtures should complete");
        let event = soak_event("raft-soak", &summary, config, &[], Duration::ZERO);

        assert_eq!(event["status"], "pass");
        assert_eq!(event["liveness_reports"].as_array().map(Vec::len), Some(8));
        assert_eq!(event["liveness_features"].as_array().map(Vec::len), Some(8));
    }

    fn base_soak_reports() -> (SoakSummary, SoakConfig, Vec<serde_json::Value>) {
        let config = SoakConfig::new(SimSeed(0x51_7e), 0);
        let summary = run_raft_random_soak(three_node_configs(2), config)
            .expect("zero-step soak should complete measured liveness monitors");
        let reports = summary.liveness_reports_json();
        (summary, config, reports)
    }

    fn report_mut<'a>(
        reports: &'a mut [serde_json::Value],
        feature_id: &str,
    ) -> &'a mut serde_json::Value {
        reports
            .iter_mut()
            .find(|report| report["feature_id"] == feature_id)
            .unwrap_or_else(|| panic!("missing {feature_id} report"))
    }

    fn assert_harness_error(event: &serde_json::Value, message: &str) {
        assert_eq!(event["status"], "error");
        assert_eq!(event["classification"], "harness-error");
        assert!(event["message"]
            .as_str()
            .is_some_and(|actual| actual.contains(message)));
    }
}

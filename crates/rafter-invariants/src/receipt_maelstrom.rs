use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::RunnerContract;
use crate::{CheckCompletion, CheckReceipt, EvidenceDescriptor, EvidenceStatus, ResultBundle};

const OBSERVATIONS: [&str; 17] = [
    "trials",
    "valid_trials",
    "operation_count",
    "ok_count",
    "read_ok",
    "write_ok",
    "cas_ok",
    "membership_enter",
    "membership_leave",
    "membership_complete",
    "restarts",
    "post_restart_progress",
    "crashpoints",
    "post_crash_progress",
    "snapshots_compacted",
    "snapshots_applied",
    "post_restart_snapshots_applied",
];

pub(super) fn validate(
    bundle: &ResultBundle,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
    contract: &RunnerContract,
) -> Result<(), &'static str> {
    validate_configuration(contract)?;
    let required = expected
        .iter()
        .filter(|(_, descriptor)| descriptor.layer == "maelstrom")
        .map(|(evidence_id, descriptor)| (evidence_id.clone(), scenario(descriptor.path.as_str())))
        .collect::<BTreeMap<_, _>>();
    if required.len() != 10 || bundle.execution.checks.len() != 5 {
        return Err("Maelstrom receipt must contain five scenarios covering ten E2E records");
    }
    let rd06 = expected
        .iter()
        .find(|(_, descriptor)| {
            descriptor.layer == "maelstrom" && descriptor.invariant_id == "RD-06"
        })
        .map(|(evidence_id, _)| evidence_id)
        .ok_or("Maelstrom registry omitted RD-06 evidence")?;
    let rd06_owners = bundle
        .execution
        .checks
        .iter()
        .filter(|check| check.evidence_ids.contains(rd06))
        .map(|check| check.execution_id.as_str())
        .collect::<Vec<_>>();
    let [rd06_owner] = rd06_owners.as_slice() else {
        return Err("exactly one Maelstrom scenario must own RD-06 evidence");
    };
    for tool in ["java", "maelstrom", "dot", "gnuplot"] {
        if !bundle.execution.source.tools.contains_key(tool) {
            return Err("Maelstrom receipt lacks external tool provenance");
        }
    }
    if bundle.execution.source.tools["maelstrom"].sha256
        != contract.configuration["maelstrom_executable_sha256"]
        || crate::producer::java_major(&bundle.execution.source.tools["java"].version) != Some(21)
    {
        return Err("Maelstrom receipt external tool identity does not match the profile pin");
    }
    if bundle.execution.source.build_profile != "maelstrom-debug"
        || !bundle.execution.source.features.is_empty()
    {
        return Err("Maelstrom receipt build identity does not match the profile contract");
    }
    let trials = contract.configuration["trials"]
        .parse::<u64>()
        .map_err(|_| "Maelstrom trial count is invalid")?;
    for check in &bundle.execution.checks {
        let scenario = check
            .check_id
            .strip_prefix("maelstrom/")
            .ok_or("Maelstrom check ID is invalid")?;
        let mut expected_ids = required
            .iter()
            .filter(|(_, expected_scenario)| **expected_scenario == Some(scenario))
            .map(|(evidence_id, _)| evidence_id)
            .collect::<BTreeSet<_>>();
        expected_ids.remove(rd06);
        if check.execution_id == *rd06_owner {
            expected_ids.insert(rd06);
        }
        if expected_ids.is_empty()
            || check.evidence_ids.iter().collect::<BTreeSet<_>>() != expected_ids
            || check
                .observations
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                != OBSERVATIONS.into_iter().collect()
            || observed(check, "trials") != trials
        {
            return Err("Maelstrom scenario identity, fanout, or observations are incomplete");
        }
        validate_completion(bundle, check, scenario, trials)?;
    }
    Ok(())
}

fn validate_configuration(contract: &RunnerContract) -> Result<(), &'static str> {
    let values = &contract.configuration;
    let fixed = [
        ("build", "locked-debug"),
        ("evidence_semantics", "nondeterministic-sampled-e2e"),
        ("fault_markers", "required"),
        ("java_major", "21"),
        (
            "maelstrom_archive_sha256",
            "301ec71d6b12af0d765edb413f5cf5aa1046b5609bd4e31376a0b549548e5799",
        ),
        (
            "maelstrom_executable_sha256",
            "aba82f628ca088d25e8952c2c49834565406b9239d1c79953a54bf2c26cfdf20",
        ),
        (
            "maelstrom_jar_sha256",
            "7d35db06546a737134a4dd4eb3b7dfb0955537df992d922d18cc716080853f67",
        ),
        ("maelstrom_version", "v0.2.4"),
        ("operation_floor", "read-write-cas-per-trial"),
        ("replay", "retained-store"),
        ("scenarios", "base,membership,restart,app-crash,snapshot"),
        ("scheduler_seed", "unavailable"),
        ("structural_edn", "required"),
    ];
    if fixed
        .iter()
        .any(|(key, expected)| values.get(*key).map(String::as_str) != Some(*expected))
        || values.get("rate").map(String::as_str) != Some("100")
        || values
            .get("duration_seconds")
            .and_then(|value| value.parse::<u64>().ok())
            .is_none_or(|value| value == 0)
        || values
            .get("trials")
            .and_then(|value| value.parse::<u64>().ok())
            .is_none_or(|value| value == 0)
    {
        return Err("Maelstrom profile configuration is not the reviewed sampled-E2E contract");
    }
    Ok(())
}

fn validate_completion(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    scenario: &str,
    trials: u64,
) -> Result<(), &'static str> {
    let trials_usize = usize::try_from(trials).map_err(|_| "Maelstrom trial count is too large")?;
    let statuses = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .map(|result| (result.invariant_id.as_str(), result.status))
        .collect::<Vec<_>>();
    match check.completion {
        CheckCompletion::Completed => {
            if statuses
                .iter()
                .any(|(_, status)| *status != EvidenceStatus::Pass)
                || observed(check, "valid_trials") != trials
                || observed(check, "read_ok") < trials
                || observed(check, "write_ok") < trials
                || observed(check, "cas_ok") < trials
                || !markers_cover(check, scenario, trials)
                || artifact_count(check, "maelstrom-results") != trials_usize
                || artifact_count(check, "maelstrom-process-log") != trials_usize
                || artifact_count(check, "maelstrom-runner") != trials_usize
                || artifact_count(check, "maelstrom-binary") != trials_usize
                || artifact_count(check, "maelstrom-tool-jar") != trials_usize
                || artifact_count(check, "maelstrom-node-log") == 0
                || (requires_proxy(scenario)
                    && artifact_count(check, "maelstrom-proxy-binary") != trials_usize)
                || (requires_durable(scenario)
                    && artifact_count(check, "maelstrom-durable-file") < trials_usize)
            {
                return Err(
                    "passing Maelstrom scenario lacks checker, operation, or fault coverage",
                );
            }
        }
        CheckCompletion::Counterexample => {
            let failed = statuses
                .iter()
                .filter(|(_, status)| *status == EvidenceStatus::Fail)
                .map(|(invariant, _)| *invariant)
                .collect::<Vec<_>>();
            if failed.as_slice() != ["RD-06"]
                || statuses.iter().any(|(invariant, status)| {
                    (*status == EvidenceStatus::Fail && *invariant != "RD-06")
                        || !matches!(status, EvidenceStatus::Fail | EvidenceStatus::Incomplete)
                })
            {
                return Err("Maelstrom counterexample is attributed beyond client linearizability");
            }
        }
        CheckCompletion::CoverageNotReached
        | CheckCompletion::BudgetExhausted
        | CheckCompletion::Timeout => {
            if statuses
                .iter()
                .any(|(_, status)| *status != EvidenceStatus::Incomplete)
            {
                return Err("incomplete Maelstrom scenario must leave every result incomplete");
            }
        }
        CheckCompletion::HarnessError => {
            if statuses
                .iter()
                .any(|(_, status)| *status != EvidenceStatus::Error)
            {
                return Err("Maelstrom harness error must mark every result errored");
            }
        }
        CheckCompletion::FrontierExhausted => {
            return Err("Maelstrom scenario cannot claim exhaustive frontier completion");
        }
    }
    Ok(())
}

fn requires_proxy(scenario: &str) -> bool {
    matches!(scenario, "restart" | "app-crash" | "snapshot")
}

fn requires_durable(scenario: &str) -> bool {
    matches!(scenario, "restart" | "app-crash" | "snapshot")
}

fn markers_cover(check: &CheckReceipt, scenario: &str, trials: u64) -> bool {
    match scenario {
        "base" => true,
        "membership" => {
            observed(check, "membership_enter") >= trials
                && observed(check, "membership_leave") >= trials
                && observed(check, "membership_complete") >= trials
        }
        "restart" => {
            observed(check, "restarts") >= 3 * trials
                && observed(check, "post_restart_progress") >= trials
        }
        "app-crash" => {
            observed(check, "crashpoints") >= trials
                && observed(check, "post_crash_progress") >= trials
        }
        "snapshot" => {
            observed(check, "restarts") >= trials
                && observed(check, "snapshots_compacted") >= trials
                && observed(check, "snapshots_applied") >= trials
                && observed(check, "post_restart_snapshots_applied") >= trials
        }
        _ => false,
    }
}

fn scenario(path: &str) -> Option<&'static str> {
    match path {
        "scripts/maelstrom-lin-kv" => Some("base"),
        "scripts/maelstrom-lin-kv-membership-change" => Some("membership"),
        "scripts/maelstrom-lin-kv-repeated-restart" => Some("restart"),
        "scripts/maelstrom-lin-kv-app-persist-crash" => Some("app-crash"),
        "scripts/maelstrom-lin-kv-forced-snapshot" => Some("snapshot"),
        _ => None,
    }
}

fn observed(check: &CheckReceipt, name: &str) -> u64 {
    check.observations.get(name).copied().unwrap_or_default()
}

fn artifact_count(check: &CheckReceipt, kind: &str) -> usize {
    check
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == kind)
        .count()
}

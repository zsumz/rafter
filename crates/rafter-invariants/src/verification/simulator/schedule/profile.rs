//! Rederivation of reviewed PR and scheduled simulator work inventories.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

#[cfg(test)]
use super::events::scan_machine_events;
use super::events::ScannedSimulatorLog;
use crate::{
    contract::profile::{SimulatorRunnerConfiguration, SimulatorStateFloors},
    verification::AggregateError,
};

pub(super) fn validate_scanned_simulator_schedule(
    profile: &str,
    source_ref: &str,
    configuration: &SimulatorRunnerConfiguration,
    logs: &[ScannedSimulatorLog<'_>],
) -> Result<(), AggregateError> {
    if profile == "pr" {
        return validate_pr_soak_schedule(configuration, logs);
    }
    let seed_count = configuration
        .seed_count
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| AggregateError::new("scheduled seed count is missing".to_owned()))?;
    let Some(expected_seeds) =
        crate::contract::profile::scheduled_simulator_seeds(profile, source_ref, seed_count)
    else {
        return Ok(());
    };
    if logs.len() != 1 {
        return Err(AggregateError::new(format!(
            "scheduled simulator receipt must retain exactly one profile log, found {}",
            logs.len()
        )));
    }
    // The log proves the model profile that ran, which is not the lane name
    // once a lane runs a sibling's profile (weekly currently runs nightly's).
    let model_profile = crate::contract::profile::scheduled_model_profile(profile)
        .ok_or_else(|| AggregateError::new(format!("unknown simulator profile {profile}")))?;
    let expected_profile = format!("model-check profile={model_profile} ");
    let expected_seed_line =
        format!("model-check {model_profile}-soak seeds source=replay seeds={expected_seeds}");
    if !logs[0]
        .source
        .lines()
        .any(|line| line == "exit_code: Some(0)")
        || !logs[0]
            .source
            .lines()
            .any(|line| line.starts_with(&expected_profile))
        || !logs[0]
            .source
            .lines()
            .any(|line| line == expected_seed_line)
        || !profile_total_is_rederived(model_profile, &configuration.state_floors, &logs[0].events)
        || !soak_seeds_are_rederived(
            model_profile,
            &expected_seeds,
            configuration.soak_steps,
            logs[0].events.iter(),
        )
    {
        return Err(AggregateError::new(format!(
            "scheduled simulator log does not prove the source-derived {profile} execution plan"
        )));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn validate_simulator_schedule(
    profile: &str,
    source_ref: &str,
    configuration: &SimulatorRunnerConfiguration,
    logs: &[String],
) -> Result<(), AggregateError> {
    let scanned = logs
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let (events, diagnostics) =
                scan_machine_events(source, &format!("simulator test log {index}"));
            if let Some(diagnostic) = diagnostics.into_iter().next() {
                return Err(AggregateError::new(diagnostic));
            }
            Ok(ScannedSimulatorLog { source, events })
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_scanned_simulator_schedule(profile, source_ref, configuration, &scanned)
}

fn validate_pr_soak_schedule(
    configuration: &SimulatorRunnerConfiguration,
    logs: &[ScannedSimulatorLog<'_>],
) -> Result<(), AggregateError> {
    const EXPECTED_SEEDS: &str = "0x9103,0x9104,0x9105,0x9106";
    if logs.len() != 2 {
        return Err(AggregateError::new(format!(
            "PR simulator receipt must retain exactly two profile logs, found {}",
            logs.len()
        )));
    }
    let expected_seed_line =
        format!("model-check raft-soak seeds source=curated seeds={EXPECTED_SEEDS}");
    let seed_line_count = logs
        .iter()
        .flat_map(|log| log.source.lines())
        .filter(|line| *line == expected_seed_line)
        .count();
    if seed_line_count != 1
        || !soak_seeds_are_rederived(
            "raft",
            EXPECTED_SEEDS,
            configuration.soak_steps,
            logs.iter().flat_map(|log| log.events.iter()),
        )
    {
        return Err(AggregateError::new(
            "PR simulator log does not prove the exact reviewed soak seed inventory".to_owned(),
        ));
    }
    Ok(())
}

fn profile_total_is_rederived(
    model_profile: &str,
    state_floors: &SimulatorStateFloors,
    events: &[Value],
) -> bool {
    let (protocol_floor, verifier_floor) = match state_floors {
        SimulatorStateFloors::Aggregate { protocol, verifier } => (*protocol, *verifier),
        SimulatorStateFloors::PerEvidence => return false,
    };
    let exhaustive = events
        .iter()
        .filter(|event| event["event"] == "exhaustive-check")
        .collect::<Vec<_>>();
    let Some(protocol_total) = exhaustive.iter().try_fold(0_u64, |total, event| {
        total.checked_add(event["unique_protocol_states"].as_u64()?)
    }) else {
        return false;
    };
    let Some(verifier_total) = exhaustive.iter().try_fold(0_u64, |total, event| {
        total.checked_add(event["unique_verifier_states"].as_u64()?)
    }) else {
        return false;
    };
    let profile_totals = events
        .iter()
        .filter(|event| event["event"] == "profile-total" && event["profile"] == model_profile)
        .collect::<Vec<_>>();
    profile_totals.len() == 1
        && !exhaustive.is_empty()
        && exhaustive.iter().all(|event| event["status"] == "pass")
        && profile_totals[0]["status"] == "pass"
        && profile_totals[0]["target_protocol_states"] == protocol_floor
        && profile_totals[0]["target_verifier_states"] == verifier_floor
        && profile_totals[0]["unique_protocol_states"] == protocol_total
        && profile_totals[0]["unique_verifier_states"] == verifier_total
        && protocol_total >= protocol_floor
        && verifier_total >= verifier_floor
}

fn soak_seeds_are_rederived<'a>(
    model_profile: &str,
    expected_seeds: &str,
    expected_steps: u64,
    events: impl IntoIterator<Item = &'a Value>,
) -> bool {
    let expected_values = expected_seeds
        .split(',')
        .map(|seed| u64::from_str_radix(seed.trim_start_matches("0x"), 16))
        .collect::<Result<Vec<_>, _>>();
    let Ok(expected_values) = expected_values else {
        return false;
    };
    let expected_checks = [
        format!("{model_profile}-soak"),
        format!("{model_profile}-soak-lease"),
        format!("{model_profile}-soak-membership"),
    ];
    let expected = expected_checks
        .iter()
        .flat_map(|check| {
            expected_values
                .iter()
                .map(move |seed| (check.clone(), *seed))
        })
        .collect::<BTreeSet<_>>();
    let mut observed = BTreeMap::<(String, u64), usize>::new();
    for event in events
        .into_iter()
        .filter(|event| event["event"] == "soak-check")
    {
        let (Some(check), Some(seed), Some(steps)) = (
            event["check_id"].as_str(),
            event["seed"].as_u64(),
            event["steps"].as_u64(),
        ) else {
            return false;
        };
        if event["status"] != "pass"
            || steps != expected_steps
            || !check.starts_with(&format!("{model_profile}-soak"))
        {
            return false;
        }
        *observed.entry((check.to_owned(), seed)).or_default() += 1;
    }
    observed.len() == expected.len()
        && observed.keys().cloned().collect::<BTreeSet<_>>() == expected
        && observed.values().all(|count| *count == 1)
}

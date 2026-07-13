use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use serde_json::Value;

use crate::{aggregate::AggregateError, ResultBundle};

use super::EVENT_PREFIX;

pub(super) fn verify_simulator_schedule(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<(), AggregateError> {
    let configuration = bundle
        .execution
        .plan
        .contract
        .runners
        .get("simulator")
        .ok_or_else(|| AggregateError::new("simulator runner contract is missing".to_owned()))?
        .simulator_configuration()
        .map_err(|error| {
            AggregateError::new(format!("parse typed simulator runner contract: {error}"))
        })?;
    configuration
        .validate_profile(&bundle.profile)
        .map_err(|error| AggregateError::new(format!("validate simulator contract: {error}")))?;
    let logs = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "simulator-log")
        .collect::<Vec<_>>();
    let sources = logs
        .iter()
        .map(|log| {
            fs::read_to_string(root.join(&log.path)).map_err(|error| {
                AggregateError::new(format!(
                    "read scheduled simulator log {}: {error}",
                    log.path
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    verify_simulator_invocations(bundle, root, &sources)?;
    validate_simulator_schedule(
        &bundle.profile,
        &bundle.source_ref,
        &configuration,
        &sources,
    )
}

fn verify_simulator_invocations(
    bundle: &ResultBundle,
    root: &Path,
    sources: &[String],
) -> Result<(), AggregateError> {
    let binary = bundle
        .execution
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "simulator-binary")
        .ok_or_else(|| AggregateError::new("simulator binary artifact is missing".to_owned()))?;
    let environment_sha256 = bundle.execution.source.environment_sha256.as_str();
    let current_dir = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize simulator root: {error}")))?
        .to_string_lossy()
        .into_owned();
    let expected: Vec<(String, Vec<String>)> = match bundle.profile.as_str() {
        "pr" => vec![
            (
                "fast".to_owned(),
                vec!["--profile".to_owned(), "fast".to_owned()],
            ),
            (
                "raft-soak".to_owned(),
                vec!["--profile".to_owned(), "raft-soak".to_owned()],
            ),
        ],
        profile @ ("nightly" | "weekly") => {
            let label = format!("raft-{profile}");
            let seeds = crate::producer::expected_scheduled_seeds(profile, &bundle.source_ref)
                .ok_or_else(|| AggregateError::new("scheduled seeds are missing".to_owned()))?;
            vec![(
                label.clone(),
                vec!["--profile".to_owned(), label, "--seed".to_owned(), seeds],
            )]
        }
        profile => {
            return Err(AggregateError::new(format!(
                "unknown simulator profile {profile}"
            )))
        }
    };
    if sources.len() != expected.len() {
        return Err(AggregateError::new(
            "simulator log count does not match the execution plan".to_owned(),
        ));
    }
    for (label, arguments) in expected {
        let source = sources
            .iter()
            .find(|source| source.lines().any(|line| line == format!("label: {label}")))
            .ok_or_else(|| AggregateError::new(format!("simulator log {label} is missing")))?;
        let invocations = crate::producer::process::parse_combined_invocations(source)
            .map_err(|error| AggregateError::new(format!("parse simulator invocation: {error}")))?;
        let [observed] = invocations.as_slice() else {
            return Err(AggregateError::new(format!(
                "simulator log {label} must contain exactly one invocation"
            )));
        };
        if observed.label != label
            || observed.invocation.arguments != arguments
            || observed.invocation.program_sha256 != binary.sha256
            || observed.invocation.current_dir != current_dir
            || observed.invocation.environment_sha256 != environment_sha256
            || crate::producer::process::digest_environment(&observed.invocation.environment)
                != environment_sha256
            || !Path::new(&observed.invocation.program).is_absolute()
        {
            return Err(AggregateError::new(format!(
                "simulator log {label} does not match the exact invocation plan"
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_simulator_schedule(
    profile: &str,
    source_ref: &str,
    configuration: &crate::catalog::SimulatorRunnerConfiguration,
    logs: &[String],
) -> Result<(), AggregateError> {
    if profile == "pr" {
        return validate_pr_soak_schedule(configuration, logs);
    }
    let seed_count = configuration
        .seed_count
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| AggregateError::new("scheduled seed count is missing".to_owned()))?;
    let Some(expected_seeds) =
        crate::producer::expected_scheduled_seeds_with_count(profile, source_ref, seed_count)
    else {
        return Ok(());
    };
    if logs.len() != 1 {
        return Err(AggregateError::new(format!(
            "scheduled simulator receipt must retain exactly one profile log, found {}",
            logs.len()
        )));
    }
    let model_profile = format!("raft-{profile}");
    let expected_profile = format!("model-check profile={model_profile} ");
    let expected_seed_line =
        format!("model-check {model_profile}-soak seeds source=replay seeds={expected_seeds}");
    let events = parse_machine_events(&logs[0])?;
    if !logs[0].lines().any(|line| line == "exit_code: Some(0)")
        || !logs[0]
            .lines()
            .any(|line| line.starts_with(&expected_profile))
        || !logs[0].lines().any(|line| line == expected_seed_line)
        || !profile_total_is_rederived(&model_profile, &configuration.state_floors, &events)
        || !soak_seeds_are_rederived(
            &model_profile,
            &expected_seeds,
            configuration.soak_steps,
            &events,
        )
    {
        return Err(AggregateError::new(format!(
            "scheduled simulator log does not prove the source-derived {profile} execution plan"
        )));
    }
    Ok(())
}

fn validate_pr_soak_schedule(
    configuration: &crate::catalog::SimulatorRunnerConfiguration,
    logs: &[String],
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
        .flat_map(|log| log.lines())
        .filter(|line| *line == expected_seed_line)
        .count();
    let events = logs
        .iter()
        .map(|log| parse_machine_events(log))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if seed_line_count != 1
        || !soak_seeds_are_rederived("raft", EXPECTED_SEEDS, configuration.soak_steps, &events)
    {
        return Err(AggregateError::new(
            "PR simulator log does not prove the exact reviewed soak seed inventory".to_owned(),
        ));
    }
    Ok(())
}

fn parse_machine_events(log: &str) -> Result<Vec<Value>, AggregateError> {
    log.lines()
        .filter_map(|line| line.strip_prefix(EVENT_PREFIX))
        .map(|line| {
            serde_json::from_str(line).map_err(|error| {
                AggregateError::new(format!("parse scheduled simulator event: {error}"))
            })
        })
        .collect()
}

fn profile_total_is_rederived(
    model_profile: &str,
    state_floors: &crate::catalog::SimulatorStateFloors,
    events: &[Value],
) -> bool {
    let (protocol_floor, verifier_floor) = match state_floors {
        crate::catalog::SimulatorStateFloors::Aggregate { protocol, verifier } => {
            (*protocol, *verifier)
        }
        crate::catalog::SimulatorStateFloors::PerEvidence => return false,
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

fn soak_seeds_are_rederived(
    model_profile: &str,
    expected_seeds: &str,
    expected_steps: u64,
    events: &[Value],
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
    for event in events.iter().filter(|event| event["event"] == "soak-check") {
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

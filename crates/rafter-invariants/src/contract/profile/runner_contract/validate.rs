//! Shared runner identity and dispatch validation.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::super::RunnerContract;

pub(in crate::contract::profile) fn validate_runner(
    profile: &str,
    layer: &str,
    runner: &RunnerContract,
) -> Result<(), String> {
    let (producer, minimum_observed_checks) = expected_identity(layer)?;
    let expected_command = [
        "cargo",
        "run",
        "--locked",
        "-p",
        "rafter-invariants",
        "--",
        "run",
        "--profile",
        profile,
        "--layer",
        layer,
    ]
    .map(str::to_owned)
    .to_vec();
    if runner.producer != producer
        || runner.command != expected_command
        || runner.minimum_observed_checks != minimum_observed_checks
        || !runner.require_peak_rss
    {
        return Err(
            "producer, command, coverage floor, or resource contract is not canonical".to_owned(),
        );
    }

    match layer {
        "tests" => super::tests_runner::validate(profile, &runner.configuration),
        "simulator" => super::simulator::validate(&runner.configuration),
        "tla" => super::tla::validate(profile, &runner.configuration),
        "maelstrom" => super::maelstrom::validate(profile, &runner.configuration),
        _ => unreachable!("layer was matched above"),
    }
}

fn expected_identity(layer: &str) -> Result<(&'static str, usize), String> {
    match layer {
        "tests" => Ok(("rafter-invariants-tests-v14", 82)),
        "simulator" => Ok(("rafter-invariants-simulator-v19", 79)),
        "tla" => Ok(("rafter-invariants-tla-v15", 1)),
        "maelstrom" => Ok(("rafter-invariants-maelstrom-v10", 6)),
        _ => Err(format!("unsupported runner layer {layer}")),
    }
}

pub(super) fn decode_configuration<T>(configuration: &BTreeMap<String, String>) -> Result<T, String>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(serde_json::to_value(configuration).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

//! Manifest duration parsing and immutable layer-budget construction.

use std::{
    collections::BTreeMap,
    error::Error,
    time::{Duration, Instant},
};

use crate::contract::profile::RunnerContract;

use super::{ActiveLayerBudget, ProcessPolicy};

pub(in crate::producer::process) fn layer_budget(
    profile: &str,
    layer: &str,
    runner: &RunnerContract,
) -> Result<Option<ActiveLayerBudget>, Box<dyn Error>> {
    if !matches!(layer, "tests" | "simulator" | "tla" | "maelstrom") {
        return Err(format!("unsupported producer layer {layer}").into());
    }
    let total = configured_duration(
        runner,
        if layer == "tla" {
            "total_timeout"
        } else {
            "layer_timeout"
        },
    )?;
    let finalization_reserve = configured_duration(runner, "finalization_reserve")?;
    let execution_window = total
        .checked_sub(finalization_reserve)
        .filter(|window| !window.is_zero())
        .ok_or("producer finalization reserve must be smaller than its layer budget")?;
    let optional = |name| {
        runner
            .configuration
            .get(name)
            .map(|value| parse_contract_duration(name, value))
            .transpose()
    };
    let started = Instant::now();
    Ok(Some(ActiveLayerBudget {
        profile: profile.to_owned(),
        layer: layer.to_owned(),
        finalization_deadline: started
            .checked_add(execution_window)
            .ok_or("producer execution deadline overflow")?,
        total_deadline: started
            .checked_add(total)
            .ok_or("producer total deadline overflow")?,
        finalization_reserve,
        compile_timeout: optional("compile_timeout")?,
        discovery_timeout: optional("discovery_timeout")?,
        execution_timeout: optional("execution_timeout")?,
        policy: process_policy(runner)?,
    }))
}

fn process_policy(runner: &RunnerContract) -> Result<ProcessPolicy, Box<dyn Error>> {
    process_policy_from_configuration(&runner.configuration)
}

fn process_policy_from_configuration(
    configuration: &BTreeMap<String, String>,
) -> Result<ProcessPolicy, Box<dyn Error>> {
    Ok(ProcessPolicy {
        termination_grace: configured_map_duration(configuration, "termination_grace")?,
        kill_confirmation_timeout: configured_map_duration(
            configuration,
            "kill_confirmation_timeout",
        )?,
        receipt_finalization_allowance: configured_map_duration(
            configuration,
            "receipt_finalization_allowance",
        )?,
    })
}

fn configured_duration(runner: &RunnerContract, name: &str) -> Result<Duration, Box<dyn Error>> {
    let value = runner
        .configuration
        .get(name)
        .ok_or_else(|| format!("runner configuration omitted {name}"))?;
    parse_contract_duration(name, value)
}

fn configured_map_duration(
    configuration: &BTreeMap<String, String>,
    name: &str,
) -> Result<Duration, Box<dyn Error>> {
    let value = configuration
        .get(name)
        .ok_or_else(|| format!("runner configuration omitted {name}"))?;
    parse_contract_duration(name, value)
}

fn parse_contract_duration(name: &str, value: &str) -> Result<Duration, Box<dyn Error>> {
    let (amount, multiplier) = if let Some(value) = value.strip_suffix('s') {
        (value, 1)
    } else if let Some(value) = value.strip_suffix('m') {
        (value, 60)
    } else if let Some(value) = value.strip_suffix('h') {
        (value, 60 * 60)
    } else {
        return Err(
            format!("runner duration {name} must use whole seconds, minutes, or hours").into(),
        );
    };
    let amount = amount.parse::<u64>()?;
    let seconds = amount
        .checked_mul(multiplier)
        .ok_or_else(|| format!("runner duration {name} overflows"))?;
    if seconds == 0 {
        return Err(format!("runner duration {name} must be positive").into());
    }
    Ok(Duration::from_secs(seconds))
}

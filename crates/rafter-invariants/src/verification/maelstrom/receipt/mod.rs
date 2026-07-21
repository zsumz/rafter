//! Structural validation of Maelstrom result receipts.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    contract::{catalog::EvidenceDescriptor, profile::RunnerContract},
    evidence::{format::java, ResultBundle},
};

use super::{observation::OBSERVATIONS, scenario::Scenario};

mod completion;
mod configuration;

pub(crate) fn validate(
    bundle: &ResultBundle,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
    contract: &RunnerContract,
) -> Result<(), &'static str> {
    configuration::validate(contract)?;
    let required = expected
        .iter()
        .filter(|(_, descriptor)| descriptor.layer == "maelstrom")
        .map(|(evidence_id, descriptor)| {
            (
                evidence_id.clone(),
                Scenario::from_evidence_path(descriptor.path.as_str()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if required.len() != 19 || bundle.execution.checks.len() != 6 {
        return Err(
            "Maelstrom receipt must contain six scenarios covering nineteen clause-bound E2E records",
        );
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
    validate_toolchain(bundle, contract)?;
    let trials = contract.configuration["trials"]
        .parse::<u64>()
        .map_err(|_| "Maelstrom trial count is invalid")?;
    for check in &bundle.execution.checks {
        let scenario = Scenario::from_check_id(&check.check_id)
            .map_err(|_| "Maelstrom check ID is invalid")?;
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
            || completion::observed(check, "trials") != trials
        {
            return Err("Maelstrom scenario identity, fanout, or observations are incomplete");
        }
        completion::validate(bundle, check, scenario, trials)?;
    }
    Ok(())
}

fn validate_toolchain(
    bundle: &ResultBundle,
    contract: &RunnerContract,
) -> Result<(), &'static str> {
    for tool in ["java", "maelstrom", "dot", "gnuplot"] {
        if !bundle.execution.source.tools.contains_key(tool) {
            return Err("Maelstrom receipt lacks external tool provenance");
        }
    }
    if bundle.execution.source.tools["maelstrom"].sha256
        != contract.configuration["maelstrom_executable_sha256"]
        || java::major(&bundle.execution.source.tools["java"].version) != Some(21)
    {
        return Err("Maelstrom receipt external tool identity does not match the profile pin");
    }
    if bundle.execution.source.build_profile != "maelstrom-debug"
        || !bundle.execution.source.features.is_empty()
    {
        return Err("Maelstrom receipt build identity does not match the profile contract");
    }
    Ok(())
}

#[cfg(test)]
pub(crate) use completion::valid_counterexample_attribution;

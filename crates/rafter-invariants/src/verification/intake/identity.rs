//! Trusted active-contract identity and exact runner-set selection.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use super::{IntakeDefect, VerificationRequest};
use crate::{evidence::ResultBundle, verification::AggregateError};

pub(super) fn validate_request(request: VerificationRequest<'_>) -> Result<(), AggregateError> {
    request
        .manifest
        .validate(request.catalog)
        .map_err(|error| AggregateError::new(error.to_string()))?;
    let contract = request
        .manifest
        .profiles
        .get(&request.active_plan.profile)
        .ok_or_else(|| {
            AggregateError::new(format!(
                "unknown active profile {}",
                request.active_plan.profile
            ))
        })?;
    if request.active_plan.contract != *contract {
        return Err(AggregateError::new(
            "active plan contract does not match the selected profile".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn expected_runners(
    request: VerificationRequest<'_>,
    required_layer: Option<&str>,
) -> Result<BTreeSet<String>, AggregateError> {
    if let Some(layer) = required_layer {
        if !request
            .active_plan
            .contract
            .required_layers
            .iter()
            .any(|candidate| candidate.as_str() == layer)
        {
            return Err(AggregateError::new(format!(
                "layer {layer} is not required by profile {}",
                request.active_plan.profile
            )));
        }
        return Ok(BTreeSet::from([layer.to_owned()]));
    }
    Ok(request
        .active_plan
        .contract
        .required_layers
        .iter()
        .map(|layer| layer.as_str().to_owned())
        .collect())
}

pub(super) fn select_bundles(
    request: VerificationRequest<'_>,
    expected_runners: &BTreeSet<String>,
    decoded: Vec<(PathBuf, ResultBundle)>,
    defects: &mut Vec<IntakeDefect>,
) -> Vec<(String, PathBuf, ResultBundle)> {
    let mut by_runner = BTreeMap::<String, Vec<(PathBuf, ResultBundle)>>::new();
    for (path, bundle) in decoded {
        if let Err(defect) = validate_bundle(request, expected_runners, &bundle) {
            defects.push(defect);
            continue;
        }
        by_runner
            .entry(bundle.runner.clone())
            .or_default()
            .push((path, bundle));
    }
    let complete = expected_runners.iter().all(|runner| {
        by_runner
            .get(runner)
            .is_some_and(|bundles| bundles.len() == 1)
    });
    if !complete {
        let observed = by_runner
            .iter()
            .map(|(runner, bundles)| format!("{runner}={}", bundles.len()))
            .collect::<Vec<_>>()
            .join(",");
        defects.push(IntakeDefect::unverifiable(format!(
            "result receipts require exactly one bundle for each trusted runner; observed [{observed}]"
        )));
    }
    expected_runners
        .iter()
        .filter_map(|runner| {
            let bundles = by_runner.remove(runner)?;
            let [(path, bundle)] = <[_; 1]>::try_from(bundles).ok()?;
            Some((runner.clone(), path, bundle))
        })
        .collect()
}

fn validate_bundle(
    request: VerificationRequest<'_>,
    expected_runners: &BTreeSet<String>,
    bundle: &ResultBundle,
) -> Result<(), IntakeDefect> {
    if bundle.execution.plan != *request.active_plan {
        return Err(IntakeDefect::stale(format!(
            "evidence {} does not match the active execution plan",
            bundle.runner
        )));
    }
    if bundle.profile != request.active_plan.profile {
        return Err(IntakeDefect::stale(format!(
            "runner {} reported profile {} instead of {}",
            bundle.runner, bundle.profile, request.active_plan.profile
        )));
    }
    if bundle.source_ref != request.source_ref {
        return Err(IntakeDefect::stale(format!(
            "runner {} evidence is stale: source {} != {}",
            bundle.runner, bundle.source_ref, request.source_ref
        )));
    }
    if !expected_runners.contains(&bundle.runner) {
        return Err(IntakeDefect::unverifiable(format!(
            "runner {} is not in the trusted runner set",
            bundle.runner
        )));
    }
    Ok(())
}

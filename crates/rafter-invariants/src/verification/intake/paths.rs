//! Path decoding and unavoidable authentication before receipt acceptance.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use super::{
    identity, preflight,
    receipt_file::{ReceiptFile, ReceiptRoot},
    verify, EvidenceIntake, IntakeDefect, VerificationRequest,
};
use crate::{
    evidence::ResultBundle,
    verification::{bundle::ProfileBudget, AggregateError},
};

mod authentication;

pub(super) enum SourceVerifierState {
    Pending,
    Ready(Box<crate::verification::source::SourceVerifier>),
    Failed,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum VerificationMode {
    Receipt,
    Aggregate,
}

#[cfg(test)]
pub(crate) fn verify_paths(
    request: VerificationRequest<'_>,
    paths: &[PathBuf],
    defects: Vec<IntakeDefect>,
) -> Result<EvidenceIntake, AggregateError> {
    verify_paths_for_layer(request, paths, defects, None, VerificationMode::Receipt)
}

pub(crate) fn verify_aggregate_paths(
    request: VerificationRequest<'_>,
    paths: &[PathBuf],
    defects: Vec<IntakeDefect>,
) -> Result<EvidenceIntake, AggregateError> {
    verify_paths_for_layer(request, paths, defects, None, VerificationMode::Aggregate)
}

pub(crate) fn verify_layer_paths(
    request: VerificationRequest<'_>,
    layer: &str,
    path: PathBuf,
) -> Result<EvidenceIntake, AggregateError> {
    verify_paths_for_layer(
        request,
        &[path],
        Vec::new(),
        Some(layer),
        VerificationMode::Receipt,
    )
}

fn verify_paths_for_layer(
    request: VerificationRequest<'_>,
    paths: &[PathBuf],
    mut defects: Vec<IntakeDefect>,
    required_layer: Option<&str>,
    mode: VerificationMode,
) -> Result<EvidenceIntake, AggregateError> {
    identity::validate_request(request)?;
    let expected_runners = identity::expected_runners(request, required_layer)?;
    let expected_paths = expected_runners.len();
    let profile_budget = ProfileBudget::for_trusted(&request.active_plan.profile, expected_paths)?;
    if paths.len() > expected_paths {
        defects.push(IntakeDefect::unverifiable(format!(
            "{} verification accepts at most {expected_paths} result receipt paths, found {}",
            required_layer.unwrap_or(&request.active_plan.profile),
            paths.len()
        )));
        return verify::accept(request, &[], defects);
    }
    if paths.len() != expected_paths {
        defects.push(IntakeDefect::unverifiable(format!(
            "{} verification requires exactly {expected_paths} result receipt paths, found {}",
            required_layer.unwrap_or(&request.active_plan.profile),
            paths.len()
        )));
    }

    let duplicate_paths = duplicate_paths(paths, &mut defects);
    let mut receipts = open_receipts(paths, &duplicate_paths, &mut defects);
    let receipt_bytes = receipts.iter().try_fold(0_u64, |total, receipt| {
        total.checked_add(receipt.length()).ok_or_else(|| {
            AggregateError::new("result receipt aggregate size overflowed u64".to_owned())
        })
    })?;

    let mut decoded = Vec::new();
    if receipt_bytes > profile_budget.receipt_bytes() {
        defects.push(IntakeDefect::unverifiable(format!(
            "result receipts declare {receipt_bytes} bytes, exceeding the {}-byte aggregate limit",
            profile_budget.receipt_bytes()
        )));
    } else {
        for receipt in &mut receipts {
            match decode(receipt) {
                Ok(bundle) => decoded.push((receipt.path().to_path_buf(), bundle)),
                Err(defect) => defects.push(defect),
            }
        }
    }

    let trusted = identity::select_bundles(request, &expected_runners, decoded, &mut defects);
    let mut bundles = Vec::new();
    let mut artifact_guards = Vec::new();
    let mut source_verifier = SourceVerifierState::Pending;
    match preflight::profile_artifacts(&request, &trusted, profile_budget) {
        Ok(()) => {
            let mut authenticator = authentication::ReceiptAuthenticator {
                request,
                source_verifier: &mut source_verifier,
                accepted: &mut bundles,
                artifact_guards: &mut artifact_guards,
                defects: &mut defects,
            };
            for (trusted_runner, path, bundle) in trusted {
                authenticator.authenticate(&path, bundle, &trusted_runner);
            }
        }
        Err(error) => defects.push(IntakeDefect::unverifiable(error.to_string())),
    }

    let mut intake = verify::accept(request, &bundles, defects)?;
    intake.attach_artifact_guards(artifact_guards);
    if mode == VerificationMode::Aggregate {
        super::replay::apply(request, &mut source_verifier, &mut intake);
    }
    let mut trailing_defects = Vec::new();
    authentication::revalidate_source(&source_verifier, request.root, &mut trailing_defects);
    for receipt in &receipts {
        if let Err(defect) = receipt.revalidate() {
            trailing_defects.push(defect);
        }
    }
    if required_layer.is_some() && bundles.len() != 1 {
        trailing_defects.push(IntakeDefect::unverifiable(format!(
            "layer verification requires exactly one authenticated result bundle, found {}",
            bundles.len()
        )));
    }
    intake.extend_defects(trailing_defects);
    Ok(intake)
}

fn open_receipts(
    paths: &[PathBuf],
    duplicate_paths: &BTreeSet<PathBuf>,
    defects: &mut Vec<IntakeDefect>,
) -> Vec<ReceiptFile> {
    let Some(first) = paths.iter().find(|path| !duplicate_paths.contains(*path)) else {
        return Vec::new();
    };
    let root = match ReceiptRoot::capture(first) {
        Ok(root) => root,
        Err(defect) => {
            defects.push(defect);
            return Vec::new();
        }
    };
    let mut receipts = Vec::new();
    for path in paths {
        if duplicate_paths.contains(path) {
            continue;
        }
        match ReceiptFile::open(&root, path) {
            Ok(receipt) => receipts.push(receipt),
            Err(defect) => defects.push(defect),
        }
    }
    receipts
}

fn duplicate_paths(paths: &[PathBuf], defects: &mut Vec<IntakeDefect>) -> BTreeSet<PathBuf> {
    let mut counts = BTreeMap::new();
    for path in paths {
        *counts.entry(path.clone()).or_insert(0_usize) += 1;
    }
    counts
        .into_iter()
        .filter_map(|(path, count)| {
            (count > 1).then(|| {
                defects.push(IntakeDefect::unverifiable(format!(
                    "duplicate evidence result path: {}",
                    path.display()
                )));
                path
            })
        })
        .collect()
}

fn decode(receipt: &mut ReceiptFile) -> Result<ResultBundle, IntakeDefect> {
    let path = receipt.path().to_path_buf();
    let source = receipt.read()?;
    let value = super::json::decode_unique_value(&source)
        .map_err(|error| IntakeDefect::malformed(format!("parse {}: {error}", path.display())))?;
    drop(source);
    crate::evidence::validate_result_value(&value).map_err(|error| {
        IntakeDefect::malformed(format!(
            "validate result schema for {}: {error}",
            path.display()
        ))
    })?;
    serde_json::from_value(value)
        .map_err(|error| IntakeDefect::malformed(format!("decode {}: {error}", path.display())))
}

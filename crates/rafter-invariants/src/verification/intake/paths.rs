//! Path decoding and unavoidable authentication before receipt acceptance.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use super::{
    identity, preflight,
    receipt_file::{ReceiptFile, ReceiptRoot},
    verify, EvidenceIntake, IntakeDefect, VerificationRequest,
};
use crate::{
    evidence::ResultBundle,
    verification::{bundle::ProfileBudget, source::SourceAuthenticationError, AggregateError},
};

enum SourceVerifierState {
    Pending,
    Ready(Box<crate::verification::source::SourceVerifier>),
    Failed,
}

pub(crate) fn verify_paths(
    request: VerificationRequest<'_>,
    paths: &[PathBuf],
    defects: Vec<IntakeDefect>,
) -> Result<EvidenceIntake, AggregateError> {
    verify_paths_for_layer(request, paths, defects, None)
}

pub(crate) fn verify_layer_paths(
    request: VerificationRequest<'_>,
    layer: &str,
    path: PathBuf,
) -> Result<EvidenceIntake, AggregateError> {
    verify_paths_for_layer(request, &[path], Vec::new(), Some(layer))
}

fn verify_paths_for_layer(
    request: VerificationRequest<'_>,
    paths: &[PathBuf],
    mut defects: Vec<IntakeDefect>,
    required_layer: Option<&str>,
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
    let mut source_verifier = SourceVerifierState::Pending;
    match preflight::profile_artifacts(&request, &trusted, profile_budget) {
        Ok(()) => {
            for (trusted_runner, path, bundle) in trusted {
                authenticate(
                    request,
                    &path,
                    bundle,
                    &trusted_runner,
                    &mut source_verifier,
                    &mut bundles,
                    &mut defects,
                );
            }
        }
        Err(error) => defects.push(IntakeDefect::unverifiable(error.to_string())),
    }

    revalidate_source(&source_verifier, request.root, &mut defects);
    for receipt in &receipts {
        if let Err(defect) = receipt.revalidate() {
            defects.push(defect);
        }
    }
    if required_layer.is_some() && bundles.len() != 1 {
        defects.push(IntakeDefect::unverifiable(format!(
            "layer verification requires exactly one authenticated result bundle, found {}",
            bundles.len()
        )));
    }
    verify::accept(request, &bundles, defects)
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

fn revalidate_source(state: &SourceVerifierState, root: &Path, defects: &mut Vec<IntakeDefect>) {
    let SourceVerifierState::Ready(verifier) = state else {
        return;
    };
    match verifier.revalidate(root) {
        Ok(()) => {}
        Err(SourceAuthenticationError::Stale(error)) => defects.push(IntakeDefect::stale(format!(
            "revalidate active source after evidence verification: {error}"
        ))),
        Err(error @ SourceAuthenticationError::Unverifiable(_)) => {
            defects.push(IntakeDefect::unverifiable(format!(
                "revalidate active source after evidence verification: {}",
                error.message()
            )));
        }
    }
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

fn authenticate(
    request: VerificationRequest<'_>,
    path: &Path,
    bundle: ResultBundle,
    trusted_runner: &str,
    source_verifier: &mut SourceVerifierState,
    accepted: &mut Vec<ResultBundle>,
    defects: &mut Vec<IntakeDefect>,
) {
    if !authenticate_source(
        source_verifier,
        trusted_runner,
        &bundle.execution.source,
        request.root,
        path,
        defects,
    ) {
        return;
    }
    let SourceVerifierState::Ready(source_verifier) = source_verifier else {
        defects.push(IntakeDefect::unverifiable(
            "source verifier became unavailable after authentication".to_owned(),
        ));
        return;
    };
    match crate::verification::verify_bundle_artifacts(
        &bundle,
        request.root,
        source_verifier.source_root(),
        request.catalog,
        &request.active_plan.profile,
        trusted_runner,
    ) {
        Ok(diagnostics) => {
            defects.extend(diagnostics.into_iter().map(|message| {
                IntakeDefect::unverifiable(format!("verify {}: {message}", path.display()))
            }));
            accepted.push(bundle);
        }
        Err(error) => defects.push(IntakeDefect::unverifiable(format!(
            "verify {}: {error}",
            path.display()
        ))),
    }
}

fn authenticate_source(
    state: &mut SourceVerifierState,
    layer: &str,
    source: &crate::evidence::SourceReceipt,
    root: &std::path::Path,
    path: &Path,
    defects: &mut Vec<IntakeDefect>,
) -> bool {
    if matches!(state, SourceVerifierState::Pending) {
        match crate::verification::source::SourceVerifier::capture(root) {
            Ok(verifier) => *state = SourceVerifierState::Ready(Box::new(verifier)),
            Err(error) => {
                defects.push(IntakeDefect::unverifiable(format!(
                    "observe active source for {}: {error}",
                    path.display()
                )));
                *state = SourceVerifierState::Failed;
                return false;
            }
        }
    }
    let SourceVerifierState::Ready(verifier) = state else {
        return false;
    };
    match verifier.authenticate(layer, source, root) {
        Ok(()) => true,
        Err(SourceAuthenticationError::Stale(error)) => {
            defects.push(IntakeDefect::stale(format!(
                "verify source identity for {}: {error}",
                path.display()
            )));
            false
        }
        Err(error @ SourceAuthenticationError::Unverifiable(_)) => {
            defects.push(IntakeDefect::unverifiable(format!(
                "verify source identity for {}: {}",
                path.display(),
                error.message()
            )));
            false
        }
    }
}

fn decode(receipt: &mut ReceiptFile) -> Result<ResultBundle, IntakeDefect> {
    let path = receipt.path().to_path_buf();
    let source = receipt.read()?;
    let value: serde_json::Value = serde_json::from_slice(&source)
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

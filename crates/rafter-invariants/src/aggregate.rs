use std::{collections::BTreeMap, fmt, fs, path::PathBuf};

use crate::{
    catalog::{Catalog, ProfileManifest},
    receipt::collect_results,
    types::{
        EvidenceResult, EvidenceStatus, FailureClassification, InvariantVerdict, ResultBundle,
        VerdictIssue, VerdictReport, VerdictStatus, VerdictSummary, RESULT_SCHEMA_VERSION,
    },
};

#[derive(Debug)]
/// Error loading or configuring deterministic evidence aggregation.
pub struct AggregateError(String);

impl AggregateError {
    pub(super) const fn new(message: String) -> Self {
        Self(message)
    }
}

impl fmt::Display for AggregateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for AggregateError {}

#[derive(Debug, Default)]
/// Valid bundles plus fail-closed errors discovered while loading evidence.
pub struct LoadedEvidence {
    pub bundles: Vec<ResultBundle>,
    pub harness_errors: Vec<String>,
}

/// Loads strict result bundles from the requested artifact paths.
///
/// # Errors
///
/// Returns an error when a path is unreadable or its JSON does not match the
/// result bundle type.
pub fn load_bundles(paths: &[PathBuf]) -> Result<Vec<ResultBundle>, AggregateError> {
    paths.iter().map(load_bundle).collect()
}

/// Loads every usable bundle while retaining each load failure as a harness error.
#[must_use]
pub fn load_evidence(paths: &[PathBuf]) -> LoadedEvidence {
    let mut loaded = LoadedEvidence::default();
    for path in paths {
        match load_bundle(path) {
            Ok(bundle) => loaded.bundles.push(bundle),
            Err(error) => loaded.harness_errors.push(error.to_string()),
        }
    }
    loaded
}

fn load_bundle(path: &PathBuf) -> Result<ResultBundle, AggregateError> {
    let source = fs::read_to_string(path)
        .map_err(|error| AggregateError(format!("read {}: {error}", path.display())))?;
    let bundle: ResultBundle = serde_json::from_str(&source)
        .map_err(|error| AggregateError(format!("parse {}: {error}", path.display())))?;
    crate::producer::source::verify_checkout(&bundle.execution.source).map_err(|error| {
        AggregateError(format!(
            "verify source identity for {}: {error}",
            path.display()
        ))
    })?;
    crate::artifact_verify::verify(&bundle, std::path::Path::new("."))?;
    Ok(bundle)
}

/// Produces one fail-closed verdict for every reviewed invariant.
///
/// # Errors
///
/// Returns an error when the profile manifest is invalid or the selected
/// profile does not exist. Evidence defects are represented as red verdicts.
pub fn aggregate(
    catalog: &Catalog,
    manifest: &ProfileManifest,
    profile: &str,
    source_ref: &str,
    bundles: &[ResultBundle],
) -> Result<VerdictReport, AggregateError> {
    aggregate_with_harness_errors(catalog, manifest, profile, source_ref, bundles, &[])
}

/// Produces all 44 verdicts while incorporating evidence-load failures as red.
///
/// # Errors
///
/// Returns an error when the registry or selected profile contract is invalid.
pub fn aggregate_with_harness_errors(
    catalog: &Catalog,
    manifest: &ProfileManifest,
    profile: &str,
    source_ref: &str,
    bundles: &[ResultBundle],
    load_errors: &[String],
) -> Result<VerdictReport, AggregateError> {
    manifest
        .validate(catalog)
        .map_err(|error| AggregateError(error.to_string()))?;
    let contract = manifest
        .profiles
        .get(profile)
        .ok_or_else(|| AggregateError(format!("unknown profile {profile}")))?;
    let required = catalog.required_evidence(contract);
    let expected = required
        .values()
        .flatten()
        .map(|evidence| (evidence.evidence_id(), evidence))
        .collect::<BTreeMap<_, _>>();

    let (accepted, mut harness_errors, artifacts) =
        collect_results(bundles, &expected, contract, profile, source_ref);
    harness_errors.extend(load_errors.iter().cloned());
    let invariants = catalog
        .ids
        .iter()
        .map(|invariant_id| {
            invariant_verdict(
                invariant_id,
                &required[invariant_id],
                &accepted,
                &harness_errors,
                catalog.canonical_ids.contains(invariant_id),
                contract.canonical_minimum_independent_layers,
            )
        })
        .collect::<Vec<_>>();
    let green = invariants
        .iter()
        .filter(|verdict| verdict.status == VerdictStatus::Green)
        .count();
    Ok(VerdictReport {
        schema_version: RESULT_SCHEMA_VERSION,
        profile: profile.to_owned(),
        source_ref: source_ref.to_owned(),
        summary: VerdictSummary {
            total: invariants.len(),
            green,
            red: invariants.len() - green,
        },
        artifacts,
        invariants,
    })
}

/// Independently validates one complete runner layer against its profile contract.
///
/// # Errors
///
/// Returns an error when the bundle is stale, malformed, incomplete, or contains
/// any non-passing evidence result.
pub fn verify_layer_bundle(
    catalog: &Catalog,
    manifest: &ProfileManifest,
    profile: &str,
    layer: &str,
    bundle: &ResultBundle,
) -> Result<(), AggregateError> {
    manifest
        .validate(catalog)
        .map_err(|error| AggregateError(error.to_string()))?;
    let contract = manifest
        .profiles
        .get(profile)
        .ok_or_else(|| AggregateError(format!("unknown profile {profile}")))?;
    if bundle.runner != layer {
        return Err(AggregateError(format!(
            "runner {} does not match requested layer {layer}",
            bundle.runner
        )));
    }
    let required = catalog.required_evidence(contract);
    let expected = required
        .values()
        .flatten()
        .map(|evidence| (evidence.evidence_id(), evidence))
        .collect::<BTreeMap<_, _>>();
    let expected_layer = expected
        .iter()
        .filter(|(_, descriptor)| descriptor.layer == layer)
        .map(|(evidence_id, _)| evidence_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let (accepted, errors, _) = collect_results(
        std::slice::from_ref(bundle),
        &expected,
        contract,
        profile,
        &bundle.source_ref,
    );
    let accepted_ids = accepted
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    if !errors.is_empty() {
        return Err(AggregateError(errors.join("; ")));
    }
    if accepted_ids != expected_layer
        || accepted
            .values()
            .any(|result| result.status != EvidenceStatus::Pass)
    {
        return Err(AggregateError(format!(
            "{profile}/{layer} evidence is missing, incomplete, or red"
        )));
    }
    Ok(())
}

fn invariant_verdict(
    invariant_id: &str,
    required: &[crate::EvidenceDescriptor],
    accepted: &BTreeMap<String, EvidenceResult>,
    harness_errors: &[String],
    canonical: bool,
    canonical_minimum_layers: usize,
) -> InvariantVerdict {
    let mut issues = Vec::new();
    let mut passed = 0;
    if required.is_empty() {
        issues.push(VerdictIssue {
            evidence_id: format!("{invariant_id}/coverage"),
            status: EvidenceStatus::Incomplete,
            classification: FailureClassification::CoverageNotReached,
            message: "profile has no direct or end-to-end registry evidence".to_owned(),
            artifacts: Vec::new(),
        });
    }
    let independent_layers = required
        .iter()
        .map(|evidence| evidence.layer.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    if canonical && independent_layers < canonical_minimum_layers {
        issues.push(VerdictIssue {
            evidence_id: format!("{invariant_id}/independent-layers"),
            status: EvidenceStatus::Incomplete,
            classification: FailureClassification::CoverageNotReached,
            message: format!(
                "canonical safety requires {canonical_minimum_layers} independent layers, registry has {independent_layers}"
            ),
            artifacts: Vec::new(),
        });
    }
    for evidence in required {
        let evidence_id = evidence.evidence_id();
        match accepted.get(&evidence_id) {
            Some(result) if result.status == EvidenceStatus::Pass => passed += 1,
            Some(result) => issues.push(issue_from_result(result)),
            None => issues.push(VerdictIssue {
                evidence_id,
                status: EvidenceStatus::Error,
                classification: FailureClassification::HarnessError,
                message: "required evidence result is missing".to_owned(),
                artifacts: Vec::new(),
            }),
        }
    }
    issues.extend(harness_errors.iter().map(|message| VerdictIssue {
        evidence_id: "aggregate/harness".to_owned(),
        status: EvidenceStatus::Error,
        classification: FailureClassification::HarnessError,
        message: message.clone(),
        artifacts: Vec::new(),
    }));
    InvariantVerdict {
        invariant_id: invariant_id.to_owned(),
        status: if issues.is_empty() {
            VerdictStatus::Green
        } else {
            VerdictStatus::Red
        },
        required_evidence: required.len(),
        passed_evidence: passed,
        issues,
    }
}

fn issue_from_result(result: &EvidenceResult) -> VerdictIssue {
    VerdictIssue {
        evidence_id: result.evidence_id.clone(),
        status: result.status,
        classification: result
            .classification
            .unwrap_or(FailureClassification::HarnessError),
        message: result
            .message
            .clone()
            .unwrap_or_else(|| "non-pass result omitted its message".to_owned()),
        artifacts: result.artifacts.clone(),
    }
}

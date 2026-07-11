use std::{collections::BTreeMap, fmt, fs, path::PathBuf};

use crate::{
    catalog::{Catalog, ProfileManifest},
    types::{
        EvidenceResult, EvidenceStatus, FailureClassification, InvariantVerdict, ResultBundle,
        VerdictIssue, VerdictReport, VerdictStatus, VerdictSummary, RESULT_SCHEMA_VERSION,
    },
};

#[derive(Debug)]
/// Error loading or configuring deterministic evidence aggregation.
pub struct AggregateError(String);

impl fmt::Display for AggregateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for AggregateError {}

/// Loads strict result bundles from the requested artifact paths.
///
/// # Errors
///
/// Returns an error when a path is unreadable or its JSON does not match the
/// result bundle type.
pub fn load_bundles(paths: &[PathBuf]) -> Result<Vec<ResultBundle>, AggregateError> {
    paths
        .iter()
        .map(|path| {
            let source = fs::read_to_string(path)
                .map_err(|error| AggregateError(format!("read {}: {error}", path.display())))?;
            serde_json::from_str(&source)
                .map_err(|error| AggregateError(format!("parse {}: {error}", path.display())))
        })
        .collect()
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

    let (accepted, harness_errors) = collect_results(bundles, &expected, profile, source_ref);
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
        invariants,
    })
}

fn collect_results(
    bundles: &[ResultBundle],
    expected: &BTreeMap<String, &crate::EvidenceDescriptor>,
    profile: &str,
    source_ref: &str,
) -> (BTreeMap<String, EvidenceResult>, Vec<String>) {
    let mut accepted = BTreeMap::<String, EvidenceResult>::new();
    let mut harness_errors = Vec::new();
    for bundle in bundles {
        if bundle.schema_version != RESULT_SCHEMA_VERSION {
            harness_errors.push(format!(
                "runner {} used unsupported result schema {}",
                bundle.runner, bundle.schema_version
            ));
            continue;
        }
        if bundle.profile != profile {
            harness_errors.push(format!(
                "runner {} reported profile {} instead of {profile}",
                bundle.runner, bundle.profile
            ));
            continue;
        }
        if bundle.source_ref != source_ref {
            harness_errors.push(format!(
                "runner {} evidence is stale: source {} != {source_ref}",
                bundle.runner, bundle.source_ref
            ));
            continue;
        }
        for result in &bundle.results {
            let Some(descriptor) = expected.get(&result.evidence_id) else {
                harness_errors.push(format!(
                    "runner {} reported unknown evidence {}",
                    bundle.runner, result.evidence_id
                ));
                continue;
            };
            if result.invariant_id != descriptor.invariant_id || bundle.runner != descriptor.layer {
                harness_errors.push(format!(
                    "evidence {} identity does not match registry invariant/layer",
                    result.evidence_id
                ));
                continue;
            }
            if let Err(message) = validate_result(result) {
                harness_errors.push(format!("evidence {}: {message}", result.evidence_id));
                continue;
            }
            if accepted
                .insert(result.evidence_id.clone(), result.clone())
                .is_some()
            {
                harness_errors.push(format!(
                    "duplicate result for evidence {}",
                    result.evidence_id
                ));
            }
        }
    }
    (accepted, harness_errors)
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

fn validate_result(result: &EvidenceResult) -> Result<(), &'static str> {
    let expected = match result.status {
        EvidenceStatus::Pass => None,
        EvidenceStatus::Fail => Some(FailureClassification::InvariantViolation),
        EvidenceStatus::Incomplete => Some(FailureClassification::CoverageNotReached),
        EvidenceStatus::Error => Some(FailureClassification::HarnessError),
    };
    if result.classification != expected {
        return Err("status and classification disagree");
    }
    if result.status != EvidenceStatus::Pass
        && result
            .message
            .as_deref()
            .is_none_or(|message| message.trim().is_empty())
    {
        return Err("non-pass result must include a message");
    }
    Ok(())
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

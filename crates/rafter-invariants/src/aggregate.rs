use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use crate::{
    contract::{catalog::Catalog, profile::ProfileManifest},
    evidence::{EvidenceResult, EvidenceStatus, FailureClassification, ResultBundle},
    receipt::collect_results,
    verdict::{
        ClauseVerdict, InvariantVerdict, VerdictIssue, VerdictReport, VerdictStatus, VerdictSummary,
    },
};

pub(crate) use crate::verification::AggregateError;

#[derive(Debug, Default)]
/// Valid bundles plus fail-closed errors discovered while loading evidence.
pub(crate) struct LoadedEvidence {
    pub(crate) bundles: Vec<ResultBundle>,
    pub(crate) harness_errors: Vec<String>,
}

struct InvariantEvidence<'a> {
    clauses: &'a [crate::ClauseDescriptor],
    required: &'a [crate::EvidenceDescriptor],
    accepted: &'a BTreeMap<String, EvidenceResult>,
    harness_errors: &'a [String],
}

/// Loads every usable bundle while retaining each load failure as a harness error.
#[must_use]
pub(crate) fn load_evidence(paths: &[PathBuf]) -> LoadedEvidence {
    load_evidence_at(paths, Path::new("."))
}

#[must_use]
pub(crate) fn load_evidence_at(paths: &[PathBuf], root: &Path) -> LoadedEvidence {
    let mut loaded = LoadedEvidence::default();
    for path in paths {
        match load_bundle(path, root) {
            Ok((bundle, diagnostics)) => {
                loaded.bundles.push(bundle);
                loaded.harness_errors.extend(
                    diagnostics
                        .into_iter()
                        .map(|error| format!("verify {}: {error}", path.display())),
                );
            }
            Err(error) => loaded.harness_errors.push(error.to_string()),
        }
    }
    loaded
}

fn load_bundle(path: &PathBuf, root: &Path) -> Result<(ResultBundle, Vec<String>), AggregateError> {
    let source = fs::read_to_string(path)
        .map_err(|error| AggregateError::new(format!("read {}: {error}", path.display())))?;
    let value: serde_json::Value = serde_json::from_str(&source)
        .map_err(|error| AggregateError::new(format!("parse {}: {error}", path.display())))?;
    crate::evidence::validate_result_value(&value).map_err(|error| {
        AggregateError::new(format!(
            "validate result schema for {}: {error}",
            path.display()
        ))
    })?;
    let bundle: ResultBundle = serde_json::from_value(value)
        .map_err(|error| AggregateError::new(format!("decode {}: {error}", path.display())))?;
    crate::producer::source::verify_checkout_at(&bundle.execution.source, root).map_err(
        |error| {
            AggregateError::new(format!(
                "verify source identity for {}: {error}",
                path.display()
            ))
        },
    )?;
    let diagnostics = crate::artifact_verify::verify(&bundle, root)?;
    Ok((bundle, diagnostics))
}

/// Produces one fail-closed verdict for every reviewed invariant.
///
/// # Errors
///
/// Returns an error when the profile manifest is invalid or the selected
/// profile does not exist. Evidence defects are represented as red verdicts.
#[cfg(test)]
pub(crate) fn aggregate(
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
pub(crate) fn aggregate_with_harness_errors(
    catalog: &Catalog,
    manifest: &ProfileManifest,
    profile: &str,
    source_ref: &str,
    bundles: &[ResultBundle],
    load_errors: &[String],
) -> Result<VerdictReport, AggregateError> {
    manifest
        .validate(catalog)
        .map_err(|error| AggregateError::new(error.to_string()))?;
    let contract = manifest
        .profiles
        .get(profile)
        .ok_or_else(|| AggregateError::new(format!("unknown profile {profile}")))?;
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
                &InvariantEvidence {
                    clauses: &catalog.clauses_for(invariant_id),
                    required: &required[invariant_id],
                    accepted: &accepted,
                    harness_errors: &harness_errors,
                },
                catalog.canonical_ids.contains(invariant_id),
                contract.canonical_minimum_independent_layers,
                &contract.required_clause_strength,
            )
        })
        .collect::<Vec<_>>();
    let green = invariants
        .iter()
        .filter(|verdict| verdict.status == VerdictStatus::Green)
        .count();
    Ok(VerdictReport {
        schema_version: crate::verdict::VERDICT_SCHEMA_VERSION,
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
pub(crate) fn verify_layer_bundle(
    catalog: &Catalog,
    manifest: &ProfileManifest,
    profile: &str,
    layer: &str,
    bundle: &ResultBundle,
) -> Result<(), AggregateError> {
    manifest
        .validate(catalog)
        .map_err(|error| AggregateError::new(error.to_string()))?;
    let contract = manifest
        .profiles
        .get(profile)
        .ok_or_else(|| AggregateError::new(format!("unknown profile {profile}")))?;
    if bundle.runner != layer {
        return Err(AggregateError::new(format!(
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
        return Err(AggregateError::new(errors.join("; ")));
    }
    if accepted_ids != expected_layer
        || accepted
            .values()
            .any(|result| result.status != EvidenceStatus::Pass)
    {
        return Err(AggregateError::new(format!(
            "{profile}/{layer} evidence is missing, incomplete, or red"
        )));
    }
    Ok(())
}

fn invariant_verdict(
    invariant_id: &str,
    evidence: &InvariantEvidence<'_>,
    canonical: bool,
    canonical_minimum_layers: usize,
    required_clause_strength: &str,
) -> InvariantVerdict {
    let mut issues = Vec::new();
    let clause_verdicts = evidence
        .clauses
        .iter()
        .map(|clause| {
            let clause_evidence = evidence
                .required
                .iter()
                .filter(|evidence| evidence.clause_id == clause.clause_id)
                .collect::<Vec<_>>();
            clause_verdict(
                clause,
                &clause_evidence,
                evidence.accepted,
                required_clause_strength,
            )
        })
        .collect::<Vec<_>>();
    let required_clauses = evidence
        .clauses
        .iter()
        .filter(|clause| clause.required)
        .count();
    let passed_clauses = evidence
        .clauses
        .iter()
        .zip(&clause_verdicts)
        .filter(|(clause, verdict)| clause.required && verdict.status == VerdictStatus::Green)
        .count();
    issues.extend(
        evidence
            .clauses
            .iter()
            .zip(&clause_verdicts)
            .filter(|(clause, _)| clause.required)
            .flat_map(|(_, verdict)| verdict.issues.iter().cloned()),
    );
    let independent_layers = evidence
        .required
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
    let passed = evidence
        .required
        .iter()
        .filter(|descriptor| {
            evidence
                .accepted
                .get(&descriptor.evidence_id())
                .is_some_and(|result| result.status == EvidenceStatus::Pass)
        })
        .count();
    issues.extend(evidence.harness_errors.iter().map(|message| VerdictIssue {
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
        required_clauses,
        passed_clauses,
        required_evidence: evidence.required.len(),
        passed_evidence: passed,
        clauses: clause_verdicts,
        issues,
    }
}

fn clause_verdict(
    clause: &crate::ClauseDescriptor,
    required: &[&crate::EvidenceDescriptor],
    accepted: &BTreeMap<String, EvidenceResult>,
    required_strength: &str,
) -> ClauseVerdict {
    let mut issues = Vec::new();
    if required.is_empty() {
        issues.push(VerdictIssue {
            evidence_id: format!("{}/coverage", clause.clause_id),
            status: EvidenceStatus::Incomplete,
            classification: FailureClassification::CoverageNotReached,
            message: format!(
                "required clause {} has no profile-selected executable evidence",
                clause.clause_id
            ),
            artifacts: Vec::new(),
        });
    } else if !required
        .iter()
        .any(|evidence| evidence.strength == required_strength)
    {
        issues.push(VerdictIssue {
            evidence_id: format!("{}/direct-evidence", clause.clause_id),
            status: EvidenceStatus::Incomplete,
            classification: FailureClassification::CoverageNotReached,
            message: format!(
                "required clause {} has no {required_strength} evidence",
                clause.clause_id
            ),
            artifacts: Vec::new(),
        });
    }
    let mut passed = 0;
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
    ClauseVerdict {
        clause_id: clause.clause_id.clone(),
        statement: clause.statement.clone(),
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

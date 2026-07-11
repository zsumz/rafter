use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::{Component, Path, PathBuf},
};

use sha2::{Digest, Sha256};

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
            let bundle = serde_json::from_str(&source)
                .map_err(|error| AggregateError(format!("parse {}: {error}", path.display())))?;
            verify_bundle_artifacts(&bundle, Path::new("."))?;
            Ok(bundle)
        })
        .collect()
}

fn verify_bundle_artifacts(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    let mut artifacts = bundle.execution.artifacts.iter().collect::<BTreeSet<_>>();
    artifacts.extend(
        bundle
            .execution
            .checks
            .iter()
            .flat_map(|check| check.artifacts.iter()),
    );
    artifacts.extend(
        bundle
            .results
            .iter()
            .flat_map(|result| result.artifacts.iter()),
    );
    for artifact in artifacts {
        let path = Path::new(&artifact.path);
        if path.is_absolute()
            || path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(AggregateError(format!(
                "artifact path must be repository-relative: {}",
                artifact.path
            )));
        }
        let bytes = fs::read(root.join(path))
            .map_err(|error| AggregateError(format!("read artifact {}: {error}", artifact.path)))?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        if artifact.size_bytes != bytes.len() as u64 || artifact.sha256 != digest {
            return Err(AggregateError(format!(
                "artifact integrity mismatch: {}",
                artifact.path
            )));
        }
    }
    if bundle.runner == "tests" {
        verify_test_logs(bundle, root)?;
    }
    Ok(())
}

fn verify_test_logs(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    for check in &bundle.execution.checks {
        let passing = bundle.results.iter().any(|result| {
            result.execution_id == check.execution_id && result.status == EvidenceStatus::Pass
        });
        if !passing {
            continue;
        }
        let test_name = check
            .check_id
            .rsplit_once('#')
            .map(|(_, test_name)| test_name)
            .ok_or_else(|| AggregateError(format!("invalid tests check ID {}", check.check_id)))?;
        let log = check
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "test-log")
            .ok_or_else(|| AggregateError(format!("test log missing for {}", check.check_id)))?;
        let source = fs::read_to_string(root.join(&log.path)).map_err(|error| {
            AggregateError(format!("read exact test log {}: {error}", log.path))
        })?;
        if !source.lines().any(|line| line.trim() == "running 1 test")
            || !source
                .lines()
                .any(|line| line.trim() == format!("test {test_name} ... ok"))
            || !source
                .lines()
                .any(|line| line.contains("1 passed; 0 failed; 0 ignored"))
        {
            return Err(AggregateError(format!(
                "test log does not prove one exact pass for {}",
                check.check_id
            )));
        }
    }
    Ok(())
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

    let (accepted, harness_errors, artifacts) =
        collect_results(bundles, &expected, contract, profile, source_ref);
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

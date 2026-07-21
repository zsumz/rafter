//! Pure fail-closed reduction of typed evidence intake into invariant verdicts.

use std::collections::BTreeMap;

use crate::{
    contract::{
        catalog::{Catalog, ClauseDescriptor, EvidenceDescriptor},
        profile::ProfileManifest,
    },
    evidence::{EvidenceResult, EvidenceStatus, FailureClassification},
    verdict::{
        ClauseVerdict, InvariantVerdict, VerdictIssue, VerdictReport, VerdictStatus, VerdictSummary,
    },
    verification::{EvidenceIntake, IntakeDefect},
};

use crate::verification::AggregateError;

struct InvariantEvidence<'a> {
    clauses: &'a [ClauseDescriptor],
    required: &'a [EvidenceDescriptor],
    accepted: &'a BTreeMap<String, EvidenceResult>,
    defects: &'a [IntakeDefect],
}

/// Produces one fail-closed verdict for every reviewed invariant.
///
/// # Errors
///
/// Returns an error when the profile manifest is invalid or the selected
/// profile does not exist. Evidence defects are represented as red verdicts.
pub(crate) fn reduce(
    catalog: &Catalog,
    manifest: &ProfileManifest,
    intake: &EvidenceIntake,
) -> Result<VerdictReport, AggregateError> {
    let profile = intake.profile();
    let source_ref = intake.source_ref();
    manifest
        .validate(catalog)
        .map_err(|error| AggregateError::new(error.to_string()))?;
    let contract = manifest
        .profiles
        .get(profile)
        .ok_or_else(|| AggregateError::new(format!("unknown profile {profile}")))?;
    let required = catalog.required_evidence(contract);
    let invariants = catalog
        .ids
        .iter()
        .map(|invariant_id| {
            invariant_verdict(
                invariant_id,
                &InvariantEvidence {
                    clauses: &catalog.clauses_for(invariant_id),
                    required: &required[invariant_id],
                    accepted: intake.accepted(),
                    defects: intake.defects(),
                },
                catalog.canonical_ids.contains(invariant_id),
                contract.canonical_minimum_independent_layers,
                contract.required_clause_strength.as_str(),
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
        artifacts: intake.artifacts().to_vec(),
        invariants,
    })
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
    issues.extend(evidence.defects.iter().map(|defect| VerdictIssue {
        evidence_id: "aggregate/harness".to_owned(),
        status: EvidenceStatus::Error,
        classification: FailureClassification::HarnessError,
        message: defect.message().to_owned(),
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
    clause: &ClauseDescriptor,
    required: &[&EvidenceDescriptor],
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

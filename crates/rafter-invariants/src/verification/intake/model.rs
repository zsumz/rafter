//! Intake candidates, accepted evidence, and exhaustive structural defect classes.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use crate::{
    contract::{catalog::Catalog, profile::ProfileManifest},
    evidence::{
        ArtifactRef, EvidenceResult, EvidenceStatus, ExecutionPlanReceipt, FailureClassification,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IntakeDefectKind {
    Missing,
    Malformed,
    Stale,
    Unverifiable,
}

impl IntakeDefectKind {
    #[cfg(test)]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Malformed => "malformed",
            Self::Stale => "stale",
            Self::Unverifiable => "unverifiable",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IntakeDefect {
    kind: IntakeDefectKind,
    message: String,
}

impl IntakeDefect {
    pub(crate) fn missing(message: impl Into<String>) -> Self {
        Self::new(IntakeDefectKind::Missing, message)
    }

    pub(crate) fn malformed(message: impl Into<String>) -> Self {
        Self::new(IntakeDefectKind::Malformed, message)
    }

    pub(crate) fn stale(message: impl Into<String>) -> Self {
        Self::new(IntakeDefectKind::Stale, message)
    }

    pub(crate) fn unverifiable(message: impl Into<String>) -> Self {
        Self::new(IntakeDefectKind::Unverifiable, message)
    }

    fn new(kind: IntakeDefectKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    #[cfg(test)]
    pub(crate) const fn kind(&self) -> IntakeDefectKind {
        self.kind
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Copy)]
pub(crate) struct VerificationRequest<'a> {
    pub(crate) catalog: &'a Catalog,
    pub(crate) manifest: &'a ProfileManifest,
    pub(crate) active_plan: &'a ExecutionPlanReceipt,
    pub(crate) source_ref: &'a str,
    pub(crate) root: &'a Path,
    pub(crate) context: VerificationContext,
}

/// Which flow is running the verifier.
///
/// This is set by the invoking gate command and travels only in the request.
/// It is deliberately not serialized state: a receipt or an artifact must not
/// be able to declare itself aggregate-context and so excuse itself from a
/// check that the producing job could have made.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VerificationContext {
    /// The job that produced the evidence, whose working tree still holds
    /// everything the run built and read.
    ProducingJob,
    /// A later job re-verifying published evidence from a fresh checkout,
    /// which has the repository but none of the producing job's build outputs.
    Aggregate,
}

impl<'a> VerificationRequest<'a> {
    pub(crate) const fn new(
        catalog: &'a Catalog,
        manifest: &'a ProfileManifest,
        active_plan: &'a ExecutionPlanReceipt,
        source_ref: &'a str,
        root: &'a Path,
        context: VerificationContext,
    ) -> Self {
        Self {
            catalog,
            manifest,
            active_plan,
            source_ref,
            root,
            context,
        }
    }
}

#[derive(Debug)]
pub(crate) struct EvidenceIntake {
    profile: String,
    source_ref: String,
    accepted: BTreeMap<String, EvidenceResult>,
    artifacts: Vec<ArtifactRef>,
    defects: Vec<IntakeDefect>,
    artifact_guards: Vec<crate::verification::AuthenticatedArtifacts>,
    detector_replay_guard: Option<crate::verification::detector_replay::ReplayArtifactGuard>,
}

impl EvidenceIntake {
    pub(super) fn new(
        profile: impl Into<String>,
        source_ref: impl Into<String>,
        accepted: BTreeMap<String, EvidenceResult>,
        artifacts: Vec<ArtifactRef>,
        defects: Vec<IntakeDefect>,
    ) -> Self {
        Self {
            profile: profile.into(),
            source_ref: source_ref.into(),
            accepted,
            artifacts,
            defects,
            artifact_guards: Vec::new(),
            detector_replay_guard: None,
        }
    }

    pub(crate) fn profile(&self) -> &str {
        &self.profile
    }

    pub(crate) fn source_ref(&self) -> &str {
        &self.source_ref
    }

    pub(crate) fn accepted(&self) -> &BTreeMap<String, EvidenceResult> {
        &self.accepted
    }

    pub(crate) fn artifacts(&self) -> &[ArtifactRef] {
        &self.artifacts
    }

    pub(crate) fn defects(&self) -> &[IntakeDefect] {
        &self.defects
    }

    pub(crate) fn defect_messages(&self) -> Vec<String> {
        self.defects
            .iter()
            .map(|defect| defect.message.clone())
            .collect()
    }

    pub(super) fn extend_defects(&mut self, defects: impl IntoIterator<Item = IntakeDefect>) {
        self.defects.extend(defects);
    }

    pub(super) fn attach_artifact_guards(
        &mut self,
        guards: Vec<crate::verification::AuthenticatedArtifacts>,
    ) {
        self.artifact_guards = guards;
    }

    pub(crate) fn revalidate_artifacts(&mut self) {
        let producer_error = self
            .artifact_guards
            .iter()
            .find_map(|guard| guard.revalidate_paths().err());
        if let Some(error) = producer_error {
            self.defects.push(IntakeDefect::unverifiable(format!(
                "producer artifact integrity changed before verdict publication: {error}"
            )));
            self.artifact_guards.clear();
        }
        let replay_error = self
            .detector_replay_guard
            .as_ref()
            .and_then(|guard| guard.revalidate().err());
        if let Some(error) = replay_error {
            self.defects.push(IntakeDefect::unverifiable(format!(
                "detector replay artifact integrity changed before verdict publication: {error}"
            )));
            self.detector_replay_guard = None;
        }
    }

    pub(in crate::verification) fn apply_detector_replay(
        &mut self,
        assessment: crate::verification::detector_replay::DetectorReplayAssessment,
    ) -> Result<(), String> {
        let crate::verification::detector_replay::DetectorReplayAssessment {
            qualifications,
            artifacts,
            artifact_guard,
        } = assessment;
        if artifact_guard.is_none()
            && qualifications
                .values()
                .any(crate::verification::detector_replay::EvidenceReplayQualification::is_passed)
        {
            return Err(
                "passing detector replay qualifications require a complete artifact guard"
                    .to_owned(),
            );
        }
        let mut published = self.artifacts.iter().cloned().collect::<BTreeSet<_>>();
        published.extend(artifacts.iter().cloned());
        let fallback = published.iter().next().cloned();
        for (evidence_id, qualification) in &qualifications {
            let Some(result) = self.accepted.get(evidence_id) else {
                continue;
            };
            if result.invariant_id != qualification.invariant_id() {
                return Err(format!(
                    "detector replay identity mismatch for evidence {evidence_id}"
                ));
            }
        }
        for (evidence_id, qualification) in qualifications {
            let Some(result) = self.accepted.get_mut(&evidence_id) else {
                continue;
            };
            let mut artifacts = result.artifacts.iter().cloned().collect::<BTreeSet<_>>();
            artifacts.extend(qualification.artifacts().iter().cloned());
            if !qualification.is_passed() && result.status != EvidenceStatus::Fail {
                result.status = EvidenceStatus::Error;
                result.classification = Some(FailureClassification::HarnessError);
                result.message = Some(format!(
                    "aggregate detector replay: {}",
                    qualification
                        .message()
                        .unwrap_or("fixture did not produce a passing qualification")
                ));
                if artifacts.is_empty() {
                    artifacts.extend(fallback.iter().cloned());
                }
            }
            result.artifacts = artifacts.into_iter().collect();
        }
        self.artifacts = published.into_iter().collect();
        self.detector_replay_guard = artifact_guard;
        Ok(())
    }
}

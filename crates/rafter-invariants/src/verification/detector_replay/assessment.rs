//! Evidence-local qualifications produced by verifier-owned replay.

use std::collections::{BTreeMap, BTreeSet};

use crate::evidence::ArtifactRef;

use super::{artifact::ReplayArtifactGuard, model::ReplayEvidence};

pub(in crate::verification) enum EvidenceReplayQualification {
    Passed {
        invariant_id: String,
        artifacts: Vec<ArtifactRef>,
    },
    Failed {
        invariant_id: String,
        message: String,
        artifacts: Vec<ArtifactRef>,
    },
}

impl EvidenceReplayQualification {
    pub(in crate::verification) fn passed(
        invariant_id: String,
        artifacts: Vec<ArtifactRef>,
    ) -> Self {
        Self::Passed {
            invariant_id,
            artifacts,
        }
    }

    pub(in crate::verification) fn failed(
        invariant_id: String,
        message: impl Into<String>,
        artifacts: Vec<ArtifactRef>,
    ) -> Self {
        Self::Failed {
            invariant_id,
            message: message.into(),
            artifacts,
        }
    }

    pub(in crate::verification) fn invariant_id(&self) -> &str {
        match self {
            Self::Passed { invariant_id, .. } | Self::Failed { invariant_id, .. } => invariant_id,
        }
    }

    pub(in crate::verification) fn is_passed(&self) -> bool {
        matches!(self, Self::Passed { .. })
    }

    pub(in crate::verification) fn message(&self) -> Option<&str> {
        match self {
            Self::Passed { .. } => None,
            Self::Failed { message, .. } => Some(message),
        }
    }

    pub(in crate::verification) fn artifacts(&self) -> &[ArtifactRef] {
        match self {
            Self::Passed { artifacts, .. } | Self::Failed { artifacts, .. } => artifacts,
        }
    }

    pub(super) fn attach_artifact(&mut self, artifact: ArtifactRef) {
        match self {
            Self::Passed { artifacts, .. } | Self::Failed { artifacts, .. } => {
                artifacts.push(artifact);
            }
        }
    }
}

pub(in crate::verification) struct DetectorReplayAssessment {
    pub(in crate::verification) qualifications: BTreeMap<String, EvidenceReplayQualification>,
    pub(in crate::verification) artifacts: Vec<ArtifactRef>,
    pub(in crate::verification) artifact_guard: Option<ReplayArtifactGuard>,
}

impl DetectorReplayAssessment {
    pub(super) fn new(
        qualifications: BTreeMap<String, EvidenceReplayQualification>,
        artifacts: Vec<ArtifactRef>,
    ) -> Result<Self, String> {
        let evidence_artifacts = qualifications
            .values()
            .flat_map(|qualification| qualification.artifacts().iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        let execution_artifacts = artifacts.into_iter().collect::<BTreeSet<_>>();
        if evidence_artifacts
            .difference(&execution_artifacts)
            .next()
            .is_some()
        {
            return Err("detector replay evidence references an unpublished artifact".to_owned());
        }
        Ok(Self {
            qualifications,
            artifacts: execution_artifacts.into_iter().collect(),
            artifact_guard: None,
        })
    }

    pub(super) fn with_artifact_guard(
        mut self,
        guard: ReplayArtifactGuard,
    ) -> Result<Self, String> {
        let guarded = guard.references();
        let published = self.artifacts.iter().cloned().collect::<BTreeSet<_>>();
        if guarded != published {
            return Err(
                "detector replay artifact guard does not cover the published inventory".to_owned(),
            );
        }
        self.artifact_guard = Some(guard);
        Ok(self)
    }

    pub(super) fn harness_error(
        inventory: impl IntoIterator<Item = ReplayEvidence>,
        message: &str,
        artifacts: Vec<ArtifactRef>,
    ) -> Result<Self, String> {
        let mut qualifications = BTreeMap::new();
        for evidence in inventory {
            let evidence_id = evidence.evidence_id;
            let previous = qualifications.insert(
                evidence_id.clone(),
                EvidenceReplayQualification::failed(
                    evidence.invariant_id,
                    message,
                    artifacts.clone(),
                ),
            );
            if previous.is_some() {
                return Err(format!(
                    "detector replay failure inventory contains duplicate evidence {evidence_id}"
                ));
            }
        }
        Self::new(qualifications, artifacts)
    }

    pub(in crate::verification) fn fail_closed(
        self,
        inventory: impl IntoIterator<Item = ReplayEvidence>,
        message: &str,
    ) -> Result<Self, String> {
        let Self {
            artifacts,
            artifact_guard,
            ..
        } = self;
        let mut failure = Self::harness_error(inventory, message, artifacts)?;
        failure.artifact_guard = artifact_guard;
        Ok(failure)
    }
}

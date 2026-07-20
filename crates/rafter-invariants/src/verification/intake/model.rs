//! Intake candidates, accepted evidence, and exhaustive structural defect classes.

use std::{collections::BTreeMap, path::Path};

use crate::{
    contract::{catalog::Catalog, profile::ProfileManifest},
    evidence::{ArtifactRef, EvidenceResult, ExecutionPlanReceipt},
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
}

impl<'a> VerificationRequest<'a> {
    pub(crate) const fn new(
        catalog: &'a Catalog,
        manifest: &'a ProfileManifest,
        active_plan: &'a ExecutionPlanReceipt,
        source_ref: &'a str,
        root: &'a Path,
    ) -> Self {
        Self {
            catalog,
            manifest,
            active_plan,
            source_ref,
            root,
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
}

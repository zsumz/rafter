//! Closed profile-policy vocabulary with stable serialized identities.

use serde::{Deserialize, Serialize};

/// Registry-evidence selection policy for one profile.
///
/// This enum is exhaustive because the profile schema defines a closed policy
/// vocabulary; adding a variant requires an explicit schema migration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum EvidencePolicy {
    /// Select every registry record matching the profile's layer and strength sets.
    #[serde(rename = "all_matching_registry_evidence")]
    AllMatchingRegistryEvidence,
}

impl std::fmt::Display for EvidencePolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::AllMatchingRegistryEvidence => "all_matching_registry_evidence",
        })
    }
}

/// Normative-clause selection policy for one profile.
///
/// This enum is exhaustive because the profile schema defines a closed policy
/// vocabulary; adding a variant requires an explicit schema migration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum ClausePolicy {
    /// Require evidence for every normative clause marked required.
    #[serde(rename = "all_required_clauses")]
    AllRequiredClauses,
}

/// Minimum evidence strength accepted for a required normative clause.
///
/// This enum is exhaustive because the profile schema defines a closed policy
/// vocabulary; adding a variant requires an explicit schema migration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum RequiredClauseStrength {
    /// Require a direct executable oracle.
    #[serde(rename = "direct")]
    Direct,
}

impl RequiredClauseStrength {
    /// Returns the stable registry wire identity.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
        }
    }
}

/// Evidence-producing verification layer.
///
/// This enum is exhaustive because the profile schema defines the complete set
/// of evidence layers; adding a variant requires an explicit schema migration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceLayer {
    /// Ordinary deterministic Rust tests.
    Tests,
    /// Deterministic simulator checks.
    Simulator,
    /// TLA+ model checking.
    Tla,
    /// Maelstrom end-to-end workloads.
    Maelstrom,
}

impl EvidenceLayer {
    /// Returns the stable runner and registry wire identity.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tests => "tests",
            Self::Simulator => "simulator",
            Self::Tla => "tla",
            Self::Maelstrom => "maelstrom",
        }
    }
}

impl std::fmt::Display for EvidenceLayer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Executable-evidence strength selected by a profile.
///
/// This enum is exhaustive because the profile schema defines the complete set
/// of evidence strengths; adding a variant requires an explicit schema migration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStrength {
    /// Direct property oracle.
    Direct,
    /// Client-visible end-to-end evidence.
    E2e,
}

impl EvidenceStrength {
    /// Returns the stable registry wire identity.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::E2e => "e2e",
        }
    }
}

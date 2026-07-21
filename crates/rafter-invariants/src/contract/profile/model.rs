//! Serialized profile and runner policy vocabulary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{
    policy::{
        ClausePolicy, EvidenceLayer, EvidencePolicy, EvidenceStrength, RequiredClauseStrength,
    },
    replay::DetectorReplayContract,
};

pub(super) const PROFILE_SCHEMA_VERSION: u32 = 9;

/// Explicit invariant IDs and evidence policies for every scheduled profile.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileManifest {
    pub schema_version: u32,
    pub reviewed_ids: Vec<String>,
    pub profiles: BTreeMap<String, ProfileContract>,
    pub verifiers: BTreeMap<String, VerifierContract>,
}

/// Aggregate-verifier policy selected independently from runner contracts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VerifierContract {
    pub detector_replay: DetectorReplayContract,
}

/// Evidence-selection and independent-layer policy for one profile.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileContract {
    pub description: String,
    pub evidence_policy: EvidencePolicy,
    pub clause_policy: ClausePolicy,
    pub required_clause_strength: RequiredClauseStrength,
    pub required_layers: Vec<EvidenceLayer>,
    pub required_strengths: Vec<EvidenceStrength>,
    pub canonical_minimum_independent_layers: usize,
    pub runners: BTreeMap<String, RunnerContract>,
}

/// Required deterministic producer identity and bounds for one layer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerContract {
    pub producer: String,
    /// Human-facing reproduction hint; actual argv is recorded separately.
    pub command: Vec<String>,
    pub configuration: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub simulator_checks: BTreeMap<String, SimulatorCheckContract>,
    pub minimum_observed_checks: usize,
    pub require_peak_rss: bool,
}

/// Profile-owned exploration and semantic floors for one simulator check.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SimulatorCheckContract {
    pub minimum_protocol_states: u64,
    pub minimum_verifier_states: u64,
    pub required_observations: Vec<String>,
}

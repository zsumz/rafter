//! Serialized profile and runner policy vocabulary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub(super) const PROFILE_SCHEMA_VERSION: u32 = 5;

/// Explicit invariant IDs and evidence policies for every scheduled profile.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileManifest {
    pub schema_version: u32,
    pub reviewed_ids: Vec<String>,
    pub profiles: BTreeMap<String, ProfileContract>,
}

/// Evidence-selection and independent-layer policy for one profile.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileContract {
    pub description: String,
    pub evidence_policy: String,
    pub clause_policy: String,
    pub required_clause_strength: String,
    pub required_layers: Vec<String>,
    pub required_strengths: Vec<String>,
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

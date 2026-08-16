//! Serialized profile and runner policy vocabulary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{
    policy::{
        ClausePolicy, EvidenceLayer, EvidencePolicy, EvidenceStrength, RequiredClauseStrength,
    },
    replay::DetectorReplayContract,
};

pub(super) const PROFILE_SCHEMA_VERSION: u32 = 10;

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
    /// Focused proof obligations this layer must discharge before its primary
    /// configuration runs. Empty is the identity: a runner with no obligations
    /// behaves exactly as it did before the vocabulary existed, which is why
    /// the field is `default` and skipped when empty rather than required.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub obligations: Vec<ProofObligationContract>,
    pub minimum_observed_checks: usize,
    pub require_peak_rss: bool,
}

/// One focused model-checking obligation owned by the profile.
///
/// An obligation is a small, self-contained model that states a specific
/// theorem the primary configuration cannot reach in its own budget: it must
/// drain its queue inside its own `soft_timeout`, in one run, from scratch.
///
/// # Checkpoint-free by construction
///
/// Obligations deliberately carry no checkpoint vocabulary. They never write a
/// checkpoint namespace, never recover one, and never participate in a cache
/// key. The reasoning is definitional rather than economical: an obligation
/// that cannot exhaust its frontier in a single bounded run is not an
/// obligation, it is a second monolith, and it belongs in the primary
/// configuration's continuation instead. Keeping obligations outside the
/// serialized `configuration` map is what makes that stick -- the primary
/// checkpoint contract digests only that map, so adding, retuning, or removing
/// an obligation cannot invalidate accumulated primary TLC state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProofObligationContract {
    /// Kebab-case identity; names the log artifact, process label, and
    /// observation keys this obligation contributes to the layer receipt.
    pub id: String,
    /// TLC configuration file name resolved under `specs/tla/raft/`.
    pub config: String,
    pub completion: ObligationCompletion,
    /// Per-obligation ratchets. These are calibrated against the obligation's
    /// own measured state space and are intentionally unrelated to the primary
    /// configuration's monolith floors.
    pub minimum_generated_states: u64,
    pub minimum_distinct_states: u64,
    /// Whole-minute wall budget for this obligation alone.
    pub soft_timeout: String,
    pub seed: String,
}

/// Terminal condition an obligation must reach to be accepted.
///
/// Only frontier exhaustion is legal. A timed-out or coverage-short obligation
/// proves nothing about the states it never enumerated, so there is no weaker
/// variant to select. This enum is deliberately exhaustive so an unreviewed
/// completion fails during decoding rather than being silently accepted.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ObligationCompletion {
    #[serde(rename = "frontier-exhausted")]
    FrontierExhausted,
}

/// Profile-owned exploration and semantic floors for one simulator check.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SimulatorCheckContract {
    pub minimum_protocol_states: u64,
    pub minimum_verifier_states: u64,
    pub required_observations: Vec<String>,
}

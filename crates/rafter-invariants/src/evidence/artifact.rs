//! Immutable artifact references crossing producer and verifier boundaries.

use serde::{Deserialize, Serialize};

/// Replayable log, trace, counterexample, or related evidence artifact.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub kind: String,
    pub path: String,
    pub sha256: String,
    pub size_bytes: u64,
}

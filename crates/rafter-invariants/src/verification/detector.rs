//! Public detector-fixture source binding backed by independent verification.

use std::collections::BTreeMap;

use crate::contract::TestIdentity;

mod source;
mod transcript;

pub(crate) use transcript::{qualify_detector_execution, verify_detector_transcript};

/// Verifier-owned semantic contract derived from one exact fixture source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DetectorFixtureContract {
    pub(crate) registered_identity: String,
    pub(crate) witnesses: BTreeMap<String, usize>,
    pub(crate) source_graph_sha256: String,
}

impl DetectorFixtureContract {
    pub(crate) fn registered_identity(&self) -> &str {
        &self.registered_identity
    }

    pub(crate) fn witnesses(&self) -> &BTreeMap<String, usize> {
        &self.witnesses
    }

    #[cfg(test)]
    pub(crate) fn source_graph_sha256(&self) -> &str {
        &self.source_graph_sha256
    }
}

/// Content-sensitive analyzer reused across a verifier-owned fixture batch.
#[doc(hidden)]
#[derive(Default)]
pub struct DetectorFixtureAnalysis {
    cache: source::DetectorSourceCache,
}

/// Compatibility name for the detector-fixture source analyzer.
#[doc(hidden)]
pub type DetectorFixtureSourceBatch = DetectorFixtureAnalysis;

impl std::fmt::Debug for DetectorFixtureAnalysis {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DetectorFixtureAnalysis")
            .finish_non_exhaustive()
    }
}

impl DetectorFixtureAnalysis {
    pub(crate) fn analyze(
        &mut self,
        binding: &DetectorFixtureSourceBinding<'_>,
    ) -> Result<DetectorFixtureContract, String> {
        source::verify_invocation_bound_detector_cached(binding, &mut self.cache).map(|contract| {
            DetectorFixtureContract {
                registered_identity: contract.registered_identity().to_owned(),
                witnesses: contract.witnesses().clone(),
                source_graph_sha256: contract.source_graph_sha256().to_owned(),
            }
        })
    }

    /// Verifies a source binding while retaining immutable target analysis.
    ///
    /// # Errors
    ///
    /// Returns an error unless the exact fixture reaches its registered
    /// rejecting detector under the reviewed source policy.
    pub fn validate(&mut self, binding: &DetectorFixtureSourceBinding<'_>) -> Result<(), String> {
        self.analyze(binding).map(|_| ())
    }

    #[cfg(test)]
    pub(crate) fn target_analysis_count(&self) -> usize {
        self.cache.target_analysis_count()
    }

    #[cfg(test)]
    pub(crate) fn source_parse_count(&self) -> usize {
        self.cache.source_parse_count()
    }
}

/// Exact fixture and detector sources bound to one detector test identity.
#[doc(hidden)]
#[derive(Debug)]
pub struct DetectorFixtureSourceBinding<'a> {
    pub fixture_source: &'a str,
    pub detector_source: &'a str,
    pub source_root: &'a std::path::Path,
    pub fixture_path: &'a std::path::Path,
    pub detector_path: &'a std::path::Path,
    pub test_identity: &'a TestIdentity,
    pub fixture: &'a str,
    pub detector: &'a str,
}

/// Verifies the exact source-level path from a negative fixture to its detector.
///
/// # Errors
///
/// Returns an error unless the reviewed detector invocation and witness are
/// unconditionally reachable from the exact compiled fixture identity.
#[doc(hidden)]
pub fn validate_detector_fixture_sources(
    binding: &DetectorFixtureSourceBinding<'_>,
) -> Result<(), String> {
    DetectorFixtureAnalysis::default().validate(binding)
}

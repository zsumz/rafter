//! Public detector-fixture source binding backed by independent verification.

use crate::contract::TestIdentity;

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
    crate::artifact_verify::validate_detector_fixture_sources(binding)
}

use std::path::Path;

use crate::{verification::AggregateError, ResultBundle};

mod compile;
mod detector_source;
mod resource_metrics;
mod simulator;
mod simulator_schedule;
mod test_logs;

#[cfg(test)]
use crate::verification::verify_producer_invocation_paths;
use compile::verify_compile_invocations;
#[cfg(test)]
use resource_metrics::verify_resource_metrics;
use resource_metrics::verify_resource_metrics_authenticated;
use simulator::verify_simulator_logs;
#[cfg(test)]
use simulator::{verify_liveness_observations, verify_simulator_observations};
#[cfg(test)]
use simulator_schedule::validate_simulator_schedule;
use test_logs::verify_test_logs;

const EVENT_PREFIX: &str = "RAFTER_EVENT ";

/// Reuses content-sensitive target analysis across a detector-fixture batch.
#[doc(hidden)]
#[derive(Default)]
pub struct DetectorFixtureSourceBatch {
    cache: detector_source::DetectorSourceCache,
}

impl std::fmt::Debug for DetectorFixtureSourceBatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DetectorFixtureSourceBatch")
            .finish_non_exhaustive()
    }
}

impl DetectorFixtureSourceBatch {
    /// Verifies one binding while retaining immutable target analysis.
    ///
    /// # Errors
    ///
    /// Returns an error unless the exact fixture reaches its registered
    /// rejecting detector under the reviewed source policy.
    pub fn validate(
        &mut self,
        binding: &crate::DetectorFixtureSourceBinding<'_>,
    ) -> Result<(), String> {
        detector_source::verify_invocation_bound_detector_cached(binding, &mut self.cache)
            .map(|_| ())
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

pub(crate) fn validate_detector_fixture_sources(
    binding: &crate::DetectorFixtureSourceBinding<'_>,
) -> Result<(), String> {
    DetectorFixtureSourceBatch::default().validate(binding)
}

pub(super) fn verify(
    bundle: &ResultBundle,
    root: &Path,
    source_root: &Path,
    catalog: &crate::Catalog,
    budget: crate::verification::BundleBudget,
    trusted_runner: &str,
) -> Result<Vec<String>, AggregateError> {
    let artifacts = crate::verification::authenticate_bundle(bundle, root, budget, trusted_runner)?;
    verify_resource_metrics_authenticated(bundle, &artifacts)?;
    verify_compile_invocations(bundle, root, catalog, &artifacts)?;
    let diagnostics = match bundle.runner.as_str() {
        "tests" => verify_test_logs(bundle, root, catalog, &artifacts).map(|()| Vec::new()),
        "simulator" => verify_simulator_logs(bundle, root, source_root, catalog, &artifacts),
        "tla" => {
            crate::artifact_verify_tla::verify_authenticated(bundle, root, source_root, &artifacts)
        }
        "maelstrom" => crate::artifact_verify_maelstrom::verify_authenticated(
            bundle,
            root,
            source_root,
            &artifacts,
        ),
        runner => Err(AggregateError::new(format!(
            "no semantic artifact verifier exists for runner {runner}"
        ))),
    }?;
    artifacts.revalidate_paths()?;
    Ok(diagnostics)
}

#[cfg(test)]
#[path = "artifact_verify/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "artifact_verify/compile_tests.rs"]
mod compile_tests;

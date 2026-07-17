use std::path::Path;

use crate::{aggregate::AggregateError, ResultBundle};

mod compile;
mod detector_source;
mod integrity;
mod resource_metrics;
mod simulator;
mod simulator_schedule;
mod test_logs;

use compile::verify_compile_invocations;
#[cfg(test)]
use integrity::verify_producer_invocation_paths;
use resource_metrics::verify_resource_metrics;
use simulator::verify_simulator_logs;
#[cfg(test)]
use simulator::verify_simulator_observations;
#[cfg(test)]
use simulator_schedule::validate_simulator_schedule;
use test_logs::verify_test_logs;

const EVENT_PREFIX: &str = "RAFTER_EVENT ";

#[derive(Default)]
pub(crate) struct DetectorFixtureSourceBatchVerifier {
    cache: detector_source::DetectorSourceCache,
}

impl DetectorFixtureSourceBatchVerifier {
    pub(crate) fn validate(
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

pub(crate) fn is_reserved_oracle_macro(name: &str) -> bool {
    detector_source::is_reserved_oracle_macro(name)
}

pub(crate) fn validate_detector_fixture_sources(
    binding: &crate::DetectorFixtureSourceBinding<'_>,
) -> Result<(), String> {
    DetectorFixtureSourceBatchVerifier::default().validate(binding)
}

pub(super) fn verify(bundle: &ResultBundle, root: &Path) -> Result<Vec<String>, AggregateError> {
    integrity::verify(bundle, root)?;
    verify_resource_metrics(bundle, root)?;
    verify_compile_invocations(bundle, root)?;
    match bundle.runner.as_str() {
        "tests" => verify_test_logs(bundle, root).map(|()| Vec::new()),
        "simulator" => verify_simulator_logs(bundle, root),
        "tla" => crate::artifact_verify_tla::verify(bundle, root),
        "maelstrom" => crate::artifact_verify_maelstrom::verify(bundle, root),
        _ => Ok(Vec::new()),
    }
}

#[cfg(test)]
#[path = "artifact_verify/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "artifact_verify/compile_tests.rs"]
mod compile_tests;

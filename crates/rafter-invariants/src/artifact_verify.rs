use std::path::Path;

use crate::{verification::AggregateError, ResultBundle};

mod compile;
mod resource_metrics;
mod simulator;
#[cfg(test)]
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

#[cfg(test)]
const EVENT_PREFIX: &str = "RAFTER_EVENT ";

fn detector_log_verifier() -> crate::verification::simulator::DetectorLogVerifier {
    crate::verification::simulator::DetectorLogVerifier::new(
        test_logs::verify_detector_harness_error_invocations,
        test_logs::verify_test_invocations,
        test_logs::require_detector_witness_contract,
        test_logs::require_exact_test_pass,
    )
}

pub(super) fn verify(
    bundle: &ResultBundle,
    root: &Path,
    source_root: &Path,
    catalog: &crate::Catalog,
    budget: crate::verification::BundleBudget,
    trusted_runner: &str,
) -> Result<(Vec<String>, crate::verification::AuthenticatedArtifacts), AggregateError> {
    let artifacts = crate::verification::authenticate_bundle(bundle, root, budget, trusted_runner)?;
    verify_resource_metrics_authenticated(bundle, &artifacts)?;
    verify_compile_invocations(bundle, root, catalog, &artifacts)?;
    let diagnostics = match bundle.runner.as_str() {
        "tests" => {
            verify_test_logs(bundle, root, source_root, catalog, &artifacts).map(|()| Vec::new())
        }
        "simulator" => verify_simulator_logs(
            bundle,
            root,
            source_root,
            catalog,
            &artifacts,
            detector_log_verifier(),
        ),
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
    Ok((diagnostics, artifacts))
}

#[cfg(test)]
#[path = "artifact_verify/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "artifact_verify/compile_tests.rs"]
mod compile_tests;

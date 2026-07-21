//! Trusted artifact authentication and runner-family dispatch.

use std::path::Path;

use crate::{contract::catalog::Catalog, evidence::ResultBundle};

use super::super::AggregateError;

pub(crate) fn detector_log_verifier() -> &'static dyn super::super::simulator::DetectorLogVerifier {
    super::test_runner::exact_test_verifier()
}

pub(crate) fn verify_bundle(
    bundle: &ResultBundle,
    root: &Path,
    source_root: &Path,
    catalog: &Catalog,
    trusted_profile: &str,
    trusted_runner: &str,
) -> Result<(Vec<String>, super::super::AuthenticatedArtifacts), AggregateError> {
    if bundle.profile != trusted_profile || bundle.runner != trusted_runner {
        return Err(AggregateError::new(format!(
            "artifact verification identity mismatch: receipt profile/runner {}/{} != trusted {trusted_profile}/{trusted_runner}",
            bundle.profile, bundle.runner
        )));
    }
    let budget = super::super::BundleBudget::for_trusted(trusted_profile, trusted_runner)?;
    let artifacts = super::super::authenticate_bundle(bundle, root, budget, trusted_runner)?;
    super::metrics::verify_resource_metrics_authenticated(bundle, &artifacts)?;
    let compilation =
        super::compiler::verify_compile_invocations(bundle, root, catalog, &artifacts)?;
    let diagnostics = match trusted_runner {
        "tests" => super::test_runner::verify_test_logs(
            bundle,
            root,
            source_root,
            catalog,
            &artifacts,
            &compilation,
        )
        .map(|()| Vec::new()),
        "simulator" => super::super::simulator::verify_simulator_logs(
            bundle,
            root,
            source_root,
            catalog,
            &artifacts,
            detector_log_verifier(),
        ),
        "tla" => super::super::tla::verify_authenticated(bundle, root, source_root, &artifacts),
        "maelstrom" => {
            super::super::maelstrom::verify_authenticated(bundle, root, source_root, &artifacts)
        }
        runner => Err(AggregateError::new(format!(
            "no semantic artifact verifier exists for runner {runner}"
        ))),
    }?;
    artifacts.revalidate_paths()?;
    Ok((diagnostics, artifacts))
}

use std::path::Path;

use crate::{aggregate::AggregateError, ResultBundle};

mod compile;
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

pub(super) fn verify(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    integrity::verify(bundle, root)?;
    verify_resource_metrics(bundle, root)?;
    verify_compile_invocations(bundle, root)?;
    match bundle.runner.as_str() {
        "tests" => verify_test_logs(bundle, root),
        "simulator" => verify_simulator_logs(bundle, root),
        "tla" => crate::artifact_verify_tla::verify(bundle, root),
        "maelstrom" => crate::artifact_verify_maelstrom::verify(bundle, root),
        _ => Ok(()),
    }
}

#[cfg(test)]
#[path = "artifact_verify/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "artifact_verify/compile_tests.rs"]
mod compile_tests;

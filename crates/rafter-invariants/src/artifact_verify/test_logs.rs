//! Exact libtest evidence verification.

mod detector;
mod environment;
mod invocation;
mod outcome;
mod policy;
mod registry;
mod runner;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) use detector::require_detector_witness;
pub(super) use detector::{
    require_detector_witness_contract, verify_detector_harness_error_invocations,
};
pub(super) use environment::test_execution_profile;
#[cfg(test)]
pub(super) use environment::verify_exact_environment;
#[cfg(test)]
pub(super) use invocation::{
    require_unique_discovery, verify_reconstructed_test_observations,
    verify_runner_test_observations,
};
pub(super) use outcome::{is_passing, require_exact_test_pass, verify_test_invocations};
use outcome::{
    require_exact_test_failure, verify_harness_error_test_invocations,
    verify_incomplete_test_invocations, verify_oracle_failure_invocations,
};
use registry::{registered_test_binding, registered_test_name};
pub(super) use runner::verify_test_logs;

//! Test-only exact-test verifier access for compatibility scenarios.

pub(crate) use super::detector::{
    require_detector_witness, require_detector_witness_contract,
    require_detector_witness_contract_in_streams, require_detector_witness_in_streams,
    verify_detector_harness_challenge,
};
pub(crate) use super::environment::verify_exact_environment;
pub(crate) use super::invocation::{
    require_unique_discovery, verify_reconstructed_test_observations,
    verify_runner_test_observations,
};
pub(crate) use super::policy::{classify_exact_execution, ExactTestExecution};

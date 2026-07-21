//! Test-only exact-test-verifier compatibility facade.

pub(crate) use crate::verification::artifact::test_support::test_runner::{
    require_detector_witness, require_detector_witness_contract, require_unique_discovery,
    verify_exact_environment, verify_reconstructed_test_observations,
    verify_runner_test_observations,
};

pub(crate) mod detector {
    pub(crate) use crate::verification::artifact::test_support::test_runner::{
        require_detector_witness_contract_in_streams, require_detector_witness_in_streams,
        verify_detector_harness_challenge,
    };
}

pub(crate) mod policy {
    pub(crate) use crate::verification::artifact::test_support::test_runner::{
        classify_exact_execution, ExactTestExecution,
    };
}

#[path = "../verification/artifact/test_runner/tests.rs"]
mod tests;

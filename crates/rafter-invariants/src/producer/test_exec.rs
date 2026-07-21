//! Exact libtest evidence production and detector-proof orchestration.

mod artifact_log;
mod detector_policy;
mod detector_proof;
mod discovery;
mod evaluation;
mod execution;
#[cfg(test)]
mod fixtures;
mod outcome;

#[cfg(test)]
pub(crate) use crate::evidence::format::libtest::{exact_pass, oracle_token};

#[cfg(all(test, unix))]
use super::process;
pub(super) use evaluation::{evaluate, evaluate_detector};
#[cfg(all(test, unix))]
use execution::run_exact_process;
#[cfg(test)]
pub(crate) use fixtures::{
    capture_detector_witness_fixture_log, capture_fabricated_detector_witness_fixture_log,
    capture_hidden_proof_socket_fixture_log,
    capture_qualified_helper_forged_transcript_fixture_log,
    capture_registered_detector_fixture_log, capture_removed_token_detector_fixture_log,
};
pub(super) use outcome::TestOutcome;

#[cfg(test)]
pub(super) use execution::reset_test_scratch;
#[cfg(test)]
pub(crate) use execution::{classify_exact_execution, ExactTestExecution};

#[cfg(all(test, unix))]
#[path = "test_exec/process_tests.rs"]
mod process_tests;

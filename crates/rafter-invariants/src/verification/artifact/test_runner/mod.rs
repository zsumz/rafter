//! Exact libtest evidence verification.

use crate::evidence::{CheckReceipt, ResultBundle};

mod detector;
mod environment;
mod invocation;
mod outcome;
mod policy;
mod registry;
mod runner;

pub(super) use detector::{
    require_detector_witness_contract, verify_detector_harness_error_invocations,
};
use outcome::{
    require_exact_test_failure, verify_harness_error_test_invocations,
    verify_incomplete_test_invocations, verify_oracle_failure_invocations,
};
pub(super) use outcome::{require_exact_test_pass, verify_test_invocations};
use registry::{registered_test_binding, registered_test_name};
pub(super) use runner::verify_test_logs;

struct ExactTestVerifier;

static EXACT_TEST_VERIFIER: ExactTestVerifier = ExactTestVerifier;

pub(super) fn exact_test_verifier(
) -> &'static dyn crate::verification::simulator::DetectorLogVerifier {
    &EXACT_TEST_VERIFIER
}

impl crate::verification::simulator::DetectorLogVerifier for ExactTestVerifier {
    fn verify_harness_error(
        &self,
        bundle: &ResultBundle,
        check: &CheckReceipt,
        source: &str,
        test_name: &str,
        oracle_check_id: &str,
        root: &std::path::Path,
    ) -> Result<(), crate::verification::AggregateError> {
        verify_detector_harness_error_invocations(
            bundle,
            check,
            source,
            test_name,
            oracle_check_id,
            root,
        )
    }

    fn verify_passing_invocations(
        &self,
        bundle: &ResultBundle,
        check: &CheckReceipt,
        source: &str,
        test_name: &str,
        oracle_check_id: &str,
        root: &std::path::Path,
    ) -> Result<(), crate::verification::AggregateError> {
        verify_test_invocations(bundle, check, source, test_name, oracle_check_id, root)
    }

    fn verify_witness_contract(
        &self,
        bundle: &ResultBundle,
        source: &str,
        check_id: &str,
        detector: &str,
        witnesses: &std::collections::BTreeMap<String, usize>,
    ) -> Result<(), crate::verification::AggregateError> {
        require_detector_witness_contract(bundle, source, check_id, detector, witnesses)
    }

    fn require_exact_pass(
        &self,
        source: &str,
        test_name: &str,
        check_id: &str,
    ) -> Result<(), crate::verification::AggregateError> {
        require_exact_test_pass(source, test_name, check_id)
    }
}

#[cfg(test)]
pub(crate) mod test_support;

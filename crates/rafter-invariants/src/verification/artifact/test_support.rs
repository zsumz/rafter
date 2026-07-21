//! Test-only access to compatibility verifier scenarios.

pub(crate) mod compiler {
    pub(crate) use super::super::compiler::{
        target_directory_matches, verify_compile_invocations, verify_target_process_binding,
        CargoTargetKey, EmittedTestExecutable,
    };
}

pub(crate) mod test_runner {
    pub(crate) use super::super::test_runner::test_support::*;
}

pub(crate) use super::metrics::verify_resource_metrics;

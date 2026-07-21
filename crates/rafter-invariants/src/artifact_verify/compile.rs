//! Test-only compiler-verifier compatibility facade.

pub(crate) use crate::verification::artifact::test_support::compiler::{
    target_directory_matches, verify_compile_invocations, verify_target_process_binding,
    CargoTargetKey, EmittedTestExecutable,
};

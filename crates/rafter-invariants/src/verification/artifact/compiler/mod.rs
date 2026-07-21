//! Source-bound Cargo compilation and executable evidence verification.

mod cargo_output;
mod invocation;
mod model;
mod outcome;
mod receipt;
mod simulator;
mod test_target;

pub(super) use model::CompilationEvidence;
pub(crate) use receipt::verify_compile_invocations;

#[cfg(test)]
pub(crate) use invocation::target_directory_matches;
#[cfg(test)]
pub(crate) use model::{CargoTargetKey, EmittedTestExecutable};
#[cfg(test)]
pub(crate) use test_target::verify_target_process_binding;

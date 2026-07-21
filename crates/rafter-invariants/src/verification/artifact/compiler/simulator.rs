//! Exact simulator compiler-plan validation.

use std::path::Path;

use crate::{
    evidence::ResultBundle,
    verification::{AggregateError, RecordedWorkspace},
};

use super::invocation::target_directory_matches;

pub(super) fn verify_simulator_compile(
    bundle: &ResultBundle,
    observed: &crate::evidence::format::process::LabeledProcess,
    workspace: &RecordedWorkspace,
) -> Result<(), AggregateError> {
    let expected_arguments = [
        "build",
        "--release",
        "--locked",
        "-p",
        "rafter-sim",
        "--bin",
        "rafter-model-check-fast",
        "--message-format=json-render-diagnostics",
    ];
    let source_prefix = bundle.source_ref.get(..12).unwrap_or(&bundle.source_ref);
    let expected_target = format!(
        "target/rafter-invariants/simulator-build/{source_prefix}/{}",
        bundle.profile
    );
    let expected_target_dir = workspace.producer_path(Path::new(&expected_target))?;
    let mut base_environment = observed.invocation.environment.clone();
    let target = base_environment.remove("CARGO_TARGET_DIR");
    if observed.label != "simulator compile" {
        return Err(AggregateError::new(
            "simulator compile log has the wrong label".to_owned(),
        ));
    }
    if observed.invocation.arguments != expected_arguments {
        return Err(AggregateError::new(
            "simulator compile log has the wrong Cargo arguments".to_owned(),
        ));
    }
    if !target_directory_matches(target.as_deref(), &expected_target_dir) {
        return Err(AggregateError::new(
            "simulator compile log has the wrong Cargo target directory".to_owned(),
        ));
    }
    if base_environment != bundle.execution.invocation.environment {
        return Err(AggregateError::new(
            "simulator compile log has the wrong base environment".to_owned(),
        ));
    }
    Ok(())
}

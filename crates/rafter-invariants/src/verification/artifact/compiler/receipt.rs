//! Cross-log compiler receipt orchestration.

use std::{collections::BTreeMap, path::Path};

use crate::{
    contract::catalog::Catalog,
    evidence::ResultBundle,
    verification::{AggregateError, AuthenticatedArtifacts, RecordedWorkspace},
};

use super::{
    model::{CompilationEvidence, EmittedTestExecutable},
    outcome::verify_compile_process_outcome,
    simulator::verify_simulator_compile,
    test_target::{
        preserved_test_binaries, verify_test_compile, verify_test_programs_were_emitted,
    },
};

pub(crate) fn verify_compile_invocations(
    bundle: &ResultBundle,
    root: &Path,
    catalog: &Catalog,
    authenticated: &AuthenticatedArtifacts,
) -> Result<CompilationEvidence, AggregateError> {
    let logs = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "compile-log")
        .collect::<Vec<_>>();
    if !matches!(bundle.runner.as_str(), "tests" | "simulator") {
        return Ok(CompilationEvidence::default());
    }
    if logs.is_empty() {
        return Err(AggregateError::new(format!(
            "{} execution has no compile invocation log",
            bundle.runner
        )));
    }
    let workspace = RecordedWorkspace::new(bundle, root)?;
    let current_dir = workspace.producer().to_string_lossy().into_owned();
    let preserved_test_binaries = preserved_test_binaries(bundle, catalog)?;
    let mut emitted_test_executables = BTreeMap::<_, EmittedTestExecutable>::new();
    let mut evidence = CompilationEvidence::default();
    for log in logs {
        let invocations = authenticated.combined_v4(log)?;
        let [observed] = invocations.as_ref() else {
            return Err(AggregateError::new(
                "compile log must contain exactly one invocation".to_owned(),
            ));
        };
        if observed.invocation.program != "cargo"
            || observed.invocation.program_sha256 != bundle.execution.source.cargo_sha256
            || observed.invocation.current_dir != current_dir
            || !crate::verification::process_invocation_matches_source(
                &observed.invocation,
                &bundle.execution.source,
            )
        {
            return Err(AggregateError::new(
                "compile executable or working directory does not match source provenance"
                    .to_owned(),
            ));
        }
        if bundle.runner == "tests" || observed.label != "simulator compile" {
            if let Some(executable) =
                verify_test_compile(bundle, observed, &workspace, &preserved_test_binaries)?
            {
                let target = executable.target.clone();
                if emitted_test_executables
                    .insert(target.clone(), executable)
                    .is_some()
                {
                    return Err(AggregateError::new(format!(
                        "Cargo target {target:?} has multiple successful compile receipts"
                    )));
                }
            }
        } else {
            verify_simulator_compile(bundle, observed, &workspace)?;
        }
        evidence.record_failures(verify_compile_process_outcome(bundle, observed)?);
    }
    if emitted_test_executables.len() != preserved_test_binaries.len()
        || emitted_test_executables
            .keys()
            .ne(preserved_test_binaries.keys())
    {
        return Err(AggregateError::new(
            "successful test compiler targets do not exactly match preserved test binaries"
                .to_owned(),
        ));
    }
    verify_test_programs_were_emitted(bundle, catalog, &emitted_test_executables, authenticated)?;
    Ok(evidence)
}

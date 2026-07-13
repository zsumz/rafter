use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path},
};

use sha2::{Digest, Sha256};

use crate::{aggregate::AggregateError, ResultBundle};

pub(super) fn verify(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    for input in [
        &bundle.execution.plan.registry,
        &bundle.execution.plan.manifest,
        &bundle.execution.plan.result_schema,
        &bundle.execution.plan.verdict_schema,
    ] {
        crate::plan::verify_plan_input(input, root).map_err(|error| {
            AggregateError::new(format!(
                "verify execution-plan input {}: {error}",
                input.path
            ))
        })?;
    }
    let mut artifacts = bundle.execution.artifacts.iter().collect::<BTreeSet<_>>();
    artifacts.extend(
        bundle
            .execution
            .checks
            .iter()
            .flat_map(|check| check.artifacts.iter()),
    );
    artifacts.extend(
        bundle
            .results
            .iter()
            .flat_map(|result| result.artifacts.iter()),
    );
    for artifact in artifacts {
        let path = Path::new(&artifact.path);
        if path.is_absolute()
            || path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(AggregateError::new(format!(
                "artifact path must be repository-relative: {}",
                artifact.path
            )));
        }
        let bytes = fs::read(root.join(path)).map_err(|error| {
            AggregateError::new(format!("read artifact {}: {error}", artifact.path))
        })?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        if artifact.size_bytes != bytes.len() as u64 || artifact.sha256 != digest {
            return Err(AggregateError::new(format!(
                "artifact integrity mismatch: {}",
                artifact.path
            )));
        }
    }
    verify_producer_invocation_paths(bundle, root)
}

pub(super) fn verify_producer_invocation_paths(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<(), AggregateError> {
    let repository = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize producer root: {error}")))?;
    let current_dir =
        fs::canonicalize(&bundle.execution.invocation.current_dir).map_err(|error| {
            AggregateError::new(format!("canonicalize producer working directory: {error}"))
        })?;
    if current_dir != repository {
        return Err(AggregateError::new(
            "producer working directory does not match the canonical source checkout".to_owned(),
        ));
    }
    let program = fs::canonicalize(&bundle.execution.invocation.program)
        .map_err(|error| AggregateError::new(format!("canonicalize producer program: {error}")))?;
    if !program.starts_with(&repository) {
        return Err(AggregateError::new(
            "producer program is outside the canonical source checkout".to_owned(),
        ));
    }
    let binaries = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "producer-binary")
        .collect::<Vec<_>>();
    let [binary] = binaries.as_slice() else {
        return Err(AggregateError::new(
            "producer invocation requires exactly one preserved binary".to_owned(),
        ));
    };
    let program_digest =
        format!(
            "{:x}",
            Sha256::digest(fs::read(program).map_err(|error| {
                AggregateError::new(format!("read producer program: {error}"))
            })?)
        );
    if program_digest != binary.sha256
        || program_digest != bundle.execution.invocation.program_sha256
    {
        return Err(AggregateError::new(
            "claimed producer program does not match the preserved producer binary".to_owned(),
        ));
    }
    Ok(())
}

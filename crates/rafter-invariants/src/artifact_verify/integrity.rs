use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path},
};

use sha2::{Digest, Sha256};

use crate::{aggregate::AggregateError, ArtifactRef, ResultBundle};

pub(super) fn verify(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    let repository = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize artifact root: {error}")))?;
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
    artifacts.insert(&bundle.execution.producer.executable);
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
        verify_artifact(artifact, &repository)?;
    }
    verify_producer_invocation_paths(bundle, &repository)
}

fn verify_artifact(artifact: &ArtifactRef, repository: &Path) -> Result<(), AggregateError> {
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
    let expected = repository.join(path);
    let metadata = fs::symlink_metadata(&expected).map_err(|error| {
        AggregateError::new(format!("read artifact {}: {error}", artifact.path))
    })?;
    let canonical = fs::canonicalize(&expected).map_err(|error| {
        AggregateError::new(format!("canonicalize artifact {}: {error}", artifact.path))
    })?;
    if !metadata.file_type().is_file() || canonical != expected {
        return Err(AggregateError::new(format!(
            "artifact must be a regular non-symlink file inside the repository: {}",
            artifact.path
        )));
    }
    let bytes = fs::read(canonical).map_err(|error| {
        AggregateError::new(format!("read artifact {}: {error}", artifact.path))
    })?;
    let digest = format!("{:x}", Sha256::digest(&bytes));
    if artifact.size_bytes != bytes.len() as u64 || artifact.sha256 != digest {
        return Err(AggregateError::new(format!(
            "artifact integrity mismatch: {}",
            artifact.path
        )));
    }
    Ok(())
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
    let program = Path::new(&bundle.execution.invocation.program);
    let expected_program =
        crate::producer_image::image_path(&repository, &bundle.execution.invocation.program_sha256);
    if program != expected_program {
        return Err(AggregateError::new(
            "producer program does not have the exact managed content-addressed path".to_owned(),
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
    if bundle.execution.producer.binding != crate::producer_image::PRODUCER_BINDING
        || bundle.execution.producer.executable.kind != "producer-binary"
        || &bundle.execution.producer.executable != *binary
        || binary.sha256 != bundle.execution.invocation.program_sha256
    {
        return Err(AggregateError::new(
            "producer invocation is not bound to its immutable executable artifact".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use sha2::{Digest, Sha256};

    use super::verify_artifact;

    #[test]
    fn artifact_integrity_rejects_modified_missing_and_symlinked_files() {
        use std::os::unix::fs::symlink;

        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "rafter-artifact-integrity-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).expect("create artifact scratch root");
        let repository = std::fs::canonicalize(&root).expect("canonical artifact scratch root");
        let path = repository.join("artifact");
        let bytes = b"preserved artifact";
        std::fs::write(&path, bytes).expect("write preserved artifact");
        let artifact = crate::ArtifactRef {
            kind: "producer-binary".to_owned(),
            path: "artifact".to_owned(),
            sha256: format!("{:x}", Sha256::digest(bytes)),
            size_bytes: bytes.len() as u64,
        };

        verify_artifact(&artifact, &repository).expect("regular artifact verifies");
        std::fs::write(&path, b"modified artifact").expect("modify artifact");
        assert!(verify_artifact(&artifact, &repository).is_err());
        std::fs::remove_file(&path).expect("remove modified artifact");
        assert!(verify_artifact(&artifact, &repository).is_err());

        let target = repository.join("target");
        std::fs::write(&target, bytes).expect("write symlink target");
        symlink(&target, &path).expect("create artifact symlink");
        assert!(verify_artifact(&artifact, &repository).is_err());
        std::fs::remove_dir_all(root).expect("remove artifact scratch root");
    }
}

//! Artifact confinement, hashing, and immutable snapshot construction.

use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
};

#[cfg(test)]
use std::{collections::BTreeSet, sync::Arc};

use crate::evidence::ResultBundle;

#[cfg(test)]
use crate::evidence::{limits::MAX_ARTIFACT_BYTES, ArtifactRef};

use super::{
    super::{filesystem::VerificationRoot, AggregateError},
    budget::BundleBudget,
};

mod file;
mod model;
mod preflight;

use file::{authenticate_artifact_at, authenticate_plan_input, reject_file_alias};
pub(crate) use model::AuthenticatedArtifacts;
pub(crate) use preflight::declared_artifacts;
use preflight::{artifacts as preflight_artifacts, plan_inputs as preflight_plan_inputs};

#[cfg(test)]
use file::authenticate_artifact;
#[cfg(test)]
use preflight::retain_semantic_bytes;

pub(crate) fn authenticate(
    bundle: &ResultBundle,
    root: &Path,
    budget: BundleBudget,
    trusted_runner: &str,
) -> Result<AuthenticatedArtifacts, AggregateError> {
    let plan_inputs = preflight_plan_inputs(bundle)?;
    let artifacts = preflight_artifacts(bundle, budget, trusted_runner)?;
    let repository = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize artifact root: {error}")))?;
    let directory = VerificationRoot::open(&repository)
        .map_err(|error| AggregateError::new(format!("open artifact root: {error}")))?;

    let mut files = Vec::new();
    for input in plan_inputs {
        let authenticated = authenticate_plan_input(input, &directory)?;
        reject_file_alias(&files, &authenticated)?;
        files.push(authenticated);
    }

    let mut bytes_by_artifact = BTreeMap::new();
    let mut interned = BTreeMap::new();
    for artifact in artifacts.values() {
        let retain = preflight::retain_semantic_bytes(trusted_runner, &artifact.kind)?;
        let key = (artifact.sha256.clone(), artifact.size_bytes);
        let existing = retain.then(|| interned.get(&key)).flatten().cloned();
        let authenticated =
            authenticate_artifact_at(artifact, &directory, retain && existing.is_none())?;
        reject_file_alias(&files, &authenticated.file)?;
        if retain {
            let bytes = match existing {
                Some(bytes) => bytes,
                None => interned
                    .entry(key)
                    .or_insert(authenticated.bytes.ok_or_else(|| {
                        AggregateError::new(format!(
                            "semantic artifact was not retained: {}",
                            artifact.path
                        ))
                    })?)
                    .clone(),
            };
            bytes_by_artifact.insert((*artifact).clone(), bytes);
        }
        files.push(authenticated.file);
    }
    verify_producer_invocation_paths(bundle, &repository)?;
    Ok(AuthenticatedArtifacts::new(bytes_by_artifact, files))
}

#[cfg(test)]
/// Freezes confined fixture bytes for focused semantic-verifier tests.
///
/// These tests deliberately mutate framed content without rebuilding receipt
/// digests. Whole-bundle tests call `authenticate` and exercise the strict
/// integrity contract independently.
pub(crate) fn snapshot_available_artifacts(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<AuthenticatedArtifacts, AggregateError> {
    let repository = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize artifact root: {error}")))?;
    let artifacts = bundle
        .execution
        .artifacts
        .iter()
        .chain(
            bundle
                .execution
                .checks
                .iter()
                .flat_map(|check| check.artifacts.iter()),
        )
        .chain(
            bundle
                .results
                .iter()
                .flat_map(|result| result.artifacts.iter()),
        )
        .collect::<BTreeSet<_>>();
    let mut bytes_by_artifact = BTreeMap::new();
    for artifact in artifacts {
        if let Ok(bytes) = snapshot_existing_file(&artifact.path, &repository) {
            bytes_by_artifact.insert(artifact.clone(), bytes);
        }
    }
    Ok(AuthenticatedArtifacts::new(bytes_by_artifact, Vec::new()))
}

#[cfg(test)]
fn snapshot_existing_file(
    declared_path: &str,
    repository: &Path,
) -> Result<Arc<[u8]>, AggregateError> {
    let path = Path::new(declared_path);
    let normalized = path.components().collect::<PathBuf>();
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || normalized.as_os_str() != path.as_os_str()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(AggregateError::new(format!(
            "fixture artifact path must be repository-relative: {declared_path}"
        )));
    }
    let expected = repository.join(path);
    let metadata = fs::symlink_metadata(&expected).map_err(|error| {
        AggregateError::new(format!("read fixture artifact {declared_path}: {error}"))
    })?;
    let canonical = fs::canonicalize(&expected).map_err(|error| {
        AggregateError::new(format!(
            "canonicalize fixture artifact {declared_path}: {error}"
        ))
    })?;
    if !metadata.file_type().is_file()
        || metadata.len() > MAX_ARTIFACT_BYTES
        || canonical != expected
    {
        return Err(AggregateError::new(format!(
            "fixture artifact must be a bounded regular non-symlink file inside the repository: {declared_path}"
        )));
    }
    fs::read(canonical).map(Into::into).map_err(|error| {
        AggregateError::new(format!("read fixture artifact {declared_path}: {error}"))
    })
}

#[cfg(test)]
pub(crate) fn verify(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    let budget = BundleBudget::for_trusted(&bundle.profile, &bundle.runner)?;
    let authenticated = authenticate(bundle, root, budget, &bundle.runner)?;
    authenticated.revalidate_paths()
}

pub(crate) fn verify_producer_invocation_paths(
    bundle: &ResultBundle,
    _root: &Path,
) -> Result<(), AggregateError> {
    let current_dir = Path::new(&bundle.execution.invocation.current_dir);
    if !is_clean_absolute_path(current_dir) {
        return Err(AggregateError::new(
            "producer working directory must be a clean absolute lexical root".to_owned(),
        ));
    }
    let program = Path::new(&bundle.execution.invocation.program);
    let expected_program = crate::provenance::image::image_path(
        current_dir,
        &bundle.execution.invocation.program_sha256,
    );
    if !is_clean_absolute_path(program) || program.as_os_str() != expected_program.as_os_str() {
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
    if bundle.execution.producer.binding != crate::provenance::image::PRODUCER_BINDING
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

fn is_clean_absolute_path(path: &Path) -> bool {
    if !path.is_absolute() {
        return false;
    }
    let normalized = path.components().collect::<PathBuf>();
    if normalized.as_os_str() != path.as_os_str() {
        return false;
    }
    let mut has_normal_component = false;
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {}
            Component::Normal(_) => has_normal_component = true,
            Component::CurDir | Component::ParentDir => return false,
        }
    }
    has_normal_component
}

#[cfg(test)]
#[path = "integrity/tests.rs"]
mod tests;

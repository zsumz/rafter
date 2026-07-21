//! Strict Cargo compiler transcript and fresh executable binding.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use sha2::Digest;

mod transcript;

use crate::{
    evidence::limits::MAX_ARTIFACT_BYTES,
    execution::filesystem::{HeldDirectory, HeldFile, OperationDeadline},
    verification::target::verify_protected_compiler_artifacts,
};

use super::{metadata::CompilationGraph, ReplayTarget};

pub(super) struct CompiledReplayTarget {
    executable: PathBuf,
    handle: HeldFile,
    sha256: String,
}

impl CompiledReplayTarget {
    fn bind(executable: PathBuf, handle: HeldFile) -> Result<Self, String> {
        let sha256 = executable_sha256(&handle)?;
        Ok(Self {
            executable,
            handle,
            sha256,
        })
    }

    pub(super) fn executable(&self) -> &Path {
        &self.executable
    }

    pub(super) fn revalidate(&self) -> Result<(), String> {
        self.handle
            .verify_path_binding()
            .map_err(|error| format!("compiled replay executable changed: {error}"))?;
        let observed = executable_sha256(&self.handle)?;
        if observed != self.sha256 {
            return Err("compiled replay executable bytes changed after compilation".to_owned());
        }
        Ok(())
    }
}

pub(super) fn bind_fresh_executables(
    bytes: &[u8],
    graph: &CompilationGraph,
    source_root: &Path,
    target: &HeldDirectory,
    expected: impl Iterator<Item = ReplayTarget>,
) -> Result<BTreeMap<ReplayTarget, CompiledReplayTarget>, String> {
    let expected = expected.collect::<BTreeSet<_>>();
    let target_root = fs::canonicalize(target.external_path())
        .map_err(|error| format!("canonicalize replay target root: {error}"))?;
    let candidates = transcript::parse(bytes, graph, &target_root, &expected)?;
    verify_protected_compiler_artifacts(bytes, source_root, source_root)?;
    if candidates.len() != expected.len() {
        return Err(format!(
            "Cargo compiler transcript selected {} of {} replay targets",
            candidates.len(),
            expected.len()
        ));
    }
    bind_executables(candidates, expected, target, &target_root)
}

fn bind_executables(
    mut candidates: BTreeMap<ReplayTarget, Vec<PathBuf>>,
    expected: BTreeSet<ReplayTarget>,
    target: &HeldDirectory,
    target_root: &Path,
) -> Result<BTreeMap<ReplayTarget, CompiledReplayTarget>, String> {
    let mut compiled = BTreeMap::new();
    for key in expected {
        let paths = candidates.remove(&key).unwrap_or_default();
        let [path] = paths.as_slice() else {
            return Err(format!(
                "Cargo compiler transcript contains {} executables for {key:?}",
                paths.len()
            ));
        };
        let executable = canonical_executable(path, target_root, &key)?;
        let relative = executable
            .strip_prefix(target_root)
            .map_err(|_| "compiled replay executable escaped target root".to_owned())?;
        let handle = target
            .hold_file(relative)
            .map_err(|error| format!("hold compiled replay executable: {error}"))?;
        compiled.insert(key, CompiledReplayTarget::bind(executable, handle)?);
    }
    Ok(compiled)
}

fn executable_sha256(handle: &HeldFile) -> Result<String, String> {
    let bytes = handle
        .read_bounded(
            OperationDeadline::none("hash compiled replay executable"),
            MAX_ARTIFACT_BYTES,
        )
        .map_err(|error| format!("read compiled replay executable: {error}"))?;
    Ok(format!("{:x}", sha2::Sha256::digest(bytes)))
}

fn canonical_executable(
    path: &Path,
    target_root: &Path,
    target: &ReplayTarget,
) -> Result<PathBuf, String> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(format!(
            "Cargo emitted a noncanonical executable for {target:?}"
        ));
    }
    let executable = fs::canonicalize(path)
        .map_err(|error| format!("canonicalize replay executable {}: {error}", path.display()))?;
    if !executable.starts_with(target_root) {
        return Err(format!(
            "Cargo executable for {target:?} escaped the private target"
        ));
    }
    let name = executable
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "Cargo replay executable name is not UTF-8".to_owned())?;
    let prefix = target.name.replace('-', "_");
    if name != prefix
        && !name
            .strip_prefix(&prefix)
            .is_some_and(|suffix| suffix.starts_with('-'))
    {
        return Err(format!("Cargo executable {name} does not match {target:?}"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if fs::metadata(&executable)
            .map_err(|error| format!("inspect replay executable mode: {error}"))?
            .permissions()
            .mode()
            & 0o111
            == 0
        {
            return Err(format!("Cargo executable for {target:?} is not executable"));
        }
    }
    Ok(executable)
}

#[cfg(test)]
#[path = "compiler/tests.rs"]
mod tests;

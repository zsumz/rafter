use std::{
    error::Error,
    fs,
    path::{Component, Path},
};

use sha2::{Digest, Sha256};

use crate::ArtifactRef;

pub(super) fn validate_output_dir(path: &Path) -> Result<(), Box<dyn Error>> {
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err("producer output directory must be repository-relative".into());
    }
    Ok(())
}

pub(super) fn write(
    output_dir: &Path,
    relative_name: &Path,
    kind: &str,
    bytes: &[u8],
) -> Result<ArtifactRef, Box<dyn Error>> {
    let path = output_dir.join(relative_name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, bytes)?;
    Ok(reference(&path, kind, bytes))
}

pub(super) fn capture(
    output_dir: &Path,
    namespace: &Path,
    source: &Path,
    kind: &str,
) -> Result<ArtifactRef, Box<dyn Error>> {
    let root = fs::canonicalize(".")?;
    let canonical = fs::canonicalize(source)?;
    canonical
        .strip_prefix(&root)
        .map_err(|_| "artifact is outside the repository worktree")?;
    let bytes = fs::read(&canonical)?;
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let relative_name = namespace.join(format!("{kind}-{digest}"));
    write(output_dir, &relative_name, kind, &bytes)
}

pub(super) fn capture_as(
    output_dir: &Path,
    relative_name: &Path,
    source: &Path,
    kind: &str,
) -> Result<ArtifactRef, Box<dyn Error>> {
    let root = fs::canonicalize(".")?;
    let canonical = fs::canonicalize(source)?;
    canonical
        .strip_prefix(&root)
        .map_err(|_| "artifact is outside the repository worktree")?;
    write(output_dir, relative_name, kind, &fs::read(canonical)?)
}

pub(super) fn capture_external(
    output_dir: &Path,
    namespace: &Path,
    source: &Path,
    kind: &str,
) -> Result<ArtifactRef, Box<dyn Error>> {
    let bytes = fs::read(fs::canonicalize(source)?)?;
    let digest = format!("{:x}", Sha256::digest(&bytes));
    write(
        output_dir,
        &namespace.join(format!("{kind}-{digest}")),
        kind,
        &bytes,
    )
}

fn reference(path: &Path, kind: &str, bytes: &[u8]) -> ArtifactRef {
    ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_string_lossy().into_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        size_bytes: bytes.len() as u64,
    }
}

pub(super) fn stable_id(namespace: &str, value: &str) -> String {
    let digest = format!("{:x}", Sha256::digest(format!("{namespace}\0{value}")));
    format!("{namespace}-{}", &digest[..16])
}

pub(crate) fn deterministic_u64(namespace: &str, value: &str) -> u64 {
    let digest = Sha256::digest(format!("{namespace}\0{value}"));
    let mut prefix = [0; 8];
    prefix.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(prefix)
}

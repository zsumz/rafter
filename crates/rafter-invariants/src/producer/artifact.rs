//! Producer artifact capture, naming, integrity, and output confinement.

use std::{
    error::Error,
    fs,
    io::Read,
    path::{Component, Path},
};

use sha2::{Digest, Sha256};

use crate::{
    evidence::{limits::MAX_ARTIFACT_BYTES, ArtifactRef},
    execution::filesystem::HeldDirectory,
};

/// Characters `actions/upload-artifact` rejects outright for Windows
/// portability. A published evidence tree that contains any of them cannot be
/// uploaded at all, so the whole layer fails after the work is already done.
pub(crate) const UNPORTABLE_FILENAME_CHARACTERS: [char; 9] =
    [':', '*', '?', '"', '<', '>', '|', '\\', '\0'];

/// Maps an artifact kind onto a filename component that is safe to upload.
///
/// Artifact kinds are receipt vocabulary and several are structured with a
/// colon (`tla-detector-config:LogMatching`, `tla-obligation-log:read-fencing`).
/// The kind is the compatibility identity and does not change; only the name of
/// the file carrying it does. `:` becomes `-`, which is the same normalization
/// the source-identity policy already applies when it recognizes those files.
pub(crate) fn portable_filename(kind: &str) -> String {
    kind.replace(':', "-")
}

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
    require_bounded(bytes, kind)?;
    let path = output_dir.join(relative_name);
    let workspace = HeldDirectory::workspace()?;
    workspace.write_atomic(&path, bytes)?;
    let persisted = workspace.read(&path)?;
    if persisted != bytes {
        return Err(format!("artifact changed during publication: {}", path.display()).into());
    }
    Ok(reference(&path, kind, &persisted))
}

pub(super) fn capture(
    output_dir: &Path,
    namespace: &Path,
    source: &Path,
    kind: &str,
) -> Result<ArtifactRef, Box<dyn Error>> {
    let bytes = read_confined(source)?;
    capture_bytes(output_dir, namespace, &bytes, kind)
}

pub(super) fn capture_bytes(
    output_dir: &Path,
    namespace: &Path,
    bytes: &[u8],
    kind: &str,
) -> Result<ArtifactRef, Box<dyn Error>> {
    require_bounded(bytes, kind)?;
    let digest = format!("{:x}", Sha256::digest(bytes));
    let relative_name = namespace.join(format!("{}-{digest}", portable_filename(kind)));
    let path = output_dir.join(relative_name);
    let workspace = HeldDirectory::workspace()?;
    match workspace.read(&path) {
        Ok(persisted) if persisted == bytes => return Ok(reference(&path, kind, &persisted)),
        Ok(_) => {
            return Err(format!(
                "conflicting content at content-addressed artifact {}",
                path.display()
            )
            .into())
        }
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) => {}
        Err(error) => return Err(error),
    }
    workspace.write_atomic(&path, bytes)?;
    let persisted = workspace.read(&path)?;
    if persisted != bytes {
        return Err(format!("artifact changed during publication: {}", path.display()).into());
    }
    Ok(reference(&path, kind, &persisted))
}

pub(super) fn capture_external(
    output_dir: &Path,
    namespace: &Path,
    source: &Path,
    kind: &str,
) -> Result<ArtifactRef, Box<dyn Error>> {
    let path = fs::canonicalize(source)?;
    let mut file = fs::File::open(&path)?;
    let length = file.metadata()?.len();
    if length > MAX_ARTIFACT_BYTES {
        return Err(format!(
            "external artifact {} is {length} bytes, exceeding the {MAX_ARTIFACT_BYTES}-byte limit",
            path.display()
        )
        .into());
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(usize::try_from(length)?)?;
    file.by_ref()
        .take(MAX_ARTIFACT_BYTES + 1)
        .read_to_end(&mut bytes)?;
    require_bounded(&bytes, kind)?;
    capture_bytes(output_dir, namespace, &bytes, kind)
}

fn reference(path: &Path, kind: &str, bytes: &[u8]) -> ArtifactRef {
    ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_string_lossy().into_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        size_bytes: bytes.len() as u64,
    }
}

fn read_confined(source: &Path) -> Result<Vec<u8>, Box<dyn Error>> {
    crate::execution::filesystem::read_file_bounded(source, MAX_ARTIFACT_BYTES)
}

fn require_bounded(bytes: &[u8], kind: &str) -> Result<(), Box<dyn Error>> {
    if bytes.len() as u64 > MAX_ARTIFACT_BYTES {
        return Err(format!(
            "{kind} artifact is {} bytes, exceeding the {MAX_ARTIFACT_BYTES}-byte limit",
            bytes.len()
        )
        .into());
    }
    Ok(())
}

pub(super) fn stable_id(namespace: &str, value: &str) -> String {
    let digest = format!("{:x}", Sha256::digest(format!("{namespace}\0{value}")));
    format!("{namespace}-{}", &digest[..16])
}

#[cfg(test)]
#[path = "artifact_naming_tests.rs"]
mod naming_tests;

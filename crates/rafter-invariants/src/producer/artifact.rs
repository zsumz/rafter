use std::{
    error::Error,
    fs,
    path::{Component, Path},
};

use sha2::{Digest, Sha256};

use crate::{execution::filesystem::HeldDirectory, ArtifactRef};

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
    let digest = format!("{:x}", Sha256::digest(bytes));
    let relative_name = namespace.join(format!("{kind}-{digest}"));
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
    let bytes = fs::read(fs::canonicalize(source)?)?;
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
    crate::execution::filesystem::read_file(source)
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

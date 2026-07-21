//! Descriptor-free exact intake of a sealed verifier artifact directory.

use std::{collections::BTreeMap, error::Error, fs, io::Read, path::Path};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use super::{
    manifest::canonical_name,
    model::{ArtifactSet, MAX_FILES, MAX_TOTAL_BYTES},
    VerifierArchiveExpectation,
};

pub(super) fn read(
    root: &Path,
    manifest: &Path,
    expected_manifest_sha256: &str,
    expectation: &VerifierArchiveExpectation,
) -> Result<ArtifactSet, Box<dyn Error>> {
    let root = exact_path(root, true, "verifier artifact root")?;
    require_read_only(&root, "verifier artifact root")?;
    let manifest = exact_path(manifest, false, "verifier artifact manifest")?;
    if manifest.parent() != Some(root.as_path()) {
        return Err("verifier artifact manifest is outside its exact root".into());
    }
    let manifest_name = manifest
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| canonical_name(name))
        .ok_or("verifier artifact manifest filename is noncanonical")?;
    let mut files = BTreeMap::new();
    let mut total_bytes = 0_usize;
    for entry in fs::read_dir(&root)? {
        if files.len() >= MAX_FILES {
            return Err("verifier artifact root exceeds its file-count limit".into());
        }
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        let name = entry
            .file_name()
            .into_string()
            .ok()
            .filter(|name| canonical_name(name))
            .ok_or("verifier artifact filename is noncanonical")?;
        if !metadata.file_type().is_file() || fs::canonicalize(&path)? != path {
            return Err(format!("verifier artifact {name} is not an exact regular file").into());
        }
        require_read_only(&path, &format!("verifier artifact {name}"))?;
        let bytes = read_bounded(&path)?;
        total_bytes = total_bytes
            .checked_add(bytes.len())
            .ok_or("verifier artifact byte count overflow")?;
        if total_bytes > MAX_TOTAL_BYTES {
            return Err("verifier artifact root exceeds its aggregate byte limit".into());
        }
        if files.insert(name.clone(), bytes).is_some() {
            return Err(format!("verifier artifact filename repeats: {name}").into());
        }
    }
    ArtifactSet::verify(files, manifest_name, expected_manifest_sha256, expectation)
        .map_err(Into::into)
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut file = fs::File::open(path)?;
    let length = usize::try_from(file.metadata()?.len())?;
    if length > super::model::MAX_FILE_BYTES {
        return Err("verifier artifact exceeds its byte limit".into());
    }
    let mut bytes = Vec::with_capacity(length);
    file.by_ref()
        .take(u64::try_from(super::model::MAX_FILE_BYTES)? + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() != length || bytes.len() > super::model::MAX_FILE_BYTES {
        return Err("verifier artifact changed while it was read".into());
    }
    Ok(bytes)
}

fn exact_path(path: &Path, directory: bool, context: &str) -> Result<std::path::PathBuf, String> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| format!("inspect {context}: {error}"))?;
    if (directory && !metadata.file_type().is_dir())
        || (!directory && !metadata.file_type().is_file())
    {
        return Err(format!("{context} has the wrong file type"));
    }
    let canonical =
        fs::canonicalize(path).map_err(|error| format!("canonicalize {context}: {error}"))?;
    Ok(canonical)
}

#[cfg(unix)]
fn require_read_only(path: &Path, context: &str) -> Result<(), String> {
    let mode = fs::metadata(path)
        .map_err(|error| format!("inspect {context} permissions: {error}"))?
        .permissions()
        .mode();
    if mode & 0o222 != 0 {
        return Err(format!("{context} is writable"));
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_read_only(_path: &Path, _context: &str) -> Result<(), String> {
    Err("verifier artifact publication requires Unix permission semantics".to_owned())
}

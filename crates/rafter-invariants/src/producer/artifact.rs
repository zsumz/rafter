use std::{
    error::Error,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Component, Path},
    sync::atomic::{AtomicU64, Ordering},
};

use sha2::{Digest, Sha256};

use crate::ArtifactRef;

static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);

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
    let parent = path.parent().ok_or("artifact path has no parent")?;
    fs::create_dir_all(parent)?;
    verify_confined_parent(parent)?;
    let temporary = parent.join(format!(
        ".artifact.tmp-{}-{}",
        std::process::id(),
        NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    let publish = (|| -> Result<(), Box<dyn Error>> {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, &path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    let _ = fs::remove_file(&temporary);
    publish?;
    let persisted = fs::read(&path)?;
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
    let root = fs::canonicalize(".")?;
    let canonical = fs::canonicalize(source)?;
    canonical
        .strip_prefix(&root)
        .map_err(|_| "artifact is outside the repository worktree")?;
    let bytes = fs::read(&canonical)?;
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
    let parent = path.parent().ok_or("artifact path has no parent")?;
    fs::create_dir_all(parent)?;
    verify_confined_parent(parent)?;
    let persisted = crate::producer_image::publish_content_addressed(&path, bytes, false)?;
    Ok(reference(&path, kind, &persisted))
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

fn verify_confined_parent(parent: &Path) -> Result<(), Box<dyn Error>> {
    if parent.is_absolute() {
        return Err("artifact parent must be repository-relative".into());
    }
    let root = fs::canonicalize(".")?;
    let canonical = fs::canonicalize(parent)?;
    if canonical != root.join(parent) {
        return Err("artifact parent traverses a symlink or leaves the repository".into());
    }
    Ok(())
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

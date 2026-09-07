//! Publication-root resolution, archive budgets, and read-only inventory.
//!
//! Every published artifact is hardened unwritable and read back against its
//! recorded digest, and the run directory must contain exactly the files the
//! inventory names, so a later mutation cannot pass revalidation.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    time::{SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};

use crate::{
    evidence::limits::{
        MAX_ARTIFACT_BYTES, MAX_VERIFIER_ARCHIVE_BYTES, MAX_VERIFIER_ARCHIVE_FILES,
    },
    execution::filesystem::{EntryKind, HeldDirectory, OperationDeadline},
};

use super::{
    PublishedArtifact, DEFAULT_PUBLICATION_ROOT, INVOCATION_SEQUENCE, PUBLICATION_ROOT_ENV,
};

pub(super) fn validate_archive_budget(
    published_files: usize,
    published_bytes: usize,
    additional_bytes: usize,
) -> Result<(), &'static str> {
    if published_files >= MAX_VERIFIER_ARCHIVE_FILES {
        return Err("verifier artifact inventory exceeds its file-count limit");
    }
    let total = published_bytes
        .checked_add(additional_bytes)
        .ok_or("verifier artifact byte count overflow")?;
    if total > MAX_VERIFIER_ARCHIVE_BYTES {
        return Err("verifier artifact inventory exceeds its total-byte limit");
    }
    Ok(())
}

pub(super) fn publication_root() -> Result<PathBuf, Box<dyn Error>> {
    match std::env::var_os(PUBLICATION_ROOT_ENV) {
        Some(path) if path.is_empty() => Err(format!("{PUBLICATION_ROOT_ENV} is empty").into()),
        Some(path) => Ok(PathBuf::from(path)),
        None => Ok(PathBuf::from(DEFAULT_PUBLICATION_ROOT)),
    }
}

impl PublishedArtifact {
    pub(super) fn revalidate(&self, deadline: OperationDeadline) -> Result<(), Box<dyn Error>> {
        deadline.check()?;
        self.file.verify_path_binding()?;
        require_read_only(&self.file.external_path())?;
        let bytes = self.file.read_bounded(deadline, MAX_ARTIFACT_BYTES)?;
        if bytes.len() as u64 != self.reference.size_bytes
            || format!("{:x}", Sha256::digest(&bytes)) != self.reference.sha256
        {
            return Err(format!(
                "verifier artifact changed after publication: {}",
                self.reference.path
            )
            .into());
        }
        Ok(())
    }
}

pub(super) fn invocation_name(identity: &str) -> Result<String, Box<dyn Error>> {
    let sequence = INVOCATION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let invocation = format!(
        "{:x}",
        Sha256::digest(format!(
            "{identity}\0{}\0{timestamp}\0{sequence}",
            std::process::id()
        ))
    );
    Ok(format!("run-{}-{}", &identity[..16], &invocation[..16]))
}

pub(super) fn require_exact_inventory(
    root: &HeldDirectory,
    published: &BTreeMap<String, PublishedArtifact>,
    deadline: OperationDeadline,
) -> Result<(), Box<dyn Error>> {
    let expected = published
        .keys()
        .map(OsString::from)
        .collect::<BTreeSet<_>>();
    let entries = root.entries(deadline)?;
    let observed = entries
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>();
    if observed != expected || entries.iter().any(|(_, kind)| *kind != EntryKind::File) {
        return Err(format!(
            "verifier artifact tree inventory changed: expected {} files, observed {} entries",
            expected.len(),
            observed.len()
        )
        .into());
    }
    Ok(())
}

pub(super) fn harden_read_only(
    path: &Path,
    deadline: OperationDeadline,
) -> Result<(), Box<dyn Error>> {
    deadline.check()?;
    let mut permissions = fs::metadata(path)?.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(permissions.mode() & !0o222);
    }
    #[cfg(not(unix))]
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions)?;
    deadline.check()?;
    require_read_only(path)
}

pub(super) fn require_read_only(path: &Path) -> Result<(), Box<dyn Error>> {
    let permissions = fs::metadata(path)?.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if permissions.mode() & 0o222 != 0 {
            return Err(format!("verifier artifact remains writable: {}", path.display()).into());
        }
    }
    #[cfg(not(unix))]
    if !permissions.readonly() {
        return Err(format!("verifier artifact remains writable: {}", path.display()).into());
    }
    Ok(())
}

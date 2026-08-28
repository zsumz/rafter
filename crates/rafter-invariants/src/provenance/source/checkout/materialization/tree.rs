//! Git tree-entry decoding and bounded reads of the tracked worktree.
//!
//! Accepts only regular blob entries with canonical paths, reads each one back
//! against the recorded object ID, and frames every field into the
//! materialization digest.

use std::{
    collections::BTreeSet,
    error::Error,
    fs,
    path::{Component, Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use sha1::Sha1;
use sha2::{Digest, Sha256};

use super::{GitObjectFormat, TreeEntry};

pub(super) fn parse_tree_inventory(inventory: &str) -> Result<Vec<TreeEntry>, Box<dyn Error>> {
    let mut paths = BTreeSet::new();
    inventory
        .split('\0')
        .filter(|record| !record.is_empty())
        .map(|record| {
            let (header, path_text) = record
                .split_once('\t')
                .ok_or("Git tree entry omitted its path separator")?;
            let mut fields = header.split(' ');
            let mode = fields.next().ok_or("Git tree entry omitted its mode")?;
            let kind = fields.next().ok_or("Git tree entry omitted its kind")?;
            let oid = fields
                .next()
                .ok_or("Git tree entry omitted its object ID")?;
            if fields.next().is_some() {
                return Err("Git tree entry has unexpected header fields".into());
            }
            if mode == "120000" && kind == "blob" {
                return Err(
                    "Git symlinks are outside the raw source materialization contract".into(),
                );
            }
            if !matches!(mode, "100644" | "100755") || kind != "blob" {
                if mode == "160000" && kind == "commit" {
                    return Err(
                        "Git submodules are outside the raw source materialization contract".into(),
                    );
                }
                return Err(
                    format!("unsupported Git tree entry mode and kind: {mode} {kind}").into(),
                );
            }
            if !matches!(oid.len(), 40 | 64)
                || !oid
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                return Err(format!("invalid Git tree object ID: {oid:?}").into());
            }
            let path = PathBuf::from(path_text);
            if path_text.is_empty()
                || !path
                    .components()
                    .all(|component| matches!(component, Component::Normal(_)))
            {
                return Err(format!("Git tree contains a noncanonical path: {path_text:?}").into());
            }
            if !paths.insert(path.clone()) {
                return Err(format!("Git tree contains duplicate path: {path_text:?}").into());
            }
            Ok(TreeEntry {
                mode: mode.to_owned(),
                oid: oid.to_owned(),
                path,
                path_text: path_text.to_owned(),
            })
        })
        .collect()
}

pub(super) fn read_bound_entry(root: &Path, entry: &TreeEntry) -> Result<Vec<u8>, Box<dyn Error>> {
    let path = root.join(&entry.path);
    let parent = path
        .parent()
        .ok_or_else(|| format!("tracked path has no parent: {}", path.display()))?;
    if fs::canonicalize(parent)? != parent {
        return Err(format!(
            "tracked path traverses a filesystem alias or symlink: {}",
            path.display()
        )
        .into());
    }
    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.file_type().is_file() || fs::canonicalize(&path)? != path {
        return Err(format!(
            "tracked regular file traverses a filesystem alias or changed type: {}",
            path.display()
        )
        .into());
    }
    #[cfg(unix)]
    {
        let executable = metadata.permissions().mode() & 0o100 != 0;
        if executable != (entry.mode == "100755") {
            return Err(format!(
                "tracked executable mode differs from Git tree: {}",
                path.display()
            )
            .into());
        }
    }
    #[cfg(not(unix))]
    if entry.mode == "100755" {
        return Err("tracked executable modes require Unix permission support".into());
    }
    Ok(fs::read(path)?)
}

pub(super) fn git_blob_oid(format: GitObjectFormat, content: &[u8]) -> String {
    let header = format!("blob {}\0", content.len());
    match format {
        GitObjectFormat::Sha1 => {
            let mut digest = Sha1::new();
            digest.update(header.as_bytes());
            digest.update(content);
            format!("{:x}", digest.finalize())
        }
        GitObjectFormat::Sha256 => {
            let mut digest = Sha256::new();
            digest.update(header.as_bytes());
            digest.update(content);
            format!("{:x}", digest.finalize())
        }
    }
}

pub(super) fn digest_frame(digest: &mut Sha256, bytes: &[u8]) {
    digest.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(bytes);
}

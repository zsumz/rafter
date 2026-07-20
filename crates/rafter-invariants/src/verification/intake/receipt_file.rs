//! Descriptor-held, bounded reads of untrusted result receipts.

use std::{
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use super::IntakeDefect;
use crate::verification::{
    bundle::MAX_RECEIPT_BYTES,
    filesystem::{VerificationFile, VerificationRoot as FileRoot},
};

pub(super) struct ReceiptRoot {
    original: PathBuf,
    canonical: PathBuf,
    directory: FileRoot,
}

impl ReceiptRoot {
    pub(super) fn capture(path: &Path) -> Result<Self, IntakeDefect> {
        let original = receipt_parent(path).to_path_buf();
        let canonical =
            std::fs::canonicalize(&original).map_err(|error| classify_io(&original, &error))?;
        let directory = FileRoot::open(&canonical).map_err(|error| {
            IntakeDefect::unverifiable(format!(
                "open result receipt root {}: {error}",
                original.display()
            ))
        })?;
        Ok(Self {
            original,
            canonical,
            directory,
        })
    }
}

pub(super) struct ReceiptFile {
    path: PathBuf,
    file: VerificationFile,
    length: u64,
    digest: Option<String>,
    original_parent: PathBuf,
    canonical_parent: PathBuf,
}

impl ReceiptFile {
    pub(super) fn open(root: &ReceiptRoot, path: &Path) -> Result<Self, IntakeDefect> {
        if receipt_parent(path) != root.original {
            return Err(IntakeDefect::unverifiable(format!(
                "result receipts must share one evidence root: {}",
                path.display()
            )));
        }
        let name = path.file_name().ok_or_else(|| {
            IntakeDefect::unverifiable(format!(
                "result path has no regular-file name: {}",
                path.display()
            ))
        })?;
        let file = root
            .directory
            .hold_file(Path::new(name))
            .map_err(|error| classify_open(path, error.as_ref()))?;
        let length = file
            .try_clone_std()
            .and_then(|file| Ok(file.metadata()?.len()))
            .map_err(|error| {
                IntakeDefect::unverifiable(format!("inspect {}: {error}", path.display()))
            })?;
        if length > MAX_RECEIPT_BYTES {
            return Err(IntakeDefect::unverifiable(format!(
                "result receipt {} is {length} bytes, exceeding the {MAX_RECEIPT_BYTES}-byte limit",
                path.display()
            )));
        }
        Ok(Self {
            path: path.to_path_buf(),
            file,
            length,
            digest: None,
            original_parent: root.original.clone(),
            canonical_parent: root.canonical.clone(),
        })
    }

    pub(super) const fn length(&self) -> u64 {
        self.length
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn read(&mut self) -> Result<Vec<u8>, IntakeDefect> {
        let (bytes, digest) = read_exact_snapshot(&self.file, &self.path, self.length)?;
        self.digest = Some(digest);
        Ok(bytes)
    }

    pub(super) fn revalidate(&self) -> Result<(), IntakeDefect> {
        let Some(expected_digest) = self.digest.as_deref() else {
            return Ok(());
        };
        let (_, digest) = read_exact_snapshot(&self.file, &self.path, self.length)?;
        if expected_digest != digest {
            return Err(IntakeDefect::unverifiable(format!(
                "result receipt changed during verification: {}",
                self.path.display()
            )));
        }
        self.file.verify_path_binding().map_err(|error| {
            IntakeDefect::unverifiable(format!(
                "revalidate result receipt {}: {error}",
                self.path.display()
            ))
        })?;
        let current_parent = std::fs::canonicalize(&self.original_parent).map_err(|error| {
            IntakeDefect::unverifiable(format!(
                "revalidate result receipt root {}: {error}",
                self.original_parent.display()
            ))
        })?;
        if current_parent != self.canonical_parent {
            return Err(IntakeDefect::unverifiable(format!(
                "result receipt root changed during verification: {}",
                self.original_parent.display()
            )));
        }
        Ok(())
    }
}

fn receipt_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn read_exact_snapshot(
    held: &VerificationFile,
    path: &Path,
    expected_length: u64,
) -> Result<(Vec<u8>, String), IntakeDefect> {
    let mut file = held.try_clone_std().map_err(|error| {
        IntakeDefect::unverifiable(format!("clone {}: {error}", path.display()))
    })?;
    let metadata = file.metadata().map_err(|error| {
        IntakeDefect::unverifiable(format!("inspect {}: {error}", path.display()))
    })?;
    if !metadata.is_file() || metadata.len() != expected_length {
        return Err(IntakeDefect::unverifiable(format!(
            "result receipt size or file type changed: {}",
            path.display()
        )));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| IntakeDefect::unverifiable(format!("seek {}: {error}", path.display())))?;

    let capacity = usize::try_from(expected_length).map_err(|error| {
        IntakeDefect::unverifiable(format!(
            "represent receipt size {}: {error}",
            path.display()
        ))
    })?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(capacity).map_err(|error| {
        IntakeDefect::unverifiable(format!("reserve receipt {}: {error}", path.display()))
    })?;
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            IntakeDefect::unverifiable(format!("read {}: {error}", path.display()))
        })?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(|error| {
                IntakeDefect::unverifiable(format!("convert receipt read length: {error}"))
            })?)
            .ok_or_else(|| IntakeDefect::unverifiable("receipt read length overflowed"))?;
        if total > expected_length || total > MAX_RECEIPT_BYTES {
            return Err(IntakeDefect::unverifiable(format!(
                "result receipt grew while being read: {}",
                path.display()
            )));
        }
        digest.update(&buffer[..read]);
        bytes.extend_from_slice(&buffer[..read]);
    }
    if total != expected_length {
        return Err(IntakeDefect::unverifiable(format!(
            "result receipt changed size while being read: {}",
            path.display()
        )));
    }
    Ok((bytes, format!("{:x}", digest.finalize())))
}

fn classify_open(path: &Path, error: &(dyn std::error::Error + 'static)) -> IntakeDefect {
    if error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        IntakeDefect::missing(format!("read {}: {error}", path.display()))
    } else {
        IntakeDefect::unverifiable(format!("open {}: {error}", path.display()))
    }
}

fn classify_io(path: &Path, error: &std::io::Error) -> IntakeDefect {
    if error.kind() == std::io::ErrorKind::NotFound {
        IntakeDefect::missing(format!("read {}: {error}", path.display()))
    } else {
        IntakeDefect::unverifiable(format!("open {}: {error}", path.display()))
    }
}

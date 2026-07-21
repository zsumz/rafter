//! Deterministic tar publication and bounded no-extraction readback.

use std::{
    collections::BTreeMap,
    error::Error,
    fs::{self, File},
    io::{Read, Write},
    path::Path,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use sha2::{Digest, Sha256};

use super::{
    manifest::canonical_name,
    model::{ArtifactSet, MAX_FILES, MAX_FILE_BYTES, MAX_TOTAL_BYTES},
    VerifierArchiveExpectation,
};

const MAX_ARCHIVE_BYTES: usize = MAX_TOTAL_BYTES + MAX_FILES * 1024 + 20 * 512;

pub(super) fn publish(set: &ArtifactSet, output: &Path) -> Result<String, Box<dyn Error>> {
    if output.exists() {
        return Err("verifier artifact archive output already exists".into());
    }
    let parent = output
        .parent()
        .ok_or("verifier artifact archive has no parent")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    {
        let mut builder = tar::Builder::new(temporary.as_file_mut());
        for (name, bytes) in set.files() {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_mode(0o444);
            header.set_uid(0);
            header.set_gid(0);
            header.set_mtime(0);
            header.set_size(u64::try_from(bytes.len())?);
            header.set_cksum();
            builder.append_data(&mut header, name, bytes.as_slice())?;
        }
        builder.finish()?;
    }
    temporary.as_file_mut().flush()?;
    temporary.as_file().sync_all()?;
    let persisted = temporary.persist_noclobber(output)?;
    persisted.sync_all()?;
    set_read_only(output)?;
    file_sha256(output)
}

pub(super) fn verify(
    archive: &Path,
    expected_archive_sha256: &str,
    expected_manifest_sha256: &str,
    expectation: &VerifierArchiveExpectation,
) -> Result<(), Box<dyn Error>> {
    let metadata = fs::symlink_metadata(archive)?;
    if !metadata.file_type().is_file() {
        return Err("downloaded verifier archive is not an exact regular file".into());
    }
    if metadata.len() > u64::try_from(MAX_ARCHIVE_BYTES)? {
        return Err("downloaded verifier archive exceeds its byte limit".into());
    }
    if file_sha256(archive)? != expected_archive_sha256 {
        return Err("downloaded verifier archive digest changed".into());
    }
    let mut files = BTreeMap::new();
    let mut total = 0_usize;
    for entry in tar::Archive::new(File::open(archive)?).entries()? {
        let mut entry = entry?;
        if !entry.header().entry_type().is_file() || files.len() >= MAX_FILES {
            return Err("verifier archive contains an unsupported entry".into());
        }
        if entry.header().mode()? != 0o444
            || entry.header().uid()? != 0
            || entry.header().gid()? != 0
            || entry.header().mtime()? != 0
        {
            return Err("verifier archive entry metadata is noncanonical".into());
        }
        let name = std::str::from_utf8(entry.path_bytes().as_ref())
            .ok()
            .filter(|name| canonical_name(name))
            .ok_or("verifier archive entry name is noncanonical")?
            .to_owned();
        let size = usize::try_from(entry.size())?;
        if size > MAX_FILE_BYTES {
            return Err("verifier archive entry exceeds its byte limit".into());
        }
        total = total
            .checked_add(size)
            .ok_or("verifier archive byte count overflow")?;
        if total > MAX_TOTAL_BYTES {
            return Err("verifier archive exceeds its aggregate byte limit".into());
        }
        let mut bytes = Vec::with_capacity(size);
        entry.read_to_end(&mut bytes)?;
        if bytes.len() != size || files.insert(name.clone(), bytes).is_some() {
            return Err(format!("verifier archive entry is incomplete or repeated: {name}").into());
        }
    }
    let manifests = files
        .keys()
        .filter(|name| name.starts_with("verifier-artifact-manifest-"))
        .cloned()
        .collect::<Vec<_>>();
    let [manifest] = manifests.as_slice() else {
        return Err("verifier archive does not contain exactly one manifest".into());
    };
    ArtifactSet::verify(files, manifest, expected_manifest_sha256, expectation)?;
    Ok(())
}

fn file_sha256(path: &Path) -> Result<String, Box<dyn Error>> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[cfg(unix)]
fn set_read_only(path: &Path) -> Result<(), std::io::Error> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o444))
}

#[cfg(not(unix))]
fn set_read_only(path: &Path) -> Result<(), std::io::Error> {
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions)
}

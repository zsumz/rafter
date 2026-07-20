//! Immutable, content-addressed producer image publication and re-execution.

use std::{
    env,
    error::Error,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};

pub(crate) const PRODUCER_BINDING: &str = "immutable-reexec-v1";
const PRODUCER_DIGEST_ENV: &str = "RAFTER_INVARIANT_PRODUCER_SHA256";

/// Re-executes producer commands from an immutable, content-addressed image.
///
/// # Errors
///
/// Returns an error when the executable cannot be captured, safely published,
/// verified, or re-executed.
pub fn ensure_immutable_producer() -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(".")?;
    let program = fs::canonicalize(env::current_exe()?)?;
    let bytes = fs::read(&program)?;
    let digest = sha256(&bytes);
    let image_root = image_root(&root);

    match env::var(PRODUCER_DIGEST_ENV) {
        Ok(expected) => verify_managed_image(&root, &program, &bytes, &expected),
        Err(env::VarError::NotPresent) => {
            if program.starts_with(&image_root) {
                return Err(
                    "managed producer image was invoked without its re-exec binding".into(),
                );
            }
            let image = publish_image(&root, &bytes, &digest)?;
            reexec(&image, &digest)
        }
        Err(error) => Err(format!("read producer re-exec binding: {error}").into()),
    }
}

pub(crate) fn verify_capture(program: &Path, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(".")?;
    let expected = env::var(PRODUCER_DIGEST_ENV)
        .map_err(|error| format!("producer invocation omitted re-exec binding: {error}"))?;
    verify_managed_image(&root, program, bytes, &expected)
}

pub(crate) fn publish_content_addressed(
    path: &Path,
    bytes: &[u8],
    executable: bool,
) -> Result<Vec<u8>, Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::symlink_metadata(path) {
        Ok(_) => return adopt_existing(path, bytes, executable),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let parent = path
        .parent()
        .ok_or("content-addressed path has no parent")?;
    let mut temporary = None;
    for attempt in 0..100_u64 {
        let candidate = parent.join(format!(
            ".producer-image.tmp-{}-{attempt}",
            std::process::id()
        ));
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&candidate)
        {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    let (temporary, mut file) = temporary.ok_or("allocate producer image temporary file")?;
    let publish = (|| -> Result<(), Box<dyn Error>> {
        file.write_all(bytes)?;
        file.sync_all()?;
        set_mode(&temporary, executable)?;
        file.sync_all()?;
        drop(file);
        match fs::hard_link(&temporary, path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                adopt_existing(path, bytes, executable)?;
            }
            Err(error) => return Err(error.into()),
        }
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    let _ = fs::remove_file(&temporary);
    publish?;
    verify_published(path, bytes, executable)
}

fn publish_image(root: &Path, bytes: &[u8], digest: &str) -> Result<PathBuf, Box<dyn Error>> {
    let path = image_path(root, digest);
    publish_content_addressed(&path, bytes, true)?;
    Ok(path)
}

fn verify_managed_image(
    root: &Path,
    program: &Path,
    bytes: &[u8],
    expected_digest: &str,
) -> Result<(), Box<dyn Error>> {
    if !valid_sha256(expected_digest) {
        return Err("producer re-exec binding is not a SHA-256 digest".into());
    }
    let actual_digest = sha256(bytes);
    if actual_digest != expected_digest {
        return Err("producer re-exec digest does not match the running image".into());
    }
    let expected_path = image_path(root, expected_digest);
    if program != expected_path {
        return Err("producer re-exec path is not the managed content-addressed image".into());
    }
    verify_published(&expected_path, bytes, true)?;
    Ok(())
}

fn verify_published(
    path: &Path,
    expected: &[u8],
    executable: bool,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(format!(
            "content-addressed path is not a regular non-symlink file: {}",
            path.display()
        )
        .into());
    }
    let actual = fs::read(path)?;
    if actual != expected || sha256(&actual) != sha256(expected) {
        return Err(format!(
            "conflicting content at content-addressed path {}",
            path.display()
        )
        .into());
    }
    if !mode_matches(path, executable)? {
        return Err(format!(
            "content-addressed file has mutable or incorrect permissions: {}",
            path.display()
        )
        .into());
    }
    Ok(actual)
}

fn adopt_existing(
    path: &Path,
    expected: &[u8],
    executable: bool,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || fs::read(path)? != expected {
        return Err(format!(
            "conflicting content at content-addressed path {}",
            path.display()
        )
        .into());
    }
    set_mode(path, executable)?;
    verify_published(path, expected, executable)
}

fn image_root(root: &Path) -> PathBuf {
    root.join("target/rafter-invariants/producer-images")
}

pub(crate) fn image_path(root: &Path, digest: &str) -> PathBuf {
    image_root(root).join(digest).join("rafter-invariants")
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(unix)]
fn set_mode(path: &Path, executable: bool) -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt;

    let mode = if executable { 0o500 } else { 0o400 };
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, executable: bool) -> Result<(), Box<dyn Error>> {
    if executable {
        return Err("immutable producer re-exec requires Unix executable permissions".into());
    }
    Ok(())
}

#[cfg(unix)]
fn mode_matches(path: &Path, executable: bool) -> Result<bool, Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt;

    let expected = if executable { 0o500 } else { 0o400 };
    Ok(fs::symlink_metadata(path)?.permissions().mode() & 0o777 == expected)
}

#[cfg(not(unix))]
fn mode_matches(_path: &Path, executable: bool) -> Result<bool, Box<dyn Error>> {
    Ok(!executable)
}

#[cfg(unix)]
fn reexec(image: &Path, digest: &str) -> Result<(), Box<dyn Error>> {
    use std::os::unix::process::CommandExt;

    let error = Command::new(image)
        .args(env::args_os().skip(1))
        .env(PRODUCER_DIGEST_ENV, digest)
        .exec();
    Err(format!("re-exec immutable producer image: {error}").into())
}

#[cfg(not(unix))]
fn reexec(_image: &Path, _digest: &str) -> Result<(), Box<dyn Error>> {
    Err("immutable producer re-exec is supported on Linux and macOS".into())
}

#[cfg(test)]
mod tests;

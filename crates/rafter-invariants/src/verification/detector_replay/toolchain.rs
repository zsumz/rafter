//! Descriptor-held Cargo and rustc capabilities for verifier-owned replay.

use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};

use sha2::{Digest, Sha256};

use crate::verification::source::AuthenticatedCompilationSource;

pub(super) struct ReplayToolchain {
    cargo_path: PathBuf,
    cargo_sha256: String,
    rustc: BoundRustc,
}

struct BoundRustc {
    path: PathBuf,
    file: fs::File,
    sha256: String,
    #[cfg(unix)]
    descriptor: OwnedFd,
    child_path: PathBuf,
}

impl ReplayToolchain {
    pub(super) fn bind(source: &AuthenticatedCompilationSource<'_>) -> Result<Self, String> {
        source.revalidate()?;
        let rustc = BoundRustc::bind(source.rustc_program(), source.rustc_sha256())?;
        let toolchain = Self {
            cargo_path: source.cargo_program().to_owned(),
            cargo_sha256: source.cargo_sha256().to_owned(),
            rustc,
        };
        toolchain.revalidate(source)?;
        Ok(toolchain)
    }

    pub(super) fn cargo_path(&self) -> &Path {
        &self.cargo_path
    }

    pub(super) fn cargo_sha256(&self) -> &str {
        &self.cargo_sha256
    }

    pub(super) fn rustc_child_path(&self) -> &Path {
        &self.rustc.child_path
    }

    #[cfg(unix)]
    pub(super) fn rustc_descriptor(&self) -> BorrowedFd<'_> {
        self.rustc.descriptor.as_fd()
    }

    pub(super) fn revalidate(
        &self,
        source: &AuthenticatedCompilationSource<'_>,
    ) -> Result<(), String> {
        source.revalidate()?;
        if source.cargo_program() != self.cargo_path || source.cargo_sha256() != self.cargo_sha256 {
            return Err("detector replay Cargo identity changed".to_owned());
        }
        self.rustc
            .revalidate(source.rustc_program(), source.rustc_sha256())
    }
}

impl BoundRustc {
    fn bind(path: &Path, expected_sha256: &str) -> Result<Self, String> {
        let canonical = fs::canonicalize(path)
            .map_err(|error| format!("canonicalize replay rustc: {error}"))?;
        if canonical != path {
            return Err("replay rustc path is not canonical".to_owned());
        }
        let file = fs::File::open(&canonical)
            .map_err(|error| format!("open replay rustc capability: {error}"))?;
        if !file
            .metadata()
            .map_err(|error| format!("inspect replay rustc: {error}"))?
            .is_file()
        {
            return Err("replay rustc is not a regular file".to_owned());
        }
        let sha256 = file_sha256(&file)?;
        if sha256 != expected_sha256 {
            return Err(
                "opened replay rustc digest does not match authenticated source".to_owned(),
            );
        }
        #[cfg(unix)]
        let descriptor = rustix::io::fcntl_dupfd_cloexec(&file, 3)
            .map_err(|error| format!("duplicate replay rustc descriptor: {error}"))?;
        #[cfg(target_os = "linux")]
        let child_path = PathBuf::from(format!("/proc/self/fd/{}", descriptor.as_raw_fd()));
        #[cfg(all(unix, not(target_os = "linux")))]
        let child_path = PathBuf::from(format!("/dev/fd/{}", descriptor.as_raw_fd()));
        #[cfg(not(unix))]
        let child_path = canonical.clone();
        Ok(Self {
            path: canonical,
            file,
            sha256,
            #[cfg(unix)]
            descriptor,
            child_path,
        })
    }

    fn revalidate(&self, expected_path: &Path, expected_sha256: &str) -> Result<(), String> {
        let canonical = fs::canonicalize(expected_path)
            .map_err(|error| format!("recanonicalize replay rustc: {error}"))?;
        let held_sha256 = file_sha256(&self.file)?;
        let path_sha256 = crate::provenance::source::file_sha256(&canonical)
            .map_err(|error| error.to_string())?;
        if canonical != self.path
            || held_sha256 != self.sha256
            || path_sha256 != self.sha256
            || expected_sha256 != self.sha256
        {
            return Err("detector replay rustc path or bytes changed".to_owned());
        }
        Ok(())
    }
}

fn file_sha256(file: &fs::File) -> Result<String, String> {
    let mut file = file
        .try_clone()
        .map_err(|error| format!("clone replay rustc capability: {error}"))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|error| format!("rewind replay rustc capability: {error}"))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("hash replay rustc capability: {error}"))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

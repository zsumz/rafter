//! Bounded PEM-file convenience for explicit leaf-certificate mappings.

use std::{
    error::Error,
    fmt,
    fs::File,
    io::{Read, Take},
    path::{Path, PathBuf},
};

use rustls::pki_types::{pem::PemObject, CertificateDer};

use crate::{CertificateDirectoryBuilder, CertificateDirectoryError, PeerId};

/// Maximum bytes read from one explicit certificate PEM file.
pub const MAX_CERTIFICATE_PEM_BYTES: usize = 1024 * 1024;

/// Failure while loading one explicit leaf certificate from PEM.
#[derive(Debug)]
#[non_exhaustive]
pub enum CertificatePemError {
    /// The configured file could not be opened or read.
    Read {
        /// Configured certificate path.
        path: PathBuf,
        /// Filesystem failure.
        source: std::io::Error,
    },
    /// The configured file exceeded the finite PEM input bound.
    TooLarge {
        /// Configured certificate path.
        path: PathBuf,
        /// Maximum accepted bytes.
        maximum: usize,
    },
    /// The PEM stream could not be parsed.
    Parse {
        /// Configured certificate path.
        path: PathBuf,
        /// PEM parser failure.
        source: rustls::pki_types::pem::Error,
    },
    /// The PEM stream contained no certificate.
    Empty {
        /// Configured certificate path.
        path: PathBuf,
    },
    /// The loaded leaf could not be installed in the bounded directory.
    Directory {
        /// Directory construction failure.
        source: CertificateDirectoryError,
    },
}

impl fmt::Display for CertificatePemError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, source } => write!(
                formatter,
                "could not read certificate PEM {}: {source}",
                path.display()
            ),
            Self::TooLarge { path, maximum } => write!(
                formatter,
                "certificate PEM {} exceeds the {maximum}-byte limit",
                path.display()
            ),
            Self::Parse { path, source } => write!(
                formatter,
                "could not parse certificate PEM {}: {source}",
                path.display()
            ),
            Self::Empty { path } => write!(
                formatter,
                "certificate PEM {} contains no certificate",
                path.display()
            ),
            Self::Directory { source } => {
                write!(formatter, "could not map certificate leaf: {source}")
            }
        }
    }
}

impl Error for CertificatePemError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. } => Some(source),
            Self::Parse { source, .. } => Some(source),
            Self::Directory { source } => Some(source),
            Self::TooLarge { .. } | Self::Empty { .. } => None,
        }
    }
}

impl CertificateDirectoryBuilder {
    /// Maps the first certificate in one bounded PEM file to a stable principal.
    ///
    /// The first certificate is the leaf, matching the ordering required by
    /// [`crate::TlsIdentity`]. Following certificates are intermediates and do
    /// not participate in the explicit leaf-fingerprint lookup.
    ///
    /// # Errors
    ///
    /// Returns [`CertificatePemError`] when the file cannot be read or parsed,
    /// exceeds [`MAX_CERTIFICATE_PEM_BYTES`], contains no certificate, or
    /// violates a directory bound.
    pub fn map_pem_certificate_file(
        self,
        path: impl AsRef<Path>,
        peer_id: PeerId,
    ) -> Result<Self, CertificatePemError> {
        let path = path.as_ref().to_path_buf();
        let bytes = read_bounded(&path)?;
        let leaf = CertificateDer::pem_slice_iter(&bytes)
            .next()
            .transpose()
            .map_err(|source| CertificatePemError::Parse {
                path: path.clone(),
                source,
            })?
            .ok_or_else(|| CertificatePemError::Empty { path: path.clone() })?;
        self.map_certificate(leaf.as_ref(), peer_id)
            .map_err(|source| CertificatePemError::Directory { source })
    }
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, CertificatePemError> {
    let file = File::open(path).map_err(|source| CertificatePemError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let limit = u64::try_from(MAX_CERTIFICATE_PEM_BYTES).unwrap_or(u64::MAX);
    let mut reader: Take<File> = file.take(limit + 1);
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|source| CertificatePemError::Read {
            path: path.to_path_buf(),
            source,
        })?;
    if bytes.len() > MAX_CERTIFICATE_PEM_BYTES {
        return Err(CertificatePemError::TooLarge {
            path: path.to_path_buf(),
            maximum: MAX_CERTIFICATE_PEM_BYTES,
        });
    }
    Ok(bytes)
}

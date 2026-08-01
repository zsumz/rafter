//! Finite PEM input and parsing for one local TLS identity.

use std::{
    fs::File,
    io::{Read, Take},
    path::Path,
};

use rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    RootCertStore,
};

use super::{TlsIdentityError, TlsIdentityFile};

/// Maximum bytes read from one local certificate-chain PEM file.
pub const MAX_CERTIFICATE_CHAIN_PEM_BYTES: usize = 1024 * 1024;
/// Maximum bytes read from one local private-key PEM file.
pub const MAX_PRIVATE_KEY_PEM_BYTES: usize = 256 * 1024;
/// Maximum bytes read from one trust-root PEM file.
pub const MAX_TRUST_ROOTS_PEM_BYTES: usize = 4 * 1024 * 1024;

pub(super) fn read_identity_file(
    field: TlsIdentityFile,
    path: &Path,
) -> Result<Vec<u8>, TlsIdentityError> {
    let maximum = maximum_bytes(field);
    let file = File::open(path).map_err(|source| TlsIdentityError::ReadFile {
        field,
        path: path.to_path_buf(),
        source,
    })?;
    let limit = u64::try_from(maximum).unwrap_or(u64::MAX);
    let mut reader: Take<File> = file.take(limit + 1);
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|source| TlsIdentityError::ReadFile {
            field,
            path: path.to_path_buf(),
            source,
        })?;
    if bytes.len() > maximum {
        return Err(TlsIdentityError::FileTooLarge {
            field,
            path: path.to_path_buf(),
            maximum,
        });
    }
    Ok(bytes)
}

pub(super) fn parse_certificates(
    field: TlsIdentityFile,
    input: &[u8],
) -> Result<Vec<CertificateDer<'static>>, TlsIdentityError> {
    CertificateDer::pem_slice_iter(input)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsIdentityError::ParsePem { field, source })
}

pub(super) fn parse_private_key(input: &[u8]) -> Result<PrivateKeyDer<'static>, TlsIdentityError> {
    let mut keys = PrivateKeyDer::pem_slice_iter(input)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsIdentityError::ParsePem {
            field: TlsIdentityFile::PrivateKey,
            source,
        })?;
    match keys.len() {
        0 => Err(TlsIdentityError::MissingPrivateKey),
        1 => keys.pop().ok_or(TlsIdentityError::MissingPrivateKey),
        _ => Err(TlsIdentityError::MultiplePrivateKeys),
    }
}

pub(super) fn build_root_store(
    certificates: Vec<CertificateDer<'static>>,
) -> Result<RootCertStore, TlsIdentityError> {
    let mut roots = RootCertStore::empty();
    for (index, certificate) in certificates.into_iter().enumerate() {
        roots
            .add(certificate)
            .map_err(|source| TlsIdentityError::InvalidTrustRoot { index, source })?;
    }
    Ok(roots)
}

const fn maximum_bytes(field: TlsIdentityFile) -> usize {
    match field {
        TlsIdentityFile::CertificateChain => MAX_CERTIFICATE_CHAIN_PEM_BYTES,
        TlsIdentityFile::PrivateKey => MAX_PRIVATE_KEY_PEM_BYTES,
        TlsIdentityFile::TrustRoots => MAX_TRUST_ROOTS_PEM_BYTES,
    }
}

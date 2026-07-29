//! Version-1 current-snapshot manifest grammar and filename validation.
//!
//! This module owns RFSM framing, the selected immutable snapshot file name,
//! checksum mapping, and the plain-file-name restriction. Reading the manifest
//! file and choosing its path remain snapshot-store responsibilities.

use std::{error::Error, fmt};

use crate::format::{
    finish_checksummed, verify_checksum, ChecksumError, CursorError, Reader, Writer,
};

pub(super) const SNAPSHOT_MANIFEST_MAGIC: [u8; 4] = *b"RFSM";
pub(super) const SNAPSHOT_MANIFEST_VERSION: u8 = 1;

/// Errors returned while encoding the current-snapshot manifest.
///
/// This enum is exhaustive because manifest encoding can currently fail only
/// when the generated file name exceeds the manifest length prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftSnapshotManifestEncodeError {
    /// The immutable snapshot file name exceeds the manifest length prefix.
    FileNameTooLong {
        /// Encoded file-name length in bytes.
        len: usize,
    },
}

/// Errors returned while decoding the current-snapshot manifest.
///
/// This enum is exhaustive because the manifest format is closed over these
/// corruption and format failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftSnapshotManifestDecodeError {
    /// The manifest ended before the requested field could be read.
    UnexpectedEof {
        /// Bytes required by the field.
        needed: usize,
        /// Bytes remaining in the manifest.
        remaining: usize,
    },
    /// The manifest magic was not the version-1 RFSM marker.
    InvalidMagic([u8; 4]),
    /// The manifest version is not supported.
    UnsupportedVersion(u8),
    /// The manifest checksum did not match its bytes.
    ManifestChecksumMismatch {
        /// Checksum stored in the manifest.
        expected: u32,
        /// Checksum computed from the manifest bytes.
        actual: u32,
    },
    /// The selected snapshot file name was not valid UTF-8.
    InvalidFileNameUtf8,
    /// The selected snapshot file name was not a safe plain file name.
    InvalidFileName,
    /// Valid manifest bytes were followed by unused trailing bytes.
    TrailingBytes(usize),
}

impl fmt::Display for RaftSnapshotManifestEncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FileNameTooLong { len } => write!(
                formatter,
                "Raft snapshot manifest file name with length {len} does not fit in the manifest format"
            ),
        }
    }
}

impl Error for RaftSnapshotManifestEncodeError {}

impl fmt::Display for RaftSnapshotManifestDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                formatter,
                "Raft snapshot manifest needs {needed} bytes but only {remaining} remain"
            ),
            Self::InvalidMagic(magic) => write!(
                formatter,
                "Raft snapshot manifest magic {magic:02x?} is not RFSM"
            ),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "Raft snapshot manifest version {version} is not supported"
            ),
            Self::ManifestChecksumMismatch { expected, actual } => write!(
                formatter,
                "Raft snapshot manifest stored checksum {expected:#010x} does not match computed checksum {actual:#010x}"
            ),
            Self::InvalidFileNameUtf8 => {
                formatter.write_str("Raft snapshot manifest file name is not valid utf-8")
            }
            Self::InvalidFileName => {
                formatter.write_str("Raft snapshot manifest file name is not a plain file name")
            }
            Self::TrailingBytes(remaining) => write!(
                formatter,
                "Raft snapshot manifest has {remaining} trailing bytes"
            ),
        }
    }
}

impl Error for RaftSnapshotManifestDecodeError {}

impl From<CursorError> for RaftSnapshotManifestDecodeError {
    fn from(error: CursorError) -> Self {
        match error {
            CursorError::UnexpectedEof { needed, remaining } => {
                Self::UnexpectedEof { needed, remaining }
            }
            CursorError::TrailingBytes(remaining) => Self::TrailingBytes(remaining),
        }
    }
}

impl From<ChecksumError> for RaftSnapshotManifestDecodeError {
    fn from(error: ChecksumError) -> Self {
        match error {
            ChecksumError::UnexpectedEof { needed, remaining } => {
                Self::UnexpectedEof { needed, remaining }
            }
            ChecksumError::Mismatch { expected, actual } => {
                Self::ManifestChecksumMismatch { expected, actual }
            }
        }
    }
}

/// Current-snapshot manifest value selected by the RFSM file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotManifest {
    pub(crate) sequence: u64,
    pub(crate) file_name: String,
}

/// Encodes one canonical RFSM manifest.
///
/// # Errors
///
/// Returns [`RaftSnapshotManifestEncodeError::FileNameTooLong`] when the plain
/// file name does not fit the version-1 u16 length prefix.
pub(crate) fn encode_manifest(
    manifest: &SnapshotManifest,
) -> Result<Vec<u8>, RaftSnapshotManifestEncodeError> {
    let file_name = manifest.file_name.as_bytes();
    let file_name_len = u16::try_from(file_name.len()).map_err(|_| {
        RaftSnapshotManifestEncodeError::FileNameTooLong {
            len: file_name.len(),
        }
    })?;

    let mut writer = Writer::new();
    writer.bytes(&SNAPSHOT_MANIFEST_MAGIC);
    writer.u8(SNAPSHOT_MANIFEST_VERSION);
    writer.u64(manifest.sequence);
    writer.u16(file_name_len);
    writer.bytes(file_name);
    Ok(finish_checksummed(writer))
}

/// Decodes one strict RFSM manifest.
///
/// # Errors
///
/// Returns [`RaftSnapshotManifestDecodeError`] when framing, checksum, UTF-8,
/// filename, or trailing-byte validation fails.
pub(crate) fn decode_manifest(
    input: &[u8],
) -> Result<SnapshotManifest, RaftSnapshotManifestDecodeError> {
    let body = verify_checksum(input)?;
    let mut reader = Reader::new(body);
    let magic = reader.magic()?;
    if magic != SNAPSHOT_MANIFEST_MAGIC {
        return Err(RaftSnapshotManifestDecodeError::InvalidMagic(magic));
    }
    let version = reader.u8()?;
    if version != SNAPSHOT_MANIFEST_VERSION {
        return Err(RaftSnapshotManifestDecodeError::UnsupportedVersion(version));
    }
    let sequence = reader.u64()?;
    let file_name_len = usize::from(reader.u16()?);
    let file_name_bytes = reader.take(file_name_len)?;
    let file_name = std::str::from_utf8(file_name_bytes)
        .map_err(|_| RaftSnapshotManifestDecodeError::InvalidFileNameUtf8)?
        .to_owned();
    validate_manifest_file_name(&file_name)?;
    reader.finish()?;
    Ok(SnapshotManifest {
        sequence,
        file_name,
    })
}

fn validate_manifest_file_name(file_name: &str) -> Result<(), RaftSnapshotManifestDecodeError> {
    if file_name.is_empty()
        || file_name.contains('/')
        || file_name.contains('\\')
        || file_name == "."
        || file_name == ".."
    {
        return Err(RaftSnapshotManifestDecodeError::InvalidFileName);
    }
    Ok(())
}

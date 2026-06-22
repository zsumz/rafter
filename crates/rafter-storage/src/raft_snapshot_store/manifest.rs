use std::{
    error::Error,
    fmt,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use crate::crc32;

use super::OpenRaftSnapshotStoreError;

const SNAPSHOT_MANIFEST_MAGIC: [u8; 4] = *b"RFSM";
const SNAPSHOT_MANIFEST_VERSION: u8 = 1;
const SNAPSHOT_MANIFEST_CHECKSUM_LEN: usize = 4;

/// Errors returned while encoding the current-snapshot manifest.
///
/// This enum is exhaustive because manifest encoding can currently fail only
/// when the generated file name exceeds the manifest length prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftSnapshotManifestEncodeError {
    FileNameTooLong { len: usize },
}

/// Errors returned while decoding the current-snapshot manifest.
///
/// This enum is exhaustive because the manifest format is closed over these
/// corruption and format failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftSnapshotManifestDecodeError {
    UnexpectedEof { needed: usize, remaining: usize },
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    ManifestChecksumMismatch { expected: u32, actual: u32 },
    InvalidFileNameUtf8,
    InvalidFileName,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SnapshotManifest {
    pub(super) sequence: u64,
    pub(super) file_name: String,
}

pub(super) fn read_manifest(
    manifest_path: &Path,
) -> Result<Option<SnapshotManifest>, OpenRaftSnapshotStoreError> {
    if !manifest_path.exists() {
        return Ok(None);
    }
    let mut file = File::open(manifest_path).map_err(|error| OpenRaftSnapshotStoreError::Io {
        operation: "open raft snapshot manifest",
        path: manifest_path.to_path_buf(),
        message: error.to_string(),
    })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| OpenRaftSnapshotStoreError::Io {
            operation: "read raft snapshot manifest",
            path: manifest_path.to_path_buf(),
            message: error.to_string(),
        })?;
    decode_manifest(&bytes)
        .map(Some)
        .map_err(OpenRaftSnapshotStoreError::Manifest)
}

pub(super) fn manifest_path(directory: &Path) -> PathBuf {
    directory.join("current.snapshot")
}

pub(super) fn encode_manifest(
    manifest: &SnapshotManifest,
) -> Result<Vec<u8>, RaftSnapshotManifestEncodeError> {
    let file_name = manifest.file_name.as_bytes();
    let file_name_len = u16::try_from(file_name.len()).map_err(|_| {
        RaftSnapshotManifestEncodeError::FileNameTooLong {
            len: file_name.len(),
        }
    })?;

    let mut body = Vec::new();
    body.extend_from_slice(&SNAPSHOT_MANIFEST_MAGIC);
    body.push(SNAPSHOT_MANIFEST_VERSION);
    body.extend_from_slice(&manifest.sequence.to_be_bytes());
    body.extend_from_slice(&file_name_len.to_be_bytes());
    body.extend_from_slice(file_name);
    let checksum = crc32(&body);
    body.extend_from_slice(&checksum.to_be_bytes());
    Ok(body)
}

fn decode_manifest(input: &[u8]) -> Result<SnapshotManifest, RaftSnapshotManifestDecodeError> {
    if input.len() < SNAPSHOT_MANIFEST_CHECKSUM_LEN {
        return Err(RaftSnapshotManifestDecodeError::UnexpectedEof {
            needed: SNAPSHOT_MANIFEST_CHECKSUM_LEN,
            remaining: input.len(),
        });
    }
    let checksum_offset = input.len() - SNAPSHOT_MANIFEST_CHECKSUM_LEN;
    let body = &input[..checksum_offset];
    let checksum_bytes = &input[checksum_offset..];
    let expected = u32::from_be_bytes([
        checksum_bytes[0],
        checksum_bytes[1],
        checksum_bytes[2],
        checksum_bytes[3],
    ]);
    let actual = crc32(body);
    if actual != expected {
        return Err(RaftSnapshotManifestDecodeError::ManifestChecksumMismatch { expected, actual });
    }

    let mut reader = ManifestReader::new(body);
    let magic = reader.magic()?;
    if magic != SNAPSHOT_MANIFEST_MAGIC {
        return Err(RaftSnapshotManifestDecodeError::InvalidMagic(magic));
    }
    let version = reader.u8()?;
    if version != SNAPSHOT_MANIFEST_VERSION {
        return Err(RaftSnapshotManifestDecodeError::UnsupportedVersion(version));
    }
    let sequence = reader.u64()?;
    let file_name_len = reader.u16()? as usize;
    let file_name_bytes = reader.take(file_name_len)?;
    let file_name = std::str::from_utf8(file_name_bytes)
        .map_err(|_| RaftSnapshotManifestDecodeError::InvalidFileNameUtf8)?
        .to_string();
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

struct ManifestReader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> ManifestReader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn finish(&self) -> Result<(), RaftSnapshotManifestDecodeError> {
        let remaining = self.input.len() - self.offset;
        if remaining == 0 {
            Ok(())
        } else {
            Err(RaftSnapshotManifestDecodeError::TrailingBytes(remaining))
        }
    }

    fn magic(&mut self) -> Result<[u8; 4], RaftSnapshotManifestDecodeError> {
        let bytes = self.take(4)?;
        Ok([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    fn u8(&mut self) -> Result<u8, RaftSnapshotManifestDecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, RaftSnapshotManifestDecodeError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u64(&mut self) -> Result<u64, RaftSnapshotManifestDecodeError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], RaftSnapshotManifestDecodeError> {
        let remaining = self.input.len() - self.offset;
        if remaining < len {
            return Err(RaftSnapshotManifestDecodeError::UnexpectedEof {
                needed: len,
                remaining,
            });
        }
        let start = self.offset;
        self.offset += len;
        Ok(&self.input[start..self.offset])
    }
}

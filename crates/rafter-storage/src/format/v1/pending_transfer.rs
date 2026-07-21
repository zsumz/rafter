//! Version-1 pending snapshot-transfer manifest grammar.
//!
//! This module owns RFPT framing, the nested metadata-only RFSN envelope, body
//! progress fields, checksum mapping, and descriptor validation. Staging-body
//! I/O, resumability policy, and crash cleanup remain snapshot-store concerns.

use std::{error::Error, fmt};

use rafter::{NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotTransferId};

use super::snapshot::{
    decode_raft_snapshot, encode_raft_snapshot_metadata_envelope, DecodeRaftSnapshotError,
    EncodeRaftSnapshotError,
};
use crate::format::{
    finish_checksummed, verify_checksum, ChecksumError, CursorError, Reader, Writer,
};

pub(crate) const PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC: [u8; 4] = *b"RFPT";
pub(crate) const PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION: u8 = 1;

/// The largest accepted nested RFSN metadata envelope.
///
/// Version 1 can encode just over 2 MiB of joint-membership metadata at its
/// maximum u16 member counts. Four MiB leaves deliberate headroom while
/// preventing a corrupt manifest from smuggling arbitrary payload bytes inside
/// the metadata-only nested snapshot envelope.
pub(crate) const MAX_PENDING_SNAPSHOT_METADATA_ENVELOPE_BYTES: u64 = 4 * 1024 * 1024;

/// Durable descriptor of one resumable inbound snapshot transfer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PendingTransferManifest {
    pub(crate) leader_id: NodeId,
    pub(crate) transfer_id: SnapshotTransferId,
    pub(crate) metadata: RaftSnapshotMetadata,
    pub(crate) total_payload_len: u64,
    pub(crate) application_payload_crc32: u32,
    pub(crate) received_payload_len: u64,
    pub(crate) body_checksum: u32,
}

/// Errors returned while decoding pending snapshot-transfer staging metadata.
///
/// This enum is exhaustive because the current manifest format is closed over
/// these envelope, nested-snapshot, and descriptor-validation failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodePendingSnapshotTransferError {
    UnexpectedEof {
        needed: usize,
        remaining: usize,
    },
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    EnvelopeChecksumMismatch {
        expected: u32,
        actual: u32,
    },
    SnapshotEnvelopeTooLarge {
        len: u64,
    },
    SnapshotEnvelopePayloadNotEmpty {
        len: usize,
    },
    Snapshot(DecodeRaftSnapshotError),
    ReceivedPayloadTooLong {
        received_bytes: u64,
        total_payload_len: u64,
    },
    TransferIdMismatch {
        expected: SnapshotTransferId,
        actual: SnapshotTransferId,
    },
    TrailingBytes(usize),
}

impl fmt::Display for DecodePendingSnapshotTransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                formatter,
                "pending Raft snapshot transfer manifest needs {needed} bytes but only {remaining} remain"
            ),
            Self::InvalidMagic(magic) => write!(
                formatter,
                "pending Raft snapshot transfer manifest magic {magic:02x?} is not RFPT"
            ),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "pending Raft snapshot transfer manifest version {version} is not supported"
            ),
            Self::EnvelopeChecksumMismatch { expected, actual } => write!(
                formatter,
                "pending Raft snapshot transfer manifest stored checksum {expected:#010x} does not match computed checksum {actual:#010x}"
            ),
            Self::SnapshotEnvelopeTooLarge { len } => write!(
                formatter,
                "pending Raft snapshot transfer metadata envelope with length {len} exceeds the version-1 limit"
            ),
            Self::SnapshotEnvelopePayloadNotEmpty { len } => write!(
                formatter,
                "pending Raft snapshot transfer metadata envelope carries {len} unexpected application payload bytes"
            ),
            Self::Snapshot(error) => write!(
                formatter,
                "pending Raft snapshot transfer snapshot metadata is corrupt: {error}"
            ),
            Self::ReceivedPayloadTooLong {
                received_bytes,
                total_payload_len,
            } => write!(
                formatter,
                "pending Raft snapshot transfer received {received_bytes} bytes, more than the total payload length {total_payload_len}"
            ),
            Self::TransferIdMismatch { expected, actual } => write!(
                formatter,
                "pending Raft snapshot transfer id {actual} does not match descriptor-derived id {expected}"
            ),
            Self::TrailingBytes(remaining) => write!(
                formatter,
                "pending Raft snapshot transfer manifest has {remaining} trailing bytes"
            ),
        }
    }
}

impl Error for DecodePendingSnapshotTransferError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Snapshot(error) => Some(error),
            Self::UnexpectedEof { .. }
            | Self::InvalidMagic(_)
            | Self::UnsupportedVersion(_)
            | Self::EnvelopeChecksumMismatch { .. }
            | Self::SnapshotEnvelopeTooLarge { .. }
            | Self::SnapshotEnvelopePayloadNotEmpty { .. }
            | Self::ReceivedPayloadTooLong { .. }
            | Self::TransferIdMismatch { .. }
            | Self::TrailingBytes(_) => None,
        }
    }
}

impl From<CursorError> for DecodePendingSnapshotTransferError {
    fn from(error: CursorError) -> Self {
        match error {
            CursorError::UnexpectedEof { needed, remaining } => {
                Self::UnexpectedEof { needed, remaining }
            }
            CursorError::TrailingBytes(remaining) => Self::TrailingBytes(remaining),
        }
    }
}

impl From<ChecksumError> for DecodePendingSnapshotTransferError {
    fn from(error: ChecksumError) -> Self {
        match error {
            ChecksumError::UnexpectedEof { needed, remaining } => {
                Self::UnexpectedEof { needed, remaining }
            }
            ChecksumError::Mismatch { expected, actual } => {
                Self::EnvelopeChecksumMismatch { expected, actual }
            }
        }
    }
}

/// Encodes one canonical RFPT manifest.
///
/// # Errors
///
/// Returns [`EncodeRaftSnapshotError`] when the nested metadata-only RFSN
/// envelope cannot represent the snapshot metadata.
pub(crate) fn encode_pending_snapshot_transfer_manifest(
    manifest: &PendingTransferManifest,
) -> Result<Vec<u8>, EncodeRaftSnapshotError> {
    let metadata_envelope = encode_raft_snapshot_metadata_envelope(&manifest.metadata)?;

    let mut writer = Writer::new();
    writer.bytes(&PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC);
    writer.u8(PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION);
    writer.u64(manifest.leader_id.0);
    writer.u64(manifest.transfer_id.0);
    writer.u64(manifest.total_payload_len);
    writer.u32(manifest.application_payload_crc32);
    writer.u64(manifest.received_payload_len);
    writer.u32(manifest.body_checksum);
    writer.u64(metadata_envelope.len() as u64);
    writer.bytes(&metadata_envelope);
    Ok(finish_checksummed(writer))
}

/// Decodes one strict RFPT manifest and validates its snapshot descriptor.
///
/// # Errors
///
/// Returns [`DecodePendingSnapshotTransferError`] when framing, checksum,
/// nested-metadata, progress, identity, or trailing-byte validation fails.
pub(crate) fn decode_pending_snapshot_transfer_manifest(
    envelope: &[u8],
) -> Result<PendingTransferManifest, DecodePendingSnapshotTransferError> {
    let body = verify_checksum(envelope)?;
    let mut reader = Reader::new(body);
    let magic = reader.magic()?;
    if magic != PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC {
        return Err(DecodePendingSnapshotTransferError::InvalidMagic(magic));
    }
    let version = reader.u8()?;
    if version != PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION {
        return Err(DecodePendingSnapshotTransferError::UnsupportedVersion(
            version,
        ));
    }
    let leader_id = NodeId(reader.u64()?);
    let transfer_id = SnapshotTransferId(reader.u64()?);
    let total_payload_len = reader.u64()?;
    let application_payload_crc32 = reader.u32()?;
    let received_payload_len = reader.u64()?;
    let body_checksum = reader.u32()?;
    let metadata_envelope_len = reader.u64()?;
    if metadata_envelope_len > MAX_PENDING_SNAPSHOT_METADATA_ENVELOPE_BYTES {
        return Err(
            DecodePendingSnapshotTransferError::SnapshotEnvelopeTooLarge {
                len: metadata_envelope_len,
            },
        );
    }
    let metadata_envelope_len = usize::try_from(metadata_envelope_len).map_err(|_| {
        DecodePendingSnapshotTransferError::SnapshotEnvelopeTooLarge {
            len: metadata_envelope_len,
        }
    })?;
    let metadata_snapshot = decode_raft_snapshot(reader.take(metadata_envelope_len)?)
        .map_err(DecodePendingSnapshotTransferError::Snapshot)?;
    if !metadata_snapshot.application_payload.is_empty() {
        return Err(
            DecodePendingSnapshotTransferError::SnapshotEnvelopePayloadNotEmpty {
                len: metadata_snapshot.application_payload.len(),
            },
        );
    }
    reader.finish()?;

    if received_payload_len > total_payload_len {
        return Err(DecodePendingSnapshotTransferError::ReceivedPayloadTooLong {
            received_bytes: received_payload_len,
            total_payload_len,
        });
    }
    let expected_transfer_id = RaftSnapshot::new(
        metadata_snapshot.metadata.clone(),
        total_payload_len,
        application_payload_crc32,
    )
    .transfer_id();
    if transfer_id != expected_transfer_id {
        return Err(DecodePendingSnapshotTransferError::TransferIdMismatch {
            expected: expected_transfer_id,
            actual: transfer_id,
        });
    }

    Ok(PendingTransferManifest {
        leader_id,
        transfer_id,
        metadata: metadata_snapshot.metadata,
        total_payload_len,
        application_payload_crc32,
        received_payload_len,
        body_checksum,
    })
}

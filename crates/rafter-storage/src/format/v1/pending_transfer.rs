//! Version-1 pending snapshot-transfer manifest grammar.
//!
//! This module owns RFPT framing, the nested metadata-only RFSN envelope, body
//! progress fields, checksum mapping, and descriptor validation. Staging-body
//! I/O, resumability policy, and crash cleanup remain snapshot-store concerns.

use std::{error::Error, fmt};

use rafter::{NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotTransferId};

use super::snapshot::{decode_raft_snapshot, DecodeRaftSnapshotError};
use crate::format::{ChecksumError, CursorError, Reader};

mod encode;
pub(crate) use encode::encode_pending_snapshot_transfer_manifest;

pub(crate) const PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC: [u8; 4] = *b"RFPT";
pub(crate) const PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION: u8 = 1;
const MAX_METADATA_ENVELOPE_LEN: usize = 4 * 1024 * 1024;

/// The largest accepted nested RFSN metadata envelope.
///
/// Version 1 can encode just over 2 MiB of joint-membership metadata at its
/// maximum u16 member counts. Four MiB leaves deliberate headroom while
/// preventing a corrupt manifest from smuggling arbitrary payload bytes inside
/// the metadata-only nested snapshot envelope.
pub(crate) const MAX_PENDING_SNAPSHOT_METADATA_ENVELOPE_BYTES: u64 =
    MAX_METADATA_ENVELOPE_LEN as u64;
// The fixed RFPT header precedes the already-bounded nested metadata envelope.
pub(crate) const MAX_PENDING_SNAPSHOT_MANIFEST_BODY_LEN: usize = 53 + MAX_METADATA_ENVELOPE_LEN;

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
    /// The manifest ended before the requested field could be read.
    UnexpectedEof {
        /// Bytes required by the field.
        needed: usize,
        /// Bytes remaining in the manifest.
        remaining: usize,
    },
    /// The manifest magic was not the version-1 RFPT marker.
    InvalidMagic([u8; 4]),
    /// The manifest version is not supported.
    UnsupportedVersion(u8),
    /// The manifest checksum did not match its bytes.
    EnvelopeChecksumMismatch {
        /// Checksum stored in the manifest.
        expected: u32,
        /// Checksum computed from the manifest bytes.
        actual: u32,
    },
    /// The nested metadata envelope exceeds the defensive size limit.
    SnapshotEnvelopeTooLarge {
        /// Encoded nested-envelope size in bytes.
        len: u64,
    },
    /// The metadata-only nested snapshot unexpectedly carried payload bytes.
    SnapshotEnvelopePayloadNotEmpty {
        /// Unexpected payload length in bytes.
        len: usize,
    },
    /// The nested snapshot metadata envelope was invalid.
    Snapshot(DecodeRaftSnapshotError),
    /// Persisted progress exceeds the declared payload length.
    ReceivedPayloadTooLong {
        /// Bytes recorded as durably received.
        received_bytes: u64,
        /// Declared complete payload length.
        total_payload_len: u64,
    },
    /// The manifest's transfer identifier disagrees with its snapshot metadata.
    TransferIdMismatch {
        /// Transfer identifier derived from the snapshot metadata.
        expected: SnapshotTransferId,
        /// Transfer identifier stored in the manifest.
        actual: SnapshotTransferId,
    },
    /// Valid manifest bytes were followed by unused trailing bytes.
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

/// Decodes one strict RFPT manifest and validates its snapshot descriptor.
///
/// # Errors
///
/// Returns [`DecodePendingSnapshotTransferError`] when framing, checksum,
/// nested-metadata, progress, identity, or trailing-byte validation fails.
#[cfg(test)]
pub(crate) fn decode_pending_snapshot_transfer_manifest(
    envelope: &[u8],
) -> Result<PendingTransferManifest, DecodePendingSnapshotTransferError> {
    let body = crate::format::verify_checksum(envelope)?;
    decode_pending_manifest_body(body, 0)
}

pub(crate) fn decode_pending_manifest_body(
    body: &[u8],
    omitted_trailing_bytes: usize,
) -> Result<PendingTransferManifest, DecodePendingSnapshotTransferError> {
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
    // Streaming admission retains every possible field but can omit a suffix.
    // Keep this check before progress/identity validation, matching slice decode.
    let remaining = reader.remaining().saturating_add(omitted_trailing_bytes);
    if remaining != 0 {
        return Err(DecodePendingSnapshotTransferError::TrailingBytes(remaining));
    }

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

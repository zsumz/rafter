//! Version-1 persisted-snapshot envelope and payload framing.
//!
//! This module owns the RFSN magic, version, field order, payload bytes, and
//! the payload and envelope checksums that guard them.

use rafter::RaftSnapshotMetadata;

use crate::{
    checksum::crc32,
    format::{
        finish_checksummed,
        v1::snapshot_metadata::{decode_snapshot_metadata, encode_snapshot_metadata},
        verify_checksum, Reader, Writer,
    },
};

use super::{
    DecodeRaftSnapshotError, EncodeRaftSnapshotError, PersistedRaftSnapshot, SnapshotEnvelopeHeader,
};

/// Magic prefix for the persisted Raft snapshot envelope.
pub const RAFT_SNAPSHOT_MAGIC: [u8; 4] = *b"RFSN";
/// Current persisted Raft snapshot envelope version.
pub const RAFT_SNAPSHOT_VERSION: u8 = 1;

/// Encodes one Raft snapshot into a versioned, checksummed envelope.
///
/// Layout:
///
/// ```text
/// magic[4] | version[1] |
/// group_id_len[u16] | group_id |
/// writer_node_id[u64] |
/// last_included_index[u64] | last_included_term[u64] |
/// hard_state_term[u64] |
/// application_kind_len[u16] | application_kind |
/// application_version[u16] |
/// committed_configuration_present[u8] |
/// committed_configuration if present:
///   configuration_present[u8] |
///   configuration_index[u64] | configuration_id[u64] if present |
///   membership |
/// application_payload_len[u64] | application_payload |
/// application_payload_crc32[u32] |
/// envelope_crc32[u32]
/// ```
///
/// The application payload checksum covers only the opaque application payload.
/// The envelope checksum covers every byte before the envelope checksum field,
/// including the application payload checksum.
///
/// Both checksums are CRC32 accidental-corruption checks, not adversarial
/// integrity proofs.
///
/// # Errors
///
/// Returns [`EncodeRaftSnapshotError`] when snapshot metadata or membership
/// counts cannot be represented in the envelope format.
pub fn encode_raft_snapshot(
    snapshot: &PersistedRaftSnapshot,
) -> Result<Vec<u8>, EncodeRaftSnapshotError> {
    let header = encode_raft_snapshot_header(
        &snapshot.metadata,
        snapshot.application_payload.len() as u64,
    )?;
    let mut writer = Writer::new();
    writer.bytes(&header);
    writer.bytes(&snapshot.application_payload);
    writer.u32(crc32(&snapshot.application_payload));

    Ok(finish_checksummed(writer))
}

/// Encodes snapshot metadata as the metadata-only RFSN envelope embedded in an
/// RFPT pending-transfer manifest.
///
/// This intentionally preserves the version-1 nested-envelope bytes while
/// sharing the metadata field grammar with complete snapshot encoding.
pub(crate) fn encode_raft_snapshot_metadata_envelope(
    metadata: &RaftSnapshotMetadata,
) -> Result<Vec<u8>, EncodeRaftSnapshotError> {
    let header = encode_raft_snapshot_header(metadata, 0)?;
    let mut writer = Writer::new();
    writer.bytes(&header);
    writer.u32(crc32(&[]));
    Ok(finish_checksummed(writer))
}

/// Encodes the envelope prefix — every byte before the application payload —
/// at the current version. Streaming writers emit this, then the payload,
/// then the payload and envelope checksums.
///
/// # Errors
///
/// Returns [`EncodeRaftSnapshotError`] when snapshot metadata or membership
/// counts cannot be represented in the envelope format.
pub(crate) fn encode_raft_snapshot_header(
    metadata: &RaftSnapshotMetadata,
    payload_len: u64,
) -> Result<Vec<u8>, EncodeRaftSnapshotError> {
    let mut writer = Writer::new();
    writer.bytes(&RAFT_SNAPSHOT_MAGIC);
    writer.u8(RAFT_SNAPSHOT_VERSION);
    encode_snapshot_metadata(&mut writer, metadata)?;
    writer.u64(payload_len);
    Ok(writer.finish())
}

/// Decodes the envelope prefix from `prefix`, which must hold at least the
/// complete header (callers read a bounded prefix of the file; headers are
/// metadata-sized). Verifies no checksums — streaming readers verify the
/// payload and envelope checksums as they consume the payload bytes.
///
/// # Errors
///
/// Returns [`DecodeRaftSnapshotError`] when the header is malformed, uses an
/// unsupported version, or carries invalid typed metadata.
pub(crate) fn decode_raft_snapshot_header(
    prefix: &[u8],
) -> Result<SnapshotEnvelopeHeader, DecodeRaftSnapshotError> {
    let mut reader = Reader::new(prefix);
    let magic = reader.magic()?;
    if magic != RAFT_SNAPSHOT_MAGIC {
        return Err(DecodeRaftSnapshotError::InvalidMagic(magic));
    }

    let version = reader.u8()?;
    if version != RAFT_SNAPSHOT_VERSION {
        return Err(DecodeRaftSnapshotError::UnsupportedVersion(version));
    }

    let metadata = decode_snapshot_metadata(&mut reader)?;
    let payload_len = reader.u64()?;

    Ok(SnapshotEnvelopeHeader {
        metadata,
        payload_len,
        header_len: reader.position() as u64,
    })
}

/// Decodes and verifies one Raft snapshot envelope.
///
/// # Errors
///
/// Returns [`DecodeRaftSnapshotError`] when the envelope is malformed, uses an
/// unsupported version, fails checksum verification, or contains invalid typed
/// Raft snapshot metadata.
///
/// # Panics
///
/// Does not panic on any input: the internal length conversions are bounded
/// by the size of the in-memory envelope slice.
pub fn decode_raft_snapshot(
    envelope: &[u8],
) -> Result<PersistedRaftSnapshot, DecodeRaftSnapshotError> {
    let body = verify_checksum(envelope)?;
    let header = decode_raft_snapshot_header(body)?;
    let mut reader = Reader::new(body);
    let header_len =
        usize::try_from(header.header_len).map_err(|_| DecodeRaftSnapshotError::UnexpectedEof {
            needed: usize::MAX,
            remaining: body.len(),
        })?;
    reader.take(header_len)?;
    let remaining = body.len() - header_len;
    if header.payload_len > remaining as u64 {
        return Err(DecodeRaftSnapshotError::UnexpectedEof {
            needed: usize::try_from(header.payload_len).unwrap_or(usize::MAX),
            remaining,
        });
    }
    let payload_len = usize::try_from(header.payload_len).map_err(|_| {
        DecodeRaftSnapshotError::UnexpectedEof {
            needed: usize::MAX,
            remaining,
        }
    })?;
    let application_payload = reader.take(payload_len)?.to_vec();
    let expected_payload_checksum = reader.u32()?;
    let actual_payload_checksum = crc32(&application_payload);
    if expected_payload_checksum != actual_payload_checksum {
        return Err(DecodeRaftSnapshotError::PayloadChecksumMismatch {
            expected: expected_payload_checksum,
            actual: actual_payload_checksum,
        });
    }
    reader.finish()?;

    Ok(PersistedRaftSnapshot {
        metadata: header.metadata,
        application_payload,
    })
}

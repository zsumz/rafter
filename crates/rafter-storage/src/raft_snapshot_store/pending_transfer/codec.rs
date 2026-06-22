use crate::{
    crc32, decode_raft_snapshot, encode_raft_snapshot, EncodeRaftSnapshotError,
    PersistedRaftSnapshot,
};

use super::{
    constants::{
        PENDING_SNAPSHOT_TRANSFER_MANIFEST_CHECKSUM_LEN, PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC,
        PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION,
    },
    error::DecodePendingSnapshotTransferError,
    manifest::PendingTransferManifest,
};

pub(super) fn encode_pending_snapshot_transfer_manifest(
    manifest: &PendingTransferManifest,
) -> Result<Vec<u8>, EncodeRaftSnapshotError> {
    let metadata_envelope = encode_raft_snapshot(&PersistedRaftSnapshot {
        metadata: manifest.metadata.clone(),
        application_payload: Vec::new(),
    })?;

    let mut body = Vec::new();
    body.extend_from_slice(&PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC);
    body.push(PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION);
    body.extend_from_slice(&manifest.leader_id.0.to_be_bytes());
    body.extend_from_slice(&manifest.transfer_id.0.to_be_bytes());
    body.extend_from_slice(&manifest.total_payload_len.to_be_bytes());
    body.extend_from_slice(&manifest.application_payload_crc32.to_be_bytes());
    body.extend_from_slice(&manifest.received_payload_len.to_be_bytes());
    body.extend_from_slice(&manifest.body_checksum.to_be_bytes());
    body.extend_from_slice(&(metadata_envelope.len() as u64).to_be_bytes());
    body.extend_from_slice(&metadata_envelope);
    let checksum = crc32(&body);
    body.extend_from_slice(&checksum.to_be_bytes());
    Ok(body)
}

pub(super) fn decode_pending_snapshot_transfer_manifest(
    envelope: &[u8],
) -> Result<PendingTransferManifest, DecodePendingSnapshotTransferError> {
    let without_checksum_len = envelope
        .len()
        .checked_sub(PENDING_SNAPSHOT_TRANSFER_MANIFEST_CHECKSUM_LEN)
        .ok_or(DecodePendingSnapshotTransferError::UnexpectedEof {
            needed: PENDING_SNAPSHOT_TRANSFER_MANIFEST_CHECKSUM_LEN,
            remaining: envelope.len(),
        })?;
    let expected_checksum = {
        let checksum_bytes = &envelope[without_checksum_len..];
        u32::from_be_bytes([
            checksum_bytes[0],
            checksum_bytes[1],
            checksum_bytes[2],
            checksum_bytes[3],
        ])
    };
    let actual_checksum = crc32(&envelope[..without_checksum_len]);
    if expected_checksum != actual_checksum {
        return Err(
            DecodePendingSnapshotTransferError::EnvelopeChecksumMismatch {
                expected: expected_checksum,
                actual: actual_checksum,
            },
        );
    }

    let mut reader = PendingTransferReader::new(&envelope[..without_checksum_len]);
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
    let leader_id = rafter::NodeId(reader.u64()?);
    let transfer_id = rafter::SnapshotTransferId(reader.u64()?);
    let total_payload_len = reader.u64()?;
    let application_payload_crc32 = reader.u32()?;
    let received_payload_len = reader.u64()?;
    let body_checksum = reader.u32()?;
    let metadata_envelope_len = reader.u64()?;
    let metadata_envelope_len = usize::try_from(metadata_envelope_len).map_err(|_| {
        DecodePendingSnapshotTransferError::SnapshotEnvelopeTooLarge {
            len: metadata_envelope_len,
        }
    })?;
    let metadata_snapshot = decode_raft_snapshot(reader.take(metadata_envelope_len)?)
        .map_err(DecodePendingSnapshotTransferError::Snapshot)?;
    reader.finish()?;

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

struct PendingTransferReader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> PendingTransferReader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn finish(&self) -> Result<(), DecodePendingSnapshotTransferError> {
        let remaining = self.input.len() - self.offset;
        if remaining == 0 {
            Ok(())
        } else {
            Err(DecodePendingSnapshotTransferError::TrailingBytes(remaining))
        }
    }

    fn magic(&mut self) -> Result<[u8; 4], DecodePendingSnapshotTransferError> {
        let bytes = self.take(4)?;
        Ok([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    fn u8(&mut self) -> Result<u8, DecodePendingSnapshotTransferError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, DecodePendingSnapshotTransferError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64, DecodePendingSnapshotTransferError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodePendingSnapshotTransferError> {
        let remaining = self.input.len() - self.offset;
        if remaining < len {
            return Err(DecodePendingSnapshotTransferError::UnexpectedEof {
                needed: len,
                remaining,
            });
        }
        let start = self.offset;
        self.offset += len;
        Ok(&self.input[start..self.offset])
    }
}

#[cfg(test)]
mod tests {
    use rafter::{
        ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
        NodeId, RaftSnapshotMetadata, SnapshotGroupId, SnapshotTransferId, Term,
    };

    use super::*;

    fn test_metadata() -> RaftSnapshotMetadata {
        RaftSnapshotMetadata::new(
            SnapshotGroupId::new("data-group-10").expect("valid group id"),
            NodeId(1),
            LogIndex(7),
            Term(6),
            Term(6),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
                ApplicationSnapshotVersion::new(1).expect("valid version"),
            ),
        )
        .expect("valid snapshot metadata")
    }

    fn test_manifest() -> PendingTransferManifest {
        PendingTransferManifest {
            leader_id: NodeId(1),
            transfer_id: SnapshotTransferId(123),
            metadata: test_metadata(),
            total_payload_len: 64,
            application_payload_crc32: 0,
            received_payload_len: 12,
            body_checksum: 0,
        }
    }

    #[test]
    fn decode_rejects_unsupported_pending_transfer_manifest_version() {
        let mut encoded =
            encode_pending_snapshot_transfer_manifest(&test_manifest()).expect("manifest encodes");
        encoded[PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC.len()] =
            PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION + 1;
        let checksum_start = encoded.len() - PENDING_SNAPSHOT_TRANSFER_MANIFEST_CHECKSUM_LEN;
        let checksum = crc32(&encoded[..checksum_start]);
        encoded[checksum_start..].copy_from_slice(&checksum.to_be_bytes());

        assert_eq!(
            decode_pending_snapshot_transfer_manifest(&encoded),
            Err(DecodePendingSnapshotTransferError::UnsupportedVersion(
                PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION + 1,
            ))
        );
    }
}

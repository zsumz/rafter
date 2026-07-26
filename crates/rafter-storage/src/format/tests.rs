//! Shared cursor, checksum, and log-position-bound mechanics scenarios.

use rafter::LogIndex;

use super::*;

#[test]
fn writer_and_reader_preserve_big_endian_field_order() {
    let mut writer = Writer::new();
    writer.u8(0x12);
    writer.u16(0x3456);
    writer.u32(0x789a_bcde);
    writer.u64(0xf012_3456_789a_bcde);
    writer.bytes(b"tail");
    let bytes = writer.finish();

    let mut reader = Reader::new(&bytes);
    assert_eq!(reader.u8(), Ok(0x12));
    assert_eq!(reader.u16(), Ok(0x3456));
    assert_eq!(reader.u32(), Ok(0x789a_bcde));
    assert_eq!(reader.u64(), Ok(0xf012_3456_789a_bcde));
    assert_eq!(reader.take(4), Ok(b"tail".as_slice()));
    assert_eq!(reader.finish(), Ok(()));
}

#[test]
fn reader_reports_exact_eof_and_trailing_byte_shapes() {
    let mut short = Reader::new(&[1, 2, 3]);
    assert_eq!(
        short.u32(),
        Err(CursorError::UnexpectedEof {
            needed: 4,
            remaining: 3,
        })
    );

    let mut trailing = Reader::new(&[1, 2]);
    assert_eq!(trailing.u8(), Ok(1));
    assert_eq!(trailing.finish(), Err(CursorError::TrailingBytes(1)));
}

#[test]
fn checksum_helpers_round_trip_and_reject_short_or_corrupt_input() {
    let mut writer = Writer::new();
    writer.bytes(b"durable bytes");
    let envelope = finish_checksummed(writer);

    assert_eq!(verify_checksum(&envelope), Ok(b"durable bytes".as_slice()));
    assert_eq!(
        verify_checksum(b"bad"),
        Err(ChecksumError::UnexpectedEof {
            needed: 4,
            remaining: 3,
        })
    );

    let mut corrupt = envelope;
    corrupt[0] ^= 1;
    assert!(matches!(
        verify_checksum(&corrupt),
        Err(ChecksumError::Mismatch { .. })
    ));
}

#[test]
fn the_advanceable_bound_admits_every_index_with_a_successor() {
    for raw in [0, 1, 2, u64::MAX / 2, u64::MAX - 2, u64::MAX - 1] {
        assert_eq!(
            advanceable_log_index(raw),
            Some(LogIndex(raw)),
            "index {raw} has a successor and must be admitted"
        );
    }
    assert_eq!(
        advanceable_log_index(u64::MAX),
        None,
        "u64::MAX is the one index whose successor does not exist"
    );
}

#[test]
fn the_compaction_marker_decoder_rejects_only_the_unadvanceable_boundary() {
    use crate::raft_log_compaction::{
        decode_raft_log_compaction_marker, encode_raft_log_compaction_marker,
        DecodeRaftLogCompactionMarkerError,
    };

    let largest = encode_raft_log_compaction_marker(LogIndex(u64::MAX - 1));
    assert_eq!(
        decode_raft_log_compaction_marker(&largest),
        Ok(LogIndex(u64::MAX - 1))
    );

    // The encoder cannot be asked for an out-of-bound marker without producing
    // one, so the corrupt artifact is built from the encoder's own bytes with
    // the boundary field raised and the checksum repaired.
    let mut corrupt = largest;
    corrupt[5..13].copy_from_slice(&u64::MAX.to_be_bytes());
    let checksum = crate::crc32(&corrupt[..corrupt.len() - 4]);
    let len = corrupt.len();
    corrupt[len - 4..].copy_from_slice(&checksum.to_be_bytes());

    assert_eq!(
        decode_raft_log_compaction_marker(&corrupt),
        Err(DecodeRaftLogCompactionMarkerError::CompactedThroughAtMaximum)
    );
}

#[test]
fn the_log_entry_codec_rejects_only_the_unadvanceable_index() {
    use crate::{
        decode_raft_log_entry, encode_raft_log_entry, DecodeRaftLogEntryError,
        EncodeRaftLogEntryError, PersistedRaftLogEntry,
    };
    use rafter::Term;

    let largest = encode_raft_log_entry(&PersistedRaftLogEntry::noop(
        LogIndex(u64::MAX - 1),
        Term(1),
    ))
    .expect("the largest advanceable index encodes");
    assert_eq!(
        decode_raft_log_entry(&largest).map(|entry| entry.index),
        Ok(LogIndex(u64::MAX - 1))
    );

    assert_eq!(
        encode_raft_log_entry(&PersistedRaftLogEntry::noop(LogIndex(u64::MAX), Term(1))),
        Err(EncodeRaftLogEntryError::IndexAtMaximum)
    );

    let mut corrupt = largest;
    corrupt[5..13].copy_from_slice(&u64::MAX.to_be_bytes());
    let checksum = crate::crc32(&corrupt[..corrupt.len() - 4]);
    let len = corrupt.len();
    corrupt[len - 4..].copy_from_slice(&checksum.to_be_bytes());

    assert_eq!(
        decode_raft_log_entry(&corrupt),
        Err(DecodeRaftLogEntryError::IndexAtMaximum)
    );
}

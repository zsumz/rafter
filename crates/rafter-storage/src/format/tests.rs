//! Shared cursor and checksum-mechanics scenarios.

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

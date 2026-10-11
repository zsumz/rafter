//! Streaming install acceptance requires complete, verified, uncancelled bytes.

#[path = "snapshot_reader/support.rs"]
mod support;

use rafter::LogIndex;
use rafter_runtime::snapshot_io::{
    SnapshotCancellation, SnapshotPayloadReader, SnapshotReadError, SnapshotReadOptions,
};
use std::io::{self, Read};
use support::{Fault, Source};

fn reader(source: &Source, cancel: SnapshotCancellation) -> SnapshotPayloadReader<'_> {
    SnapshotPayloadReader::new(
        &source.descriptor,
        source,
        LogIndex(41),
        SnapshotReadOptions::new(source.descriptor.application_payload_len).with_chunk_bytes(4),
        cancel,
    )
    .unwrap()
}

#[test]
fn reads_payload_larger_than_memory_budget_and_binds_verified_descriptor() {
    // Independent fixture checksum for 2 MiB + 17 copies of 0x5a. No complete
    // payload exists in the fixture, reader, or destination.
    let source = Source::new(2 * 1024 * 1024 + 17, 0xed60_5685);
    let mut reader = SnapshotPayloadReader::new(
        &source.descriptor,
        &source,
        LogIndex(41),
        SnapshotReadOptions::new(3 * 1024 * 1024).with_chunk_bytes(4096),
        SnapshotCancellation::new(),
    )
    .unwrap();
    let mut decoded = [0; 113];
    let mut count = 0;
    loop {
        let read = reader.read(&mut decoded).unwrap();
        if read == 0 {
            break;
        }
        assert!(decoded[..read].iter().all(|byte| *byte == 0x5a));
        count += read as u64;
    }
    assert_eq!(count, source.descriptor.application_payload_len);
    assert_eq!(source.calls.get(), 513);
    assert_eq!(source.largest.get(), 4096);
    let verified = reader.finish().unwrap();
    assert_eq!(verified.snapshot(), &source.descriptor);
    assert_eq!(verified.applied_index(), LogIndex(42));
}

#[test]
fn refuses_limits_and_regressing_floor_before_requesting_source_bytes() {
    let source = Source::small();
    for (floor, options, cancel, expected) in [
        (
            LogIndex(0),
            SnapshotReadOptions::new(9).with_chunk_bytes(0),
            SnapshotCancellation::new(),
            SnapshotReadError::ZeroChunkSize,
        ),
        (
            LogIndex(0),
            SnapshotReadOptions::new(8),
            SnapshotCancellation::new(),
            SnapshotReadError::PayloadLimit {
                declared: 9,
                limit: 8,
            },
        ),
        (
            LogIndex(43),
            SnapshotReadOptions::new(9),
            SnapshotCancellation::new(),
            SnapshotReadError::BehindAppliedFloor {
                snapshot: LogIndex(42),
                applied: LogIndex(43),
            },
        ),
        (
            LogIndex(0),
            SnapshotReadOptions::new(9),
            {
                let cancel = SnapshotCancellation::new();
                cancel.cancel();
                cancel
            },
            SnapshotReadError::Cancelled,
        ),
    ] {
        assert_eq!(
            SnapshotPayloadReader::new(&source.descriptor, &source, floor, options, cancel)
                .unwrap_err(),
            expected
        );
        assert_eq!(source.calls.get(), 0);
    }
    let huge = Source::new(u64::MAX, 0);
    assert!(matches!(
        SnapshotPayloadReader::new(
            &huge.descriptor,
            &huge,
            LogIndex(0),
            SnapshotReadOptions::new(4096),
            SnapshotCancellation::new()
        ),
        Err(SnapshotReadError::PayloadLimit { .. })
    ));
    assert_eq!(huge.calls.get(), 0);
}

#[test]
fn finish_requires_consumption_not_just_prefetch_or_an_empty_read() {
    let source = Source::small();
    let mut partial = reader(&source, SnapshotCancellation::new());
    assert_eq!(partial.read(&mut []).unwrap(), 0);
    assert_eq!(source.calls.get(), 0);
    partial.read_exact(&mut [0; 1]).unwrap();
    assert_eq!(source.calls.get(), 1); // Four fetched, only one consumed.
    assert_eq!(
        partial.finish().unwrap_err(),
        SnapshotReadError::Incomplete {
            consumed: 1,
            expected: 9
        }
    );
    let mut complete = reader(&source, SnapshotCancellation::new());
    complete.read_exact(&mut [0; 9]).unwrap();
    // Calling Read again for EOF is not required: finish verifies explicitly.
    assert_eq!(complete.finish().unwrap().applied_index(), LogIndex(42));
}

#[test]
fn empty_payload_still_requires_its_checksum_and_never_calls_the_source() {
    let mut source = Source::new(0, 0);
    source.fault = Fault::Panic;
    let mut empty = reader(&source, SnapshotCancellation::new());
    assert_eq!(empty.read(&mut [0; 8]).unwrap(), 0);
    empty.finish().unwrap();
    assert_eq!(source.calls.get(), 0);
    source.descriptor.application_payload_crc32 = 1;
    assert_eq!(
        reader(&source, SnapshotCancellation::new())
            .finish()
            .unwrap_err(),
        SnapshotReadError::ChecksumMismatch {
            expected: 1,
            actual: 0
        }
    );
}

#[test]
fn source_refusals_wrong_lengths_and_overallocation_are_sticky() {
    for (fault, expected) in [
        (
            Fault::Missing,
            SnapshotReadError::SourceUnavailable { offset: 0 },
        ),
        (
            Fault::Short,
            SnapshotReadError::ChunkLength {
                offset: 0,
                expected: 4,
                actual: 3,
            },
        ),
        (
            Fault::Long,
            SnapshotReadError::ChunkLength {
                offset: 0,
                expected: 4,
                actual: 5,
            },
        ),
        (
            Fault::Overallocated,
            SnapshotReadError::ChunkCapacity {
                actual: 36,
                limit: 4,
            },
        ),
    ] {
        let mut source = Source::small();
        source.fault = fault;
        let mut reader = reader(&source, SnapshotCancellation::new());
        let mut output = [0xff; 20];
        let error = reader.read(&mut output).unwrap_err();
        assert_eq!(
            error.get_ref().unwrap().downcast_ref::<SnapshotReadError>(),
            Some(&expected)
        );
        assert_eq!(output, [0xff; 20]);
        assert!(reader.read(&mut output).is_err());
        assert_eq!(source.calls.get(), 1);
        assert_eq!(reader.finish().unwrap_err(), expected);
    }
}

#[test]
fn corruption_cannot_produce_a_completion_receipt_even_after_decoding_bytes() {
    let mut source = Source::small();
    source.fault = Fault::Corrupt;
    let expected = SnapshotReadError::ChecksumMismatch {
        expected: source.descriptor.application_payload_crc32,
        actual: rafter_crc32::crc32(&[0x5b; 9]),
    };
    let mut reader = reader(&source, SnapshotCancellation::new());
    reader.read_exact(&mut [0; 9]).unwrap();
    let error = reader.read(&mut [0; 1]).unwrap_err();
    assert_eq!(
        error.get_ref().unwrap().downcast_ref::<SnapshotReadError>(),
        Some(&expected)
    );
    assert_eq!(reader.finish().unwrap_err(), expected);
}

#[test]
fn cancellation_during_source_call_returns_no_bytes_and_standard_copy_does_not_retry() {
    let cancel = SnapshotCancellation::new();
    let mut source = Source::small();
    source.cancel = Some(cancel.clone());
    let mut reader = reader(&source, cancel);
    let error = io::copy(&mut reader, &mut io::sink()).unwrap_err();
    assert_ne!(error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(source.calls.get(), 1);
    assert_eq!(reader.finish().unwrap_err(), SnapshotReadError::Cancelled);
}

#[test]
fn cancellation_before_finish_refuses_even_a_fully_read_payload() {
    let source = Source::small();
    let cancel = SnapshotCancellation::new();
    let mut reader = reader(&source, cancel.clone());
    io::copy(&mut reader, &mut io::sink()).unwrap();
    cancel.cancel();
    assert_eq!(reader.finish().unwrap_err(), SnapshotReadError::Cancelled);
}

#[test]
fn caught_source_panic_leaves_the_reader_unusable() {
    let mut source = Source::small();
    source.fault = Fault::Panic;
    let mut reader = reader(&source, SnapshotCancellation::new());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader.read(&mut [0; 8])))
            .is_err()
    );
    assert!(reader.read(&mut [0; 8]).is_err());
    assert_eq!(
        reader.finish().unwrap_err(),
        SnapshotReadError::SourceInterrupted { offset: 0 }
    );
    assert_eq!(source.calls.get(), 1);
}

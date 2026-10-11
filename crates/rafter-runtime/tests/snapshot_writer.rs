//! A descriptor is issued only for a bounded, successful, uncancelled encoding.

#[path = "snapshot_writer/support.rs"]
mod support;

use rafter::LogIndex;
use rafter_runtime::snapshot_io::{
    SnapshotCancellation, SnapshotPayloadWriter, SnapshotWriteError, SnapshotWriteOptions,
};
use std::io::{self, Write};
use support::{metadata, Fault, Sink};

fn writer(sink: &mut Sink, limit: u64) -> SnapshotPayloadWriter<&mut Sink> {
    let token = sink.cancel.clone();
    SnapshotPayloadWriter::new(
        sink,
        metadata(),
        LogIndex(42),
        SnapshotWriteOptions::new(limit).with_chunk_bytes(4),
        token,
    )
    .unwrap()
}

#[test]
fn partial_writes_and_retryable_interruptions_count_each_byte_exactly_once() {
    let mut sink = Sink::default();
    sink.write_fault.set(Fault::Interrupted);
    sink.flush_fault.set(Fault::Interrupted);
    let mut writer = writer(&mut sink, 9);
    writer.write_all(b"123456789").unwrap();
    assert_eq!(writer.bytes_written(), 9);
    let (sink, snapshot) = writer.finish().unwrap();
    assert_eq!(sink.bytes, b"123456789");
    assert_eq!(sink.writes, 4); // One interruption, three successful partial writes.
    assert_eq!(sink.flushes, 2);
    assert_eq!(sink.largest, 4);
    assert_eq!(snapshot.metadata, metadata());
    assert_eq!(snapshot.application_payload_len, 9);
    assert_eq!(snapshot.application_payload_crc32, 0xcbf4_3926);
}

#[test]
fn large_encoding_has_no_payload_buffer_and_bounds_every_sink_call() {
    struct PatternSink {
        written: u64,
        largest: usize,
    }
    impl Write for PatternSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            assert!(bytes.iter().all(|byte| *byte == 0x5a));
            self.largest = self.largest.max(bytes.len());
            let count = bytes.len().min(1023);
            self.written += count as u64;
            Ok(count)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    const LENGTH: u64 = 2 * 1024 * 1024 + 17;
    let mut writer = SnapshotPayloadWriter::new(
        PatternSink {
            written: 0,
            largest: 0,
        },
        metadata(),
        LogIndex(42),
        SnapshotWriteOptions::new(LENGTH).with_chunk_bytes(4096),
        SnapshotCancellation::new(),
    )
    .unwrap();
    let encoded = [0x5a; 7000];
    let mut remaining = LENGTH;
    while remaining != 0 {
        let count = usize::try_from(remaining.min(encoded.len() as u64)).unwrap();
        writer.write_all(&encoded[..count]).unwrap();
        remaining -= count as u64;
    }
    let (sink, snapshot) = writer.finish().unwrap();
    assert_eq!(sink.written, LENGTH);
    assert_eq!(sink.largest, 4096);
    assert_eq!(snapshot.application_payload_len, LENGTH);
    assert_eq!(snapshot.application_payload_crc32, 0xed60_5685);
}

#[test]
fn constructor_requires_exact_floor_and_nonzero_chunks_without_sink_calls() {
    let mut sink = Sink::default();
    for floor in [LogIndex(41), LogIndex(43)] {
        assert!(matches!(
            SnapshotPayloadWriter::new(
                &mut sink,
                metadata(),
                floor,
                SnapshotWriteOptions::new(9),
                SnapshotCancellation::new()
            ),
            Err(SnapshotWriteError::AppliedFloorMismatch { .. })
        ));
    }
    assert!(matches!(
        SnapshotPayloadWriter::new(
            &mut sink,
            metadata(),
            LogIndex(42),
            SnapshotWriteOptions::new(9).with_chunk_bytes(0),
            SnapshotCancellation::new()
        ),
        Err(SnapshotWriteError::ZeroChunkSize)
    ));
    let cancel = SnapshotCancellation::new();
    cancel.cancel();
    assert!(matches!(
        SnapshotPayloadWriter::new(
            &mut sink,
            metadata(),
            LogIndex(42),
            SnapshotWriteOptions::new(9),
            cancel
        ),
        Err(SnapshotWriteError::Cancelled)
    ));
    assert_eq!((sink.writes, sink.flushes), (0, 0));
}

#[test]
fn empty_payload_flushes_and_has_the_empty_checksum() {
    let mut sink = Sink::default();
    let mut writer = writer(&mut sink, 0);
    assert_eq!(writer.write(&[]).unwrap(), 0);
    let (sink, snapshot) = writer.finish().unwrap();
    assert_eq!((sink.writes, sink.flushes), (0, 1));
    assert_eq!(snapshot.application_payload_len, 0);
    assert_eq!(snapshot.application_payload_crc32, 0);
}

#[test]
fn budget_refusal_precedes_partial_sink_writes_and_permanently_fences_the_attempt() {
    let mut sink = Sink::default();
    let mut writer = writer(&mut sink, 9);
    writer.write_all(b"12345").unwrap();
    let error = writer.write(b"67890").unwrap_err();
    assert!(matches!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<SnapshotWriteError>(),
        Some(SnapshotWriteError::PayloadLimit {
            written: 5,
            attempted: 5,
            limit: 9
        })
    ));
    assert_eq!(writer.bytes_written(), 5);
    assert!(writer.write(b"6").is_err());
    assert!(writer.flush().is_err());
    assert!(matches!(
        writer.finish(),
        Err(SnapshotWriteError::PayloadLimit { .. })
    ));
    assert_eq!(sink.bytes, b"12345");
    assert_eq!((sink.writes, sink.flushes), (2, 0));
}

#[test]
fn no_progress_overreported_counts_and_sink_errors_never_produce_a_descriptor() {
    for fault in [Fault::Zero, Fault::Overreported, Fault::Error] {
        let mut sink = Sink::default();
        sink.write_fault.set(fault);
        let mut writer = writer(&mut sink, 9);
        let error = writer.write_all(b"123456789").unwrap_err();
        let typed = error
            .get_ref()
            .unwrap()
            .downcast_ref::<SnapshotWriteError>()
            .unwrap();
        match fault {
            Fault::Zero => assert!(matches!(
                typed,
                SnapshotWriteError::WriteZero { written: 0 }
            )),
            Fault::Overreported => assert!(matches!(
                typed,
                SnapshotWriteError::InvalidWriteCount {
                    offered: 4,
                    reported: 5
                }
            )),
            Fault::Error => {
                assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
                let SnapshotWriteError::Sink { error, .. } = typed else {
                    panic!("wrong error")
                };
                assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
                assert_eq!(error.to_string(), "sink denied");
                assert!(std::error::Error::source(typed).is_some());
            }
            _ => unreachable!(),
        }
        assert!(writer.write(b"1").is_err());
        assert!(writer.flush().is_err());
        assert!(writer.finish().is_err());
        assert_eq!((sink.writes, sink.flushes), (1, 0));
    }
}

#[test]
fn a_sink_error_after_progress_remains_terminal_when_the_sink_would_recover() {
    for flush in [false, true] {
        let mut sink = Sink::default();
        let fault = if flush {
            sink.flush_fault.clone()
        } else {
            sink.write_fault.clone()
        };
        let mut writer = writer(&mut sink, 9);
        writer.write_all(b"123").unwrap();
        fault.set(Fault::Error);
        let error = if flush {
            writer.flush()
        } else {
            writer.write_all(b"456")
        }
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(matches!(fault.get(), Fault::None)); // The sink would now succeed.
        assert!(writer.write_all(b"456").is_err());
        assert_eq!(writer.bytes_written(), 3);
        assert!(matches!(
            writer.finish(),
            Err(SnapshotWriteError::Sink { written: 3, .. })
        ));
        assert_eq!(sink.bytes, b"123");
        assert_eq!(sink.writes + sink.flushes, 2);
    }
}

#[test]
fn cancellation_during_write_stops_standard_retries_and_withholds_descriptor() {
    for fault in [Fault::Cancel, Fault::CancelInterrupted] {
        let mut sink = Sink::default();
        sink.write_fault.set(fault);
        let mut writer = writer(&mut sink, 9);
        let error = writer.write_all(b"123456789").unwrap_err();
        assert_ne!(error.kind(), io::ErrorKind::Interrupted);
        assert!(matches!(
            writer.finish(),
            Err(SnapshotWriteError::Cancelled)
        ));
        assert_eq!((sink.writes, sink.flushes), (1, 0));
    }
}

#[test]
fn cancellation_after_a_successful_write_reports_accepted_bytes_but_refuses_completion() {
    let mut sink = Sink::default();
    sink.write_fault.set(Fault::Cancel);
    let mut writer = writer(&mut sink, 9);
    assert_eq!(writer.write(b"123456789").unwrap(), 3);
    assert_eq!(writer.bytes_written(), 3);
    assert!(writer.write(b"456").is_err());
    assert!(matches!(
        writer.finish(),
        Err(SnapshotWriteError::Cancelled)
    ));
    assert_eq!(sink.bytes, b"123");
    assert_eq!((sink.writes, sink.flushes), (1, 0));
}

#[test]
fn cancellation_before_finish_does_not_even_flush_staging() {
    let mut sink = Sink::default();
    let cancel = sink.cancel.clone();
    let mut writer = writer(&mut sink, 9);
    writer.write_all(b"123456789").unwrap();
    cancel.cancel();
    assert!(matches!(
        writer.finish(),
        Err(SnapshotWriteError::Cancelled)
    ));
    assert_eq!(sink.flushes, 0);
}

#[test]
fn failed_or_cancelled_flush_cannot_finish_even_after_all_writes_succeed() {
    for fault in [Fault::Error, Fault::Cancel, Fault::CancelInterrupted] {
        let mut sink = Sink::default();
        sink.flush_fault.set(fault);
        let mut writer = writer(&mut sink, 9);
        writer.write_all(b"123456789").unwrap();
        assert!(writer.finish().is_err());
        assert_eq!(sink.bytes, b"123456789");
        assert_eq!(sink.flushes, 1);
    }
}

#[test]
fn caught_write_or_flush_panic_leaves_an_unusable_attempt() {
    for flush in [false, true] {
        let mut sink = Sink::default();
        if flush {
            sink.flush_fault.set(Fault::Panic);
        } else {
            sink.write_fault.set(Fault::Panic);
        }
        let mut writer = writer(&mut sink, 9);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if flush {
                writer.flush()
            } else {
                writer.write_all(b"123")
            }
        }))
        .is_err());
        assert!(writer.write_all(b"123").is_err());
        assert!(writer.flush().is_err());
        assert!(matches!(
            writer.finish(),
            Err(SnapshotWriteError::SinkInterrupted { written: 0 })
        ));
        assert_eq!(sink.writes + sink.flushes, 1);
    }
}
